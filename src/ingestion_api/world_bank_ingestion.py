"""
Ingestão RAW da World Bank API (GDP per capita).

Responsabilidades principais (de acordo com o plano):
- Implementar ingestão da World Bank API para o indicador NY.GDP.PCAP.CD.
- Aplicar lógica incremental baseada em checkpoint (last_year_loaded_world_bank).
- Salvar dados RAW em formato JSONL, alinhado ao schema definido na seção RAW 1.1.
- Calcular um record_hash para cada registro.

Este módulo foi pensado para ser usado tanto localmente quanto,
posteriormente, dentro de um handler AWS Lambda. O chamador deve garantir
que o diretório `src/` está no PYTHONPATH (por exemplo, via
`PYTHONPATH=src python -m ingestion_api.world_bank_ingestion`).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from metadata import (
    WORLD_BANK_API_SCOPE,
    end_run,
    load_checkpoint,
    save_checkpoint,
    start_run,
)

WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2/country/all/indicator"
WORLD_BANK_INDICATOR_ID = "NY.GDP.PCAP.CD"
WORLD_BANK_CHECKPOINT_KEY = "last_year_loaded_world_bank"
WORLD_BANK_DATA_SOURCE = "world_bank_api"

# Diretório local de saída para camada RAW (pensando em mapear depois para S3/raw/)
RAW_OUTPUT_DIR = Path("raw") / "world_bank_gdp"


def _now_utc_iso() -> str:
    """Retorna o horário atual em UTC em formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def _fetch_indicator_page(
    indicator_id: str,
    page: int,
    per_page: int = 1000,
    *,
    timeout: int = 30,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Busca uma página da API do World Bank para o indicador informado.

    Retorna (metadata, registros).
    """
    url = f"{WORLD_BANK_BASE_URL}/{indicator_id}"
    params = {
        "format": "json",
        "page": page,
        "per_page": per_page,
    }
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or len(data) != 2:
        raise RuntimeError(f"Resposta inesperada da World Bank API: {data!r}")

    metadata, records = data
    if not isinstance(metadata, dict) or not isinstance(records, list):
        raise RuntimeError(f"Estrutura inesperada de resposta da World Bank API: {data!r}")

    return metadata, records


def fetch_all_indicator_records(
    indicator_id: str = WORLD_BANK_INDICATOR_ID,
    *,
    per_page: int = 1000,
    timeout: int = 30,
) -> Iterable[Dict[str, Any]]:
    """
    Itera sobre todos os registros do indicador informado (todas as páginas).
    """
    page = 1
    metadata, records = _fetch_indicator_page(
        indicator_id,
        page=page,
        per_page=per_page,
        timeout=timeout,
    )
    total_pages = int(metadata.get("pages", 1))

    for record in records:
        yield record

    while page < total_pages:
        page += 1
        metadata, records = _fetch_indicator_page(
            indicator_id,
            page=page,
            per_page=per_page,
            timeout=timeout,
        )
        for record in records:
            yield record


def list_indicator_years(indicator_id: str = WORLD_BANK_INDICATOR_ID) -> List[int]:
    """
    Lista todos os anos disponíveis para o indicador na World Bank API.
    """
    years = set()
    for record in fetch_all_indicator_records(indicator_id):
        date_str = record.get("date")
        if isinstance(date_str, str) and date_str.isdigit():
            years.add(int(date_str))
    return sorted(years)


def compute_record_hash(record: Dict[str, Any]) -> str:
    """
    Calcula um hash estável do registro RAW, para controle/deduplicação,
    usando o JSON normalizado do registro original (sem campos de auditoria),
    conforme sugerido no plano (MD5/SHA1 do payload).
    """
    normalized = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _filter_records_by_years(
    records: Iterable[Dict[str, Any]],
    *,
    min_year_exclusive: Optional[int],
    max_year_inclusive: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Filtra registros por intervalo de anos.

    - Inclui somente registros com ano > min_year_exclusive (se definido).
    - Exclui registros com ano > max_year_inclusive (se definido).
    """
    filtered: List[Dict[str, Any]] = []
    for record in records:
        date_str = record.get("date")
        if not isinstance(date_str, str) or not date_str.isdigit():
            continue
        year = int(date_str)
        if min_year_exclusive is not None and year <= min_year_exclusive:
            continue
        if max_year_inclusive is not None and year > max_year_inclusive:
            continue
        filtered.append(record)
    return filtered


def ingest_world_bank_gdp_raw(
    *,
    indicator_id: str = WORLD_BANK_INDICATOR_ID,
    run_scope: str = WORLD_BANK_API_SCOPE,
    checkpoint_key: str = WORLD_BANK_CHECKPOINT_KEY,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
) -> Path:
    """
    Ingestão RAW da World Bank API com lógica incremental baseada em ano.

    Passos:
    - Recupera o checkpoint last_year_loaded_world_bank (se existir).
    - Busca todos os registros do indicador na API.
    - Filtra apenas anos > checkpoint (e dentro de [min_year, max_year], se informado).
    - Enriquece os registros com metadados e record_hash.
    - Persiste em JSONL em raw/world_bank_gdp/.
    - Atualiza checkpoint e registra run via módulo de metadata.

    Retorna:
        Caminho do arquivo JSONL gerado.
    """
    RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_id = start_run(run_scope)
    ingestion_ts_iso = _now_utc_iso()

    try:
        # 1. Carregar checkpoint (se existir)
        last_year_str = load_checkpoint(checkpoint_key)
        last_year_from_ckpt: Optional[int] = None
        if last_year_str is not None:
            try:
                last_year_from_ckpt = int(str(last_year_str))
            except ValueError:
                last_year_from_ckpt = None

        # Se min_year for maior que o checkpoint, usamos min_year como baseline.
        min_year_exclusive: Optional[int] = last_year_from_ckpt
        if min_year is not None:
            if min_year_exclusive is None or min_year > min_year_exclusive:
                min_year_exclusive = min_year - 1

        # 2. Buscar todos os registros do indicador
        all_records = list(fetch_all_indicator_records(indicator_id))

        # 3. Filtrar por intervalo de anos
        filtered_records = _filter_records_by_years(
            all_records,
            min_year_exclusive=min_year_exclusive,
            max_year_inclusive=max_year,
        )

        # Determinar o arquivo de saída antes do enriquecimento, pois
        # raw_file_path faz parte do schema RAW.
        timestamp_for_filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = RAW_OUTPUT_DIR / f"world_bank_gdp_raw_{timestamp_for_filename}.jsonl"
        raw_file_path_str = str(output_path)

        # 4. Enriquecer com metadados e record_hash
        enriched_records: List[Dict[str, Any]] = []
        max_ingested_year: Optional[int] = None

        for record in filtered_records:
            record_year_str = record.get("date")
            record_year: Optional[int] = None
            if isinstance(record_year_str, str) and record_year_str.isdigit():
                record_year = int(record_year_str)

            if record_year is not None:
                if max_ingested_year is None or record_year > max_ingested_year:
                    max_ingested_year = record_year

            # JSON normalizado do payload RAW, usado tanto em raw_payload quanto
            # no cálculo de record_hash (alinhado ao plano).
            raw_payload = json.dumps(record, sort_keys=True, ensure_ascii=False)

            enriched = dict(record)
            enriched["ingestion_run_id"] = run_id
            enriched["ingestion_ts"] = ingestion_ts_iso
            enriched["data_source"] = WORLD_BANK_DATA_SOURCE
            enriched["raw_payload"] = raw_payload
            enriched["record_hash"] = compute_record_hash(record)
            enriched["raw_file_path"] = raw_file_path_str
            enriched_records.append(enriched)

        # 5. Salvar em JSONL (mesmo se não houver registros, para rastreabilidade)
        with output_path.open("w", encoding="utf-8") as f:
            for rec in enriched_records:
                json.dump(rec, f, ensure_ascii=False)
                f.write("\n")

        rows_processed = len(enriched_records)

        # 6. Atualizar checkpoint se houve novos dados
        final_checkpoint_year: Optional[int] = last_year_from_ckpt
        if max_ingested_year is not None:
            final_checkpoint_year = max_ingested_year
            save_checkpoint(checkpoint_key, str(final_checkpoint_year))

        # 7. Registrar fim do run
        end_run(
            run_id,
            status="SUCCESS",
            rows_processed=rows_processed,
            last_checkpoint=str(final_checkpoint_year) if final_checkpoint_year is not None else None,
        )

        return output_path
    except Exception as exc:  # noqa: BLE001 - queremos registrar a falha
        end_run(
            run_id,
            status="FAILED",
            error_message=str(exc),
        )
        raise


__all__ = [
    "WORLD_BANK_BASE_URL",
    "WORLD_BANK_INDICATOR_ID",
    "WORLD_BANK_CHECKPOINT_KEY",
    "WORLD_BANK_DATA_SOURCE",
    "RAW_OUTPUT_DIR",
    "fetch_all_indicator_records",
    "list_indicator_years",
    "compute_record_hash",
    "ingest_world_bank_gdp_raw",
]

