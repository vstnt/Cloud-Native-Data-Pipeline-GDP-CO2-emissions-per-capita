"""
Crawler da Wikipedia - CO2 emissions per capita (RAW).

Responsabilidades (de acordo com o plano e os "Passos da implementação"):
- Baixar HTML da página:
  https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions_per_capita
- Identificar a tabela correta de emissões de CO2 per capita.
- Detectar/tratar inconsistências básicas (footnotes, células vazias/traços)
  ao montar uma representação em lista de linhas ("raw_table_json", ainda suja).
- Salvar a camada RAW em JSONL com o seguinte schema (1.2 do plano):
    ingestion_run_id: string
    ingestion_ts: timestamp
    data_source: string (fixo "wikipedia_co2")
    page_url: string
    table_html: string (HTML bruto da tabela)
    raw_table_json: JSON (lista de linhas ainda "suja")
    record_hash: hash do registro original (SHA1)
    raw_file_path: caminho do arquivo RAW gerado

Este módulo é pensado para ser usado tanto localmente quanto em nuvem.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from metadata import (
    WIKIPEDIA_CO2_SCOPE,
    end_run,
    start_run,
)

WIKIPEDIA_CO2_URL = "https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions_per_capita"
WIKIPEDIA_DATA_SOURCE = "wikipedia_co2"

# Diretório local de saída para camada RAW (pensando em mapear depois para S3/raw/)
RAW_OUTPUT_DIR = Path("raw") / "wikipedia_co2"


def _now_utc_iso() -> str:
    """Retorna o horário atual em UTC em formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def fetch_wikipedia_co2_html(
    url: str = WIKIPEDIA_CO2_URL,
    *,
    timeout: int = 30,
) -> str:
    """
    Faz download do HTML bruto da página de CO2 per capita da Wikipedia.

    Inclui um User-Agent explícito para evitar bloqueios 403.
    """
    headers = {
        "User-Agent": "env-econ-pipeline/1.0 (+https://www.example.com/)",
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def _clean_cell_text(text: str) -> str:
    """
    Limpa o texto de uma célula da tabela, tratando algumas inconsistências:

    - Remove marcas de footnote do tipo "[a]", "[1]", etc.
    - Normaliza espaços em branco múltiplos.
    - Mantém traços ("-") e valores vazios como estão (tratados no processed).
    """
    # Remove footnotes entre colchetes, ex.: "5.2[a]" -> "5.2"
    cleaned = re.sub(r"\[[^\]]*\]", "", text)
    # Normaliza espaços
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def find_co2_table_html(page_html: str) -> tuple[str, list[str]]:
    """
    Localiza a tabela principal de emissões de CO2 per capita na página.

    Retorna:
        table_html: HTML bruto da tabela (string).
        headers: lista de cabeçalhos das colunas extraídos da primeira linha.
    """
    soup = BeautifulSoup(page_html, "html.parser")

    candidate_tables = soup.find_all("table", class_="wikitable")
    if not candidate_tables:
        raise RuntimeError("Nenhuma tabela 'wikitable' encontrada na página da Wikipedia.")

    target_table = None
    for table in candidate_tables:
        caption = table.find("caption")
        caption_text = caption.get_text(" ", strip=True).lower() if caption else ""
        header_text = table.get_text(" ", strip=True).lower()

        if "emissions" in caption_text and "per capita" in caption_text:
            target_table = table
            break
        if "co2" in caption_text or "carbon dioxide" in caption_text:
            target_table = table
            break
        # Fallback: procura por palavras-chave no próprio conteúdo da tabela.
        if "co2" in header_text and "per capita" in header_text:
            target_table = table
            break

    if target_table is None:
        # Se não encontramos pelo critério acima, caímos na primeira "wikitable"
        target_table = candidate_tables[0]

    # Extrai cabeçalhos da primeira linha com <th> ou, se não houver, da primeira linha <tr>.
    header_row = target_table.find("tr")
    if not header_row:
        raise RuntimeError("Tabela da Wikipedia não possui linha de cabeçalho (<tr>).")

    headers: List[str] = []
    for cell in header_row.find_all(["th", "td"]):
        header_text = _clean_cell_text(cell.get_text(" ", strip=True))
        if header_text:
            headers.append(header_text)

    if not headers:
        raise RuntimeError("Não foi possível extrair cabeçalhos da tabela de CO2 da Wikipedia.")

    table_html = str(target_table)
    return table_html, headers


def parse_co2_table_rows(table_html: str, headers: List[str]) -> List[Dict[str, Any]]:
    """
    Converte o HTML da tabela em uma lista de linhas "sujas" (raw_table_json).

    - Cada linha é um dicionário: {<header>: <texto_da_célula>, ...}
    - Células extras são ignoradas; células faltantes recebem valor None.
    - Footnotes são removidos apenas do texto (ex.: "[a]", "[1]").
    """
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("HTML fornecido não contém uma tag <table>.")

    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr"):
        # Ignora a linha de cabeçalho original
        if tr.find("th"):
            continue

        cells = tr.find_all("td")
        if not cells:
            continue

        row_values: Dict[str, Any] = {}
        for idx, header in enumerate(headers):
            if idx < len(cells):
                cell_text = cells[idx].get_text(" ", strip=True)
                cell_text = _clean_cell_text(cell_text)
                row_values[header] = cell_text if cell_text != "" else None
            else:
                row_values[header] = None

        rows.append(row_values)

    return rows


def _compute_record_hash(payload: Dict[str, Any]) -> str:
    """
    Calcula um hash SHA1 estável do payload RAW (sem campos de auditoria),
    alinhado à estratégia aplicada na ingestão da World Bank API.
    """
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def crawl_wikipedia_co2_raw(
    url: str = WIKIPEDIA_CO2_URL,
    *,
    timeout: int = 30,
    run_scope: str = WIKIPEDIA_CO2_SCOPE,
) -> Path:
    """
    Executa o crawler da Wikipedia para CO2 per capita e persiste a camada RAW.

    Passos:
    - Inicia um run de metadata (escopo "wikipedia_co2").
    - Faz download da página.
    - Localiza a tabela principal de emissões per capita.
    - Converte a tabela em lista de linhas (raw_table_json, ainda "suja").
    - Calcule record_hash do payload bruto (HTML + JSON de linhas).
    - Persiste o registro em JSONL em raw/wikipedia_co2/.
    - Finaliza o run na tabela de metadata.

    Retorna:
        Caminho do arquivo JSONL gerado.
    """
    RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_id = start_run(run_scope)
    ingestion_ts_iso = _now_utc_iso()

    try:
        # 1. Download do HTML da página
        page_html = fetch_wikipedia_co2_html(url=url, timeout=timeout)

        # 2. Encontrar tabela de CO2 per capita e cabeçalhos
        table_html, headers = find_co2_table_html(page_html)

        # 3. Converter tabela em lista de linhas "sujas"
        raw_table_rows = parse_co2_table_rows(table_html, headers)

        # 4. Montar payload bruto e record_hash (sem campos de auditoria)
        raw_payload = {
            "page_url": url,
            "headers": headers,
            "rows": raw_table_rows,
        }
        record_hash = _compute_record_hash(raw_payload)

        # 5. Determinar caminho do arquivo RAW
        timestamp_for_filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = RAW_OUTPUT_DIR / f"wikipedia_co2_raw_{timestamp_for_filename}.jsonl"
        raw_file_path_str = str(output_path)

        # 6. Construir registro RAW final alinhado ao schema 1.2
        raw_record: Dict[str, Any] = {
            "ingestion_run_id": run_id,
            "ingestion_ts": ingestion_ts_iso,
            "data_source": WIKIPEDIA_DATA_SOURCE,
            "page_url": url,
            "table_html": table_html,
            "raw_table_json": raw_payload,
            "record_hash": record_hash,
            "raw_file_path": raw_file_path_str,
        }

        # 7. Persistir em JSONL (um registro por arquivo)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(raw_record, f, ensure_ascii=False)
            f.write("\n")

        rows_processed = len(raw_table_rows)

        # 8. Registrar fim do run
        end_run(
            run_id,
            status="SUCCESS",
            rows_processed=rows_processed,
            last_checkpoint=None,
        )

        return output_path
    except Exception as exc:  # noqa: BLE001
        end_run(
            run_id,
            status="FAILED",
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    # Utilitário de linha de comando para rodar o crawler localmente:
    #   PYTHONPATH=src python -m crawler.wikipedia_co2_crawler
    import argparse

    parser = argparse.ArgumentParser(
        description="Crawler da Wikipedia para CO2 per capita (camada RAW).",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=WIKIPEDIA_CO2_URL,
        help="URL da página da Wikipedia (default: página oficial de CO2 per capita).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout em segundos para a requisição HTTP (default: 30).",
    )

    args = parser.parse_args()
    output = crawl_wikipedia_co2_raw(url=args.url, timeout=args.timeout)
    print(output)


__all__ = [
    "WIKIPEDIA_CO2_URL",
    "WIKIPEDIA_DATA_SOURCE",
    "RAW_OUTPUT_DIR",
    "fetch_wikipedia_co2_html",
    "find_co2_table_html",
    "parse_co2_table_rows",
    "crawl_wikipedia_co2_raw",
]
