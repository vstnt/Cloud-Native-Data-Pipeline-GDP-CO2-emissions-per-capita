"""
Processamento da World Bank API (GDP per capita) para camada PROCESSED.

Implementa as transformações descritas na seção 2.1 do
`context/Plano do projeto - final.pdf`:

- Converter o JSON RAW em um schema tabular:
  country_code, country_name, year, gdp_per_capita_usd, indicator_id,
  indicator_name, ingestion_run_id, ingestion_ts, data_source.
- Garantir tipagem adequada (year como int, gdp_per_capita_usd como float,
  ingestion_ts como timestamp, etc.).
- Materializar um DataFrame particionado por ano.
- Persistir a camada PROCESSED em formato Parquet, particionando por ano.

Pensado para ser usado tanto localmente quanto, posteriormente,
dentro de um handler AWS Lambda.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd

from adapters import StorageAdapter
from ingestion_api.world_bank_ingestion import WORLD_BANK_DATA_SOURCE

# Diretório local de saída para camada PROCESSED
PROCESSED_OUTPUT_DIR = Path("processed") / "world_bank_gdp"

# Prefixo lógico pensado para mapeamento 1:1 em S3
PROCESSED_BASE_PREFIX = "processed/world_bank_gdp"


@dataclass
class WorldBankProcessedRecord:
    """Representa um registro já transformado para o schema PROCESSED."""

    country_code: str
    country_name: str
    year: int
    gdp_per_capita_usd: Optional[float]
    indicator_id: Optional[str]
    indicator_name: Optional[str]
    ingestion_run_id: Optional[str]
    ingestion_ts: Optional[str]
    data_source: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "year": self.year,
            "gdp_per_capita_usd": self.gdp_per_capita_usd,
            "indicator_id": self.indicator_id,
            "indicator_name": self.indicator_name,
            "ingestion_run_id": self.ingestion_run_id,
            "ingestion_ts": self.ingestion_ts,
            "data_source": self.data_source,
        }


def _load_raw_records(
    raw_file_path: Path | str,
    storage: Optional[StorageAdapter] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Carrega registros RAW a partir de um arquivo JSONL produzido por
    `ingest_world_bank_gdp_raw`.

    Quando `storage` Ac None, lA� diretamente do filesystem local. Caso
    contrA!rio, usa `storage.read_raw` (por exemplo, S3).
    """
    # Modo local: ler direto do filesystem.
    if storage is None:
        path = Path(raw_file_path)
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
        return

    # Modo abstrato (ex.: S3): usar StorageAdapter.read_raw.
    content_bytes = storage.read_raw(str(raw_file_path))
    text = content_bytes.decode("utf-8")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        yield json.loads(line)


def _transform_raw_record(record: Dict[str, Any]) -> Optional[WorldBankProcessedRecord]:
    """
    Converte um registro RAW da World Bank API em um registro PROCESSED.

    - Renomeia countryiso3code -> country_code
    - Renomeia country.value   -> country_name
    - Converte date           -> year (int)
    - Converte value          -> gdp_per_capita_usd (float)
    - Mantém indicator.id / indicator.value
    - Propaga ingestion_run_id, ingestion_ts e data_source

    Retorna None para registros sem informações mínimas para o join.
    """
    indicator = record.get("indicator") or {}
    country = record.get("country") or {}

    country_code = record.get("countryiso3code")
    country_name = country.get("value")
    date_str = record.get("date")
    value = record.get("value")

    try:
        year = int(date_str) if isinstance(date_str, str) and date_str.isdigit() else None
    except (TypeError, ValueError):
        year = None

    if not country_code or not country_name or year is None:
        # Sem chaves lógicas mínimas, descartamos o registro.
        return None

    # Converter valor de GDP para float, se possível.
    if value is None:
        gdp_per_capita_usd: Optional[float] = None
    else:
        try:
            gdp_per_capita_usd = float(value)
        except (TypeError, ValueError):
            gdp_per_capita_usd = None

    ingestion_run_id = record.get("ingestion_run_id")
    ingestion_ts = record.get("ingestion_ts")
    data_source = (record.get("data_source") or WORLD_BANK_DATA_SOURCE) or "world_bank_api"

    return WorldBankProcessedRecord(
        country_code=str(country_code),
        country_name=str(country_name),
        year=int(year),
        gdp_per_capita_usd=gdp_per_capita_usd,
        indicator_id=indicator.get("id"),
        indicator_name=indicator.get("value"),
        ingestion_run_id=str(ingestion_run_id) if ingestion_run_id is not None else None,
        ingestion_ts=str(ingestion_ts) if ingestion_ts is not None else None,
        data_source=str(data_source),
    )


def build_world_bank_gdp_dataframe(
    raw_file_path: Path | str,
    *,
    storage: Optional[StorageAdapter] = None,
) -> pd.DataFrame:
    """
    Constrói um DataFrame PROCESSED a partir de um arquivo RAW JSONL.

    Schema resultante (colunas):
        country_code: string
        country_name: string
        year: int
        gdp_per_capita_usd: float
        indicator_id: string
        indicator_name: string
        ingestion_run_id: string
        ingestion_ts: timestamp (datetime64[ns, UTC] no pandas)
        data_source: string
    """
    processed_rows: List[Dict[str, Any]] = []

    for raw_record in _load_raw_records(raw_file_path, storage=storage):
        processed = _transform_raw_record(raw_record)
        if processed is not None:
            processed_rows.append(processed.to_dict())

    # DataFrame vazio, mas com colunas definidas, para manter contrato estável.
    if not processed_rows:
        df = pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
                "indicator_id",
                "indicator_name",
                "ingestion_run_id",
                "ingestion_ts",
                "data_source",
            ]
        )
        return df

    df = pd.DataFrame(processed_rows)

    # Tipagem explícita para ficar alinhado ao plano.
    string_cols = [
        "country_code",
        "country_name",
        "indicator_id",
        "indicator_name",
        "ingestion_run_id",
        "data_source",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "year" in df.columns:
        df["year"] = df["year"].astype("int64")

    if "gdp_per_capita_usd" in df.columns:
        df["gdp_per_capita_usd"] = pd.to_numeric(df["gdp_per_capita_usd"], errors="coerce")

    if "ingestion_ts" in df.columns:
        df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], errors="coerce", utc=True)

    return df


def save_world_bank_gdp_parquet_partitions(
    df: pd.DataFrame,
    *,
    output_dir: Path | str = PROCESSED_OUTPUT_DIR,
    storage: Optional[StorageAdapter] = None,
) -> List[Union[Path, str]]:
    """
    Salva o DataFrame PROCESSED particionado por ano em formato Parquet.

    Layout lógico (mapeável para S3 posteriormente):

        processed/world_bank_gdp/year=<ano>/processed_worldbank_gdp_per_capita.parquet

    Quando `storage` é None, grava em disco local em `output_dir` e retorna
    uma lista de `Path`. Quando `storage` é fornecido, grava via
    `StorageAdapter.write_parquet` sob `PROCESSED_BASE_PREFIX` e retorna uma
    lista de chaves lógicas (strings).
    """
    if df.empty or "year" not in df.columns:
        return []

    df = df.copy()
    df["year"] = df["year"].astype("int64")

    # Modo local (filesystem)
    if storage is None:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)

        output_paths: List[Path] = []
        for year_value, df_year in df.groupby("year"):
            year_int = int(year_value)
            year_dir = output_root / f"year={year_int}"
            year_dir.mkdir(parents=True, exist_ok=True)

            file_path = year_dir / "processed_worldbank_gdp_per_capita.parquet"
            df_year.to_parquet(file_path, index=False)
            output_paths.append(file_path)

        return output_paths

    # Modo abstrato (S3 ou outro backend)
    keys: List[str] = []
    for year_value, df_year in df.groupby("year"):
        year_int = int(year_value)
        key = f"{PROCESSED_BASE_PREFIX}/year={year_int}/processed_worldbank_gdp_per_capita.parquet"
        storage.write_parquet(df_year, key)
        keys.append(key)

    return keys


def process_world_bank_gdp_raw_file(
    raw_file_path: Path | str,
    *,
    output_dir: Path | str = PROCESSED_OUTPUT_DIR,
    storage: Optional[StorageAdapter] = None,
) -> List[Union[Path, str]]:
    """
    Pipeline completo de processamento World Bank (RAW -> PROCESSED Parquet).

    - Lê o JSONL RAW produzido por `ingest_world_bank_gdp_raw`.
    - Constrói o DataFrame PROCESSED com o schema definido no plano.
    - Salva arquivos Parquet particionados por ano.

    Retorna:
        Lista de caminhos (local) ou chaves lógicas (quando usando StorageAdapter).
    """
    df = build_world_bank_gdp_dataframe(raw_file_path, storage=storage)
    return save_world_bank_gdp_parquet_partitions(
        df,
        output_dir=output_dir,
        storage=storage,
    )


if __name__ == "__main__":
    # Pequeno utilitário de linha de comando para testes locais rápidos:
    #   PYTHONPATH=src python -m transformations.world_bank_gdp_processed <caminho_raw_jsonl>
    import argparse

    parser = argparse.ArgumentParser(
        description="Processa arquivo RAW da World Bank API (GDP per capita) em Parquet particionado por ano.",
    )
    parser.add_argument(
        "raw_file_path",
        help="Caminho para o arquivo JSONL gerado pela ingestão RAW da World Bank API.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(PROCESSED_OUTPUT_DIR),
        help="Diretório raiz de saída para os arquivos Parquet (default: processed/world_bank_gdp).",
    )

    args = parser.parse_args()
    paths = process_world_bank_gdp_raw_file(args.raw_file_path, output_dir=Path(args.output_dir))
    for p in paths:
        print(p)


__all__ = [
    "PROCESSED_OUTPUT_DIR",
    "PROCESSED_BASE_PREFIX",
    "WorldBankProcessedRecord",
    "build_world_bank_gdp_dataframe",
    "save_world_bank_gdp_parquet_partitions",
    "process_world_bank_gdp_raw_file",
]
