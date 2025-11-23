"""
Processamento da World Bank API (GDP per capita) para camada PROCESSED.

Este módulo implementa as transformações descritas na seção 2.1
do "Plano do projeto - final.pdf":

- Converter o JSON RAW em um schema tabular:
  country_code, country_name, year, gdp_per_capita_usd, indicator_id,
  indicator_name, ingestion_run_id, ingestion_ts, data_source.
- Garantir tipagem adequada (year como int, gdp_per_capita_usd como float,
  ingestion_ts como timestamp, etc.).
- Materializar um DataFrame particionado por ano.
- Persistir a camada PROCESSED em formato Parquet, particionando por ano.

Pensado para ser usado tanto localmente quanto, posteriormente,
dentro de um handler AWS Lambda. O chamador deve garantir que o diretório
`src/` está no PYTHONPATH (por exemplo, via:

    PYTHONPATH=src python -m transformations.world_bank_gdp_processed
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ingestion_api.world_bank_ingestion import WORLD_BANK_DATA_SOURCE

# Diretório local de saída para camada PROCESSED (pensando em mapear depois para S3/processed/)
PROCESSED_OUTPUT_DIR = Path("processed") / "world_bank_gdp"


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


def _load_raw_records(raw_file_path: Path) -> Iterable[Dict[str, Any]]:
    """
    Carrega registros RAW a partir de um arquivo JSONL produzido por
    `ingest_world_bank_gdp_raw`.
    """
    with raw_file_path.open("r", encoding="utf-8") as f:
        for line in f:
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

    Retorna None para registros sem informações mínimas para o join
    (por exemplo, sem countryiso3code ou year inválido).
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
    gdp_per_capita_usd: Optional[float]
    if value is None:
        gdp_per_capita_usd = None
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


def build_world_bank_gdp_dataframe(raw_file_path: Path | str) -> pd.DataFrame:
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
    path = Path(raw_file_path)
    processed_rows: List[Dict[str, Any]] = []

    for raw_record in _load_raw_records(path):
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
) -> List[Path]:
    """
    Salva o DataFrame PROCESSED particionado por ano em formato Parquet.

    Layout local (mapeável para S3 posteriormente):

        processed/world_bank_gdp/year=<ano>/processed_worldbank_gdp_per_capita.parquet

    Retorna a lista de caminhos gerados.
    """
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    if df.empty or "year" not in df.columns:
        # Nenhuma partição a salvar.
        return []

    output_paths: List[Path] = []

    # Garantir que year está em tipo inteiro para evitar surpresas em diretórios.
    df = df.copy()
    df["year"] = df["year"].astype("int64")

    for year_value, df_year in df.groupby("year"):
        year_int = int(year_value)
        year_dir = output_root / f"year={year_int}"
        year_dir.mkdir(parents=True, exist_ok=True)

        file_path = year_dir / "processed_worldbank_gdp_per_capita.parquet"
        df_year.to_parquet(file_path, index=False)
        output_paths.append(file_path)

    return output_paths


def process_world_bank_gdp_raw_file(
    raw_file_path: Path | str,
    *,
    output_dir: Path | str = PROCESSED_OUTPUT_DIR,
) -> List[Path]:
    """
    Pipeline completo de processamento World Bank (RAW -> PROCESSED Parquet).

    - Lê o JSONL RAW produzido por `ingest_world_bank_gdp_raw`.
    - Constrói o DataFrame PROCESSED com o schema definido na seção 2.1.
    - Salva arquivos Parquet particionados por ano.

    Retorna:
        Lista de caminhos dos arquivos Parquet gerados.
    """
    df = build_world_bank_gdp_dataframe(raw_file_path)
    return save_world_bank_gdp_parquet_partitions(df, output_dir=output_dir)


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
    "WorldBankProcessedRecord",
    "build_world_bank_gdp_dataframe",
    "save_world_bank_gdp_parquet_partitions",
    "process_world_bank_gdp_raw_file",
]

