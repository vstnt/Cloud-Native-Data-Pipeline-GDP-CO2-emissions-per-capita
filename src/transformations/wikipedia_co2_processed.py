"""
Processamento da tabela de CO2 per capita da Wikipedia (camada PROCESSED).

Implementa as regras descritas na seção 2.2 do
`context/Plano do projeto - final.pdf`:

- Consumir o JSONL RAW gerado por `crawler.wikipedia_co2_crawler`.
- Resolver footnotes/traços já limpos no RAW para valores numéricos ou nulos.
- Converter a tabela larga (colunas para 2000/2023) em formato longo
  (uma linha por (country, year)).
- Criar campos:
    country_name              - nome original da tabela
    country_name_normalized   - nome normalizado para join
    country_code              - ISO3 (opcional, preenchido via mapping)
    year                      - 2000 ou 2023
    co2_tons_per_capita       - emissões per capita no ano
    notes                     - campo livre para observações
    ingestion_run_id, ingestion_ts, data_source
- Persistir a camada PROCESSED em Parquet, com layout:

    processed/wikipedia_co2/year=<ano>/processed_wikipedia_co2_per_capita.parquet
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd

from adapters import StorageAdapter
from crawler.wikipedia_co2_crawler import WIKIPEDIA_DATA_SOURCE

# Diretório local de saída para camada PROCESSED (pensando em mapear depois para S3/processed/)
PROCESSED_OUTPUT_DIR = Path("processed") / "wikipedia_co2"

# Prefixo lógico pensado para mapeamento 1:1 em S3
PROCESSED_BASE_PREFIX = "processed/wikipedia_co2"


@dataclass
class WikipediaCO2ProcessedRecord:
    """Representa um registro já transformado para o schema PROCESSED."""

    country_name: str
    country_name_normalized: str
    country_code: Optional[str]
    year: int
    co2_tons_per_capita: Optional[float]
    notes: Optional[str]
    ingestion_run_id: Optional[str]
    ingestion_ts: Optional[str]
    data_source: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "country_name": self.country_name,
            "country_name_normalized": self.country_name_normalized,
            "country_code": self.country_code,
            "year": self.year,
            "co2_tons_per_capita": self.co2_tons_per_capita,
            "notes": self.notes,
            "ingestion_run_id": self.ingestion_run_id,
            "ingestion_ts": self.ingestion_ts,
            "data_source": self.data_source,
        }


def normalize_country_name(name: str) -> str:
    """
    Normaliza nomes de países para facilitar joins.

    Estratégia:
    - lower case
    - remoção de acentos
    - remoção de caracteres não alfanuméricos (exceto espaço)
    - colapsar múltiplos espaços
    - trim nas pontas
    """
    if name is None:
        return ""

    s = name.lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _load_raw_records(raw_file_path: Path | str) -> Iterable[Dict[str, Any]]:
    """
    Carrega registros RAW a partir de um arquivo JSONL produzido por
    `crawl_wikipedia_co2_raw`.

    Na prática, o crawler gera um único registro por arquivo, mas mantemos
    a estrutura de iterador para consistência com outros módulos.
    """
    path = Path(raw_file_path)
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _parse_float(value: Any) -> Optional[float]:
    """
    Converte um valor textual em float, tratando casos comuns de missing.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if text == "":
        return None
    if text in {"-", "–"}:
        return None
    if text.upper() in {"NA", "N/A"}:
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_emissions_2000_2023(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Extrai emissões per capita para 2000 e 2023 a partir de uma linha da
    tabela RAW.
    """
    v_2023 = row.get("Emissions per capita (tons per year)")
    v_2000 = row.get("% change from 2000")

    return {
        "emissions_2023": _parse_float(v_2023),
        "emissions_2000": _parse_float(v_2000),
    }


def build_wikipedia_co2_dataframe(
    raw_file_path: Path | str,
    *,
    country_mapping: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Constrói um DataFrame PROCESSED a partir de um arquivo RAW JSONL.

    Schema resultante (colunas):
        country_name: string
        country_name_normalized: string
        country_code: string (ISO3, se mapping disponível)
        year: int (2000 ou 2023)
        co2_tons_per_capita: float
        notes: string
        ingestion_run_id: string
        ingestion_ts: timestamp
        data_source: string
    """
    processed_rows: List[Dict[str, Any]] = []

    for raw_record in _load_raw_records(raw_file_path):
        ingestion_run_id = raw_record.get("ingestion_run_id")
        ingestion_ts = raw_record.get("ingestion_ts")
        data_source = raw_record.get("data_source") or WIKIPEDIA_DATA_SOURCE

        raw_table = raw_record.get("raw_table_json") or {}
        rows = raw_table.get("rows") or []

        for row in rows:
            location_raw = row.get("Location")
            if not location_raw:
                continue

            country_name = str(location_raw)
            country_name_norm = normalize_country_name(country_name)

            emissions = _extract_emissions_2000_2023(row)
            em_2023 = emissions["emissions_2023"]
            em_2000 = emissions["emissions_2000"]

            notes: Optional[str] = None

            if em_2000 is not None:
                processed_rows.append(
                    WikipediaCO2ProcessedRecord(
                        country_name=country_name,
                        country_name_normalized=country_name_norm,
                        country_code=None,
                        year=2000,
                        co2_tons_per_capita=em_2000,
                        notes=notes,
                        ingestion_run_id=str(ingestion_run_id) if ingestion_run_id is not None else None,
                        ingestion_ts=str(ingestion_ts) if ingestion_ts is not None else None,
                        data_source=str(data_source),
                    ).to_dict()
                )

            if em_2023 is not None:
                processed_rows.append(
                    WikipediaCO2ProcessedRecord(
                        country_name=country_name,
                        country_name_normalized=country_name_norm,
                        country_code=None,
                        year=2023,
                        co2_tons_per_capita=em_2023,
                        notes=notes,
                        ingestion_run_id=str(ingestion_run_id) if ingestion_run_id is not None else None,
                        ingestion_ts=str(ingestion_ts) if ingestion_ts is not None else None,
                        data_source=str(data_source),
                    ).to_dict()
                )

    df = pd.DataFrame(
        processed_rows
        or [
            {
                "country_name": pd.NA,
                "country_name_normalized": pd.NA,
                "country_code": pd.NA,
                "year": pd.NA,
                "co2_tons_per_capita": pd.NA,
                "notes": pd.NA,
                "ingestion_run_id": pd.NA,
                "ingestion_ts": pd.NA,
                "data_source": pd.NA,
            }
        ]
    )

    string_cols = [
        "country_name",
        "country_name_normalized",
        "country_code",
        "notes",
        "ingestion_run_id",
        "data_source",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    if "co2_tons_per_capita" in df.columns:
        df["co2_tons_per_capita"] = pd.to_numeric(df["co2_tons_per_capita"], errors="coerce")

    if "ingestion_ts" in df.columns:
        df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], errors="coerce", utc=True)

    # Aplicar mapping de países, se fornecido
    if country_mapping is not None and not df.empty:
        required_cols = {"country_name_normalized", "country_code", "country_name"}
        missing = required_cols - set(country_mapping.columns)
        if missing:
            raise ValueError(
                f"country_mapping está faltando colunas obrigatórias: {sorted(missing)}",
            )

        mapping = country_mapping.copy()
        mapping = mapping[
            ["country_name_normalized", "country_code", "country_name"]
        ].drop_duplicates(subset=["country_name_normalized"])

        df = df.merge(
            mapping,
            how="left",
            on="country_name_normalized",
            suffixes=("", "_mapped"),
        )

        df["country_code"] = df["country_code_mapped"].combine_first(df["country_code"])
        df["country_name"] = df["country_name_mapped"].combine_first(df["country_name"])
        df = df.drop(columns=["country_code_mapped", "country_name_mapped"])

    return df


def save_wikipedia_co2_parquet_partitions(
    df: pd.DataFrame,
    *,
    output_dir: Path | str = PROCESSED_OUTPUT_DIR,
    storage: Optional[StorageAdapter] = None,
) -> List[Union[Path, str]]:
    """
    Salva o DataFrame PROCESSED particionado por ano em formato Parquet.

    Layout lógico:

        processed/wikipedia_co2/year=<ano>/processed_wikipedia_co2_per_capita.parquet

    Quando `storage` é None, grava em disco local em `output_dir` e retorna
    uma lista de `Path`. Quando `storage` é fornecido, grava via
    `StorageAdapter.write_parquet` sob `PROCESSED_BASE_PREFIX` e retorna uma
    lista de chaves lógicas (strings).
    """
    if df.empty or "year" not in df.columns:
        return []

    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    if storage is None:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)

        output_paths: List[Path] = []
        for year_value, df_year in df.groupby("year"):
            if pd.isna(year_value):
                continue
            year_int = int(year_value)
            year_dir = output_root / f"year={year_int}"
            year_dir.mkdir(parents=True, exist_ok=True)

            file_path = year_dir / "processed_wikipedia_co2_per_capita.parquet"
            df_year.to_parquet(file_path, index=False)
            output_paths.append(file_path)

        return output_paths

    keys: List[str] = []
    for year_value, df_year in df.groupby("year"):
        if pd.isna(year_value):
            continue
        year_int = int(year_value)
        key = f"{PROCESSED_BASE_PREFIX}/year={year_int}/processed_wikipedia_co2_per_capita.parquet"
        storage.write_parquet(df_year, key)
        keys.append(key)

    return keys


def process_wikipedia_co2_raw_file(
    raw_file_path: Path | str,
    *,
    output_dir: Path | str = PROCESSED_OUTPUT_DIR,
    country_mapping: Optional[pd.DataFrame] = None,
    storage: Optional[StorageAdapter] = None,
) -> List[Union[Path, str]]:
    """
    Pipeline completo de processamento Wikipedia CO2 (RAW -> PROCESSED Parquet).

    - Lê o JSONL RAW produzido por `crawl_wikipedia_co2_raw`.
    - Constrói o DataFrame PROCESSED com o schema definido no plano.
    - Aplica, opcionalmente, o mapping de países para preencher country_code.
    - Salva arquivos Parquet particionados por ano.
    """
    df = build_wikipedia_co2_dataframe(raw_file_path, country_mapping=country_mapping)
    return save_wikipedia_co2_parquet_partitions(
        df,
        output_dir=output_dir,
        storage=storage,
    )


if __name__ == "__main__":
    # Utilitário de linha de comando para testes locais rápidos:
    #   PYTHONPATH=src python -m transformations.wikipedia_co2_processed <caminho_raw_jsonl>
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Processa arquivo RAW da Wikipedia (CO2 per capita) "
            "em Parquet particionado por ano."
        ),
    )
    parser.add_argument(
        "raw_file_path",
        help="Caminho para o arquivo JSONL gerado pelo crawler da Wikipedia.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(PROCESSED_OUTPUT_DIR),
        help=(
            "Diretório raiz de saída para os arquivos Parquet "
            "(default: processed/wikipedia_co2)."
        ),
    )

    args = parser.parse_args()
    paths = process_wikipedia_co2_raw_file(
        args.raw_file_path,
        output_dir=Path(args.output_dir),
    )
    for p in paths:
        print(p)


__all__ = [
    "PROCESSED_OUTPUT_DIR",
    "PROCESSED_BASE_PREFIX",
    "WikipediaCO2ProcessedRecord",
    "normalize_country_name",
    "build_wikipedia_co2_dataframe",
    "save_wikipedia_co2_parquet_partitions",
    "process_wikipedia_co2_raw_file",
]

