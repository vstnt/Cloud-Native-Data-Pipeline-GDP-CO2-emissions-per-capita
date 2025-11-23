"""
Country mapping
---------------

Geração e persistência do mapping de países
(`processed_country_mapping`), conforme descrito no plano:

- Baseado no PROCESSED do World Bank (`processed_worldbank_gdp_per_capita`):
  extraímos a lista distinta de países (country_code, country_name) e
  calculamos `country_name_normalized`.
- Aplicamos overrides manuais a partir de
  `src/transformations/country_mapping_overrides.csv`, com prioridade
  sobre a base.
- Persistimos o resultado em:

    processed/country_mapping/country_mapping.parquet

Schema final:
    country_name_normalized: string (PK)
    country_code:            string (ISO3)
    country_name:            string (nome oficial usado no curated)
    source_precedence:       string (ex.: "world_bank", "override")
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from adapters import StorageAdapter
from .wikipedia_co2_processed import normalize_country_name
from .world_bank_gdp_processed import (
    PROCESSED_BASE_PREFIX as WORLD_BANK_PROCESSED_BASE_PREFIX,
    PROCESSED_OUTPUT_DIR as WORLD_BANK_PROCESSED_OUTPUT_DIR,
)

COUNTRY_MAPPING_BASE_PREFIX = "processed/country_mapping"

# Diretório local de saída para o mapping processado
COUNTRY_MAPPING_OUTPUT_DIR = Path("processed") / "country_mapping"

# Caminho padrão para o arquivo Parquet de mapping
COUNTRY_MAPPING_PARQUET_PATH = COUNTRY_MAPPING_OUTPUT_DIR / "country_mapping.parquet"

# Caminho padrão para o CSV de overrides manuais
COUNTRY_MAPPING_OVERRIDES_CSV = Path(__file__).with_name("country_mapping_overrides.csv")


def _load_world_bank_processed_frames(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
) -> List[pd.DataFrame]:
    """
    Carrega todos os arquivos Parquet do processed do World Bank.

    Espera encontrar arquivos em:
        processed/world_bank_gdp/year=*/processed_worldbank_gdp_per_capita.parquet
    """
    root = Path(processed_dir)
    if not root.exists():
        return []

    frames: List[pd.DataFrame] = []
    for path in root.rglob("*.parquet"):
        df = pd.read_parquet(path)
        frames.append(df)
    return frames


def _load_world_bank_processed_frames_with_storage(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    storage: Optional[StorageAdapter] = None,
) -> List[pd.DataFrame]:
    """
    Variante de _load_world_bank_processed_frames que suporta leitura via
    StorageAdapter (por exemplo, S3) alA(c)m do filesystem local.
    """
    if storage is None:
        return _load_world_bank_processed_frames(processed_dir)

    frames: List[pd.DataFrame] = []
    keys = storage.list_keys(WORLD_BANK_PROCESSED_BASE_PREFIX)
    for key in keys:
        if not key.endswith(".parquet"):
            continue
        df = storage.read_parquet(key)
        frames.append(df)
    return frames


def build_country_mapping_from_world_bank_parquet(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    *,
    storage: Optional[StorageAdapter] = None,
) -> pd.DataFrame:
    """
    Gera o mapping base a partir do PROCESSED do World Bank.

    - Lê todos os Parquet em processed/world_bank_gdp/.
    - Extrai colunas country_code e country_name.
    - Calcula country_name_normalized.
    - Remove duplicados.
    - Atribui source_precedence="world_bank".
    """
    frames = _load_world_bank_processed_frames_with_storage(processed_dir, storage=storage)
    if not frames:
        # DataFrame vazio com schema estável
        return pd.DataFrame(
            columns=[
                "country_name_normalized",
                "country_code",
                "country_name",
                "source_precedence",
            ]
        ).astype(
            {
                "country_name_normalized": "string",
                "country_code": "string",
                "country_name": "string",
                "source_precedence": "string",
            }
        )

    df_all = pd.concat(frames, ignore_index=True)

    cols = [c for c in ["country_code", "country_name"] if c in df_all.columns]
    if len(cols) < 2:
        raise ValueError(
            "Parquets do World Bank processed não contêm as colunas "
            "'country_code' e 'country_name' necessárias para o mapping.",
        )

    df = df_all[cols].dropna(subset=["country_code", "country_name"]).copy()

    df["country_code"] = df["country_code"].astype("string")
    df["country_name"] = df["country_name"].astype("string")

    df["country_name_normalized"] = df["country_name"].map(normalize_country_name).astype("string")
    df["source_precedence"] = "world_bank"

    df = df[
        [
            "country_name_normalized",
            "country_code",
            "country_name",
            "source_precedence",
        ]
    ].drop_duplicates(subset=["country_name_normalized"])

    return df


def _apply_overrides(
    base_mapping: pd.DataFrame,
    overrides_path: Path | str = COUNTRY_MAPPING_OVERRIDES_CSV,
) -> pd.DataFrame:
    """
    Aplica overrides manuais por cima do mapping base.

    CSV esperado (country_mapping_overrides.csv):
        country_name_normalized,country_code,country_name

    - Overrides têm prioridade sobre o mapping base.
    - source_precedence recebe "override" para linhas com override aplicado.
    """
    base = base_mapping.copy()

    overrides_file = Path(overrides_path)
    if not overrides_file.exists():
        return base

    overrides = pd.read_csv(overrides_file)
    if overrides.empty:
        return base

    required_cols = {"country_name_normalized", "country_code", "country_name"}
    missing = required_cols - set(overrides.columns)
    if missing:
        raise ValueError(
            f"Arquivo de overrides está faltando colunas obrigatórias: {sorted(missing)}",
        )

    overrides = overrides[list(required_cols)].copy()
    overrides["country_name_normalized"] = overrides["country_name_normalized"].astype("string")
    overrides["country_name_normalized"] = overrides["country_name_normalized"].map(normalize_country_name)
    overrides["country_code"] = overrides["country_code"].astype("string")
    overrides["country_name"] = overrides["country_name"].astype("string")

    if base.empty:
        overrides["source_precedence"] = "override"
        return overrides[
            [
                "country_name_normalized",
                "country_code",
                "country_name",
                "source_precedence",
            ]
        ]

    merged = base.merge(
        overrides,
        how="outer",
        on="country_name_normalized",
        suffixes=("", "_override"),
    )

    merged["country_code"] = merged["country_code_override"].combine_first(merged["country_code"])
    merged["country_name"] = merged["country_name_override"].combine_first(merged["country_name"])

    # Define source_precedence: override > valor existente > "world_bank"
    def _resolve_source(row: pd.Series) -> str:
        has_override = pd.notna(row.get("country_code_override")) or pd.notna(
            row.get("country_name_override"),
        )
        if has_override:
            return "override"
        existing = row.get("source_precedence")
        if isinstance(existing, str) and existing:
            return existing
        return "world_bank"

    merged["source_precedence"] = merged.apply(_resolve_source, axis=1).astype("string")

    merged = merged[
        [
            "country_name_normalized",
            "country_code",
            "country_name",
            "source_precedence",
        ]
    ]

    merged = merged.dropna(subset=["country_name_normalized"]).drop_duplicates(
        subset=["country_name_normalized"],
    )

    merged["country_name_normalized"] = merged["country_name_normalized"].astype("string")
    merged["country_code"] = merged["country_code"].astype("string")
    merged["country_name"] = merged["country_name"].astype("string")

    return merged


def build_country_mapping(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    overrides_path: Path | str = COUNTRY_MAPPING_OVERRIDES_CSV,
) -> pd.DataFrame:
    """
    Constrói o mapping completo:
    - base a partir do processed World Bank;
    - aplicação de overrides manuais (se existirem).
    """
    base = build_country_mapping_from_world_bank_parquet(processed_dir=processed_dir)
    return _apply_overrides(base, overrides_path=overrides_path)


def save_country_mapping_parquet(
    mapping_df: pd.DataFrame,
    *,
    output_dir: Path | str = COUNTRY_MAPPING_OUTPUT_DIR,
) -> Path:
    """
    Persiste o mapping em Parquet no layout:

        processed/country_mapping/country_mapping.parquet

    Retorna o caminho gerado.
    """
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    path = output_root / "country_mapping.parquet"
    mapping_df.to_parquet(path, index=False)
    return path


def build_and_save_country_mapping_from_world_bank(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    overrides_path: Path | str = COUNTRY_MAPPING_OVERRIDES_CSV,
    output_dir: Path | str = COUNTRY_MAPPING_OUTPUT_DIR,
) -> Path:
    """
    Pipeline completo:
    - Constrói o mapping a partir do processed World Bank + overrides.
    - Persiste o resultado como Parquet.

    Retorna:
        Caminho do arquivo country_mapping.parquet gerado.
    """
    mapping_df = build_country_mapping(
        processed_dir=processed_dir,
        overrides_path=overrides_path,
    )
    return save_country_mapping_parquet(mapping_df, output_dir=output_dir)


def load_country_mapping(
    path: Path | str | None = None,
) -> pd.DataFrame:
    """
    Carrega o mapping de países a partir de um arquivo Parquet.

    Se `path` não for informado, usa o caminho padrão:
        processed/country_mapping/country_mapping.parquet
    """
    if path is None:
        path = COUNTRY_MAPPING_PARQUET_PATH
    return pd.read_parquet(path)


if __name__ == "__main__":
    # Utilitário de linha de comando para gerar o mapping localmente:
    #
    #   PYTHONPATH=src python -m transformations.country_mapping
    #
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Gera o processed_country_mapping a partir do processed "
            "World Bank e dos overrides manuais (se existirem)."
        ),
    )
    parser.add_argument(
        "--world-bank-processed-dir",
        type=str,
        default=str(WORLD_BANK_PROCESSED_OUTPUT_DIR),
        help="Diretório raiz do processed do World Bank (default: processed/world_bank_gdp).",
    )
    parser.add_argument(
        "--overrides-path",
        type=str,
        default=str(COUNTRY_MAPPING_OVERRIDES_CSV),
        help=(
            "Caminho para o CSV de overrides de mapping de países "
            "(default: src/transformations/country_mapping_overrides.csv)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(COUNTRY_MAPPING_OUTPUT_DIR),
        help="Diretório de saída para o Parquet de mapping (default: processed/country_mapping).",
    )

    args = parser.parse_args()
    output_path = build_and_save_country_mapping_from_world_bank(
        processed_dir=Path(args.world_bank_processed_dir),
        overrides_path=Path(args.overrides_path),
        output_dir=Path(args.output_dir),
    )
    print(output_path)


__all__ = [
    "COUNTRY_MAPPING_OUTPUT_DIR",
    "COUNTRY_MAPPING_PARQUET_PATH",
    "COUNTRY_MAPPING_OVERRIDES_CSV",
    "build_country_mapping_from_world_bank_parquet",
    "build_country_mapping",
    "save_country_mapping_parquet",
    "build_and_save_country_mapping_from_world_bank",
    "load_country_mapping",
]
