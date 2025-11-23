"""
Curated dataset: Economic & Environmental by country-year.

Implementa a camada CURATED descrita na seção 3.1 do
`context/Plano do projeto - final.pdf` e no checklist de passos:

- Join entre os datasets PROCESSED:
  - World Bank GDP per capita (econômico)
  - Wikipedia CO₂ per capita (ambiental)
- Chave lógica: (country_code, year)
- Campos principais:
    country_code               - string (chave)
    country_name               - string
    year                       - int
    gdp_per_capita_usd         - float
    co2_tons_per_capita        - float
    co2_per_1000usd_gdp        - float (derivado)
    gdp_source_system          - string ("world_bank_api")
    co2_source_system          - string ("wikipedia_co2")
    first_ingestion_run_id     - string
    last_update_run_id         - string
    last_update_ts             - timestamp
- Particionamento local alinhado ao layout pensado para S3:

    curated/env_econ_country_year/
        year=<ano>/snapshot_date=<YYYYMMDD>/
            curated_econ_environment_country_year.parquet
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

from metadata import CURATED_JOIN_SCOPE, end_run, start_run
from .world_bank_gdp_processed import PROCESSED_OUTPUT_DIR as WORLD_BANK_PROCESSED_OUTPUT_DIR
from .wikipedia_co2_processed import PROCESSED_OUTPUT_DIR as WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR

CURATED_OUTPUT_DIR = Path("curated") / "env_econ_country_year"


@dataclass
class CuratedEconEnvironmentRecord:
    country_code: str
    country_name: str
    year: int
    gdp_per_capita_usd: Optional[float]
    co2_tons_per_capita: Optional[float]
    co2_per_1000usd_gdp: Optional[float]
    gdp_source_system: str
    co2_source_system: str
    first_ingestion_run_id: str
    last_update_run_id: str
    last_update_ts: datetime


def _load_world_bank_processed(
    processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
) -> pd.DataFrame:
    root = Path(processed_dir)
    if not root.exists():
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
            ]
        )

    frames: List[pd.DataFrame] = []
    for path in root.rglob("*.parquet"):
        df = pd.read_parquet(path)
        frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
            ]
        )

    df_all = pd.concat(frames, ignore_index=True)
    expected_cols = ["country_code", "country_name", "year", "gdp_per_capita_usd"]
    cols = [c for c in expected_cols if c in df_all.columns]
    df = df_all[cols].copy()

    if "country_code" in df.columns:
        df["country_code"] = df["country_code"].astype("string")
    if "country_name" in df.columns:
        df["country_name"] = df["country_name"].astype("string")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "gdp_per_capita_usd" in df.columns:
        df["gdp_per_capita_usd"] = pd.to_numeric(df["gdp_per_capita_usd"], errors="coerce")

    df = df.dropna(subset=["country_code", "year"])
    df = df.drop_duplicates(subset=["country_code", "year"])
    return df


def _load_wikipedia_co2_processed(
    processed_dir: Path | str = WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
) -> pd.DataFrame:
    root = Path(processed_dir)
    if not root.exists():
        return pd.DataFrame(
            columns=[
                "country_code",
                "year",
                "co2_tons_per_capita",
            ]
        )

    frames: List[pd.DataFrame] = []
    for path in root.rglob("*.parquet"):
        df = pd.read_parquet(path)
        frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "country_code",
                "year",
                "co2_tons_per_capita",
            ]
        )

    df_all = pd.concat(frames, ignore_index=True)
    expected_cols = ["country_code", "year", "co2_tons_per_capita"]
    cols = [c for c in expected_cols if c in df_all.columns]
    df = df_all[cols].copy()

    if "country_code" in df.columns:
        df["country_code"] = df["country_code"].astype("string")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "co2_tons_per_capita" in df.columns:
        df["co2_tons_per_capita"] = pd.to_numeric(df["co2_tons_per_capita"], errors="coerce")

    df = df.dropna(subset=["country_code", "year"])
    df = df.drop_duplicates(subset=["country_code", "year"])
    return df


def build_curated_econ_environment_country_year_dataframe(
    world_bank_df: pd.DataFrame,
    wikipedia_df: pd.DataFrame,
    *,
    curated_run_id: str,
    snapshot_ts: datetime,
) -> pd.DataFrame:
    if world_bank_df.empty or "country_code" not in world_bank_df.columns:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
                "co2_tons_per_capita",
                "co2_per_1000usd_gdp",
                "gdp_source_system",
                "co2_source_system",
                "first_ingestion_run_id",
                "last_update_run_id",
                "last_update_ts",
            ]
        )

    if wikipedia_df.empty or "country_code" not in wikipedia_df.columns:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
                "co2_tons_per_capita",
                "co2_per_1000usd_gdp",
                "gdp_source_system",
                "co2_source_system",
                "first_ingestion_run_id",
                "last_update_run_id",
                "last_update_ts",
            ]
        )

    wb = world_bank_df.copy()
    wb["year"] = pd.to_numeric(wb["year"], errors="coerce").astype("Int64")
    wb["country_code"] = wb["country_code"].astype("string")

    co2 = wikipedia_df.copy()
    co2["year"] = pd.to_numeric(co2["year"], errors="coerce").astype("Int64")
    co2["country_code"] = co2["country_code"].astype("string")

    joined = wb.merge(
        co2,
        on=["country_code", "year"],
        how="left",
        suffixes=("", "_co2"),
    )

    missing_co2 = joined["co2_tons_per_capita"].isna().sum()
    if missing_co2:
        print(
            f"[curated] {missing_co2} pares (country_code, year) presentes no GDP "
            "mas sem CO2; serão descartados do curated."
        )

    joined = joined[joined["co2_tons_per_capita"].notna()].copy()

    joined["gdp_per_capita_usd"] = pd.to_numeric(
        joined["gdp_per_capita_usd"],
        errors="coerce",
    )
    joined["co2_tons_per_capita"] = pd.to_numeric(
        joined["co2_tons_per_capita"],
        errors="coerce",
    )

    valid_mask = (
        joined["gdp_per_capita_usd"].notna()
        & joined["co2_tons_per_capita"].notna()
        & (joined["gdp_per_capita_usd"] > 0)
    )
    joined["co2_per_1000usd_gdp"] = pd.NA
    joined.loc[valid_mask, "co2_per_1000usd_gdp"] = (
        joined.loc[valid_mask, "co2_tons_per_capita"]
        * 1000.0
        / joined.loc[valid_mask, "gdp_per_capita_usd"]
    )

    joined["gdp_source_system"] = "world_bank_api"
    joined["co2_source_system"] = "wikipedia_co2"

    joined["first_ingestion_run_id"] = curated_run_id
    joined["last_update_run_id"] = curated_run_id
    joined["last_update_ts"] = pd.to_datetime(snapshot_ts, utc=True)

    joined["country_code"] = joined["country_code"].astype("string")
    joined["country_name"] = joined["country_name"].astype("string")
    joined["year"] = pd.to_numeric(joined["year"], errors="coerce").astype("Int64")
    joined["gdp_source_system"] = joined["gdp_source_system"].astype("string")
    joined["co2_source_system"] = joined["co2_source_system"].astype("string")
    joined["first_ingestion_run_id"] = joined["first_ingestion_run_id"].astype("string")
    joined["last_update_run_id"] = joined["last_update_run_id"].astype("string")

    cols_order = [
        "country_code",
        "country_name",
        "year",
        "gdp_per_capita_usd",
        "co2_tons_per_capita",
        "co2_per_1000usd_gdp",
        "gdp_source_system",
        "co2_source_system",
        "first_ingestion_run_id",
        "last_update_run_id",
        "last_update_ts",
    ]
    joined = joined[cols_order]

    return joined


def build_curated_econ_environment_country_year_from_processed(
    *,
    world_bank_processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    wikipedia_processed_dir: Path | str = WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
    curated_run_id: str,
    snapshot_ts: datetime,
) -> pd.DataFrame:
    wb_df = _load_world_bank_processed(processed_dir=world_bank_processed_dir)
    co2_df = _load_wikipedia_co2_processed(processed_dir=wikipedia_processed_dir)
    return build_curated_econ_environment_country_year_dataframe(
        wb_df,
        co2_df,
        curated_run_id=curated_run_id,
        snapshot_ts=snapshot_ts,
    )


def save_curated_econ_environment_country_year_parquet_partitions(
    df: pd.DataFrame,
    *,
    output_dir: Path | str = CURATED_OUTPUT_DIR,
    snapshot_date: Optional[str] = None,
) -> List[Path]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    if df.empty or "year" not in df.columns:
        return []

    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    output_paths: List[Path] = []
    for year_value, df_year in df.groupby("year"):
        if pd.isna(year_value):
            continue
        year_int = int(year_value)
        year_dir = output_root / f"year={year_int}" / f"snapshot_date={snapshot_date}"
        year_dir.mkdir(parents=True, exist_ok=True)

        file_path = year_dir / "curated_econ_environment_country_year.parquet"
        df_year.to_parquet(file_path, index=False)
        output_paths.append(file_path)

    return output_paths


def build_and_save_curated_econ_environment_country_year(
    *,
    world_bank_processed_dir: Path | str = WORLD_BANK_PROCESSED_OUTPUT_DIR,
    wikipedia_processed_dir: Path | str = WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
    output_dir: Path | str = CURATED_OUTPUT_DIR,
    run_scope: str = CURATED_JOIN_SCOPE,
) -> List[Path]:
    run_id = start_run(run_scope)
    snapshot_ts = datetime.now(timezone.utc)
    snapshot_date = snapshot_ts.strftime("%Y%m%d")

    try:
        df = build_curated_econ_environment_country_year_from_processed(
            world_bank_processed_dir=world_bank_processed_dir,
            wikipedia_processed_dir=wikipedia_processed_dir,
            curated_run_id=run_id,
            snapshot_ts=snapshot_ts,
        )

        paths = save_curated_econ_environment_country_year_parquet_partitions(
            df,
            output_dir=output_dir,
            snapshot_date=snapshot_date,
        )

        end_run(
            run_id,
            status="SUCCESS",
            rows_processed=int(df.shape[0]),
            last_checkpoint=f"snapshot_date={snapshot_date}",
        )
        return paths
    except Exception as exc:  # noqa: BLE001
        end_run(
            run_id,
            status="FAILED",
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Constrói a camada CURATED (curated_econ_environment_country_year) "
            "a partir dos datasets PROCESSED de GDP (World Bank) e CO2 (Wikipedia)."
        ),
    )
    parser.add_argument(
        "--world-bank-processed-dir",
        type=str,
        default=str(WORLD_BANK_PROCESSED_OUTPUT_DIR),
        help="Diretório raiz do processed da World Bank API (default: processed/world_bank_gdp).",
    )
    parser.add_argument(
        "--wikipedia-processed-dir",
        type=str,
        default=str(WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR),
        help="Diretório raiz do processed da Wikipedia CO2 (default: processed/wikipedia_co2).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(CURATED_OUTPUT_DIR),
        help=(
            "Diretório raiz de saída para a camada CURATED "
            "(default: curated/env_econ_country_year)."
        ),
    )

    args = parser.parse_args()
    paths = build_and_save_curated_econ_environment_country_year(
        world_bank_processed_dir=Path(args.world_bank_processed_dir),
        wikipedia_processed_dir=Path(args.wikipedia_processed_dir),
        output_dir=Path(args.output_dir),
    )
    for p in paths:
        print(p)


__all__ = [
    "CURATED_OUTPUT_DIR",
    "CuratedEconEnvironmentRecord",
    "build_curated_econ_environment_country_year_dataframe",
    "build_curated_econ_environment_country_year_from_processed",
    "save_curated_econ_environment_country_year_parquet_partitions",
    "build_and_save_curated_econ_environment_country_year",
]

