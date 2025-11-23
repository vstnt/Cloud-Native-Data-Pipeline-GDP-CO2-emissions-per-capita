"""
Analytical outputs for the curated econ-environment dataset.

Implements the "Analytical Output" section of the project plan:

- Artefact 1: gdp_vs_co2_scatter.png
  Scatterplot using only year 2023:
    - X axis: gdp_per_capita_usd
    - Y axis: co2_tons_per_capita
    - Color guided by co2_per_1000usd_gdp

- Artefact 2: correlation_summary.csv
  Small CSV with one row per year (2000 and 2023) containing:
    - year
    - pearson_correlation_gdp_co2
    - top5_countries_highest_co2_per_1000usd_gdp
    - top5_countries_lowest_co2_per_1000usd_gdp
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from adapters import StorageAdapter
from transformations import CURATED_ECON_ENVIRONMENT_OUTPUT_DIR


ANALYSIS_OUTPUT_DIR = Path("analysis")
SCATTER_PNG_NAME = "gdp_vs_co2_scatter.png"
CORRELATION_CSV_NAME = "correlation_summary.csv"

# Prefixo lógico usado quando lido via StorageAdapter
CURATED_BASE_PREFIX = "curated/env_econ_country_year"


def _load_curated_for_years(
    years: Iterable[int],
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    storage: StorageAdapter | None = None,
) -> pd.DataFrame:
    """
    Carrega dados CURATED para os anos informados, agregando todos os snapshots.

    Layout esperado (local):
        curated/env_econ_country_year/year=<year>/snapshot_date=<YYYYMMDD>/
            curated_econ_environment_country_year.parquet

    Quando `storage` é fornecido, o carregamento é feito via StorageAdapter
    usando o prefixo CURATED_BASE_PREFIX.
    """
    frames: List[pd.DataFrame] = []

    if storage is None:
        curated_root = Path(curated_root)
        for year in years:
            year_dir = curated_root / f"year={year}"
            if not year_dir.exists():
                continue
            for path in year_dir.rglob("curated_econ_environment_country_year.parquet"):
                df = pd.read_parquet(path)
                frames.append(df)
    else:
        for year in years:
            prefix = f"{CURATED_BASE_PREFIX}/year={year}/"
            keys = storage.list_keys(prefix)
            for key in keys:
                if not key.endswith("curated_econ_environment_country_year.parquet"):
                    continue
                df = storage.read_parquet(key)
                frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "gdp_per_capita_usd",
                "co2_tons_per_capita",
                "co2_per_1000usd_gdp",
            ]
        )

    df_all = pd.concat(frames, ignore_index=True)
    for col in ["gdp_per_capita_usd", "co2_tons_per_capita", "co2_per_1000usd_gdp"]:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

    if "year" in df_all.columns:
        df_all["year"] = pd.to_numeric(df_all["year"], errors="coerce").astype("Int64")

    if "country_name" in df_all.columns:
        df_all["country_name"] = df_all["country_name"].astype("string")

    return df_all


def build_gdp_vs_co2_scatter(
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    output_dir: Path | str = ANALYSIS_OUTPUT_DIR,
    year: int = 2023,
    storage: StorageAdapter | None = None,
) -> Path:
    """
    Gera o scatterplot para o ano informado (default: 2023).

    X: gdp_per_capita_usd
    Y: co2_tons_per_capita
    Cor: co2_per_1000usd_gdp
    """
    df = _load_curated_for_years([year], curated_root=curated_root, storage=storage)
    if df.empty:
        raise RuntimeError(f"No curated data available for year={year}")

    df = df.dropna(
        subset=["gdp_per_capita_usd", "co2_tons_per_capita", "co2_per_1000usd_gdp"],
    ).copy()
    if df.empty:
        raise RuntimeError(f"No valid rows for scatter plot for year={year}")

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / SCATTER_PNG_NAME

    plt.figure(figsize=(10, 6))

    scatter = plt.scatter(
        df["gdp_per_capita_usd"],
        df["co2_tons_per_capita"],
        c=df["co2_per_1000usd_gdp"],
        cmap="viridis",
        alpha=0.8,
        edgecolors="none",
    )

    plt.colorbar(scatter, label="CO2 per 1000 USD GDP")
    plt.xlabel("GDP per capita (USD)")
    plt.ylabel("CO2 tons per capita")
    plt.title(f"GDP vs CO2 per capita - {year}")
    plt.grid(True, linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return output_path


def _format_top5_countries(
    df: pd.DataFrame,
    *,
    ascending: bool,
) -> str:
    """
    Monta a string de top5 países baseada em co2_per_1000usd_gdp.
    """
    df_valid = df.dropna(subset=["co2_per_1000usd_gdp", "country_name"]).copy()
    if df_valid.empty:
        return ""

    df_sorted = df_valid.sort_values(
        by="co2_per_1000usd_gdp",
        ascending=ascending,
    ).head(5)
    names = df_sorted["country_name"].astype(str).tolist()
    return ";".join(names)


def build_correlation_summary(
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    output_dir: Path | str = ANALYSIS_OUTPUT_DIR,
    years: Tuple[int, int] = (2000, 2023),
    storage: StorageAdapter | None = None,
) -> Path:
    """
    Gera o artefato correlation_summary.csv, com uma linha por ano.
    """
    df_all = _load_curated_for_years(years, curated_root=curated_root, storage=storage)
    if df_all.empty:
        raise RuntimeError(f"No curated data available for years={years}")

    rows = []
    for year in years:
        df_year = df_all[df_all["year"] == year].copy()
        df_year = df_year.dropna(
            subset=["gdp_per_capita_usd", "co2_tons_per_capita"],
        )
        if df_year.empty:
            corr = None
        else:
            corr = df_year["gdp_per_capita_usd"].corr(
                df_year["co2_tons_per_capita"],
                method="pearson",
            )

        top_high = _format_top5_countries(df_year, ascending=False)
        top_low = _format_top5_countries(df_year, ascending=True)

        rows.append(
            {
                "year": year,
                "pearson_correlation_gdp_co2": corr,
                "top5_countries_highest_co2_per_1000usd_gdp": top_high,
                "top5_countries_lowest_co2_per_1000usd_gdp": top_low,
            },
        )

    result_df = pd.DataFrame(rows)

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / CORRELATION_CSV_NAME

    result_df.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Generate analytical outputs (scatterplot and correlation summary) "
            "from the curated econ-environment dataset."
        ),
    )
    parser.add_argument(
        "--curated-root",
        type=str,
        default=str(CURATED_ECON_ENVIRONMENT_OUTPUT_DIR),
        help="Root directory for curated env_econ_country_year.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(ANALYSIS_OUTPUT_DIR),
        help="Directory where analytical artefacts will be saved.",
    )
    parser.add_argument(
        "--skip-scatter",
        action="store_true",
        help="Skip generation of the scatter plot.",
    )
    parser.add_argument(
        "--skip-correlation",
        action="store_true",
        help="Skip generation of the correlation summary CSV.",
    )

    args = parser.parse_args()
    curated_root = Path(args.curated_root)
    output_dir = Path(args.output_dir)

    if not args.skip_scatter:
        scatter_path = build_gdp_vs_co2_scatter(
            curated_root=curated_root,
            output_dir=output_dir,
            year=2023,
        )
        print(scatter_path)

    if not args.skip_correlation:
        corr_path = build_correlation_summary(
            curated_root=curated_root,
            output_dir=output_dir,
            years=(2000, 2023),
        )
        print(corr_path)


__all__ = [
    "ANALYSIS_OUTPUT_DIR",
    "SCATTER_PNG_NAME",
    "CORRELATION_CSV_NAME",
    "build_gdp_vs_co2_scatter",
    "build_correlation_summary",
]

