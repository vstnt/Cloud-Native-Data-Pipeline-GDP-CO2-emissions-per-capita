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
from datetime import datetime, timezone
import io
from typing import Iterable, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from adapters import StorageAdapter
from transformations import CURATED_ECON_ENVIRONMENT_OUTPUT_DIR


ANALYSIS_OUTPUT_DIR = Path("analysis")
SCATTER_PNG_NAME = "gdp_vs_co2_scatter.png"
CORRELATION_CSV_NAME = "correlation_summary.csv"
CORRELATION_XLSX_NAME = "correlation_summary.xlsx"

# Prefixo lógico usado quando lido via StorageAdapter
CURATED_BASE_PREFIX = "curated/env_econ_country_year"
ANALYTICS_BASE_PREFIX = "analytics"


def _extract_snapshot_date_from_path(path_like: str) -> str | None:
    """Best‑effort extraction of snapshot_date=YYYYMMDD from a key/path.

    Works for both local file paths and S3 keys. Returns the 8‑digit
    snapshot date or None when not found.
    """
    parts = str(path_like).replace("\\", "/").split("/")
    for p in parts:
        if p.startswith("snapshot_date="):
            return p.split("=", 1)[1]
    return None


def _load_curated_for_years(
    years: Iterable[int],
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    storage: StorageAdapter | None = None,
    latest_snapshot_only: bool = True,
) -> pd.DataFrame:
    """
    Carrega dados CURATED para os anos informados.

    Por padrão, usa apenas o snapshot mais recente de cada ano
    (latest_snapshot_only=True) para evitar duplicidade de países
    quando existem múltiplos snapshots.

    Layout (local):
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
            candidates = list(year_dir.rglob("curated_econ_environment_country_year.parquet"))
            if not candidates:
                continue
            if latest_snapshot_only:
                # Choose the file under the highest snapshot_date=YYYYMMDD
                def snap(p: Path) -> str:
                    s = _extract_snapshot_date_from_path(str(p))
                    return s or ""

                candidates.sort(key=lambda p: snap(p))
                candidates = [candidates[-1]]
            for path in candidates:
                df = pd.read_parquet(path)
                frames.append(df)
    else:
        for year in years:
            prefix = f"{CURATED_BASE_PREFIX}/year={year}"
            keys = [
                k
                for k in storage.list_keys(prefix)
                if k.endswith("curated_econ_environment_country_year.parquet")
            ]
            if not keys:
                continue
            if latest_snapshot_only:
                keys.sort(key=lambda k: _extract_snapshot_date_from_path(k) or "")
                keys = [keys[-1]]
            for key in keys:
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

    # Garantir unicidade por (country_code, year) quando possível.
    dedup_keys = [c for c in ["country_code", "year"] if c in df_all.columns]
    if dedup_keys:
        df_all = df_all.sort_values(dedup_keys).drop_duplicates(dedup_keys, keep="last")

    return df_all


def build_gdp_vs_co2_scatter(
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    output_dir: Path | str = ANALYSIS_OUTPUT_DIR,
    year: int = 2023,
    storage: StorageAdapter | None = None,
    annotate_outliers: bool = True,
    outliers_top_n: int = 5,
) -> Path | str:
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

    plt.figure(figsize=(10, 6))

    x = df["gdp_per_capita_usd"].to_numpy()
    y = df["co2_tons_per_capita"].to_numpy()

    scatter = plt.scatter(
        x,
        y,
        c=df["co2_per_1000usd_gdp"],
        cmap="viridis",
        alpha=0.8,
        edgecolors="none",
    )

    plt.colorbar(scatter, label="CO2 per 1000 USD GDP")
    plt.xlabel("GDP per capita (USD)")
    plt.ylabel("CO2 tons per capita")
    # Linear regression (1st degree) + R²
    try:
        if len(x) >= 2:
            slope, intercept = np.polyfit(x, y, 1)
            x_line = np.linspace(np.nanmin(x), np.nanmax(x), 200)
            y_line = slope * x_line + intercept
            # R² = Pearson^2
            r = pd.Series(x).corr(pd.Series(y), method="pearson")
            r2 = float(r ** 2) if pd.notna(r) else float("nan")
            plt.plot(
                x_line,
                y_line,
                color="crimson",
                linewidth=2,
                label=f"Linear fit (R²={r2:.2f})",
            )

            if annotate_outliers and outliers_top_n > 0:
                y_pred = slope * x + intercept
                resid = np.abs(y - y_pred)
                # pick top-N residuals
                top_idx = np.argsort(-resid)[:outliers_top_n]
                for i in top_idx:
                    name = str(df.iloc[i]["country_name"]) if "country_name" in df.columns else ""
                    plt.annotate(
                        name,
                        (x[i], y[i]),
                        textcoords="offset points",
                        xytext=(5, 5),
                        fontsize=8,
                        color="black",
                        alpha=0.8,
                    )
            plt.legend(frameon=False)
    except Exception as e:
        # Non-blocking: proceed without regression if anything goes wrong
        print(f"[analysis] Regression fit skipped due to: {e}")

    plt.title(f"GDP vs CO2 per capita - {year}")
    plt.grid(True, linestyle="--", alpha=0.3)

    plt.tight_layout()

    if storage is None:
        # Local filesystem write
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        output_path = output_root / SCATTER_PNG_NAME
        plt.savefig(output_path, dpi=150)
        plt.close()
        return output_path

    # Write to S3 via StorageAdapter under analytics/<YYYYMMDD>/
    snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    key = f"{ANALYTICS_BASE_PREFIX}/{snapshot_date}/{SCATTER_PNG_NAME}"
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)
    location = storage.write_raw(key, buf.getvalue())
    return location


def _format_top5_countries(
    df: pd.DataFrame,
    *,
    ascending: bool,
) -> str:
    """
    Monta a string de top5 países baseada em co2_per_1000usd_gdp,
    incluindo o valor formatado (ex.: "País: 1.234").
    """
    df_valid = df.dropna(subset=["co2_per_1000usd_gdp", "country_name"]).copy()
    if df_valid.empty:
        return ""

    df_sorted = df_valid.sort_values(
        by="co2_per_1000usd_gdp",
        ascending=ascending,
    ).head(5)
    entries = [
        f"{name}: {val:.3f}"
        for name, val in zip(
            df_sorted["country_name"].astype(str).tolist(),
            df_sorted["co2_per_1000usd_gdp"].astype(float).tolist(),
        )
    ]
    return ";".join(entries)


def build_correlation_summary(
    *,
    curated_root: Path | str = CURATED_ECON_ENVIRONMENT_OUTPUT_DIR,
    output_dir: Path | str = ANALYSIS_OUTPUT_DIR,
    years: Tuple[int, int] = (2000, 2023),
    storage: StorageAdapter | None = None,
    write_xlsx: bool = False,
) -> Path | str:
    """
    Gera o artefato correlation_summary.csv, com uma linha por ano.
    """
    df_all = _load_curated_for_years(years, curated_root=curated_root, storage=storage)
    if df_all.empty:
        # Ambiente sem curated ainda; geramos CSV vazio.
        print(
            f"[analysis] No curated data available for years={years}; generating empty correlation_summary.csv",
        )
        result_df = pd.DataFrame(
            columns=[
                "year",
                "pearson_correlation_gdp_co2",
                "top5_countries_highest_co2_per_1000usd_gdp",
                "top5_countries_lowest_co2_per_1000usd_gdp",
            ]
        )
        if storage is None:
            output_root = Path(output_dir)
            output_root.mkdir(parents=True, exist_ok=True)
            output_path = output_root / CORRELATION_CSV_NAME
            result_df.to_csv(output_path, index=False)
            # Optional XLSX export (local only)
            if write_xlsx:
                try:
                    import openpyxl  # noqa: F401
                    xlsx_path = output_root / CORRELATION_XLSX_NAME
                    result_df.to_excel(xlsx_path, index=False)
                except Exception as e:
                    print(f"[analysis] Skipping XLSX export (dependency missing or error): {e}")
            return output_path
        else:
            snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
            key = f"{ANALYTICS_BASE_PREFIX}/{snapshot_date}/{CORRELATION_CSV_NAME}"
            buf = io.StringIO()
            result_df.to_csv(buf, index=False)
            location = storage.write_raw(key, buf.getvalue().encode("utf-8"))
            if write_xlsx:
                try:
                    import openpyxl  # noqa: F401
                    from io import BytesIO
                    bbuf = BytesIO()
                    with pd.ExcelWriter(bbuf, engine="openpyxl") as writer:
                        result_df.to_excel(writer, index=False, sheet_name="summary")
                    bbuf.seek(0)
                    xlsx_key = f"{ANALYTICS_BASE_PREFIX}/{snapshot_date}/{CORRELATION_XLSX_NAME}"
                    storage.write_raw(xlsx_key, bbuf.getvalue())
                except Exception as e:
                    print(f"[analysis] Skipping XLSX export to storage: {e}")
            return location

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

    if storage is None:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        output_path = output_root / CORRELATION_CSV_NAME
        result_df.to_csv(output_path, index=False)
        if write_xlsx:
            try:
                import openpyxl  # noqa: F401
                xlsx_path = output_root / CORRELATION_XLSX_NAME
                result_df.to_excel(xlsx_path, index=False)
            except Exception as e:
                print(f"[analysis] Skipping XLSX export (dependency missing or error): {e}")
        return output_path
    else:
        snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        key = f"{ANALYTICS_BASE_PREFIX}/{snapshot_date}/{CORRELATION_CSV_NAME}"
        buf = io.StringIO()
        result_df.to_csv(buf, index=False)
        location = storage.write_raw(key, buf.getvalue().encode("utf-8"))
        if write_xlsx:
            try:
                import openpyxl  # noqa: F401
                from io import BytesIO
                bbuf = BytesIO()
                with pd.ExcelWriter(bbuf, engine="openpyxl") as writer:
                    result_df.to_excel(writer, index=False, sheet_name="summary")
                bbuf.seek(0)
                xlsx_key = f"{ANALYTICS_BASE_PREFIX}/{snapshot_date}/{CORRELATION_XLSX_NAME}"
                storage.write_raw(xlsx_key, bbuf.getvalue())
            except Exception as e:
                print(f"[analysis] Skipping XLSX export to storage: {e}")
        return location


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
