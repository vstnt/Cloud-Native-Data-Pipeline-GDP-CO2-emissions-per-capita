"""
Local orchestration entrypoint for the full GDP x CO₂ pipeline.

Implements step (9) from `context/Passos da implementação.pdf`:
an entrypoint that, when executed locally, runs:

1. World Bank API ingestion (RAW)
2. World Bank processing (PROCESSED)
3. Country mapping build
4. Wikipedia crawler (RAW)
5. Wikipedia processing (PROCESSED, with country mapping)
6. Curated join (Economic & Environmental by country-year)
7. Analytical outputs (scatter + correlation summary)

Intended usage (local):

    PYTHONPATH=src python -m local_pipeline

You can also pass optional arguments to restrict the World Bank ingestion
to a specific year range, for example:

    PYTHONPATH=src python -m local_pipeline --min-year 2000 --max-year 2023
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from adapters import LocalMetadataAdapter, LocalStorageAdapter
from analysis import (
    ANALYSIS_OUTPUT_DIR,
    build_correlation_summary,
    build_gdp_vs_co2_scatter,
)
from crawler.wikipedia_co2_crawler import crawl_wikipedia_co2_raw
from ingestion_api.world_bank_ingestion import ingest_world_bank_gdp_raw
from transformations import (
    COUNTRY_MAPPING_PARQUET_PATH,
    WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
    WORLD_BANK_PROCESSED_OUTPUT_DIR,
    build_and_save_country_mapping_from_world_bank,
    load_country_mapping,
)
from transformations.curated_econ_environment_country_year import (
    CURATED_OUTPUT_DIR as CURATED_ECON_ENV_OUTPUT_DIR,
)
from transformations.curated_econ_environment_country_year import (
    build_and_save_curated_econ_environment_country_year,
)
from transformations.wikipedia_co2_processed import process_wikipedia_co2_raw_file
from transformations.world_bank_gdp_processed import process_world_bank_gdp_raw_file


def run_local_pipeline(
    *,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
) -> Dict[str, List[Path]]:
    """
    Run the full local pipeline end-to-end.

    Parameters
    ----------
    min_year, max_year:
        Optional year bounds passed to the World Bank ingestion
        (see ingest_world_bank_gdp_raw). If omitted, the ingestion
        uses the checkpoint-based incremental strategy.

    Returns
    -------
    artefacts:
        Dictionary mapping step names to lists of generated Paths.
    """
    artefacts: Dict[str, List[Path]] = {}

    # Local adapters backing the pipeline (filesystem + JSON metadata).
    storage = LocalStorageAdapter()
    metadata = LocalMetadataAdapter()

    # 1. World Bank ingestion (RAW)
    print("[1/7] Ingesting World Bank GDP RAW...")
    raw_world_bank_key = ingest_world_bank_gdp_raw(
        storage,
        metadata,
        min_year=min_year,
        max_year=max_year,
    )
    raw_world_bank_path = Path(raw_world_bank_key)
    artefacts["world_bank_raw"] = [raw_world_bank_path]
    print(f"      RAW file: {raw_world_bank_path}")

    # 2. World Bank processing (PROCESSED)
    print("[2/7] Processing World Bank RAW -> PROCESSED parquet...")
    wb_processed_paths = process_world_bank_gdp_raw_file(raw_world_bank_path)
    artefacts["world_bank_processed"] = [Path(p) for p in wb_processed_paths]
    print(f"      Generated {len(wb_processed_paths)} parquet files.")

    # 3. Country mapping build
    print("[3/7] Building country mapping from World Bank PROCESSED...")
    mapping_path = build_and_save_country_mapping_from_world_bank(
        processed_dir=WORLD_BANK_PROCESSED_OUTPUT_DIR,
    )
    artefacts["country_mapping"] = [Path(mapping_path)]
    print(f"      Mapping parquet: {mapping_path}")

    # 4. Wikipedia crawler (RAW)
    print("[4/7] Crawling Wikipedia CO2 per capita (RAW)...")
    raw_wikipedia_key = crawl_wikipedia_co2_raw(storage, metadata)
    raw_wikipedia_path = Path(raw_wikipedia_key)
    artefacts["wikipedia_raw"] = [raw_wikipedia_path]
    print(f"      RAW file: {raw_wikipedia_path}")

    # 5. Wikipedia processing (PROCESSED, with country mapping)
    print("[5/7] Processing Wikipedia RAW -> PROCESSED parquet with country mapping...")
    mapping_df = load_country_mapping(path=COUNTRY_MAPPING_PARQUET_PATH)
    wiki_processed_paths = process_wikipedia_co2_raw_file(
        raw_file_path=raw_wikipedia_path,
        output_dir=WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
        country_mapping=mapping_df,
    )
    artefacts["wikipedia_processed"] = [Path(p) for p in wiki_processed_paths]
    print(f"      Generated {len(wiki_processed_paths)} parquet files.")

    # 6. Curated join
    print("[6/7] Building curated econ_environment_country_year dataset...")
    curated_paths = build_and_save_curated_econ_environment_country_year(
        world_bank_processed_dir=WORLD_BANK_PROCESSED_OUTPUT_DIR,
        wikipedia_processed_dir=WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
        output_dir=CURATED_ECON_ENV_OUTPUT_DIR,
    )
    artefacts["curated"] = [Path(p) for p in curated_paths]
    print(f"      Generated {len(curated_paths)} curated parquet files.")

    # 7. Analytical outputs
    print("[7/7] Generating analytical outputs (scatter + correlation summary)...")
    scatter_path = build_gdp_vs_co2_scatter(
        curated_root=CURATED_ECON_ENV_OUTPUT_DIR,
        output_dir=ANALYSIS_OUTPUT_DIR,
        year=2023,
    )
    corr_path = build_correlation_summary(
        curated_root=CURATED_ECON_ENV_OUTPUT_DIR,
        output_dir=ANALYSIS_OUTPUT_DIR,
        years=(2000, 2023),
    )
    artefacts["analysis"] = [scatter_path, corr_path]
    print(f"      Scatter: {scatter_path}")
    print(f"      Correlation summary: {corr_path}")

    print("\nPipeline completed successfully.")
    return artefacts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the full local GDP x CO2 pipeline end-to-end.",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=None,
        help="Optional minimum year (inclusive) for World Bank ingestion.",
    )
    parser.add_argument(
        "--max-year",
        type=int,
        default=None,
        help="Optional maximum year (inclusive) for World Bank ingestion.",
    )

    args = parser.parse_args()
    run_local_pipeline(min_year=args.min_year, max_year=args.max_year)


__all__ = ["run_local_pipeline"]
