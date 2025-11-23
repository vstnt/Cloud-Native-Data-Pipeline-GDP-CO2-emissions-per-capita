"""
Cloud orchestration entrypoint for the GDP x CO2 pipeline.

This module wires the existing business logic to cloud-native
adapters (S3 + DynamoDB), preparing the project for the
implementation of Part 2 of the case.

It is intentionally thin: all domain rules live in the existing
ingestion, transformation, curated and analysis modules.

Environment variables
---------------------

The handler expects the following environment variables to be set
in the AWS Lambda configuration (or in your local environment
when testing):

- PIPELINE_S3_BUCKET
    Name of the S3 bucket where RAW / PROCESSED / CURATED data
    will be stored.

- PIPELINE_S3_BASE_PREFIX (optional)
    Optional logical base prefix under the bucket, for example
    "gdp-co2-pipeline". All keys will be written under this prefix.

- PIPELINE_METADATA_TABLE
    Name of the DynamoDB table used by the metadata adapter
    (runs + checkpoints), aligned with DynamoMetadataAdapter.

Lambda handler
--------------

Configure your Lambda to use:

    Handler: cloud_pipeline.lambda_handler

The event payload may optionally include:

    {
      "min_year": 2000,
      "max_year": 2023
    }

which is forwarded to the World Bank ingestion step.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

from adapters import DynamoMetadataAdapter, S3StorageAdapter
from analysis import (
    ANALYSIS_OUTPUT_DIR,
    build_correlation_summary,
    build_gdp_vs_co2_scatter,
)
from crawler.wikipedia_co2_crawler import crawl_wikipedia_co2_raw
from env_loader import load_dotenv_if_present
from ingestion_api.world_bank_ingestion import ingest_world_bank_gdp_raw
from transformations import (
    WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
    WORLD_BANK_PROCESSED_OUTPUT_DIR,
)
from transformations.country_mapping import (
    COUNTRY_MAPPING_BASE_PREFIX,
    COUNTRY_MAPPING_OVERRIDES_CSV,
    build_country_mapping_from_world_bank_parquet,
    _apply_overrides,
)
from transformations.curated_econ_environment_country_year import (
    CURATED_OUTPUT_DIR as CURATED_ECON_ENV_OUTPUT_DIR,
)
from transformations.curated_econ_environment_country_year import (
    build_and_save_curated_econ_environment_country_year,
)
from transformations.wikipedia_co2_processed import process_wikipedia_co2_raw_file
from transformations.world_bank_gdp_processed import process_world_bank_gdp_raw_file


# Carrega .env automaticamente em desenvolvimento/local, sem impactar AWS.
load_dotenv_if_present()

PIPELINE_S3_BUCKET_ENV = "PIPELINE_S3_BUCKET"
PIPELINE_S3_BASE_PREFIX_ENV = "PIPELINE_S3_BASE_PREFIX"
PIPELINE_METADATA_TABLE_ENV = "PIPELINE_METADATA_TABLE"


def _build_s3_storage_from_env() -> S3StorageAdapter:
    bucket = os.getenv(PIPELINE_S3_BUCKET_ENV)
    if not bucket:
        raise RuntimeError(
            f"Missing required environment variable {PIPELINE_S3_BUCKET_ENV!r} for S3 bucket name.",
        )

    base_prefix = os.getenv(PIPELINE_S3_BASE_PREFIX_ENV) or None
    return S3StorageAdapter(bucket=bucket, base_prefix=base_prefix)


def _build_metadata_adapter_from_env() -> DynamoMetadataAdapter:
    table_name = os.getenv(PIPELINE_METADATA_TABLE_ENV)
    if not table_name:
        raise RuntimeError(
            f"Missing required environment variable {PIPELINE_METADATA_TABLE_ENV!r} for DynamoDB table name.",
        )
    return DynamoMetadataAdapter(table_name=table_name)


def run_cloud_pipeline(
    *,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
) -> Dict[str, List[Union[Path, str]]]:
    """
    Run the full pipeline using S3 + DynamoDB adapters.

    Parameters
    ----------
    min_year, max_year:
        Optional bounds forwarded to the World Bank ingestion
        (same semantics as in run_local_pipeline).

    Returns
    -------
    artefacts:
        Dictionary mapping step names to lists of generated
        logical keys (for S3-backed steps) or local Paths
        (for analytical outputs).
    """
    artefacts: Dict[str, List[Union[Path, str]]] = {}

    storage = _build_s3_storage_from_env()
    metadata = _build_metadata_adapter_from_env()

    # 1. World Bank ingestion (RAW -> S3)
    print("[cloud 1/7] Ingesting World Bank GDP RAW to S3...")
    raw_world_bank_key = ingest_world_bank_gdp_raw(
        storage,
        metadata,
        min_year=min_year,
        max_year=max_year,
    )
    artefacts["world_bank_raw"] = [raw_world_bank_key]

    # 2. World Bank processing (PROCESSED -> S3)
    print("[cloud 2/7] Processing World Bank RAW -> PROCESSED parquet (S3)...")
    wb_processed_keys = process_world_bank_gdp_raw_file(
        raw_world_bank_key,
        output_dir=WORLD_BANK_PROCESSED_OUTPUT_DIR,
        storage=storage,
    )
    artefacts["world_bank_processed"] = [str(k) for k in wb_processed_keys]

    # 3. Country mapping build (based on PROCESSED in S3)
    print("[cloud 3/7] Building country mapping from World Bank PROCESSED (S3)...")
    base_mapping_df = build_country_mapping_from_world_bank_parquet(
        processed_dir=WORLD_BANK_PROCESSED_OUTPUT_DIR,
        storage=storage,
    )
    mapping_df = _apply_overrides(base_mapping_df, overrides_path=COUNTRY_MAPPING_OVERRIDES_CSV)

    # Persist mapping in S3 for traceability / reuse.
    mapping_key = f"{COUNTRY_MAPPING_BASE_PREFIX}/country_mapping.parquet"
    storage.write_parquet(mapping_df, mapping_key)
    artefacts["country_mapping"] = [mapping_key]

    # 4. Wikipedia crawler (RAW -> S3)
    print("[cloud 4/7] Crawling Wikipedia CO2 per capita (RAW -> S3)...")
    raw_wikipedia_key = crawl_wikipedia_co2_raw(storage, metadata)
    artefacts["wikipedia_raw"] = [raw_wikipedia_key]

    # 5. Wikipedia processing (PROCESSED -> S3, with mapping)
    print("[cloud 5/7] Processing Wikipedia RAW -> PROCESSED parquet with country mapping (S3)...")
    wiki_processed_keys = process_wikipedia_co2_raw_file(
        raw_file_path=raw_wikipedia_key,
        output_dir=WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
        country_mapping=mapping_df,
        storage=storage,
    )
    artefacts["wikipedia_processed"] = [str(k) for k in wiki_processed_keys]

    # 6. Curated join (CURATED -> S3)
    print("[cloud 6/7] Building curated econ_environment_country_year dataset (S3)...")
    curated_keys = build_and_save_curated_econ_environment_country_year(
        world_bank_processed_dir=WORLD_BANK_PROCESSED_OUTPUT_DIR,
        wikipedia_processed_dir=WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
        output_dir=CURATED_ECON_ENV_OUTPUT_DIR,
        storage=storage,
    )
    artefacts["curated"] = [str(k) for k in curated_keys]

    # 7. Analytical outputs (local filesystem, reading CURATED from S3)
    print("[cloud 7/7] Generating analytical outputs from CURATED (reading via S3)...")
    scatter_path = None
    try:
        scatter_path = build_gdp_vs_co2_scatter(
            curated_root=CURATED_ECON_ENV_OUTPUT_DIR,
            output_dir=ANALYSIS_OUTPUT_DIR,
            year=2023,
            storage=storage,
        )
    except RuntimeError as exc:
        message = str(exc)
        if message.startswith("No curated data available") or message.startswith(
            "No valid rows for scatter plot",
        ):
            print(f"[cloud] Skipping scatter plot generation: {message}")
        else:
            raise

    corr_path = build_correlation_summary(
        curated_root=CURATED_ECON_ENV_OUTPUT_DIR,
        output_dir=ANALYSIS_OUTPUT_DIR,
        years=(2000, 2023),
        storage=storage,
    )
    analysis_paths = [p for p in (scatter_path, corr_path) if p is not None]
    artefacts["analysis"] = analysis_paths

    print("\nCloud pipeline completed successfully.")
    return artefacts


def lambda_handler(event, context):  # pragma: no cover - AWS entrypoint
    """
    AWS Lambda handler for the cloud pipeline.

    The incoming `event` may optionally contain `min_year` and `max_year`
    integers. All configuration for S3 / DynamoDB is taken from
    environment variables (see module docstring).
    """
    event = event or {}
    min_year = event.get("min_year")
    max_year = event.get("max_year")

    artefacts = run_cloud_pipeline(min_year=min_year, max_year=max_year)

    serialised = {
        key: [str(item) for item in values]
        for key, values in artefacts.items()
    }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Cloud pipeline executed successfully.",
                "artefacts": serialised,
            }
        ),
    }


__all__ = ["run_cloud_pipeline", "lambda_handler"]
