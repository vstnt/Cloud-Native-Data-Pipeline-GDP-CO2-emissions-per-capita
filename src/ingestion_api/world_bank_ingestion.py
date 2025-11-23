"""
RAW ingestion from the World Bank API (GDP per capita).

This module is designed to be storage/metadata agnostic so that it can
run both locally (filesystem + JSON metadata) and in the cloud
(S3 + DynamoDB) using the same business logic.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from adapters import MetadataAdapter, StorageAdapter
from env_loader import load_dotenv_if_present
from metadata import WORLD_BANK_API_SCOPE

# Carrega .env se existir (para WORLD_BANK_INDICATOR, entre outros).
load_dotenv_if_present()

WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2/country/all/indicator"
WORLD_BANK_INDICATOR_ID = os.getenv("WORLD_BANK_INDICATOR", "NY.GDP.PCAP.CD")
WORLD_BANK_CHECKPOINT_KEY = "last_year_loaded_world_bank"
WORLD_BANK_DATA_SOURCE = "world_bank_api"

# Logical base prefix for RAW files (local FS or S3).
RAW_BASE_PREFIX = "raw/world_bank_gdp"


def _now_utc_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _fetch_indicator_page(
    indicator_id: str,
    page: int,
    per_page: int = 1000,
    *,
    timeout: int = 30,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Fetch one page from the World Bank API for the given indicator.

    Returns (metadata, records).
    """
    url = f"{WORLD_BANK_BASE_URL}/{indicator_id}"
    params = {
        "format": "json",
        "page": page,
        "per_page": per_page,
    }
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or len(data) != 2:
        raise RuntimeError(f"Unexpected response from World Bank API: {data!r}")

    metadata, records = data
    if not isinstance(metadata, dict) or not isinstance(records, list):
        raise RuntimeError(f"Unexpected structure from World Bank API: {data!r}")

    return metadata, records


def fetch_all_indicator_records(
    indicator_id: str = WORLD_BANK_INDICATOR_ID,
    *,
    per_page: int = 1000,
    timeout: int = 30,
) -> Iterable[Dict[str, Any]]:
    """Iterate over all records of the given indicator (all pages)."""
    page = 1
    metadata, records = _fetch_indicator_page(
        indicator_id,
        page=page,
        per_page=per_page,
        timeout=timeout,
    )
    total_pages = int(metadata.get("pages", 1))

    for record in records:
        yield record

    while page < total_pages:
        page += 1
        metadata, records = _fetch_indicator_page(
            indicator_id,
            page=page,
            per_page=per_page,
            timeout=timeout,
        )
        for record in records:
            yield record


def list_indicator_years(indicator_id: str = WORLD_BANK_INDICATOR_ID) -> List[int]:
    """List all available years for the given indicator."""
    years = set()
    for record in fetch_all_indicator_records(indicator_id):
        date_str = record.get("date")
        if isinstance(date_str, str) and date_str.isdigit():
            years.add(int(date_str))
    return sorted(years)


def compute_record_hash(record: Dict[str, Any]) -> str:
    """
    Compute a stable hash of the RAW record for deduplication/control.

    Uses the normalized JSON of the original payload (without audit fields),
    as suggested in the project plan.
    """
    normalized = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _filter_records_by_years(
    records: Iterable[Dict[str, Any]],
    *,
    min_year_exclusive: Optional[int],
    max_year_inclusive: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Filter records by year interval.

    - Includes only records with year > min_year_exclusive (if set).
    - Excludes records with year > max_year_inclusive (if set).
    """
    filtered: List[Dict[str, Any]] = []
    for record in records:
        date_str = record.get("date")
        if not isinstance(date_str, str) or not date_str.isdigit():
            continue
        year = int(date_str)
        if min_year_exclusive is not None and year <= min_year_exclusive:
            continue
        if max_year_inclusive is not None and year > max_year_inclusive:
            continue
        filtered.append(record)
    return filtered


def ingest_world_bank_gdp_raw(
    storage: StorageAdapter,
    metadata: MetadataAdapter,
    *,
    indicator_id: str = WORLD_BANK_INDICATOR_ID,
    run_scope: str = WORLD_BANK_API_SCOPE,
    checkpoint_key: str = WORLD_BANK_CHECKPOINT_KEY,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
) -> str:
    """
    RAW ingestion from the World Bank API with incremental logic by year.

    Steps:
    - Load checkpoint last_year_loaded_world_bank (if any).
    - Fetch all indicator records from the API.
    - Filter only years > checkpoint (and within [min_year, max_year] if provided).
    - Enrich records with metadata and record_hash.
    - Persist as JSONL under RAW_BASE_PREFIX.
    - Update checkpoint and register run via the metadata adapter.

    Returns
    -------
    raw_key:
        Logical key for the generated JSONL file, e.g.:
        "raw/world_bank_gdp/world_bank_gdp_raw_<timestamp>.jsonl"
    """
    run_id = metadata.start_run(run_scope)
    ingestion_ts_iso = _now_utc_iso()

    try:
        # 1. Load checkpoint (if any)
        last_year_str = metadata.load_checkpoint(checkpoint_key)
        last_year_from_ckpt: Optional[int] = None
        if last_year_str is not None:
            try:
                last_year_from_ckpt = int(str(last_year_str))
            except ValueError:
                last_year_from_ckpt = None

        # If min_year is greater than the checkpoint, use min_year - 1 as baseline.
        min_year_exclusive: Optional[int] = last_year_from_ckpt
        if min_year is not None:
            if min_year_exclusive is None or min_year > min_year_exclusive:
                min_year_exclusive = min_year - 1

        # 2. Fetch all records for the indicator
        all_records = list(fetch_all_indicator_records(indicator_id))

        # 3. Filter by year interval
        filtered_records = _filter_records_by_years(
            all_records,
            min_year_exclusive=min_year_exclusive,
            max_year_inclusive=max_year,
        )

        # Determine logical key before enrichment, since raw_file_path
        # is part of the RAW schema.
        timestamp_for_filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{RAW_BASE_PREFIX}/world_bank_gdp_raw_{timestamp_for_filename}.jsonl"
        raw_file_path_str = key

        # 4. Enrich with metadata and record_hash
        enriched_records: List[Dict[str, Any]] = []
        max_ingested_year: Optional[int] = None

        for record in filtered_records:
            record_year_str = record.get("date")
            record_year: Optional[int] = None
            if isinstance(record_year_str, str) and record_year_str.isdigit():
                record_year = int(record_year_str)

            if record_year is not None:
                if max_ingested_year is None or record_year > max_ingested_year:
                    max_ingested_year = record_year

            raw_payload = json.dumps(record, sort_keys=True, ensure_ascii=False)

            enriched = dict(record)
            enriched["ingestion_run_id"] = run_id
            enriched["ingestion_ts"] = ingestion_ts_iso
            enriched["data_source"] = WORLD_BANK_DATA_SOURCE
            enriched["raw_payload"] = raw_payload
            enriched["record_hash"] = compute_record_hash(record)
            enriched["raw_file_path"] = raw_file_path_str
            enriched_records.append(enriched)

        # 5. Persist JSONL (even if there are no records, for traceability)
        lines = [json.dumps(rec, ensure_ascii=False) for rec in enriched_records]
        content = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
        storage.write_raw(key, content)

        rows_processed = len(enriched_records)

        # 6. Update checkpoint if new data was ingested
        final_checkpoint_year: Optional[int] = last_year_from_ckpt
        if max_ingested_year is not None:
            final_checkpoint_year = max_ingested_year
            metadata.save_checkpoint(checkpoint_key, str(final_checkpoint_year))

        # 7. Register run completion
        metadata.end_run(
            run_id,
            status="SUCCESS",
            rows_processed=rows_processed,
            last_checkpoint=str(final_checkpoint_year) if final_checkpoint_year is not None else None,
        )

        return raw_file_path_str
    except Exception as exc:  # noqa: BLE001
        metadata.end_run(
            run_id,
            status="FAILED",
            error_message=str(exc),
        )
        raise


__all__ = [
    "WORLD_BANK_BASE_URL",
    "WORLD_BANK_INDICATOR_ID",
    "WORLD_BANK_CHECKPOINT_KEY",
    "WORLD_BANK_DATA_SOURCE",
    "RAW_BASE_PREFIX",
    "fetch_all_indicator_records",
    "list_indicator_years",
    "compute_record_hash",
    "ingest_world_bank_gdp_raw",
]
