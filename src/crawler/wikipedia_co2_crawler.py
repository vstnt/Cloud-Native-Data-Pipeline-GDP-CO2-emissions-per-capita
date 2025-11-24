"""
Wikipedia CO2 emissions per capita crawler (RAW layer).

Responsibilities (aligned with the project plan):

- Download HTML from:
  https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions_per_capita
- Identify the main CO2 per capita emissions table.
- Build a "raw_table_json" representation (still dirty).
- Persist RAW layer in JSONL with the following schema:
    ingestion_run_id: string
    ingestion_ts: timestamp (ISO 8601, UTC)
    data_source: string (fixed "wikipedia_co2")
    page_url: string
    table_html: string (HTML of the table)
    raw_table_json: JSON (list of row dicts, still dirty)
    record_hash: SHA1 hash of the payload
    raw_file_path: logical key of the generated RAW file

Designed to run both locally (filesystem + JSON metadata) and in the cloud
(S3 + DynamoDB) via the StorageAdapter and MetadataAdapter abstractions.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from adapters import MetadataAdapter, StorageAdapter
from env_loader import load_dotenv_if_present
from metadata import WIKIPEDIA_CO2_SCOPE
from common.retry import http_get_with_retries

# Carrega .env se existir (para WIKIPEDIA_URL, entre outros).
load_dotenv_if_present()

WIKIPEDIA_CO2_URL = os.getenv(
    "WIKIPEDIA_URL",
    "https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions_per_capita",
)
WIKIPEDIA_DATA_SOURCE = "wikipedia_co2"
WIKIPEDIA_CHECKPOINT_KEY = "last_revid_wikipedia_co2"

# Logical base prefix for RAW files (local FS or S3).
RAW_BASE_PREFIX = "raw/wikipedia_co2"


def _now_utc_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def fetch_wikipedia_co2_html(
    url: str = WIKIPEDIA_CO2_URL,
    *,
    timeout: int = 30,
) -> str:
    """
    Download the raw HTML of the CO2 per capita Wikipedia page.

    Uses an explicit User-Agent to reduce the chance of 403s.
    """
    headers = {
        "User-Agent": "env-econ-pipeline/1.0 (+https://www.example.com/)",
    }
    response = http_get_with_retries(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def _extract_wikipedia_title(url: str) -> str:
    """
    Extract the MediaWiki title from a standard /wiki/<Title> URL.

    Examples
    --------
    https://en.wikipedia.org/wiki/Foo_Bar → Foo_Bar
    """
    from urllib.parse import urlparse, unquote

    parsed = urlparse(url)
    path = parsed.path or ""
    if "/wiki/" in path:
        title = path.split("/wiki/", 1)[1]
    else:
        # Fallback: entire path without leading slash
        title = path.lstrip("/")
    return unquote(title)


def fetch_wikipedia_latest_revision(
    url: str = WIKIPEDIA_CO2_URL,
    *,
    timeout: int = 30,
) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Query the MediaWiki API for the latest page revision id and timestamp.

    Returns
    -------
    (pageid, revid, rev_timestamp) or (None, None, None) if unavailable.
    """
    title = _extract_wikipedia_title(url)
    api = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|timestamp",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    headers = {"User-Agent": "env-econ-pipeline/1.0 (+https://www.example.com/)"}
    resp = http_get_with_retries(api, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    try:
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            return None, None, None
        page = pages[0]
        pageid = page.get("pageid")
        revs = page.get("revisions", [])
        if not revs:
            return pageid, None, None
        rev = revs[0]
        revid = rev.get("revid")
        rev_timestamp = rev.get("timestamp")
        return pageid, revid, rev_timestamp
    except Exception:  # noqa: BLE001
        return None, None, None


def _clean_cell_text(text: str) -> str:
    """
    Clean table cell text and handle a few inconsistencies:

    - Remove footnote markers like "[a]", "[1]", etc.
    - Normalize whitespace.
    """
    cleaned = re.sub(r"\[[^\]]*\]", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def find_co2_table_html(page_html: str) -> Tuple[str, List[str]]:
    """
    Locate the main CO2 emissions per capita table in the page.

    Returns
    -------
    table_html:
        Raw HTML of the table.
    headers:
        List of column headers extracted from the first row.
    """
    soup = BeautifulSoup(page_html, "html.parser")

    candidate_tables = soup.find_all("table", class_="wikitable")
    if not candidate_tables:
        raise RuntimeError("No 'wikitable' tables found in the Wikipedia page.")

    target_table = None
    for table in candidate_tables:
        caption = table.find("caption")
        caption_text = caption.get_text(" ", strip=True).lower() if caption else ""
        header_text = table.get_text(" ", strip=True).lower()

        if "emissions" in caption_text and "per capita" in caption_text:
            target_table = table
            break
        if "co2" in caption_text or "carbon dioxide" in caption_text:
            target_table = table
            break
        # Fallback: look for keywords in the table content.
        if "co2" in header_text and "per capita" in header_text:
            target_table = table
            break

    if target_table is None:
        # Fallback to the first wikitable.
        target_table = candidate_tables[0]

    header_row = target_table.find("tr")
    if not header_row:
        raise RuntimeError("Target table has no header row (<tr>).")

    headers: List[str] = []
    for cell in header_row.find_all(["th", "td"]):
        header_text = _clean_cell_text(cell.get_text(" ", strip=True))
        if header_text:
            headers.append(header_text)

    if not headers:
        raise RuntimeError("Failed to extract headers from the CO2 table.")

    table_html = str(target_table)
    return table_html, headers


def parse_co2_table_rows(table_html: str, headers: List[str]) -> List[Dict[str, Any]]:
    """
    Convert the table HTML into a list of "raw" row dicts (raw_table_json).

    - Each row is a dict: {<header>: <cell_text>, ...}
    - Extra cells are ignored; missing cells are set to None.
    """
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Provided HTML does not contain a <table> tag.")

    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr"):
        # Skip header row
        if tr.find("th"):
            continue

        cells = tr.find_all("td")
        if not cells:
            continue

        row_values: Dict[str, Any] = {}
        for idx, header in enumerate(headers):
            if idx < len(cells):
                cell_text = cells[idx].get_text(" ", strip=True)
                cell_text = _clean_cell_text(cell_text)
                row_values[header] = cell_text if cell_text != "" else None
            else:
                row_values[header] = None

        rows.append(row_values)

    return rows


def _compute_record_hash(payload: Dict[str, Any]) -> str:
    """
    Compute a stable SHA1 hash of the RAW payload (without audit fields),
    aligned with the strategy used for the World Bank ingestion.
    """
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def crawl_wikipedia_co2_raw(
    storage: StorageAdapter,
    metadata: MetadataAdapter,
    url: str = WIKIPEDIA_CO2_URL,
    *,
    timeout: int = 30,
    run_scope: str = WIKIPEDIA_CO2_SCOPE,
) -> Dict[str, Any]:
    """
    Execute the Wikipedia crawler for CO2 per capita and persist the RAW layer.

    Steps:
    - Start a metadata run (scope "wikipedia_co2").
    - Download the page.
    - Locate the main CO2 per capita table.
    - Convert the table into a "raw_table_json" list of rows.
    - Compute record_hash for the raw payload (HTML + JSON rows).
    - Persist a single RAW record in JSONL under RAW_BASE_PREFIX.
    - Finish the run in the metadata store.

    Returns
    -------
    result:
        Dict with keys:
        - changed: bool (True if a new RAW was created)
        - raw_key: Optional[str] (present only when changed)
        - pageid: Optional[int]
        - revid: Optional[int]
        - rev_timestamp: Optional[str]
    """
    run_id = metadata.start_run(run_scope)
    ingestion_ts_iso = _now_utc_iso()

    try:
        # 0. Fetch latest revision from MediaWiki API
        pageid, revid, rev_ts = fetch_wikipedia_latest_revision(url=url, timeout=timeout)

        # 0.1 Check checkpoint and short-circuit if unchanged
        last_revid = metadata.load_checkpoint(WIKIPEDIA_CHECKPOINT_KEY)
        if revid is not None and last_revid is not None and str(revid) == str(last_revid):
            metadata.end_run(
                run_id,
                status="SKIPPED",
                rows_processed=0,
                last_checkpoint=str(revid),
            )
            return {
                "changed": False,
                "raw_key": None,
                "pageid": pageid,
                "revid": revid,
                "rev_timestamp": rev_ts,
            }

        # 1. Download the page HTML
        page_html = fetch_wikipedia_co2_html(url=url, timeout=timeout)

        # 2. Find CO2 per capita table and headers
        table_html, headers = find_co2_table_html(page_html)

        # 3. Convert the table into raw rows
        raw_table_rows = parse_co2_table_rows(table_html, headers)

        # 4. Build raw payload and record_hash (without audit fields)
        raw_payload = {
            "page_url": url,
            "headers": headers,
            "rows": raw_table_rows,
        }
        record_hash = _compute_record_hash(raw_payload)

        # 5. Determine logical RAW key
        timestamp_for_filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{RAW_BASE_PREFIX}/wikipedia_co2_raw_{timestamp_for_filename}.jsonl"
        raw_file_path_str = key

        # 6. Build final RAW record aligned with schema 1.2 (+ revision metadata)
        raw_record: Dict[str, Any] = {
            "ingestion_run_id": run_id,
            "ingestion_ts": ingestion_ts_iso,
            "data_source": WIKIPEDIA_DATA_SOURCE,
            "page_url": url,
            "pageid": pageid,
            "revid": revid,
            "rev_timestamp": rev_ts,
            "table_html": table_html,
            "raw_table_json": raw_payload,
            "record_hash": record_hash,
            "raw_file_path": raw_file_path_str,
        }

        # 7. Persist as JSONL (one record per file)
        content = (json.dumps(raw_record, ensure_ascii=False) + "\n").encode("utf-8")
        storage.write_raw(key, content)

        rows_processed = len(raw_table_rows)

        # 8. Register run completion
        metadata.save_checkpoint(WIKIPEDIA_CHECKPOINT_KEY, str(revid) if revid is not None else "")
        metadata.end_run(
            run_id,
            status="SUCCESS",
            rows_processed=rows_processed,
            last_checkpoint=str(revid) if revid is not None else None,
        )

        return {
            "changed": True,
            "raw_key": raw_file_path_str,
            "pageid": pageid,
            "revid": revid,
            "rev_timestamp": rev_ts,
        }
    except Exception as exc:  # noqa: BLE001
        metadata.end_run(
            run_id,
            status="FAILED",
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    # CLI helper for local runs:
    #   PYTHONPATH=src python -m crawler.wikipedia_co2_crawler
    import argparse

    from adapters import LocalMetadataAdapter, LocalStorageAdapter

    parser = argparse.ArgumentParser(
        description="Crawler da Wikipedia para CO2 per capita (camada RAW).",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=WIKIPEDIA_CO2_URL,
        help="URL da página da Wikipedia (default: página oficial de CO2 per capita).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout em segundos para a requisição HTTP (default: 30).",
    )

    args = parser.parse_args()

    storage = LocalStorageAdapter()
    metadata = LocalMetadataAdapter()

    output = crawl_wikipedia_co2_raw(
        storage,
        metadata,
        url=args.url,
        timeout=args.timeout,
    )
    print(json.dumps(output, ensure_ascii=False))


__all__ = [
    "WIKIPEDIA_CO2_URL",
    "WIKIPEDIA_DATA_SOURCE",
    "WIKIPEDIA_CHECKPOINT_KEY",
    "RAW_BASE_PREFIX",
    "fetch_wikipedia_co2_html",
    "fetch_wikipedia_latest_revision",
    "find_co2_table_html",
    "parse_co2_table_rows",
    "crawl_wikipedia_co2_raw",
]
