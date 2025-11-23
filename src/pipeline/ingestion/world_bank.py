from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.pipeline.metadata.models import Dataset, MetadataStore


USER_AGENT = "gdp-co2-pipeline/0.1"


def _http_get(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request) as response:
        return response.read()


def _http_get_json(url: str) -> Any:
    return json.loads(_http_get(url).decode("utf-8"))


def _iter_world_bank_pages(base_url: str, params: Dict[str, Any]) -> Iterable[List[Dict[str, Any]]]:
    page = 1
    total_pages = None

    while total_pages is None or page <= total_pages:
        params_with_page = {**params, "page": page}
        url = f"{base_url}?{urlencode(params_with_page)}"
        payload = _http_get_json(url)

        if not isinstance(payload, list) or len(payload) != 2:
            break

        metadata, data = payload
        if total_pages is None:
            total_pages = metadata.get("pages", 1)

        yield data or []
        page += 1


def fetch_dataset_from_world_bank(dataset: Dataset) -> List[Dict[str, Any]]:
    value_field_name = dataset.extra_metadata.get("value_field_name")
    if not value_field_name:
        raise ValueError(f"Dataset {dataset.id} is missing 'value_field_name' in extra_metadata.")

    base_url = dataset.source.uri
    params = {
        "format": "json",
        "per_page": 1000,
    }

    rows: List[Dict[str, Any]] = []

    for page_data in _iter_world_bank_pages(base_url, params):
        for entry in page_data:
            country = entry.get("country") or {}
            date_str = entry.get("date")
            value = entry.get("value")

            if date_str is None:
                continue

            try:
                year = int(date_str)
            except (TypeError, ValueError):
                continue

            row = {
                "country_code": country.get("id"),
                "country_name": country.get("value"),
                "year": year,
                value_field_name: value,
            }
            rows.append(row)

    return rows


def fetch_dataset_from_owid_co2(dataset: Dataset) -> List[Dict[str, Any]]:
    extra = dataset.extra_metadata
    value_field_name = extra.get("value_field_name")
    if not value_field_name:
        raise ValueError(f"Dataset {dataset.id} is missing 'value_field_name' in extra_metadata.")

    country_code_field = extra.get("country_code_field", "iso_code")
    country_name_field = extra.get("country_name_field", "country")
    year_field = extra.get("year_field", "year")

    raw_csv = _http_get(dataset.source.uri).decode("utf-8")
    reader = csv.DictReader(io.StringIO(raw_csv))

    rows: List[Dict[str, Any]] = []
    for record in reader:
        year_str = record.get(year_field)
        if not year_str:
            continue

        try:
            year = int(float(year_str))
        except (TypeError, ValueError):
            continue

        value_str = record.get(value_field_name)
        try:
            value = float(value_str) if value_str not in (None, "", "NA") else None
        except ValueError:
            value = None

        row = {
            "country_code": record.get(country_code_field) or None,
            "country_name": record.get(country_name_field) or None,
            "year": year,
            value_field_name: value,
        }
        rows.append(row)

    return rows


def save_rows_to_csv(rows: List[Dict[str, Any]], dataset: Dataset, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{dataset.id}.csv"

    column_names = [col.name for col in dataset.columns]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=column_names)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return output_path


def ingest_dataset(dataset_id: str, metadata_path: Path | None = None, output_dir: Path | None = None) -> Path:
    if metadata_path is None:
        metadata_path = Path("config") / "datasets_metadata.json"
    if output_dir is None:
        output_dir = Path("data") / "raw"

    store = MetadataStore.load_from_file(metadata_path)
    dataset = store.get_dataset(dataset_id)
    if dataset is None:
        raise KeyError(f"Dataset '{dataset_id}' not found in metadata store.")

    source_org = (dataset.extra_metadata.get("source_organization") or "").lower()
    if source_org == "world bank":
        rows = fetch_dataset_from_world_bank(dataset)
    elif source_org == "our world in data":
        rows = fetch_dataset_from_owid_co2(dataset)
    else:
        raise ValueError(f"Unsupported source organization '{source_org}' for dataset '{dataset_id}'.")
    return save_rows_to_csv(rows, dataset, output_dir)


def ingest_default_datasets(metadata_path: Path | None = None, output_dir: Path | None = None) -> Dict[str, Path]:
    dataset_ids = ["gdp_per_capita", "co2_emissions_per_capita"]
    results: Dict[str, Path] = {}
    for dataset_id in dataset_ids:
        results[dataset_id] = ingest_dataset(dataset_id, metadata_path, output_dir)
    return results


def main() -> None:
    ingest_default_datasets()


if __name__ == "__main__":
    main()
