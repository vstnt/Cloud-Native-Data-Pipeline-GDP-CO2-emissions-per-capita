import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4


# Environment variable to override local JSON path (useful for tests or cloud)
METADATA_LOCAL_FILE_ENV = "METADATA_LOCAL_FILE"

# Default local JSON file used to mock DynamoDB in the local environment
DEFAULT_METADATA_FILE = Path("local_metadata.json")


def _now_utc_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _get_metadata_file() -> Path:
    """Resolve the path to the local JSON file used as the metadata store."""
    env_value = os.getenv(METADATA_LOCAL_FILE_ENV)
    if env_value:
        return Path(env_value)
    return DEFAULT_METADATA_FILE


def _load_store() -> Dict[str, Any]:
    """
    Load the metadata store from the local JSON file.

    Structure:
    {
      "runs": [
        {
          "ingestion_run_id": str,
          "run_scope": str,
          "start_ts": str,
          "end_ts": Optional[str],
          "status": str,
          "rows_processed": Optional[int],
          "last_checkpoint": Optional[str],
          "error_message": Optional[str]
        },
        ...
      ],
      "checkpoints": {
        "<source>": "<value as string or primitive>"
      }
    }
    """
    path = _get_metadata_file()
    if not path.exists():
        return {"runs": [], "checkpoints": {}}

    with path.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Metadata file {path} is corrupted") from exc

    # Ensure keys exist
    if not isinstance(data, dict):
        raise RuntimeError(f"Metadata file {path} has invalid format (expected object)")

    data.setdefault("runs", [])
    data.setdefault("checkpoints", {})
    if not isinstance(data["runs"], list) or not isinstance(data["checkpoints"], dict):
        raise RuntimeError(f"Metadata file {path} has invalid structure")

    return data


def _save_store(store: Dict[str, Any]) -> None:
    """Persist the metadata store atomically to the local JSON file."""
    path = _get_metadata_file()
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)

    tmp_path.replace(path)


def start_run(run_scope: str) -> str:
    """
    Register the start of an ingestion run.

    Parameters
    ----------
    run_scope:
        High-level scope of the run, for example:
        - \"world_bank_api\"
        - \"wikipedia_co2\"
        - \"curated_join\"

    Returns
    -------
    ingestion_run_id:
        Identifier of the created run, to be used later in end_run().
    """
    store = _load_store()

    ingestion_run_id = str(uuid4())
    run_record: Dict[str, Any] = {
        "ingestion_run_id": ingestion_run_id,
        "run_scope": run_scope,
        "start_ts": _now_utc_iso(),
        "end_ts": None,
        "status": "RUNNING",
        "rows_processed": None,
        "last_checkpoint": None,
        "error_message": None,
    }

    store["runs"].append(run_record)
    _save_store(store)

    return ingestion_run_id


def end_run(
    ingestion_run_id: str,
    status: str = "SUCCESS",
    *,
    rows_processed: Optional[int] = None,
    last_checkpoint: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Register the end of an ingestion run.

    Parameters
    ----------
    ingestion_run_id:
        Identifier returned by start_run().
    status:
        Final status of the run. Typical values:
        - \"SUCCESS\"
        - \"FAILED\"
    rows_processed:
        Optional count of rows processed during the run.
    last_checkpoint:
        Optional last checkpoint value associated with this run
        (for example, last_year_loaded_world_bank).
        If provided, it is also persisted via save_checkpoint().
    error_message:
        Optional error description if the run failed.

    Returns
    -------
    run_record:
        The updated run record stored in the metadata JSON.
    """
    store = _load_store()
    runs: List[Dict[str, Any]] = store.get("runs", [])

    target_run: Optional[Dict[str, Any]] = None
    for run in reversed(runs):
        if run.get("ingestion_run_id") == ingestion_run_id:
            target_run = run
            break

    if target_run is None:
        raise KeyError(f"No ingestion run found with id={ingestion_run_id!r}")

    target_run["end_ts"] = _now_utc_iso()
    target_run["status"] = status

    if rows_processed is not None:
        target_run["rows_processed"] = int(rows_processed)
    if last_checkpoint is not None:
        # Store as string for portabilidade (alinhado com representação string em DynamoDB).
        checkpoint_str = str(last_checkpoint)
        target_run["last_checkpoint"] = checkpoint_str
    if error_message is not None:
        target_run["error_message"] = error_message

    _save_store(store)
    return target_run


def save_checkpoint(source: str, value: Any, _store: Optional[Dict[str, Any]] = None) -> None:
    """
    Persist a checkpoint value for a given source.

    Examples
    --------
    save_checkpoint(\"world_bank_api\", \"2023\")
    save_checkpoint(\"wikipedia_co2\", \"page_5\")
    """
    if _store is None:
        store = _load_store()
    else:
        store = _store

    checkpoints: Dict[str, Any] = store.setdefault("checkpoints", {})
    checkpoints[source] = value

    if _store is None:
        _save_store(store)


def load_checkpoint(source: str, default: Optional[Any] = None) -> Any:
    """
    Load the checkpoint value for a given source.

    If no checkpoint exists yet, returns `default`.

    Examples
    --------
    last_year = load_checkpoint(\"world_bank_api\", default=\"2000\")
    """
    store = _load_store()
    checkpoints: Dict[str, Any] = store.get("checkpoints", {})
    if source in checkpoints:
        return checkpoints[source]
    return default


def get_last_run(run_scope: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Convenience helper to get the most recent run record.

    Parameters
    ----------
    run_scope:
        If provided, filters by run_scope (e.g., \"world_bank_api\").

    Returns
    -------
    run_record or None
    """
    store = _load_store()
    runs: List[Dict[str, Any]] = store.get("runs", [])
    if not runs:
        return None

    if run_scope is None:
        return runs[-1]

    for run in reversed(runs):
        if run.get("run_scope") == run_scope:
            return run

    return None


def list_runs(run_scope: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all recorded runs, optionally filtered by run_scope.
    """
    store = _load_store()
    runs: List[Dict[str, Any]] = store.get("runs", [])
    if run_scope is None:
        return list(runs)
    return [r for r in runs if r.get("run_scope") == run_scope]


def get_all_checkpoints() -> Dict[str, Any]:
    """
    Return the full checkpoints mapping.
    """
    store = _load_store()
    return dict(store.get("checkpoints", {}))


def reset_local_store() -> Tuple[int, int]:
    """
    Utility mainly for local development/tests.

    Clears all runs and checkpoints in the local JSON file.

    Returns
    -------
    (runs_cleared, checkpoints_cleared)
    """
    store = _load_store()
    runs_count = len(store.get("runs", []))
    checkpoints_count = len(store.get("checkpoints", {}))

    new_store = {"runs": [], "checkpoints": {}}
    _save_store(new_store)

    return runs_count, checkpoints_count
