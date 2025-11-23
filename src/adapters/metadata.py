from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from metadata import (  # type: ignore
    end_run as local_end_run,
    list_runs as local_list_runs,
    load_checkpoint as local_load_checkpoint,
    save_checkpoint as local_save_checkpoint,
    start_run as local_start_run,
)


class MetadataAdapter(ABC):
    """
    Abstraction over the metadata/checkpoint store (local JSON, DynamoDB, etc.).
    """

    @abstractmethod
    def start_run(self, run_scope: str) -> str:
        """Register the start of a run and return its identifier."""

    @abstractmethod
    def end_run(
        self,
        ingestion_run_id: str,
        status: str = "SUCCESS",
        *,
        rows_processed: Optional[int] = None,
        last_checkpoint: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Mark a run as finished and persist its final state."""

    @abstractmethod
    def save_checkpoint(self, source: str, value: Any) -> None:
        """Persist a checkpoint value for a given logical source."""

    @abstractmethod
    def load_checkpoint(self, source: str, default: Optional[Any] = None) -> Any:
        """Load the checkpoint value for the given source."""

    @abstractmethod
    def list_runs(self, run_scope: Optional[str] = None) -> List[Dict[str, Any]]:
        """List runs, optionally filtered by scope."""


class LocalMetadataAdapter(MetadataAdapter):
    """
    Adapter backed by the existing local JSON store in `src/metadata`.
    """

    def start_run(self, run_scope: str) -> str:
        return local_start_run(run_scope)

    def end_run(
        self,
        ingestion_run_id: str,
        status: str = "SUCCESS",
        *,
        rows_processed: Optional[int] = None,
        last_checkpoint: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        return local_end_run(
            ingestion_run_id,
            status=status,
            rows_processed=rows_processed,
            last_checkpoint=last_checkpoint,
            error_message=error_message,
        )

    def save_checkpoint(self, source: str, value: Any) -> None:
        local_save_checkpoint(source, value)

    def load_checkpoint(self, source: str, default: Optional[Any] = None) -> Any:
        return local_load_checkpoint(source, default)

    def list_runs(self, run_scope: Optional[str] = None) -> List[Dict[str, Any]]:
        return local_list_runs(run_scope)


class DynamoMetadataAdapter(MetadataAdapter):
    """
    DynamoDB-backed metadata adapter.

    This mirrors the behaviour of the local JSON-based implementation,
    but uses a DynamoDB table to persist runs and checkpoints.
    """

    def __init__(
        self,
        table_name: str,
        *,
        boto3_resource: Optional["boto3.resources.factory.dynamodb.ServiceResource"] = None,
    ) -> None:
        import boto3  # lazy import

        self._dynamodb = boto3_resource or boto3.resource("dynamodb")
        self._table = self._dynamodb.Table(table_name)

    def start_run(self, run_scope: str) -> str:
        import uuid
        from datetime import datetime, timezone

        ingestion_run_id = str(uuid.uuid4())
        item: Dict[str, Any] = {
            "pk": f"RUN#{ingestion_run_id}",
            "sk": "META",
            "ingestion_run_id": ingestion_run_id,
            "run_scope": run_scope,
            "start_ts": datetime.now(timezone.utc).isoformat(),
            "end_ts": None,
            "status": "RUNNING",
            "rows_processed": None,
            "last_checkpoint": None,
            "error_message": None,
        }
        self._table.put_item(Item=item)
        return ingestion_run_id

    def end_run(
        self,
        ingestion_run_id: str,
        status: str = "SUCCESS",
        *,
        rows_processed: Optional[int] = None,
        last_checkpoint: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        from datetime import datetime, timezone

        key = {"pk": f"RUN#{ingestion_run_id}", "sk": "META"}
        update_expr_parts = ["set #status = :status", "end_ts = :end_ts"]
        expr_attr_values: Dict[str, Any] = {
            ":status": status,
            ":end_ts": datetime.now(timezone.utc).isoformat(),
        }
        expr_attr_names = {"#status": "status"}

        if rows_processed is not None:
            update_expr_parts.append("rows_processed = :rows")
            expr_attr_values[":rows"] = int(rows_processed)
        if last_checkpoint is not None:
            update_expr_parts.append("last_checkpoint = :ckpt")
            expr_attr_values[":ckpt"] = str(last_checkpoint)
        if error_message is not None:
            update_expr_parts.append("error_message = :err")
            expr_attr_values[":err"] = error_message

        update_expr = " ".join(update_expr_parts)
        resp = self._table.update_item(
            Key=key,
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes", {})

    def save_checkpoint(self, source: str, value: Any) -> None:
        item = {
            "pk": f"CHECKPOINT#{source}",
            "sk": "META",
            "source": source,
            "value": value,
        }
        self._table.put_item(Item=item)

    def load_checkpoint(self, source: str, default: Optional[Any] = None) -> Any:
        key = {"pk": f"CHECKPOINT#{source}", "sk": "META"}
        resp = self._table.get_item(Key=key)
        item = resp.get("Item")
        if not item:
            return default
        return item.get("value", default)

    def list_runs(self, run_scope: Optional[str] = None) -> List[Dict[str, Any]]:
        # Simple scan; can be refined with GSI if needed.
        resp = self._table.scan(
            FilterExpression="begins_with(#pk, :run_prefix)",
            ExpressionAttributeNames={"#pk": "pk"},
            ExpressionAttributeValues={":run_prefix": "RUN#"},
        )
        items: List[Dict[str, Any]] = resp.get("Items", [])
        if run_scope is None:
            return items
        return [item for item in items if item.get("run_scope") == run_scope]

