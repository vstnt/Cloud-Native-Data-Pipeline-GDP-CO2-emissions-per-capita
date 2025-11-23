from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class DataSourceType(str, Enum):
    API = "api"
    FILE = "file"
    DATABASE = "database"


@dataclass
class DataSource:
    name: str
    type: DataSourceType
    uri: str
    description: Optional[str] = None


@dataclass
class Column:
    name: str
    dtype: str
    description: Optional[str] = None
    nullable: bool = True


@dataclass
class Dataset:
    id: str
    name: str
    description: Optional[str]
    source: DataSource
    columns: List[Column]
    tags: List[str] = field(default_factory=list)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


class MetadataStore:
    def __init__(self) -> None:
        self._datasets: Dict[str, Dataset] = {}

    def register_dataset(self, dataset: Dataset) -> None:
        self._datasets[dataset.id] = dataset

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        return self._datasets.get(dataset_id)

    def list_datasets(self) -> List[Dataset]:
        return list(self._datasets.values())

    def to_dict(self) -> Dict[str, Any]:
        return {
            dataset_id: self._dataset_to_serializable(dataset)
            for dataset_id, dataset in self._datasets.items()
        }

    def save_to_file(self, path: Path) -> None:
        payload = self.to_dict()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetadataStore":
        store = cls()
        for dataset_id, payload in data.items():
            store._datasets[dataset_id] = cls._dataset_from_serializable(payload)
        return store

    @classmethod
    def load_from_file(cls, path: Path) -> "MetadataStore":
        if not path.exists():
            return cls()
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(raw)

    @staticmethod
    def _dataset_to_serializable(dataset: Dataset) -> Dict[str, Any]:
        result = asdict(dataset)
        result["source"]["type"] = dataset.source.type.value
        result["created_at"] = dataset.created_at.isoformat()
        result["updated_at"] = dataset.updated_at.isoformat()
        return result

    @staticmethod
    def _dataset_from_serializable(payload: Dict[str, Any]) -> Dataset:
        source_payload = payload["source"]
        source = DataSource(
            name=source_payload["name"],
            type=DataSourceType(source_payload["type"]),
            uri=source_payload["uri"],
            description=source_payload.get("description"),
        )

        columns = [
            Column(
                name=col["name"],
                dtype=col["dtype"],
                description=col.get("description"),
                nullable=col.get("nullable", True),
            )
            for col in payload.get("columns", [])
        ]

        return Dataset(
            id=payload["id"],
            name=payload["name"],
            description=payload.get("description"),
            source=source,
            columns=columns,
            tags=payload.get("tags", []),
            extra_metadata=payload.get("extra_metadata", {}),
            created_at=datetime.fromisoformat(payload["created_at"]),
            updated_at=datetime.fromisoformat(payload["updated_at"]),
        )

