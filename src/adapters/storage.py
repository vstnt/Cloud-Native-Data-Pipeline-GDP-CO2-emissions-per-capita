from __future__ import annotations

import io
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


class StorageAdapter(ABC):
    """
    Abstraction over the underlying storage layer (local FS, S3, etc.).

    Implementations are responsible for mapping logical keys such as
    "raw/world_bank_gdp/..." to physical locations.
    """

    @abstractmethod
    def write_raw(self, key: str, content: bytes) -> str:
        """
        Persist arbitrary bytes at the given key.

        Returns the fully-qualified location string (for tracing/logging),
        for example:
        - Local: "raw/world_bank_gdp/xxx.jsonl"
        - S3:    "s3://my-bucket/raw/world_bank_gdp/xxx.jsonl"
        """

    @abstractmethod
    def read_raw(self, key: str) -> bytes:
        """Read raw bytes previously stored at the given key."""

    @abstractmethod
    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        """
        Persist a DataFrame as a Parquet file at the given key.

        Returns the fully-qualified location string.
        """

    @abstractmethod
    def read_parquet(self, key: str) -> pd.DataFrame:
        """Load a Parquet file stored at the given key into a DataFrame."""

    @abstractmethod
    def list_keys(self, prefix: str) -> List[str]:
        """
        List logical keys under the given prefix.

        For local storage, this typically maps to a directory tree walk.
        For S3, this maps to a prefix listing.
        """


class LocalStorageAdapter(StorageAdapter):
    """
    Local filesystem-backed storage adapter.

    Keys are treated as relative paths under a root directory.
    Example:
        root_dir = Path(".")
        key      = "raw/world_bank_gdp/file.jsonl"
        -> actual path: ./raw/world_bank_gdp/file.jsonl
    """

    def __init__(self, root_dir: Path | str = ".") -> None:
        self.root_dir = Path(root_dir)

    def _resolve(self, key: str) -> Path:
        path = self.root_dir / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def write_raw(self, key: str, content: bytes) -> str:
        path = self._resolve(key)
        with path.open("wb") as f:
            f.write(content)
        return str(path)

    def read_raw(self, key: str) -> bytes:
        path = self.root_dir / key
        with path.open("rb") as f:
            return f.read()

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        path = self._resolve(key)
        df.to_parquet(path, index=False)
        return str(path)

    def read_parquet(self, key: str) -> pd.DataFrame:
        path = self.root_dir / key
        return pd.read_parquet(path)

    def list_keys(self, prefix: str) -> List[str]:
        base = self.root_dir / prefix
        if not base.exists():
            return []

        keys: List[str] = []
        for path in base.rglob("*"):
            if path.is_file():
                rel = path.relative_to(self.root_dir)
                keys.append(str(rel).replace(os.sep, "/"))
        return keys


class S3StorageAdapter(StorageAdapter):
    """
    S3-backed storage adapter using boto3.

    Keys map directly to S3 object keys under the configured bucket/prefix.
    """

    def __init__(
        self,
        bucket: str,
        *,
        base_prefix: Optional[str] = None,
        boto3_client: Optional["boto3.client"] = None,
    ) -> None:
        import boto3  # lazy import to keep local-only runs lighter

        self.bucket = bucket
        self.base_prefix = (base_prefix or "").rstrip("/")
        self._s3 = boto3_client or boto3.client("s3")

    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{key}"
        return key

    def write_raw(self, key: str, content: bytes) -> str:
        full_key = self._full_key(key)
        self._s3.put_object(Bucket=self.bucket, Key=full_key, Body=content)
        return f"s3://{self.bucket}/{full_key}"

    def read_raw(self, key: str) -> bytes:
        full_key = self._full_key(key)
        resp = self._s3.get_object(Bucket=self.bucket, Key=full_key)
        return resp["Body"].read()

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        full_key = self._full_key(key)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self._s3.put_object(Bucket=self.bucket, Key=full_key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{full_key}"

    def read_parquet(self, key: str) -> pd.DataFrame:
        full_key = self._full_key(key)
        resp = self._s3.get_object(Bucket=self.bucket, Key=full_key)
        data = resp["Body"].read()
        buffer = io.BytesIO(data)
        return pd.read_parquet(buffer)

    def list_keys(self, prefix: str) -> List[str]:
        full_prefix = self._full_key(prefix).rstrip("/") + "/"
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            contents: Iterable[dict] = page.get("Contents") or []
            for obj in contents:
                key = obj["Key"]
                # remove base_prefix so we always return logical keys
                if self.base_prefix and key.startswith(self.base_prefix + "/"):
                    key = key[len(self.base_prefix) + 1 :]
                keys.append(key)
        return keys


