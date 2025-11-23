"""
Adapters package
----------------

Abstractions for I/O and metadata so that the pipeline can run
both locally (filesystem + JSON) and in the cloud (S3 + DynamoDB)
using the same business logic.
"""

from .storage import (  # noqa: F401
    LocalStorageAdapter,
    S3StorageAdapter,
    StorageAdapter,
)
from .metadata import (  # noqa: F401
    DynamoMetadataAdapter,
    LocalMetadataAdapter,
    MetadataAdapter,
)

__all__ = [
    "StorageAdapter",
    "LocalStorageAdapter",
    "S3StorageAdapter",
    "MetadataAdapter",
    "LocalMetadataAdapter",
    "DynamoMetadataAdapter",
]

