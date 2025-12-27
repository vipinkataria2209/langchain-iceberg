"""LangChain Iceberg Toolkit - Native integration for Apache Iceberg."""

from langchain_iceberg.toolkit import IcebergToolkit
from langchain_iceberg.version import __version__

# Export exceptions for user error handling
from langchain_iceberg.exceptions import (
    IcebergConnectionError,
    IcebergError,
    IcebergInvalidFilterError,
    IcebergInvalidQueryError,
    IcebergNamespaceNotFoundError,
    IcebergSnapshotNotFoundError,
    IcebergTableNotFoundError,
    SemanticYAMLError,
)

__all__ = [
    "IcebergToolkit",
    "__version__",
    # Exceptions
    "IcebergError",
    "IcebergConnectionError",
    "IcebergTableNotFoundError",
    "IcebergNamespaceNotFoundError",
    "IcebergInvalidQueryError",
    "IcebergInvalidFilterError",
    "IcebergSnapshotNotFoundError",
    "SemanticYAMLError",
]

