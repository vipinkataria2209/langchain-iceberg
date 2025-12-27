"""Custom exceptions for langchain-iceberg."""


class IcebergError(Exception):
    """Base exception for all Iceberg toolkit errors."""

    pass


class IcebergConnectionError(IcebergError):
    """Raised when unable to connect to Iceberg catalog."""

    pass


class IcebergTableNotFoundError(IcebergError):
    """Raised when a table is not found."""

    pass


class IcebergNamespaceNotFoundError(IcebergError):
    """Raised when a namespace is not found."""

    pass


class IcebergInvalidQueryError(IcebergError):
    """Raised when a query is invalid."""

    pass


class IcebergInvalidFilterError(IcebergError):
    """Raised when a filter expression is invalid."""

    pass


class SemanticYAMLError(IcebergError):
    """Raised when semantic YAML is invalid or cannot be loaded."""

    pass


class IcebergSnapshotNotFoundError(IcebergError):
    """Raised when a snapshot is not found."""

    pass

