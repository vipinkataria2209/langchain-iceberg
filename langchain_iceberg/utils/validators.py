"""Input validation utilities."""

import re
from typing import Optional

from langchain_iceberg.exceptions import (
    IcebergInvalidFilterError,
    IcebergInvalidQueryError,
    IcebergNamespaceNotFoundError,
    IcebergTableNotFoundError,
)


def validate_table_id(table_id: str) -> tuple[str, str]:
    """
    Validate and parse table_id into namespace and table name.

    Args:
        table_id: Table identifier in format "namespace.table_name"

    Returns:
        Tuple of (namespace, table_name)

    Raises:
        IcebergInvalidQueryError: If table_id format is invalid
    """
    if not table_id or not isinstance(table_id, str):
        raise IcebergInvalidQueryError(f"Invalid table_id: {table_id}")

    # Split on last dot to support multi-part namespaces (e.g., "analytics.prod.events")
    if "." not in table_id:
        raise IcebergInvalidQueryError(
            f"table_id must be in format 'namespace.table_name', got: {table_id}"
        )

    # Find last dot to split namespace and table
    last_dot_index = table_id.rfind(".")
    namespace = table_id[:last_dot_index]
    table_name = table_id[last_dot_index + 1:]
    if not namespace or not table_name:
        raise IcebergInvalidQueryError(
            f"table_id must have both namespace and table_name, got: {table_id}"
        )

    return namespace, table_name


def validate_namespace(namespace: str) -> str:
    """
    Validate namespace format.

    Args:
        namespace: Namespace identifier

    Returns:
        Validated namespace string

    Raises:
        IcebergNamespaceNotFoundError: If namespace is invalid
    """
    if not namespace or not isinstance(namespace, str):
        raise IcebergNamespaceNotFoundError(f"Invalid namespace: {namespace}")

    # Namespace can contain dots for nested namespaces
    if not namespace.strip():
        raise IcebergNamespaceNotFoundError("Namespace cannot be empty")

    return namespace.strip()


def validate_filter_expression(filter_str: Optional[str]) -> Optional[str]:
    """
    Basic validation of filter expression syntax.

    Args:
        filter_str: Filter expression string

    Returns:
        Validated filter string or None

    Raises:
        IcebergInvalidFilterError: If filter syntax is invalid
    """
    if filter_str is None or filter_str.strip() == "":
        return None

    filter_str = filter_str.strip()

    # Basic syntax checks
    # Check for balanced parentheses
    if filter_str.count("(") != filter_str.count(")"):
        raise IcebergInvalidFilterError(
            f"Unbalanced parentheses in filter: {filter_str}"
        )

    # Check for balanced quotes
    single_quotes = filter_str.count("'")
    double_quotes = filter_str.count('"')
    if single_quotes % 2 != 0 and double_quotes % 2 != 0:
        raise IcebergInvalidFilterError(
            f"Unbalanced quotes in filter: {filter_str}"
        )

    return filter_str

