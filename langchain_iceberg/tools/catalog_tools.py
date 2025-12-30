"""Catalog exploration tools for Iceberg."""

from typing import Any, Optional

import pandas as pd

from langchain_iceberg.exceptions import (
    IcebergNamespaceNotFoundError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.formatters import ResultFormatter
from langchain_iceberg.utils.validators import validate_namespace, validate_table_id


class ListNamespacesTool(IcebergBaseTool):
    """Tool for listing all namespaces in the Iceberg catalog."""

    name: str = "iceberg_list_namespaces"
    description: str = """
    Lists all namespaces (databases) available in the Iceberg catalog.
    Use this to discover what data is available before querying.

    Input: None
    Output: List of namespace names

    Example usage:
    - iceberg_list_namespaces()
    """

    def _run(self, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            namespaces = list(self.catalog.list_namespaces())
            # Convert tuple namespaces to dot-separated strings
            formatted_namespaces = [
                ".".join(ns) if isinstance(ns, tuple) else str(ns)
                for ns in namespaces
            ]
            return ResultFormatter.format_list(
                formatted_namespaces, title="Available namespaces"
            )
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError

            raise IcebergConnectionError(
                f"Failed to list namespaces: {str(e)}"
            ) from e


class ListTablesTool(IcebergBaseTool):
    """Tool for listing tables in a namespace."""

    name: str = "iceberg_list_tables"
    description: str = """
    Lists all tables in a specific namespace.

    Input: namespace (string) - The namespace to list tables from
    Output: List of table names

    Example usage:
    - iceberg_list_tables(namespace="sales")
    - iceberg_list_tables(namespace="analytics.prod")
    """

    def _run(self, namespace: str, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            namespace = validate_namespace(namespace)

            # Handle nested namespaces (convert string to tuple if needed)
            if isinstance(namespace, str) and "." in namespace:
                namespace_tuple = tuple(namespace.split("."))
            else:
                namespace_tuple = (namespace,) if isinstance(namespace, str) else namespace

            try:
                tables = list(self.catalog.list_tables(namespace_tuple))
                # Format table names
                formatted_tables = [
                    f"{'.'.join(namespace_tuple)}.{table}"
                    if isinstance(table, str)
                    else f"{'.'.join(namespace_tuple)}.{'.'.join(table)}"
                    for table in tables
                ]
                return ResultFormatter.format_list(
                    formatted_tables, title=f"Tables in namespace '{namespace}'"
                )
            except Exception as e:
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    raise IcebergNamespaceNotFoundError(
                        f"Namespace '{namespace}' not found: {str(e)}"
                    ) from e
                raise
        except IcebergNamespaceNotFoundError:
            raise
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError

            raise IcebergConnectionError(
                f"Failed to list tables in namespace '{namespace}': {str(e)}"
            ) from e


class GetSchemaTool(IcebergBaseTool):
    """Tool for getting table schema information."""

    name: str = "iceberg_get_schema"
    description: str = """
    Get detailed schema information for an Iceberg table including:
    - Column names and types
    - Partition specification
    - Sample rows (first 3)

    Input: table_id (string) - Format: "namespace.table_name"
    Output: Schema details and sample data

    Example usage:
    - iceberg_get_schema(table_id="sales.orders")
    - iceberg_get_schema(table_id="analytics.prod.events")
    """

    def _run(self, table_id: str, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            namespace, table_name = validate_table_id(table_id)

            # Load table
            try:
                namespace_tuple = tuple(namespace.split("."))
                table = self.catalog.load_table((*namespace_tuple, table_name))
            except Exception as e:
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    raise IcebergTableNotFoundError(
                        f"Table '{table_id}' not found: {str(e)}"
                    ) from e
                raise

            # Get schema
            schema = table.schema()

            # Extract column information
            columns = []
            for field in schema.fields:
                columns.append({
                    "name": field.name,
                    "type": str(field.field_type),
                })

            # Get partition specification
            partitions = []
            if table.spec() and table.spec().fields:
                partitions = [field.name for field in table.spec().fields]

            # Get sample data (first 3 rows)
            sample_data = None
            try:
                # Scan table with limit
                scan = table.scan(limit=3)
                arrow_table = scan.to_arrow()
                if arrow_table and len(arrow_table) > 0:
                    sample_data = arrow_table.to_pandas()
            except Exception:
                # If sample data fails, continue without it
                pass

            return ResultFormatter.format_schema(
                columns=columns,
                partitions=partitions if partitions else None,
                sample_data=sample_data,
            )
        except (IcebergTableNotFoundError, IcebergNamespaceNotFoundError):
            raise
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError

            raise IcebergConnectionError(
                f"Failed to get schema for table '{table_id}': {str(e)}"
            ) from e

