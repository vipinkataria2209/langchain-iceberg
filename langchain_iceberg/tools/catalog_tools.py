"""Catalog exploration tools for Iceberg."""

from typing import Any, Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except (ImportError, AttributeError):
    # Handle NumPy 2.x compatibility issues
    HAS_PANDAS = False
    # Create a dummy pd for type hints
    pd = None

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

    def _run(self, *args: Any, **kwargs: Any) -> str:
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

    def _run(self, namespace: Optional[str] = None, *args: Any, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            # Handle case where agent passes namespace as first positional arg
            if namespace is None and args:
                namespace = args[0]
            if namespace is None:
                namespace = kwargs.get("namespace", "")
            
            # Handle JSON string input from agent
            if isinstance(namespace, str) and namespace.startswith("{") and namespace.endswith("}"):
                import json
                try:
                    parsed = json.loads(namespace)
                    namespace = parsed.get("namespace", namespace)
                except (json.JSONDecodeError, ValueError):
                    pass
            
            if not namespace:
                namespace = "test"  # Default namespace
            namespace = validate_namespace(str(namespace))

            # Handle nested namespaces (convert string to tuple if needed)
            if isinstance(namespace, str) and "." in namespace:
                namespace_tuple = tuple(namespace.split("."))
            else:
                namespace_tuple = (namespace,) if isinstance(namespace, str) else namespace

            try:
                tables = list(self.catalog.list_tables(namespace_tuple))
                # Format table names - extract just the table name from tuple
                namespace_str = ".".join(namespace_tuple) if isinstance(namespace_tuple, tuple) else str(namespace_tuple)
                formatted_tables = []
                for table in tables:
                    if isinstance(table, tuple):
                        # Table tuple may include namespace, extract just the table name (last element)
                        table_name = table[-1] if len(table) > 0 else str(table)
                        formatted_tables.append(f"{namespace_str}.{table_name}")
                    else:
                        formatted_tables.append(f"{namespace_str}.{str(table)}")
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
                    if HAS_PANDAS:
                        sample_data = arrow_table.to_pandas()
                    else:
                        sample_data = arrow_table  # Use PyArrow table directly
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

