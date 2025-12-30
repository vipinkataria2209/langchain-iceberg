"""Query execution tools for Iceberg."""

import time
from typing import Any, List, Optional

import pandas as pd

from langchain_iceberg.exceptions import (
    IcebergInvalidFilterError,
    IcebergInvalidQueryError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.filters import FilterBuilder
from langchain_iceberg.utils.formatters import ResultFormatter
from langchain_iceberg.utils.validators import validate_filter_expression, validate_table_id


class QueryTool(IcebergBaseTool):
    """Tool for executing queries on Iceberg tables."""

    name: str = "iceberg_query"
    description: str = """
    Execute a query on an Iceberg table using PyIceberg scan API.

    Inputs:
        table_id (required): Format "namespace.table_name"
        columns (optional): List of columns to select (default: all columns)
        filters (optional): Filter expression (e.g., "status = 'completed' AND amount > 100")
        limit (optional): Max rows to return (default: 100)

    Output: Query results as formatted table

    Filter operators supported:
    - =, !=, >, >=, <, <=
    - AND, OR
    - String values should be in quotes: 'value'

    Example usage:
    - iceberg_query(table_id="sales.orders", limit=10)
    - iceberg_query(table_id="sales.orders", columns=["order_id", "amount"], filters="amount > 100")
    - iceberg_query(table_id="sales.orders", filters="status = 'completed' AND amount > 100", limit=50)
    """

    def __init__(
        self,
        catalog: Any,
        query_timeout_seconds: int = 60,
        max_rows_per_query: int = 10000,
        **kwargs: Any,
    ):
        """Initialize query tool with timeout and row limits."""
        super().__init__(catalog=catalog, **kwargs)
        object.__setattr__(self, "query_timeout_seconds", query_timeout_seconds)
        object.__setattr__(self, "max_rows_per_query", max_rows_per_query)

    def _run(
        self,
        table_id: str,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> str:
        """Execute the tool."""
        start_time = time.time()

        try:
            namespace, table_name = validate_table_id(table_id)
            filters = validate_filter_expression(filters)

            # Validate limit
            if limit <= 0:
                raise IcebergInvalidQueryError(f"Limit must be positive, got: {limit}")
            if limit > self.max_rows_per_query:
                limit = self.max_rows_per_query

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

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self.query_timeout_seconds:
                raise IcebergInvalidQueryError(
                    f"Query timeout exceeded ({self.query_timeout_seconds}s)"
                )

            # Build scan with limit
            scan_builder = table.scan()

            # Apply column selection
            if columns:
                # Validate columns exist in schema
                schema = table.schema()
                schema_columns = {field.name for field in schema.fields}
                invalid_columns = set(columns) - schema_columns
                if invalid_columns:
                    raise IcebergInvalidQueryError(
                        f"Invalid columns: {invalid_columns}. "
                        f"Available columns: {sorted(schema_columns)}"
                    )
                scan_builder = scan_builder.select(*columns)

            # Apply filters
            if filters:
                try:
                    filter_expr = FilterBuilder.parse_filter(filters, table.schema())
                    if filter_expr is not None:
                        scan_builder = scan_builder.filter(filter_expr)
                except Exception as e:
                    raise IcebergInvalidFilterError(
                        f"Failed to apply filter '{filters}': {str(e)}"
                    ) from e

            # Execute scan with timeout monitoring
            scan = scan_builder
            arrow_table = scan.to_arrow()

            # Check timeout again after execution
            elapsed = time.time() - start_time
            if elapsed > self.query_timeout_seconds:
                raise IcebergInvalidQueryError(
                    f"Query execution exceeded timeout ({self.query_timeout_seconds}s)"
                )

            # Convert to pandas
            if arrow_table and len(arrow_table) > 0:
                df = arrow_table.to_pandas()
                # Apply limit after fetching (PyIceberg doesn't support limit in scan builder)
                if limit is not None and limit > 0 and len(df) > limit:
                    df = df.head(limit)
            else:
                df = pd.DataFrame()

            # Format results
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            result = ResultFormatter.format_table(df, limit=limit)

            # Add execution time info
            if execution_time > 1000:
                result += f"\n(Executed in {execution_time/1000:.2f}s)"
            else:
                result += f"\n(Executed in {execution_time:.0f}ms)"

            return result

        except (
            IcebergTableNotFoundError,
            IcebergInvalidQueryError,
            IcebergInvalidFilterError,
        ):
            raise
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError

            raise IcebergConnectionError(
                f"Failed to execute query on table '{table_id}': {str(e)}"
            ) from e
