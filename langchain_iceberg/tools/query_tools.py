"""Query execution tools for Iceberg."""

import time
from typing import Any, List, Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except (ImportError, AttributeError):
    # Handle NumPy 2.x compatibility issues
    HAS_PANDAS = False
    pd = None

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
        limit (optional): Max rows to return (default: 100). Use None or 0 for no limit (for aggregations)
        aggregation (optional): Aggregation function - "avg", "sum", "count", "min", "max"
        aggregation_column (optional): Column name for aggregation (required for avg, sum, min, max)
        group_by (optional): List of column names to group by (e.g., ["state_code"])
        order_by (optional): Column name to order results by (use with group_by)
        order_direction (optional): "asc" or "desc" for ordering (default: "desc")

    Output: Query results as formatted table, or aggregated value if aggregation is specified

    Filter operators supported:
    - =, !=, >, >=, <, <=
    - AND, OR
    - String values should be in quotes: 'value'

    Aggregation examples:
    - iceberg_query(table_id="epa.daily_summary", filters="parameter_code = '88101'", aggregation="avg", aggregation_column="arithmetic_mean")
    - iceberg_query(table_id="sales.orders", filters="status = 'completed'", aggregation="sum", aggregation_column="amount")
    - iceberg_query(table_id="sales.orders", aggregation="count")
    
    GROUP BY examples:
    - iceberg_query(table_id="epa.daily_summary", filters="parameter_code = '88101'", aggregation="avg", aggregation_column="arithmetic_mean", group_by=["state_code"], order_by="avg_arithmetic_mean", order_direction="desc", limit=3)
    - iceberg_query(table_id="sales.orders", aggregation="sum", aggregation_column="amount", group_by=["customer_id"], order_by="sum_amount", limit=10)

    Regular query examples:
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
        table_id: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: int = 100,
        aggregation: Optional[str] = None,
        aggregation_column: Optional[str] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = "desc",
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """Execute the tool."""
        start_time = time.time()

        try:
            # Handle case where agent passes table_id as first positional arg
            if table_id is None and args:
                table_id = args[0]
            if table_id is None:
                table_id = kwargs.get("table_id", "")
            
            # Handle JSON string input from agent
            if isinstance(table_id, str) and table_id.startswith("{") and table_id.endswith("}"):
                import json
                try:
                    parsed = json.loads(table_id)
                    table_id = parsed.get("table_id", table_id)
                    if columns is None:
                        columns = parsed.get("columns")
                    if filters is None:
                        filters = parsed.get("filters")
                    if limit == 100:
                        limit = parsed.get("limit", 100)
                except:
                    pass
            
            # Also get other params from kwargs if not provided
            if columns is None:
                columns = kwargs.get("columns")
            if filters is None:
                filters = kwargs.get("filters")
            if limit == 100:
                limit = kwargs.get("limit", 100)
            if aggregation is None:
                aggregation = kwargs.get("aggregation")
            if aggregation_column is None:
                aggregation_column = kwargs.get("aggregation_column")
            if group_by is None:
                group_by = kwargs.get("group_by")
            if order_by is None:
                order_by = kwargs.get("order_by")
            if order_direction == "desc":
                order_direction = kwargs.get("order_direction", "desc")
            
            namespace, table_name = validate_table_id(table_id)
            filters = validate_filter_expression(filters)

            # Validate aggregation parameters
            if aggregation:
                aggregation = aggregation.lower()
                valid_aggregations = ["avg", "sum", "count", "min", "max"]
                if aggregation not in valid_aggregations:
                    raise IcebergInvalidQueryError(
                        f"Invalid aggregation '{aggregation}'. Must be one of: {valid_aggregations}"
                    )
                if aggregation in ["avg", "sum", "min", "max"] and not aggregation_column:
                    raise IcebergInvalidQueryError(
                        f"Aggregation '{aggregation}' requires 'aggregation_column' parameter"
                    )
                # For aggregations, remove limit to get all data (unless group_by is used)
                if limit == 100 and not group_by:  # Default limit, no group_by
                    limit = None  # No limit for simple aggregations
            
            # Validate group_by parameters
            if group_by:
                if not aggregation:
                    raise IcebergInvalidQueryError(
                        "group_by requires an aggregation function"
                    )
                if not isinstance(group_by, list):
                    group_by = [group_by]  # Convert single string to list
                # For group_by, we'll apply limit after grouping
                if limit == 100:  # Default limit
                    limit = 100  # Keep default for grouped results

            # Validate limit
            if limit is not None:
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
                if HAS_PANDAS:
                    df = arrow_table.to_pandas()
                else:
                    # Fallback: use PyArrow directly
                    import pyarrow as pa
                    df = arrow_table
            else:
                if HAS_PANDAS:
                    df = pd.DataFrame()
                else:
                    import pyarrow as pa
                    df = pa.Table.from_arrays([], names=[])

            # Handle aggregations
            if aggregation:
                if not HAS_PANDAS:
                    raise IcebergInvalidQueryError(
                        "Aggregations require pandas. Install pandas to use aggregation functions."
                    )
                
                # Handle GROUP BY aggregations
                if group_by:
                    # Validate group_by columns exist
                    schema = table.schema()
                    schema_columns = {field.name for field in schema.fields}
                    invalid_group_cols = set(group_by) - schema_columns
                    if invalid_group_cols:
                        raise IcebergInvalidQueryError(
                            f"Invalid group_by columns: {invalid_group_cols}. "
                            f"Available columns: {sorted(schema_columns)}"
                        )
                    
                    # Perform grouped aggregation
                    grouped = df.groupby(group_by)
                    
                    if aggregation == "count":
                        if aggregation_column and aggregation_column in df.columns:
                            result_df = grouped[aggregation_column].count().reset_index()
                            result_df.columns = group_by + [f"count_{aggregation_column}"]
                        else:
                            result_df = grouped.size().reset_index(name="count")
                    elif aggregation == "sum":
                        if aggregation_column not in df.columns:
                            raise IcebergInvalidQueryError(
                                f"Column '{aggregation_column}' not found for sum aggregation"
                            )
                        result_df = grouped[aggregation_column].sum().reset_index()
                        result_df.columns = group_by + [f"sum_{aggregation_column}"]
                    elif aggregation == "avg":
                        if aggregation_column not in df.columns:
                            raise IcebergInvalidQueryError(
                                f"Column '{aggregation_column}' not found for avg aggregation"
                            )
                        result_df = grouped[aggregation_column].mean().reset_index()
                        result_df.columns = group_by + [f"avg_{aggregation_column}"]
                    elif aggregation == "min":
                        if aggregation_column not in df.columns:
                            raise IcebergInvalidQueryError(
                                f"Column '{aggregation_column}' not found for min aggregation"
                            )
                        result_df = grouped[aggregation_column].min().reset_index()
                        result_df.columns = group_by + [f"min_{aggregation_column}"]
                    elif aggregation == "max":
                        if aggregation_column not in df.columns:
                            raise IcebergInvalidQueryError(
                                f"Column '{aggregation_column}' not found for max aggregation"
                            )
                        result_df = grouped[aggregation_column].max().reset_index()
                        result_df.columns = group_by + [f"max_{aggregation_column}"]
                    
                    # Apply ordering if specified
                    if order_by:
                        if order_by not in result_df.columns:
                            # Try to find the aggregation column
                            agg_col = next((c for c in result_df.columns if c not in group_by), None)
                            if agg_col:
                                order_by = agg_col
                        if order_by in result_df.columns:
                            ascending = order_direction.lower() != "desc"
                            result_df = result_df.sort_values(by=order_by, ascending=ascending)
                    
                    # Apply limit
                    if limit and limit > 0 and len(result_df) > limit:
                        result_df = result_df.head(limit)
                    
                    # Format grouped results
                    execution_time = (time.time() - start_time) * 1000
                    result = ResultFormatter.format_table(result_df, limit=limit)
                    if execution_time > 1000:
                        result += f"\n(Executed in {execution_time/1000:.2f}s)"
                    else:
                        result += f"\n(Executed in {execution_time:.0f}ms)"
                    
                    return result
                
                # Simple aggregation (no group_by)
                if aggregation == "count":
                    if aggregation_column and aggregation_column in df.columns:
                        result_value = df[aggregation_column].count()
                    else:
                        result_value = len(df)
                elif aggregation == "sum":
                    if aggregation_column not in df.columns:
                        raise IcebergInvalidQueryError(
                            f"Column '{aggregation_column}' not found for sum aggregation"
                        )
                    result_value = float(df[aggregation_column].sum())
                elif aggregation == "avg":
                    if aggregation_column not in df.columns:
                        raise IcebergInvalidQueryError(
                            f"Column '{aggregation_column}' not found for avg aggregation"
                        )
                    result_value = float(df[aggregation_column].mean())
                elif aggregation == "min":
                    if aggregation_column not in df.columns:
                        raise IcebergInvalidQueryError(
                            f"Column '{aggregation_column}' not found for min aggregation"
                        )
                    result_value = float(df[aggregation_column].min())
                elif aggregation == "max":
                    if aggregation_column not in df.columns:
                        raise IcebergInvalidQueryError(
                            f"Column '{aggregation_column}' not found for max aggregation"
                        )
                    result_value = float(df[aggregation_column].max())
                
                execution_time = (time.time() - start_time) * 1000  # Convert to ms
                result = f"{aggregation.upper()}({aggregation_column if aggregation_column else 'rows'}): {result_value:.3f}"
                if execution_time > 1000:
                    result += f"\n(Executed in {execution_time/1000:.2f}s)"
                else:
                    result += f"\n(Executed in {execution_time:.0f}ms)"
                
                return result

            # Regular query - apply limit after fetching
            if HAS_PANDAS:
                if limit is not None and limit > 0 and len(df) > limit:
                    df = df.head(limit)
            else:
                import pyarrow as pa
                if limit is not None and limit > 0:
                    df = arrow_table.slice(0, limit)

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
