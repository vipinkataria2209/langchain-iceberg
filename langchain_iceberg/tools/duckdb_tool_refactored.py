"""DuckDB-based SQL query tool for multi-table operations (refactored)."""

import time
from typing import Any

from langchain_iceberg.exceptions import IcebergInvalidQueryError
from langchain_iceberg.tools.duckdb_base import DuckDBExecutorBase


class DuckDBQueryTool(DuckDBExecutorBase):
    """Execute ad-hoc SQL queries with JOINs using DuckDB + Iceberg extension.
    
    This tool allows users/agents to write custom SQL queries at runtime.
    Useful for exploratory queries and one-off questions.
    """

    name: str = "iceberg_sql_query"
    description: str = """
    Execute SQL queries with JOINs across multiple Iceberg tables using DuckDB.
    
    Use this when you need to:
    - Join 2+ tables
    - Complex aggregations across tables
    - Window functions or CTEs
    - Subqueries
    - Custom queries not covered by pre-defined metrics
    
    Input: 
        sql_query (string): Standard SQL query
        
    Important: 
    - Tables are referenced as namespace.table_name
    - DuckDB reads directly from Iceberg files (no data copying)
    - Predicates are pushed down to file level for performance
    
    Example:
    ```
    iceberg_sql_query(
        sql_query=\"\"\"
            SELECT 
                c.segment,
                COUNT(o.order_id) as order_count,
                SUM(o.amount) as total_revenue
            FROM sales.orders o
            JOIN sales.customers c ON o.customer_id = c.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.segment
            ORDER BY total_revenue DESC
        \"\"\"
    )
    ```
    """

    def __init__(
        self,
        catalog: Any,
        catalog_config: dict,
        query_timeout_seconds: int = 60,
        max_rows_per_query: int = 10000,
        **kwargs: Any,
    ):
        """Initialize DuckDB query tool.
        
        Args:
            catalog: PyIceberg catalog instance
            catalog_config: Catalog configuration dict
            query_timeout_seconds: Maximum query execution time
            max_rows_per_query: Maximum rows to return
            **kwargs: Additional arguments for base class
        """
        super().__init__(catalog=catalog, catalog_config=catalog_config, **kwargs)
        
        # Store query limits as instance attributes
        self._query_timeout_seconds = query_timeout_seconds
        self._max_rows_per_query = max_rows_per_query

    def _run(self, sql_query: str = "", **kwargs: Any) -> str:
        """Execute SQL query.
        
        Args:
            sql_query: SQL query string to execute
            **kwargs: Additional parameters (sql_query may also be in kwargs)
            
        Returns:
            Formatted query results as string
            
        Raises:
            IcebergInvalidQueryError: If query is invalid
            IcebergConnectionError: If execution fails
        """
        start_time = time.time()
        
        # Handle various input formats
        if not sql_query:
            sql_query = kwargs.get("sql_query", "")
        
        # Handle JSON string input (from some LLM frameworks)
        if isinstance(sql_query, str) and sql_query.startswith("{"):
            import json
            try:
                parsed = json.loads(sql_query)
                sql_query = parsed.get("sql_query", sql_query)
            except (json.JSONDecodeError, ValueError):
                pass
        
        # Validate input
        if not sql_query or not isinstance(sql_query, str):
            raise IcebergInvalidQueryError(
                "sql_query parameter is required and must be a string"
            )
        
        # Execute query using base class
        result_df = self._execute_sql_query(
            sql_query=sql_query,
            apply_limit=True,  # Apply LIMIT for safety
            max_rows=self._max_rows_per_query
        )
        
        # Check timeout
        elapsed = time.time() - start_time
        if elapsed > self._query_timeout_seconds:
            raise IcebergInvalidQueryError(
                f"Query timeout exceeded ({self._query_timeout_seconds}s)"
            )
        
        # Apply row limit if needed
        was_truncated = False
        if len(result_df) > self._max_rows_per_query:
            result_df = result_df.head(self._max_rows_per_query)
            was_truncated = True
        
        # Format and return results using base class
        execution_time_ms = elapsed * 1000
        return self._format_results(
            result_df=result_df,
            limit=self._max_rows_per_query,
            include_metadata=True,
            execution_time_ms=execution_time_ms,
            was_truncated=was_truncated
        )
