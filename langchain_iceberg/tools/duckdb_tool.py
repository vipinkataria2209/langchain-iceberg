"""DuckDB-based SQL query tool for multi-table operations."""

import time
from typing import Any, Optional

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    duckdb = None

try:
    import pandas as pd
    HAS_PANDAS = True
except (ImportError, AttributeError):
    HAS_PANDAS = False
    pd = None

from langchain_iceberg.exceptions import IcebergConnectionError, IcebergInvalidQueryError
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.formatters import ResultFormatter


class DuckDBQueryTool(IcebergBaseTool):
    """Execute SQL queries with JOINs using DuckDB + Iceberg extension."""

    name: str = "iceberg_sql_query"
    description: str = """
    Execute SQL queries with JOINs across multiple Iceberg tables.
    Use this when you need to:
    - Join 2+ tables
    - Complex aggregations across tables
    - Window functions or CTEs
    - Subqueries
    
    Input: 
        sql_query (string): Standard SQL query
        
    The tool automatically handles Iceberg table registration.
    
    Example:
    - iceberg_sql_query(sql_query="SELECT o.order_id, c.name, o.amount FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.customer_id WHERE o.amount > 100 LIMIT 10")
    """

    def __init__(
        self,
        catalog: Any,
        catalog_config: dict,
        query_timeout_seconds: int = 60,
        max_rows_per_query: int = 10000,
        **kwargs: Any,
    ):
        """Initialize DuckDB query tool."""
        super().__init__(catalog=catalog, **kwargs)
        object.__setattr__(self, "query_timeout_seconds", query_timeout_seconds)
        object.__setattr__(self, "max_rows_per_query", max_rows_per_query)
        object.__setattr__(self, "catalog_config", catalog_config)
        
        if not HAS_DUCKDB:
            raise ImportError(
                "DuckDB is required for SQL queries. Install with: pip install duckdb"
            )
        
        # Initialize DuckDB connection
        self.duckdb_conn = self._init_duckdb()

    def _init_duckdb(self):
        """Initialize DuckDB connection with Iceberg extension."""
        conn = duckdb.connect()
        
        # Install and load Iceberg extension
        try:
            conn.execute("INSTALL iceberg")
            conn.execute("LOAD iceberg")
        except Exception as e:
            # If extension not available, we'll use a fallback approach
            # Register tables manually from catalog
            pass
        
        # Configure catalog access based on catalog type
        catalog_type = self.catalog_config.get("type", "sql")
        warehouse = self.catalog_config.get("warehouse", "")
        
        # For SQL catalog, we can register tables directly
        # For REST/Hive catalogs, we'd need to configure secrets
        # For now, we'll use a simpler approach: register tables on-demand
        
        return conn

    def _register_table(self, table_id: str):
        """Register an Iceberg table in DuckDB."""
        try:
            # Load table from catalog
            namespace_parts = table_id.split(".")
            if len(namespace_parts) < 2:
                raise ValueError(f"Invalid table_id format: {table_id}. Expected 'namespace.table'")
            
            namespace = tuple(namespace_parts[:-1])
            table_name = namespace_parts[-1]
            
            table = self.catalog.load_table((*namespace, table_name))
            
            # Get table location/URI
            # For now, we'll scan the table and register it as a view
            # This is not optimal but works for small-medium datasets
            scan = table.scan()
            arrow_table = scan.to_arrow()
            
            if HAS_PANDAS:
                df = arrow_table.to_pandas()
            else:
                # Convert PyArrow to DuckDB directly
                df = arrow_table
            
            # Register as temporary view
            view_name = table_id.replace(".", "_")
            self.duckdb_conn.register(view_name, df)
            
            return view_name
            
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to register table '{table_id}' in DuckDB: {str(e)}"
            ) from e

    def _extract_table_references(self, sql_query: str) -> list:
        """Extract table references from SQL query."""
        import re
        
        # Simple regex to find table references
        # This is basic - could be improved with SQL parser
        table_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)\b|\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)\b)'
        matches = re.findall(table_pattern, sql_query, re.IGNORECASE)
        
        tables = []
        for match in matches:
            table_ref = match[0] or match[1]
            # Remove aliases (everything after space)
            table_name = table_ref.split()[0]
            if "." in table_name:
                tables.append(table_name)
        
        return list(set(tables))

    def _run(self, sql_query: str, *args: Any, **kwargs: Any) -> str:
        """Execute SQL query."""
        start_time = time.time()
        
        try:
            # Handle JSON string input from agent
            if isinstance(sql_query, str) and sql_query.startswith("{") and sql_query.endswith("}"):
                import json
                try:
                    parsed = json.loads(sql_query)
                    sql_query = parsed.get("sql_query", sql_query)
                except (json.JSONDecodeError, ValueError):
                    pass
            
            # Also get from kwargs if not provided
            if not sql_query:
                sql_query = kwargs.get("sql_query", "")
            
            if not sql_query:
                raise IcebergInvalidQueryError("sql_query parameter is required")
            
            # Extract table references and register them
            table_refs = self._extract_table_references(sql_query)
            registered_views = {}
            
            for table_id in table_refs:
                view_name = self._register_table(table_id)
                registered_views[table_id] = view_name
                # Replace table references in SQL with view names
                sql_query = sql_query.replace(table_id, view_name)
            
            # Add LIMIT if not present and query doesn't have aggregation
            sql_upper = sql_query.upper()
            if "LIMIT" not in sql_upper and "GROUP BY" not in sql_upper:
                sql_query = f"{sql_query.rstrip(';')} LIMIT {self.max_rows_per_query}"
            
            # Execute query
            try:
                result_df = self.duckdb_conn.execute(sql_query).fetchdf()
            except Exception as e:
                # If query failed, try with original table names (might be using different syntax)
                # Revert view name replacements
                for table_id, view_name in registered_views.items():
                    sql_query = sql_query.replace(view_name, table_id)
                raise IcebergInvalidQueryError(f"SQL query execution failed: {str(e)}") from e
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self.query_timeout_seconds:
                raise IcebergInvalidQueryError(
                    f"Query timeout exceeded ({self.query_timeout_seconds}s)"
                )
            
            # Apply limit if needed
            if HAS_PANDAS and len(result_df) > self.max_rows_per_query:
                result_df = result_df.head(self.max_rows_per_query)
            
            # Format results
            execution_time = elapsed * 1000
            formatted = ResultFormatter.format_table(result_df, limit=self.max_rows_per_query)
            formatted += f"\n(Executed in {execution_time:.0f}ms via DuckDB)"
            
            return formatted

        except (IcebergInvalidQueryError, IcebergConnectionError):
            raise
        except Exception as e:
            raise IcebergConnectionError(f"SQL query failed: {str(e)}") from e

