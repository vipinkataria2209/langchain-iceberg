"""DuckDB-based SQL query tool for multi-table operations."""

import re
import time
import warnings
from typing import Any
from pydantic import PrivateAttr

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

try:
    import sqlparse
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False
    sqlparse = None

from langchain_iceberg.exceptions import (
    IcebergConnectionError,
    IcebergInvalidQueryError,
)
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.formatters import ResultFormatter


class DuckDBQueryTool(IcebergBaseTool):
    """Execute SQL queries with JOINs using DuckDB + Iceberg extension."""

    name: str = "iceberg_sql_query"
    description: str = """
    Execute SQL queries with JOINs across multiple Iceberg tables using DuckDB.
    
    Use this when you need to:
    - Join 2+ tables
    - Complex aggregations across tables
    - Window functions or CTEs
    - Subqueries
    
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
    
    _duckdb_conn: Any = PrivateAttr()
    _catalog_config: dict = PrivateAttr()
    _query_timeout_seconds: int = PrivateAttr()
    _max_rows_per_query: int = PrivateAttr()
    _has_iceberg_extension: bool = PrivateAttr()
    _registered_tables: set[str] = PrivateAttr(default_factory=set)

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
        
        if not HAS_DUCKDB:
            raise ImportError(
                "DuckDB is required for SQL queries. "
                "Install with: pip install duckdb>=0.10.0"
            )
        
        object.__setattr__(self, "_query_timeout_seconds", query_timeout_seconds)
        object.__setattr__(self, "_max_rows_per_query", max_rows_per_query)
        object.__setattr__(self, "_catalog_config", catalog_config)
        object.__setattr__(self, "_registered_tables", set())
        
        # Initialize DuckDB connection
        object.__setattr__(self, "_duckdb_conn", self._init_duckdb())

    def _init_duckdb(self):
        """Initialize DuckDB connection with Iceberg extension."""
        conn = duckdb.connect()
        
        # Try to install Iceberg extension
        has_extension = False
        try:
            conn.execute("INSTALL iceberg")
            conn.execute("LOAD iceberg")
            # Enable version guessing for iceberg_scan (needed for local tables)
            conn.execute("SET unsafe_enable_version_guessing = true")
            has_extension = True
        except Exception as e:
            warnings.warn(
                f"DuckDB Iceberg extension not available: {e}. "
                "Will use parquet fallback (may be slower). "
                "Install DuckDB 0.10.0+ for full Iceberg support."
            )
        
        object.__setattr__(self, "_has_iceberg_extension", has_extension)
        
        # Configure cloud storage access
        self._configure_cloud_storage(conn)
        
        return conn

    def _configure_cloud_storage(self, conn):
        """Configure S3/Azure/GCS secrets for DuckDB."""
        # S3 configuration
        if "s3.access-key-id" in self._catalog_config:
            try:
                endpoint = self._catalog_config.get("s3.endpoint", "")
                use_ssl = "true" if not endpoint or "https" in endpoint else "false"
                url_style = "path" if self._catalog_config.get("s3.path-style-access") == "true" else "vhost"
                
                conn.execute(f"""
                    CREATE SECRET IF NOT EXISTS iceberg_s3 (
                        TYPE S3,
                        KEY_ID '{self._catalog_config.get("s3.access-key-id")}',
                        SECRET '{self._catalog_config.get("s3.secret-access-key")}',
                        REGION '{self._catalog_config.get("s3.region", "us-east-1")}',
                        ENDPOINT '{endpoint}',
                        USE_SSL {use_ssl},
                        URL_STYLE '{url_style}'
                    )
                """)
            except Exception as e:
                warnings.warn(f"Failed to configure S3 for DuckDB: {e}")

    def _get_table_location(self, table_id: str) -> tuple[str, str]:
        """Get physical storage location of Iceberg table.
        
        Returns:
            tuple: (table_root_directory, metadata_file_path)
        """
        namespace_parts = table_id.split(".")
        if len(namespace_parts) < 2:
            raise ValueError(
                f"Invalid table_id format: {table_id}. "
                "Expected 'namespace.table' or 'namespace.subnamespace.table'"
            )
        
        namespace = tuple(namespace_parts[:-1])
        table_name = namespace_parts[-1]
        
        try:
            table = self.catalog.load_table((*namespace, table_name))
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to load table '{table_id}' from catalog: {str(e)}"
            ) from e
        
        # Get table location and metadata file
        metadata_file = None
        table_root = None
        
        # Try to get metadata file location (try multiple attributes)
        if hasattr(table, 'metadata') and hasattr(table.metadata, 'metadata_file_location'):
            metadata_file = str(table.metadata.metadata_file_location)
            if "/metadata/" in metadata_file:
                table_root = metadata_file.rsplit("/metadata/", 1)[0]
        elif hasattr(table, 'metadata_location'):
            metadata_loc = table.metadata_location
            metadata_file = str(metadata_loc() if callable(metadata_loc) else metadata_loc)
            if "/metadata/" in metadata_file:
                table_root = metadata_file.rsplit("/metadata/", 1)[0]
        elif hasattr(table, 'location'):
            loc = table.location
            table_root = str(loc() if callable(loc) else loc)
        
        if not table_root:
            raise IcebergConnectionError(
                f"Cannot determine storage location for table '{table_id}'"
            )
        
        # Clean up file:// prefix for local paths
        table_root = table_root.replace("file://", "")
        if metadata_file:
            metadata_file = metadata_file.replace("file://", "")
        
        return (table_root, metadata_file)

    def _register_table(self, table_id: str):
        """Register Iceberg table in DuckDB (zero-copy view)."""
        # Skip if already registered
        if table_id in self._registered_tables:
            return table_id.replace(".", "_")
        
        try:
            table_root, metadata_file = self._get_table_location(table_id)
            view_name = table_id.replace(".", "_")
            
            if self._has_iceberg_extension:
                # Use native Iceberg support (best performance)
                # Use table root directory - DuckDB will find metadata automatically
                # Escape single quotes in path
                escaped_location = table_root.replace("'", "''")
                self._duckdb_conn.execute(f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM iceberg_scan('{escaped_location}')
                """)
            else:
                # Fallback: read parquet files directly
                # Still zero-copy, but may miss some Iceberg metadata
                escaped_location = table_root.replace("'", "''")
                self._duckdb_conn.execute(f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_parquet('{escaped_location}/data/**/*.parquet', 
                                               hive_partitioning=true)
                """)
            
            self._registered_tables.add(table_id)
            return view_name
            
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to register table '{table_id}' in DuckDB: {str(e)}"
            ) from e

    def _extract_table_references(self, sql_query: str) -> list:
        """Extract table references from SQL query."""
        if HAS_SQLPARSE:
            try:
                tables = set()
                
                # Parse SQL
                parsed = sqlparse.parse(sql_query)
                if not parsed:
                    raise ValueError("No SQL statements found")
                
                # Extract FROM/JOIN tokens
                for statement in parsed:
                    # Get all tokens as flat list
                    tokens = list(statement.flatten())
                    
                    # Look for FROM/JOIN keywords followed by identifiers
                    for i, token in enumerate(tokens):
                        if token.ttype is sqlparse.tokens.Keyword:
                            kw = token.value.upper()
                            
                            # Check if it's FROM or a JOIN variant
                            if kw in ('FROM', 'JOIN') or kw.endswith(' JOIN'):
                                # Look ahead for table name
                                for j in range(i + 1, min(i + 5, len(tokens))):
                                    next_token = tokens[j]
                                    
                                    # Skip whitespace
                                    if next_token.is_whitespace:
                                        continue
                                    
                                    # Stop at keywords (except table names)
                                    if next_token.ttype is sqlparse.tokens.Keyword:
                                        break
                                    
                                    # Extract table name
                                    if next_token.ttype in (sqlparse.tokens.Name, None):
                                        table_name = next_token.value.strip()
                                        # Must contain namespace separator
                                        if '.' in table_name and not table_name.startswith('('):
                                            # Remove alias if present
                                            table_name = table_name.split()[0]
                                            tables.add(table_name)
                                        break
                
                if tables:
                    return list(tables)
                
            except Exception as e:
                warnings.warn(f"sqlparse failed: {e}, using regex fallback")
        
        # Regex fallback
        tables = set()
        normalized = re.sub(r'\s+', ' ', sql_query.strip())
        
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]+)',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]+)',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, normalized, re.IGNORECASE):
                table_ref = match.group(1)
                table_name = table_ref.split()[0]
                if '.' in table_name:
                    tables.add(table_name)
        
        return list(tables)

    def _run(self, sql_query: str = "", **kwargs: Any) -> str:
        """Execute SQL query."""
        start_time = time.time()
        
        try:
            # Handle various input formats
            if not sql_query:
                sql_query = kwargs.get("sql_query", "")
            
            if isinstance(sql_query, str) and sql_query.startswith("{"):
                import json
                try:
                    parsed = json.loads(sql_query)
                    sql_query = parsed.get("sql_query", sql_query)
                except (json.JSONDecodeError, ValueError):
                    pass
            
            if not sql_query or not isinstance(sql_query, str):
                raise IcebergInvalidQueryError(
                    "sql_query parameter is required and must be a string"
                )
            
            # Extract and register tables
            table_refs = self._extract_table_references(sql_query)
            
            if not table_refs:
                raise IcebergInvalidQueryError(
                    "No table references found in query. "
                    "Tables must be qualified as 'namespace.table_name'"
                )
            
            # Register each table and build replacement map
            replacements = {}
            for table_id in table_refs:
                view_name = self._register_table(table_id)
                replacements[table_id] = view_name
            
            # Replace table names in SQL
            modified_sql = sql_query
            for table_id, view_name in replacements.items():
                # Use word boundaries to avoid partial replacements
                modified_sql = re.sub(
                    r'\b' + re.escape(table_id) + r'\b',
                    view_name,
                    modified_sql,
                    flags=re.IGNORECASE
                )
            
            # Add LIMIT if not present (for safety)
            sql_upper = modified_sql.upper()
            if "LIMIT" not in sql_upper and "GROUP BY" not in sql_upper:
                modified_sql = f"{modified_sql.rstrip(';')} LIMIT {self._max_rows_per_query}"
            
            # Execute query
            result_df = self._duckdb_conn.execute(modified_sql).fetchdf()
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self._query_timeout_seconds:
                raise IcebergInvalidQueryError(
                    f"Query timeout exceeded ({self._query_timeout_seconds}s)"
                )
            
            # Apply row limit if needed
            if len(result_df) > self._max_rows_per_query:
                result_df = result_df.head(self._max_rows_per_query)
                was_truncated = True
            else:
                was_truncated = False
            
            # Format results
            execution_time = elapsed * 1000
            formatted = ResultFormatter.format_table(result_df, limit=self._max_rows_per_query)
            
            if was_truncated:
                formatted += f"\n(Results truncated to {self._max_rows_per_query} rows)"
            formatted += f"\n(Executed in {execution_time:.0f}ms via DuckDB)"
            
            return formatted

        except (IcebergInvalidQueryError, IcebergConnectionError):
            raise
        except Exception as e:
            raise IcebergConnectionError(
                f"SQL query execution failed: {str(e)}"
            ) from e
