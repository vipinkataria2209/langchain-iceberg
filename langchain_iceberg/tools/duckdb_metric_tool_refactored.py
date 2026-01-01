"""DuckDB-based metric tool for SQL formula metrics from YAML (refactored)."""

import re
import warnings
from typing import Any, Optional

from langchain_iceberg.exceptions import IcebergInvalidQueryError
from langchain_iceberg.tools.duckdb_base import DuckDBExecutorBase


class DuckDBMetricTool(DuckDBExecutorBase):
    """Execute SQL formula metrics from YAML using DuckDB.
    
    This tool executes pre-defined SQL metrics stored in YAML configuration.
    Useful for consistent, reusable business calculations.
    """

    def __init__(
        self,
        catalog: Any,
        catalog_config: dict,
        metric_config: dict,
        semantic_config: dict,
        **kwargs: Any,
    ):
        """Initialize DuckDB metric tool.
        
        Args:
            catalog: PyIceberg catalog instance
            catalog_config: Catalog configuration dict
            metric_config: Metric definition from YAML
            semantic_config: Full semantic YAML configuration
            **kwargs: Additional arguments for base class
        """
        metric_name = metric_config["name"]
        tool_name = f"get_{metric_name}"
        tool_description = self._build_description(metric_config)
        
        # Initialize base with dynamic name and description
        super().__init__(
            catalog=catalog,
            catalog_config=catalog_config,
            name=tool_name,
            description=tool_description,
            **kwargs
        )
        
        # Store metric-specific configs as instance variables (not Pydantic fields)
        object.__setattr__(self, "_metric_config", metric_config)
        object.__setattr__(self, "_semantic_config", semantic_config)
        object.__setattr__(self, "_metric_name", metric_name)

    def _build_description(self, metric_config: dict) -> str:
        """Build tool description from metric config.
        
        Args:
            metric_config: Metric definition dictionary
            
        Returns:
            Tool description string
        """
        desc = metric_config.get("description", f"Get {metric_config['name']}")
        desc += "\n\nThis metric uses a pre-defined SQL formula for calculation."
        desc += "\n\nInputs:"
        desc += "\n  date_range (optional): Date range filter (e.g., 'Q4_2024', 'last_30_days', '2024-01-01:2024-12-31')"
        
        # Add tags if available
        if "tags" in metric_config:
            desc += f"\n\nTags: {', '.join(metric_config['tags'])}"
        
        return desc

    def _inject_date_filter(self, sql: str, date_range: str) -> str:
        """Inject date range filter into SQL query.
        
        Args:
            sql: Original SQL query
            date_range: Date range string (e.g., 'Q4_2024', 'last_30_days')
            
        Returns:
            Modified SQL with date filter injected
        """
        # Parse date range
        from langchain_iceberg.utils.date_parser import DateRangeParser
        start_date, end_date = DateRangeParser.parse(date_range)
        
        # Find date column from metric config or guess from SQL
        date_column = self._metric_config.get("date_column")
        if not date_column:
            # Try to find common date columns in SQL
            sql_lower = sql.lower()
            for candidate in ["order_date", "created_at", "timestamp", "date", "event_date"]:
                if candidate in sql_lower:
                    date_column = candidate
                    break
        
        if not date_column:
            warnings.warn(
                f"No date column found for metric '{self._metric_name}', "
                "date_range filter not applied"
            )
            return sql
        
        # Build date filter expression
        date_filter = f"{date_column} >= '{start_date}' AND {date_column} < '{end_date}'"
        
        # Check if WHERE clause exists
        if re.search(r'\bWHERE\b', sql, re.IGNORECASE):
            # Add to existing WHERE clause with AND
            sql = re.sub(
                r'(\bWHERE\b)',
                rf'\1 ({date_filter}) AND',
                sql,
                count=1,
                flags=re.IGNORECASE
            )
        else:
            # Add new WHERE clause before GROUP BY / ORDER BY / LIMIT / HAVING
            inserted = False
            for keyword in ['GROUP BY', 'ORDER BY', 'LIMIT', 'HAVING']:
                pattern = rf'\b{keyword}\b'
                if re.search(pattern, sql, re.IGNORECASE):
                    sql = re.sub(
                        pattern,
                        f'WHERE {date_filter} {keyword}',
                        sql,
                        count=1,
                        flags=re.IGNORECASE
                    )
                    inserted = True
                    break
            
            # No GROUP BY/ORDER BY/LIMIT/HAVING found, add at end
            if not inserted:
                sql = sql.rstrip().rstrip(';')
                sql += f' WHERE {date_filter}'
        
        return sql

    def _run(
        self,
        date_range: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Execute SQL formula metric.
        
        Args:
            date_range: Optional date range filter
            **kwargs: Additional parameters (ignored)
            
        Returns:
            Formatted query results as string
            
        Raises:
            IcebergInvalidQueryError: If metric has no formula
            IcebergConnectionError: If execution fails
        """
        # Get SQL formula from metric config
        sql = self._metric_config.get("formula")
        if not sql:
            raise IcebergInvalidQueryError(
                f"Metric '{self._metric_name}' has no formula defined"
            )
        
        # Inject date filter if provided
        if date_range:
            sql = self._inject_date_filter(sql, date_range)
        
        # Execute query using base class
        result_df = self._execute_sql_query(
            sql_query=sql,
            apply_limit=False,  # Trust YAML SQL (no auto-limit)
            max_rows=None
        )
        
        # Format results using base class
        formatted = self._format_results(
            result_df=result_df,
            limit=10000,  # Display limit
            include_metadata=False  # No execution time for metrics
        )
        
        # Add date range info if provided
        if date_range:
            formatted += f"\n(date_range: {date_range})"
        
        return formatted
