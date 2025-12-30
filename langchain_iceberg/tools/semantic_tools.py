"""Auto-generated semantic tools from YAML metrics."""

from typing import Any, List, Optional

import pandas as pd

from langchain_iceberg.exceptions import IcebergInvalidQueryError, IcebergTableNotFoundError
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.date_parser import DateRangeParser
from langchain_iceberg.utils.filters import FilterBuilder
from langchain_iceberg.utils.formatters import ResultFormatter
from langchain_iceberg.utils.validators import validate_filter_expression


class MetricTool(IcebergBaseTool):
    """Base class for auto-generated metric tools."""

    def __init__(
        self,
        catalog: Any,
        metric_config: dict,
        semantic_config: dict,
        **kwargs: Any,
    ):
        """Initialize metric tool with metric configuration."""
        metric_name = metric_config["name"]

        # Set name and description before calling super()
        tool_name = f"get_{metric_name}"
        tool_description = self._build_description(metric_config)

        # Initialize with name and description
        super().__init__(
            catalog=catalog,
            name=tool_name,
            description=tool_description,
            **kwargs
        )

        # Store configs as instance variables (not Pydantic fields)
        object.__setattr__(self, "metric_config", metric_config)
        object.__setattr__(self, "semantic_config", semantic_config)
        object.__setattr__(self, "metric_name", metric_name)

    def _build_description(self, metric_config: dict) -> str:
        """Build tool description from metric config."""
        metric_name = metric_config.get("name", "metric")
        desc = metric_config.get("description", f"Get {metric_name}")

        # Add parameter descriptions
        desc += "\n\nInputs:"
        desc += "\n  date_range (optional): Date range (e.g., 'Q4_2024', 'last_30_days', '2024-01-01:2024-12-31')"
        desc += "\n  group_by (optional): Dimension to group by (e.g., 'customer_segment')"

        return desc

    def _run(
        self,
        date_range: Optional[str] = None,
        group_by: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Execute the metric calculation."""
        try:
            # Get expression or formula
            if "expression" in self.metric_config:
                return self._execute_expression(date_range, group_by)
            elif "formula" in self.metric_config:
                # For calculated metrics with SQL formula, we'd need SQL execution
                # For now, return a message
                return f"Calculated metric '{self.metric_name}' requires SQL execution (not yet implemented)"
            else:
                raise IcebergInvalidQueryError(
                    f"Metric '{self.metric_name}' has no expression or formula"
                )
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError
            raise IcebergConnectionError(
                f"Failed to execute metric '{self.metric_name}': {str(e)}"
            ) from e

    def _execute_expression(
        self, date_range: Optional[str], group_by: Optional[str]
    ) -> str:
        """Execute metric using expression definition."""
        expr = self.metric_config["expression"]
        table_name = expr["table"]
        namespace = self._find_table_namespace(table_name)
        table_id = f"{namespace}.{table_name}"

        # Load table
        try:
            namespace_tuple = tuple(namespace.split("."))
            table = self.catalog.load_table((*namespace_tuple, table_name))
        except Exception as e:
            raise IcebergTableNotFoundError(
                f"Table '{table_id}' not found: {str(e)}"
            ) from e

        # Build scan
        scan_builder = table.scan()

        # Apply filters from expression
        if "filters" in expr:
            filters = self._build_filters_from_config(expr["filters"], table)
            if filters:
                scan_builder = scan_builder.filter(filters)

        # Apply date range filter
        if date_range:
            date_column = self._find_date_column(table, expr)
            if date_column:
                start, end = DateRangeParser.parse(date_range)
                date_filter = DateRangeParser.format_for_filter(start, end, date_column)
                filter_expr = FilterBuilder.parse_filter(date_filter, table.schema())
                if filter_expr:
                    scan_builder = scan_builder.filter(filter_expr)

        # Execute scan
        scan = scan_builder
        arrow_table = scan.to_arrow()

        if arrow_table and len(arrow_table) > 0:
            df = arrow_table.to_pandas()
        else:
            df = pd.DataFrame()

        # Calculate aggregation
        aggregation = expr["aggregation"]
        column = expr.get("column")

        if aggregation == "count":
            if column:
                result = df[column].count() if column in df.columns else len(df)
            else:
                result = len(df)
        elif aggregation == "sum":
            if not column:
                raise IcebergInvalidQueryError("Sum aggregation requires column")
            result = df[column].sum() if column in df.columns else 0
        elif aggregation == "avg":
            if not column:
                raise IcebergInvalidQueryError("Avg aggregation requires column")
            result = df[column].mean() if column in df.columns else 0
        elif aggregation == "min":
            if not column:
                raise IcebergInvalidQueryError("Min aggregation requires column")
            result = df[column].min() if column in df.columns else None
        elif aggregation == "max":
            if not column:
                raise IcebergInvalidQueryError("Max aggregation requires column")
            result = df[column].max() if column in df.columns else None
        else:
            raise IcebergInvalidQueryError(f"Unsupported aggregation: {aggregation}")

        # Format result
        unit = self.metric_config.get("unit", "")
        format_str = self.metric_config.get("format", "{:,.2f}")

        if result is None:
            return f"No data found for metric '{self.metric_name}'"

        try:
            formatted_result = format_str.format(result)
        except Exception:
            formatted_result = str(result)

        output = f"{self.metric_config.get('description', self.metric_name)}: {formatted_result}"
        if unit:
            output += f" {unit}"

        if date_range:
            output += f" (date_range: {date_range})"

        return output

    def _find_table_namespace(self, table_name: str) -> str:
        """Find namespace for a table from semantic config."""
        tables = self.semantic_config.get("tables", [])
        for table in tables:
            if table.get("name") == table_name:
                return table.get("namespace", "default")
        # Default namespace if not found
        return "default"

    def _find_date_column(self, table: Any, expr: dict) -> Optional[str]:
        """Find date column in table schema."""
        schema = table.schema()
        for field in schema.fields:
            field_type = str(field.field_type).lower()
            if "timestamp" in field_type or "date" in field_type:
                return field.name

        # Check if expression specifies a date column
        if "date_column" in expr:
            return expr["date_column"]

        return None

    def _build_filters_from_config(self, filters_config: List[dict], table: Any) -> Any:
        """Build PyArrow filter from YAML filter configuration."""
        if not filters_config:
            return None

        filter_parts = []
        for filter_item in filters_config:
            column = filter_item.get("column")
            operator = filter_item.get("operator", "==")
            value = filter_item.get("value")

            if not column:
                continue

            # Build filter string
            if operator == "==":
                filter_str = f"{column} = '{value}'"
            elif operator == "!=":
                filter_str = f"{column} != '{value}'"
            elif operator == ">":
                filter_str = f"{column} > {value}"
            elif operator == ">=":
                filter_str = f"{column} >= {value}"
            elif operator == "<":
                filter_str = f"{column} < {value}"
            elif operator == "<=":
                filter_str = f"{column} <= {value}"
            else:
                continue

            try:
                filter_expr = FilterBuilder.parse_filter(filter_str, table.schema())
                if filter_expr:
                    filter_parts.append(filter_expr)
            except Exception:
                # Skip invalid filters
                pass

        # Combine filters with AND
        if not filter_parts:
            return None

        result = filter_parts[0]
        for f in filter_parts[1:]:
            import pyarrow.compute as pc
            result = pc.and_(result, f)

        return result


class MetricToolGenerator:
    """Generates metric tools from semantic YAML configuration."""

    @staticmethod
    def generate_tools(
        catalog: Any, semantic_config: dict
    ) -> List[MetricTool]:
        """
        Generate metric tools from semantic configuration.

        Args:
            catalog: PyIceberg catalog instance
            semantic_config: Parsed semantic YAML configuration

        Returns:
            List of MetricTool instances
        """
        tools = []
        metrics = semantic_config.get("metrics", [])

        for metric_config in metrics:
            tool = MetricTool(
                catalog=catalog,
                metric_config=metric_config,
                semantic_config=semantic_config,
            )
            tools.append(tool)

        return tools

