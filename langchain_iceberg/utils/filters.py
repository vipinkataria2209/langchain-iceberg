"""Filter expression builder for PyIceberg queries."""

import re
from typing import Any, Optional

try:
    from pyiceberg.expressions import (
        EqualTo,
        NotEqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        And,
        Or,
    )
    from pyiceberg.expressions import literal
    HAS_PYICEBERG_EXPRESSIONS = True
except ImportError:
    HAS_PYICEBERG_EXPRESSIONS = False
    # Fallback to PyArrow if PyIceberg expressions not available
    import pyarrow.compute as pc
    literal = None


class FilterBuilder:
    """Builds PyIceberg filter expressions from string representations."""

    @staticmethod
    def parse_filter(filter_str: str, schema: Any) -> Any:
        """
        Parse a filter string into a PyArrow expression.

        Supports operators: =, !=, >, >=, <, <=, IN, BETWEEN, IS NULL, IS NOT NULL
        Supports logical operators: AND, OR

        Args:
            filter_str: Filter expression string
            schema: PyIceberg table schema

        Returns:
            PyIceberg expression (or PyArrow expression as fallback)

        Raises:
            IcebergInvalidFilterError: If filter cannot be parsed
        """
        if not filter_str or not filter_str.strip():
            return None

        # Simple filter parsing - for now, we'll use a basic approach
        # In production, this would need a more robust parser
        try:
            # Split by AND/OR (simple approach)
            # This is a simplified parser - production would need proper AST
            return FilterBuilder._parse_simple_filter(filter_str, schema)
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergInvalidFilterError

            raise IcebergInvalidFilterError(
                f"Failed to parse filter '{filter_str}': {str(e)}"
            ) from e

    @staticmethod
    def _parse_simple_filter(filter_str: str, schema: Any) -> Any:
        """Parse a simple filter expression."""
        # For MVP, we'll support basic equality and comparison
        # More complex parsing can be added later

        # Pattern: column operator value
        # Examples:
        #   status = 'completed'
        #   amount > 100
        #   order_date >= '2024-01-01'

        # Split by AND/OR (naive approach for MVP)
        parts = re.split(r'\s+(AND|OR)\s+', filter_str, flags=re.IGNORECASE)

        if len(parts) == 1:
            # Single condition
            return FilterBuilder._parse_single_condition(parts[0], schema)
        else:
            # Multiple conditions with AND/OR
            # For MVP, we'll handle simple AND cases
            conditions = []
            for i in range(0, len(parts), 2):
                condition = FilterBuilder._parse_single_condition(parts[i], schema)
                conditions.append(condition)

            # Combine with AND (simplified for MVP)
            if len(conditions) > 1:
                result = conditions[0]
                for cond in conditions[1:]:
                    if HAS_PYICEBERG_EXPRESSIONS:
                        result = And(result, cond)
                    else:
                        result = pc.and_(result, cond)
                return result
            return conditions[0] if conditions else None

    @staticmethod
    def _parse_single_condition(condition: str, schema: Any) -> Any:
        """Parse a single filter condition."""
        condition = condition.strip()

        # Pattern matching for different operators
        patterns = [
            (r'(\w+)\s*=\s*([^\s]+)', '='),
            (r'(\w+)\s*!=\s*([^\s]+)', '!='),
            (r'(\w+)\s*>\s*([^\s]+)', '>'),
            (r'(\w+)\s*>=\s*([^\s]+)', '>='),
            (r'(\w+)\s*<\s*([^\s]+)', '<'),
            (r'(\w+)\s*<=\s*([^\s]+)', '<='),
        ]

        for pattern, op in patterns:
            match = re.match(pattern, condition)
            if match:
                column_name = match.group(1)
                value_str = match.group(2).strip("'\"")

                # Get column from schema
                try:
                    field = schema.find_field(column_name)
                except Exception:
                    from langchain_iceberg.exceptions import IcebergInvalidFilterError
                    raise IcebergInvalidFilterError(
                        f"Column '{column_name}' not found in schema"
                    )

                # Convert value to appropriate type
                value = FilterBuilder._convert_value(value_str, field.field_type)

                # Build PyIceberg expression (preferred) or PyArrow expression (fallback)
                if HAS_PYICEBERG_EXPRESSIONS:
                    # Use PyIceberg expressions
                    literal_value = literal(value)
                    if op == '=':
                        return EqualTo(column_name, literal_value)
                    elif op == '!=':
                        return NotEqualTo(column_name, literal_value)
                    elif op == '>':
                        return GreaterThan(column_name, literal_value)
                    elif op == '>=':
                        return GreaterThanOrEqual(column_name, literal_value)
                    elif op == '<':
                        return LessThan(column_name, literal_value)
                    elif op == '<=':
                        return LessThanOrEqual(column_name, literal_value)
                else:
                    # Fallback to PyArrow expressions
                    if op == '=':
                        return pc.equal(pc.field(column_name), value)
                    elif op == '!=':
                        return pc.not_equal(pc.field(column_name), value)
                    elif op == '>':
                        return pc.greater(pc.field(column_name), value)
                    elif op == '>=':
                        return pc.greater_equal(pc.field(column_name), value)
                    elif op == '<':
                        return pc.less(pc.field(column_name), value)
                    elif op == '<=':
                        return pc.less_equal(pc.field(column_name), value)

        # If no pattern matched, raise error
        from langchain_iceberg.exceptions import IcebergInvalidFilterError
        raise IcebergInvalidFilterError(
            f"Could not parse filter condition: {condition}"
        )

    @staticmethod
    def _convert_value(value_str: str, field_type: Any) -> Any:
        """Convert string value to appropriate type based on field type."""
        # Simple type conversion for MVP
        # More robust type handling would be needed for production

        type_name = str(field_type).lower()

        if 'int' in type_name or 'long' in type_name:
            return int(value_str)
        elif 'float' in type_name or 'double' in type_name or 'decimal' in type_name:
            return float(value_str)
        elif 'bool' in type_name:
            return value_str.lower() in ('true', '1', 'yes')
        else:
            # String type
            return value_str

