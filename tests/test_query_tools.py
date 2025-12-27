"""Tests for query tools."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import pyarrow as pa

from langchain_iceberg.exceptions import (
    IcebergInvalidFilterError,
    IcebergInvalidQueryError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.query_tools import QueryTool


class TestQueryTool:
    """Test cases for QueryTool."""

    def test_init_with_defaults(self):
        """Test QueryTool initialization with default values."""
        mock_catalog = MagicMock()
        tool = QueryTool(catalog=mock_catalog)
        
        assert tool.catalog == mock_catalog
        assert tool.query_timeout_seconds == 60
        assert tool.max_rows_per_query == 10000

    def test_init_with_custom_values(self):
        """Test QueryTool initialization with custom timeout and limit."""
        mock_catalog = MagicMock()
        tool = QueryTool(
            catalog=mock_catalog,
            query_timeout_seconds=120,
            max_rows_per_query=5000
        )
        
        assert tool.query_timeout_seconds == 120
        assert tool.max_rows_per_query == 5000

    def test_query_success_basic(self):
        """Test successful basic query without filters."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        # Mock schema
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, StringType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
            NestedField(2, "customer_id", LongType(), required=True),
            NestedField(3, "status", StringType(), required=False),
        )
        mock_table.schema.return_value = schema
        
        # Mock scan result
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        # Create empty arrow table
        arrow_table = pa.table({
            "order_id": [1, 2],
            "customer_id": [100, 101],
            "status": ["completed", "pending"]
        })
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "limit": 10
        })
        
        assert "order_id" in result
        assert "customer_id" in result
        mock_catalog.load_table.assert_called_once()

    def test_query_with_column_selection(self):
        """Test query with specific column selection."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, StringType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
            NestedField(2, "customer_id", LongType(), required=True),
            NestedField(3, "status", StringType(), required=False),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({
            "order_id": [1, 2],
            "status": ["completed", "pending"]
        })
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "columns": ["order_id", "status"],
            "limit": 10
        })
        
        assert "order_id" in result
        assert "status" in result
        mock_scan_builder.select.assert_called_once_with(["order_id", "status"])

    def test_query_with_invalid_column(self):
        """Test query with invalid column raises error."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergInvalidQueryError) as exc_info:
            tool.run({
                "table_id": "sales.orders",
                "columns": ["order_id", "nonexistent_column"],
                "limit": 10
            })
        
        assert "Invalid columns" in str(exc_info.value)

    def test_query_with_filter(self):
        """Test query with filter expression."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, StringType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
            NestedField(2, "status", StringType(), required=False),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({
            "order_id": [1],
            "status": ["completed"]
        })
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "filters": "status = 'completed'",
            "limit": 10
        })
        
        assert "order_id" in result
        mock_scan_builder.filter.assert_called_once()

    def test_query_table_not_found(self):
        """Test query with non-existent table raises error."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("Table not found in catalog")
        
        tool = QueryTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergTableNotFoundError):
            tool.run({
                "table_id": "sales.nonexistent",
                "limit": 10
            })

    def test_query_invalid_table_id(self):
        """Test query with invalid table_id format."""
        mock_catalog = MagicMock()
        tool = QueryTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergInvalidQueryError):
            tool.run({
                "table_id": "invalid_format",
                "limit": 10
            })

    def test_query_invalid_limit(self):
        """Test query with invalid limit."""
        mock_catalog = MagicMock()
        tool = QueryTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergInvalidQueryError):
            tool.run({
                "table_id": "sales.orders",
                "limit": -1
            })

    def test_query_limit_exceeds_max(self):
        """Test query with limit exceeding max_rows_per_query."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({"order_id": []})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog, max_rows_per_query=100)
        result = tool.run({
            "table_id": "sales.orders",
            "limit": 500  # Should be capped to 100
        })
        
        # Verify limit was capped
        mock_scan_builder.limit.assert_called_with(100)

    def test_query_empty_result(self):
        """Test query returning empty result."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        # Empty arrow table
        arrow_table = pa.table({"order_id": []})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "limit": 10
        })
        
        # Should handle empty result gracefully
        assert isinstance(result, str)

    def test_query_with_invalid_filter(self):
        """Test query with invalid filter expression."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_table.scan.return_value = mock_scan_builder
        
        # Mock filter parsing to raise error
        with patch("langchain_iceberg.tools.query_tools.FilterBuilder") as mock_filter:
            mock_filter.parse_filter.side_effect = Exception("Invalid filter syntax")
            
            mock_catalog.load_table.return_value = mock_table
            tool = QueryTool(catalog=mock_catalog)
            
            with pytest.raises(IcebergInvalidFilterError):
                tool.run({
                    "table_id": "sales.orders",
                    "filters": "invalid filter syntax",
                    "limit": 10
                })

    def test_query_multi_part_namespace(self):
        """Test query with multi-part namespace."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({"order_id": [1]})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = QueryTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "analytics.prod.orders",
            "limit": 10
        })
        
        # Verify namespace was parsed correctly
        mock_catalog.load_table.assert_called_once()
        call_args = mock_catalog.load_table.call_args[0][0]
        assert call_args == ("analytics", "prod", "orders")

