"""Tests for catalog tools."""

import pytest
from unittest.mock import MagicMock, patch

from langchain_iceberg.exceptions import (
    IcebergConnectionError,
    IcebergNamespaceNotFoundError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.catalog_tools import (
    GetSchemaTool,
    ListNamespacesTool,
    ListTablesTool,
)


class TestListNamespacesTool:
    """Test cases for ListNamespacesTool."""

    def test_list_namespaces_success(self):
        """Test successful namespace listing."""
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("sales",), ("marketing",), ("finance",)]

        tool = ListNamespacesTool(catalog=mock_catalog)
        result = tool.run({})

        assert "sales" in result
        assert "marketing" in result
        assert "finance" in result

    def test_list_namespaces_empty(self):
        """Test listing when no namespaces exist."""
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = []

        tool = ListNamespacesTool(catalog=mock_catalog)
        result = tool.run({})

        assert "empty" in result.lower() or len(result) > 0

    def test_list_namespaces_error(self):
        """Test error handling when catalog fails."""
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.side_effect = Exception("Connection failed")

        tool = ListNamespacesTool(catalog=mock_catalog)

        with pytest.raises(IcebergConnectionError):
            tool.run({})


class TestListTablesTool:
    """Test cases for ListTablesTool."""

    def test_list_tables_success(self):
        """Test successful table listing."""
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = ["orders", "customers", "products"]

        tool = ListTablesTool(catalog=mock_catalog)
        result = tool.run({"namespace": "sales"})

        assert "orders" in result
        assert "customers" in result
        assert "products" in result

    def test_list_tables_namespace_not_found(self):
        """Test error when namespace doesn't exist."""
        mock_catalog = MagicMock()
        mock_catalog.list_tables.side_effect = Exception("Namespace not found")

        tool = ListTablesTool(catalog=mock_catalog)

        with pytest.raises(IcebergNamespaceNotFoundError):
            tool.run({"namespace": "nonexistent"})

    def test_list_tables_invalid_namespace(self):
        """Test error with invalid namespace."""
        mock_catalog = MagicMock()

        tool = ListTablesTool(catalog=mock_catalog)

        with pytest.raises(IcebergNamespaceNotFoundError):
            tool.run({"namespace": ""})


class TestGetSchemaTool:
    """Test cases for GetSchemaTool."""

    def test_get_schema_success(self):
        """Test successful schema retrieval."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, LongType

        mock_catalog = MagicMock()
        mock_table = MagicMock()

        # Create a mock schema
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
            NestedField(2, "customer_id", LongType(), required=True),
            NestedField(3, "status", StringType(), required=False),
        )
        mock_table.schema.return_value = schema
        mock_table.spec.return_value = MagicMock(fields=[])
        mock_table.scan.return_value.to_arrow.return_value = None

        mock_catalog.load_table.return_value = mock_table

        tool = GetSchemaTool(catalog=mock_catalog)
        result = tool.run({"table_id": "sales.orders"})

        assert "order_id" in result
        assert "customer_id" in result
        assert "status" in result

    def test_get_schema_table_not_found(self):
        """Test error when table doesn't exist."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("Table not found")

        tool = GetSchemaTool(catalog=mock_catalog)

        with pytest.raises(IcebergTableNotFoundError):
            tool.run({"table_id": "sales.nonexistent"})

    def test_get_schema_invalid_table_id(self):
        """Test error with invalid table_id format."""
        mock_catalog = MagicMock()

        tool = GetSchemaTool(catalog=mock_catalog)

        with pytest.raises(Exception):  # Should raise validation error
            tool.run({"table_id": "invalid"})

