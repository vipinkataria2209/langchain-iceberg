"""Tests for IcebergToolkit."""

import pytest
from unittest.mock import MagicMock, patch

from langchain_iceberg import IcebergToolkit
from langchain_iceberg.exceptions import IcebergConnectionError


class TestIcebergToolkit:
    """Test cases for IcebergToolkit."""

    def test_init_with_valid_config(self):
        """Test toolkit initialization with valid REST catalog config."""
        with patch("langchain_iceberg.toolkit.load_catalog") as mock_load:
            mock_catalog = MagicMock()
            mock_catalog.list_namespaces.return_value = [("sales",), ("marketing",)]
            mock_load.return_value = mock_catalog

            toolkit = IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://test-warehouse",
                },
            )

            assert toolkit.catalog_name == "test"
            assert toolkit.catalog is not None
            mock_load.assert_called_once()

    def test_init_missing_type(self):
        """Test that missing catalog type raises error."""
        with pytest.raises(IcebergConnectionError) as exc_info:
            IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "warehouse": "s3://test-warehouse",
                },
            )

        assert "type" in str(exc_info.value).lower()

    def test_init_missing_warehouse(self):
        """Test that missing warehouse raises error."""
        with pytest.raises(IcebergConnectionError) as exc_info:
            IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "type": "rest",
                    "uri": "http://localhost:8181",
                },
            )

        assert "warehouse" in str(exc_info.value).lower()

    def test_init_rest_catalog_missing_uri(self):
        """Test that REST catalog requires URI."""
        with pytest.raises(IcebergConnectionError) as exc_info:
            IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "type": "rest",
                    "warehouse": "s3://test-warehouse",
                },
            )

        assert "uri" in str(exc_info.value).lower()

    def test_get_tools(self):
        """Test that get_tools returns list of tools."""
        with patch("langchain_iceberg.toolkit.load_catalog") as mock_load:
            mock_catalog = MagicMock()
            mock_catalog.list_namespaces.return_value = [("sales",)]
            mock_load.return_value = mock_catalog

            toolkit = IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://test-warehouse",
                },
            )

            tools = toolkit.get_tools()
            assert len(tools) >= 4  # At least 4 core tools
            tool_names = [tool.name for tool in tools]
            assert "iceberg_list_namespaces" in tool_names
            assert "iceberg_list_tables" in tool_names
            assert "iceberg_get_schema" in tool_names
            assert "iceberg_query" in tool_names

    def test_get_context(self):
        """Test that get_context returns context dictionary."""
        with patch("langchain_iceberg.toolkit.load_catalog") as mock_load:
            mock_catalog = MagicMock()
            mock_catalog.list_namespaces.return_value = [("sales",), ("marketing",)]
            mock_load.return_value = mock_catalog

            toolkit = IcebergToolkit(
                catalog_name="test",
                catalog_config={
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://test-warehouse",
                },
            )

            context = toolkit.get_context()
            assert context["catalog_name"] == "test"
            assert context["catalog_type"] == "rest"
            assert context["warehouse"] == "s3://test-warehouse"
            assert "namespaces" in context

