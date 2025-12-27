"""Tests for validation utilities."""

import pytest

from langchain_iceberg.exceptions import (
    IcebergInvalidQueryError,
    IcebergNamespaceNotFoundError,
)
from langchain_iceberg.utils.validators import (
    validate_filter_expression,
    validate_namespace,
    validate_table_id,
)


class TestValidateTableId:
    """Test cases for validate_table_id."""

    def test_valid_table_id(self):
        """Test valid table_id formats."""
        namespace, table = validate_table_id("sales.orders")
        assert namespace == "sales"
        assert table == "orders"

        namespace, table = validate_table_id("analytics.prod.events")
        assert namespace == "analytics.prod"
        assert table == "events"

    def test_invalid_table_id_no_dot(self):
        """Test invalid table_id without dot."""
        with pytest.raises(IcebergInvalidQueryError):
            validate_table_id("orders")

    def test_invalid_table_id_empty(self):
        """Test invalid empty table_id."""
        with pytest.raises(IcebergInvalidQueryError):
            validate_table_id("")

    def test_invalid_table_id_none(self):
        """Test invalid None table_id."""
        with pytest.raises(IcebergInvalidQueryError):
            validate_table_id(None)


class TestValidateNamespace:
    """Test cases for validate_namespace."""

    def test_valid_namespace(self):
        """Test valid namespace formats."""
        assert validate_namespace("sales") == "sales"
        assert validate_namespace("analytics.prod") == "analytics.prod"

    def test_invalid_namespace_empty(self):
        """Test invalid empty namespace."""
        with pytest.raises(IcebergNamespaceNotFoundError):
            validate_namespace("")

    def test_invalid_namespace_none(self):
        """Test invalid None namespace."""
        with pytest.raises(IcebergNamespaceNotFoundError):
            validate_namespace(None)


class TestValidateFilterExpression:
    """Test cases for validate_filter_expression."""

    def test_valid_filter(self):
        """Test valid filter expressions."""
        assert validate_filter_expression("status = 'completed'") == "status = 'completed'"
        assert validate_filter_expression("amount > 100") == "amount > 100"
        assert validate_filter_expression(None) is None
        assert validate_filter_expression("") is None

    def test_invalid_filter_unbalanced_parens(self):
        """Test invalid filter with unbalanced parentheses."""
        from langchain_iceberg.exceptions import IcebergInvalidFilterError

        with pytest.raises(IcebergInvalidFilterError):
            validate_filter_expression("status = 'completed' AND (amount > 100")

