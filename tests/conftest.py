"""Shared test fixtures and configuration."""

import pytest
from unittest.mock import Mock, MagicMock
import tempfile
import os


@pytest.fixture
def mock_catalog():
    """Create a mock PyIceberg catalog."""
    catalog = Mock()
    catalog.name = "test_catalog"
    return catalog


@pytest.fixture
def catalog_config():
    """Sample catalog configuration."""
    return {
        "type": "sql",
        "uri": "sqlite:///:memory:",
        "warehouse": "s3://test-bucket/warehouse",
        "s3.access-key-id": "test-access-key",
        "s3.secret-access-key": "test-secret-key",
        "s3.region": "us-east-1",
        "s3.endpoint": "https://s3.amazonaws.com",
    }


@pytest.fixture
def sample_metric_config():
    """Sample metric configuration from YAML."""
    return {
        "name": "total_revenue",
        "description": "Total revenue from completed orders",
        "type": "calculated",
        "formula": """
            SELECT SUM(amount) as total_revenue
            FROM sales.orders
            WHERE status = 'completed'
        """,
        "requires_join": False,
        "date_column": "order_date",
    }


@pytest.fixture
def complex_metric_config():
    """Complex metric with JOIN."""
    return {
        "name": "revenue_by_segment",
        "description": "Revenue breakdown by customer segment",
        "type": "calculated",
        "formula": """
            SELECT 
                c.customer_segment,
                SUM(o.amount) as total_revenue
            FROM sales.orders o
            JOIN sales.customers c ON o.customer_id = c.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.customer_segment
            ORDER BY total_revenue DESC
        """,
        "requires_join": True,
        "date_column": "order_date",
    }


@pytest.fixture
def semantic_config():
    """Sample semantic YAML configuration."""
    return {
        "version": "1.0",
        "catalog": "test",
        "warehouse": "s3://test-bucket/warehouse",
        "tables": [
            {
                "name": "orders",
                "namespace": "sales",
                "description": "Customer orders",
            },
            {
                "name": "customers",
                "namespace": "sales",
                "description": "Customer information",
            },
        ],
        "relationships": [
            {
                "name": "order_to_customer",
                "type": "many_to_one",
                "from_table": "sales.orders",
                "from_column": "customer_id",
                "to_table": "sales.customers",
                "to_column": "customer_id",
            }
        ],
    }


@pytest.fixture
def mock_table():
    """Create a mock Iceberg table."""
    table = Mock()
    table.name = "orders"
    
    # Mock metadata
    metadata = Mock()
    metadata.metadata_file_location = "s3://test-bucket/warehouse/sales/orders/metadata/v1.metadata.json"
    table.metadata = metadata
    
    # Mock schema
    schema = Mock()
    field1 = Mock()
    field1.name = "order_id"
    field1.field_type = "long"
    
    field2 = Mock()
    field2.name = "order_date"
    field2.field_type = "timestamp"
    
    field3 = Mock()
    field3.name = "amount"
    field3.field_type = "double"
    
    schema.fields = [field1, field2, field3]
    table.schema = Mock(return_value=schema)
    
    return table


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample parquet structure
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir, exist_ok=True)
        yield tmpdir


@pytest.fixture
def mock_duckdb_conn():
    """Create a mock DuckDB connection."""
    conn = Mock()
    
    # Mock execute method
    result = Mock()
    result.fetchdf = Mock(return_value=Mock())
    conn.execute = Mock(return_value=result)
    
    return conn


@pytest.fixture
def sample_sql_query():
    """Sample SQL query for testing."""
    return """
        SELECT 
            c.customer_segment,
            COUNT(o.order_id) as order_count,
            SUM(o.amount) as total_revenue
        FROM sales.orders o
        JOIN sales.customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed'
        GROUP BY c.customer_segment
        ORDER BY total_revenue DESC
    """
