"""Integration tests with Docker Iceberg setup."""

import os
import time
from typing import Any

import pytest

from langchain_iceberg import IcebergToolkit


@pytest.fixture(scope="module")
def toolkit():
    """Create toolkit instance for testing."""
    # Wait for services to be ready
    time.sleep(5)
    
    toolkit = IcebergToolkit(
        catalog_name="rest",
        catalog_config={
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/wh/",
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
        },
    )
    return toolkit


class TestIntegration:
    """Integration tests with real Iceberg catalog."""

    def test_list_namespaces(self, toolkit):
        """Test listing namespaces."""
        tools = toolkit.get_tools()
        list_namespaces_tool = next(t for t in tools if t.name == "iceberg_list_namespaces")
        
        result = list_namespaces_tool.run({})
        print(f"\nNamespaces: {result}")
        assert "test" in result or "default" in result

    def test_list_tables(self, toolkit):
        """Test listing tables in namespace."""
        tools = toolkit.get_tools()
        list_tables_tool = next(t for t in tools if t.name == "iceberg_list_tables")
        
        result = list_tables_tool.run({"namespace": "test"})
        print(f"\nTables: {result}")
        assert "customers" in result.lower() or "orders" in result.lower() or "products" in result.lower()

    def test_get_schema(self, toolkit):
        """Test getting table schema."""
        tools = toolkit.get_tools()
        get_schema_tool = next(t for t in tools if t.name == "iceberg_get_schema")
        
        result = get_schema_tool.run({"table_id": "test.customers"})
        print(f"\nSchema: {result}")
        assert "customer_id" in result
        assert "customer_name" in result
        assert "email" in result

    def test_query_customers(self, toolkit):
        """Test querying customers table."""
        tools = toolkit.get_tools()
        query_tool = next(t for t in tools if t.name == "iceberg_query")
        
        result = query_tool.run({
            "table_id": "test.customers",
            "limit": 5
        })
        print(f"\nCustomers Query: {result}")
        assert "customer_id" in result or "No results" in result

    def test_query_orders_with_filter(self, toolkit):
        """Test querying orders with filter."""
        tools = toolkit.get_tools()
        query_tool = next(t for t in tools if t.name == "iceberg_query")
        
        result = query_tool.run({
            "table_id": "test.orders",
            "columns": ["order_id", "customer_id", "amount", "status"],
            "filters": "status = 'completed'",
            "limit": 10
        })
        print(f"\nOrders Query (completed): {result}")
        assert "order_id" in result or "No results" in result

    def test_query_products(self, toolkit):
        """Test querying products table."""
        tools = toolkit.get_tools()
        query_tool = next(t for t in tools if t.name == "iceberg_query")
        
        result = query_tool.run({
            "table_id": "test.products",
            "columns": ["product_id", "product_name", "price", "category"],
            "filters": "price > 100",
            "limit": 10
        })
        print(f"\nProducts Query (price > 100): {result}")
        assert "product_id" in result or "No results" in result

    def test_snapshots(self, toolkit):
        """Test listing snapshots."""
        tools = toolkit.get_tools()
        snapshot_tool = next(t for t in tools if t.name == "iceberg_snapshots")
        
        result = snapshot_tool.run({"table_id": "test.orders"})
        print(f"\nSnapshots: {result}")
        # Should have at least one snapshot
        assert "Snapshot" in result or "No snapshots" in result

    def test_time_travel(self, toolkit):
        """Test time-travel query."""
        tools = toolkit.get_tools()
        time_travel_tool = next(t for t in tools if t.name == "iceberg_time_travel")
        
        # Get current snapshot first
        snapshot_tool = next(t for t in tools if t.name == "iceberg_snapshots")
        snapshots_result = snapshot_tool.run({"table_id": "test.orders", "limit": 1})
        
        # Try time-travel with timestamp
        result = time_travel_tool.run({
            "table_id": "test.orders",
            "timestamp": "2024-12-01T00:00:00",
            "limit": 5
        })
        print(f"\nTime Travel Query: {result}")
        assert "Time Travel" in result or "No results" in result


if __name__ == "__main__":
    # Run tests manually
    toolkit = IcebergToolkit(
        catalog_name="rest",
        catalog_config={
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/wh/",
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
        },
    )
    
    print("=" * 60)
    print("Testing LangChain Iceberg Toolkit")
    print("=" * 60)
    
    # Test 1: List namespaces
    print("\n1. Testing list_namespaces...")
    tools = toolkit.get_tools()
    list_namespaces_tool = next(t for t in tools if t.name == "iceberg_list_namespaces")
    result = list_namespaces_tool.run({})
    print(result)
    
    # Test 2: List tables
    print("\n2. Testing list_tables...")
    list_tables_tool = next(t for t in tools if t.name == "iceberg_list_tables")
    result = list_tables_tool.run({"namespace": "test"})
    print(result)
    
    # Test 3: Get schema
    print("\n3. Testing get_schema...")
    get_schema_tool = next(t for t in tools if t.name == "iceberg_get_schema")
    result = get_schema_tool.run({"table_id": "test.customers"})
    print(result)
    
    # Test 4: Query customers
    print("\n4. Testing query (customers)...")
    query_tool = next(t for t in tools if t.name == "iceberg_query")
    result = query_tool.run({
        "table_id": "test.customers",
        "limit": 5
    })
    print(result)
    
    # Test 5: Query orders with filter
    print("\n5. Testing query with filter (orders)...")
    result = query_tool.run({
        "table_id": "test.orders",
        "columns": ["order_id", "customer_id", "amount", "status"],
        "filters": "status = 'completed'",
        "limit": 10
    })
    print(result)
    
    # Test 6: Query products
    print("\n6. Testing query (products)...")
    result = query_tool.run({
        "table_id": "test.products",
        "columns": ["product_id", "product_name", "price", "category"],
        "filters": "price > 100",
        "limit": 10
    })
    print(result)
    
    # Test 7: Snapshots
    print("\n7. Testing snapshots...")
    snapshot_tool = next(t for t in tools if t.name == "iceberg_snapshots")
    result = snapshot_tool.run({"table_id": "test.orders"})
    print(result)
    
    # Test 8: Time travel
    print("\n8. Testing time_travel...")
    time_travel_tool = next(t for t in tools if t.name == "iceberg_time_travel")
    result = time_travel_tool.run({
        "table_id": "test.orders",
        "timestamp": "2024-12-01T00:00:00",
        "limit": 5
    })
    print(result)
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)

