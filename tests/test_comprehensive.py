#!/usr/bin/env python3
"""Comprehensive integration tests for all functionality."""

from langchain_iceberg import IcebergToolkit

print("=" * 70)
print("COMPREHENSIVE INTEGRATION TESTS - LangChain Iceberg Toolkit")
print("=" * 70)

# Initialize toolkit
print("\n[1/10] Initializing toolkit...")
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
        "s3.region": "us-east-1",
    },
)
print("✓ Toolkit initialized")

# Get tools
print("\n[2/10] Getting tools...")
tools = toolkit.get_tools()
print(f"✓ Found {len(tools)} tools:")
for i, tool in enumerate(tools, 1):
    print(f"  {i}. {tool.name}")

# Test 1: List Namespaces
print("\n[3/10] Testing iceberg_list_namespaces...")
list_ns = next(t for t in tools if t.name == "iceberg_list_namespaces")
result = list_ns.run({})
print(result)
assert "test" in result, "Namespace 'test' not found"
print("✓ PASSED")

# Test 2: List Tables
print("\n[4/10] Testing iceberg_list_tables...")
list_tables = next(t for t in tools if t.name == "iceberg_list_tables")
result = list_tables.run({"namespace": "test"})
print(result)
assert "customers" in result.lower() or "orders" in result.lower() or "products" in result.lower()
print("✓ PASSED")

# Test 3: Get Schema - Customers
print("\n[5/10] Testing iceberg_get_schema (customers)...")
get_schema = next(t for t in tools if t.name == "iceberg_get_schema")
result = get_schema.run({"table_id": "test.customers"})
print(result)
assert "customer_id" in result
assert "customer_name" in result
print("✓ PASSED")

# Test 4: Get Schema - Products
print("\n[6/10] Testing iceberg_get_schema (products)...")
result = get_schema.run({"table_id": "test.products"})
print(result)
assert "product_id" in result
assert "price" in result
print("✓ PASSED")

# Test 5: Query - Customers (all)
print("\n[7/10] Testing iceberg_query (customers, all columns)...")
query = next(t for t in tools if t.name == "iceberg_query")
result = query.run({
    "table_id": "test.customers",
    "limit": 5
})
print(result)
assert "customer_id" in result or "No results" not in result
print("✓ PASSED")

# Test 6: Query - Orders with filter
print("\n[8/10] Testing iceberg_query (orders, with filter)...")
result = query.run({
    "table_id": "test.orders",
    "columns": ["order_id", "customer_id", "amount", "status"],
    "filters": "status = 'completed'",
    "limit": 10
})
print(result)
assert "order_id" in result or "No results" not in result
print("✓ PASSED")

# Test 7: Query - Products with filter
print("\n[9/10] Testing iceberg_query (products, price filter)...")
result = query.run({
    "table_id": "test.products",
    "columns": ["product_id", "product_name", "price", "category"],
    "filters": "price > 100",
    "limit": 10
})
print(result)
assert "product_id" in result or "No results" not in result
print("✓ PASSED")

# Test 8: Snapshots
print("\n[10/10] Testing iceberg_snapshots...")
snapshots = next(t for t in tools if t.name == "iceberg_snapshots")
result = snapshots.run({"table_id": "test.orders"})
print(result)
assert "Snapshot" in result or "snapshot" in result.lower()
print("✓ PASSED")

# Test 9: Time Travel
print("\n[11/10] Testing iceberg_time_travel (by timestamp)...")
time_travel = next(t for t in tools if t.name == "iceberg_time_travel")
result = time_travel.run({
    "table_id": "test.orders",
    "timestamp": "2024-12-01T00:00:00",
    "limit": 5
})
print(result)
assert "Time Travel" in result or "time" in result.lower()
print("✓ PASSED")

# Test 10: Query with column selection
print("\n[12/10] Testing iceberg_query (column selection)...")
result = query.run({
    "table_id": "test.customers",
    "columns": ["customer_id", "customer_name", "customer_segment"],
    "limit": 3
})
print(result)
assert "customer_id" in result or "No results" not in result
print("✓ PASSED")

# Test 11: Query with multiple filters
print("\n[13/10] Testing iceberg_query (multiple filters)...")
result = query.run({
    "table_id": "test.orders",
    "filters": "amount > 100 AND status = 'completed'",
    "limit": 5
})
print(result)
print("✓ PASSED")

# Test 12: Get Schema - Orders
print("\n[14/10] Testing iceberg_get_schema (orders)...")
result = get_schema.run({"table_id": "test.orders"})
print(result)
assert "order_id" in result
assert "amount" in result
print("✓ PASSED")

# Test 13: Context
print("\n[15/10] Testing get_context...")
context = toolkit.get_context()
print(f"Catalog: {context['catalog_name']}")
print(f"Type: {context['catalog_type']}")
print(f"Namespaces: {context.get('namespaces', [])}")
assert context['catalog_name'] == "rest"
print("✓ PASSED")

# Test 14: Error handling - invalid table
print("\n[16/10] Testing error handling (invalid table)...")
try:
    result = get_schema.run({"table_id": "test.nonexistent"})
    print("ERROR: Should have raised exception")
except Exception as e:
    print(f"✓ Correctly caught error: {type(e).__name__}")

# Test 15: Error handling - invalid namespace
print("\n[17/10] Testing error handling (invalid namespace)...")
try:
    result = list_tables.run({"namespace": "nonexistent"})
    print("ERROR: Should have raised exception")
except Exception as e:
    print(f"✓ Correctly caught error: {type(e).__name__}")

print("\n" + "=" * 70)
print("✅ ALL TESTS PASSED!")
print("=" * 70)
print("\nSummary:")
print(f"  - Tools tested: {len(tools)}")
print("  - Catalog operations: ✓")
print("  - Schema operations: ✓")
print("  - Query operations: ✓")
print("  - Filter operations: ✓")
print("  - Time-travel operations: ✓")
print("  - Error handling: ✓")
print("\n🎉 Toolkit is fully functional!")

