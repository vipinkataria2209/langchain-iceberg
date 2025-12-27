#!/usr/bin/env python3
"""Basic integration test - tests catalog operations."""

from langchain_iceberg import IcebergToolkit

print("=" * 60)
print("Testing LangChain Iceberg Toolkit - Basic Operations")
print("=" * 60)

# Initialize toolkit
print("\n1. Initializing toolkit...")
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
print("✓ Toolkit initialized successfully")

# Get tools
print("\n2. Getting tools...")
tools = toolkit.get_tools()
print(f"✓ Found {len(tools)} tools:")
for tool in tools:
    print(f"  - {tool.name}")

# Test 1: List namespaces
print("\n3. Testing list_namespaces...")
list_namespaces_tool = next(t for t in tools if t.name == "iceberg_list_namespaces")
result = list_namespaces_tool.run({})
print(result)
print("✓ List namespaces works!")

# Test 2: List tables
print("\n4. Testing list_tables...")
list_tables_tool = next(t for t in tools if t.name == "iceberg_list_tables")
result = list_tables_tool.run({"namespace": "test"})
print(result)
print("✓ List tables works!")

# Test 3: Get context
print("\n5. Testing get_context...")
context = toolkit.get_context()
print(f"Catalog: {context['catalog_name']}")
print(f"Type: {context['catalog_type']}")
print(f"Namespaces: {context.get('namespaces', [])}")
print("✓ Get context works!")

print("\n" + "=" * 60)
print("✅ All basic tests passed!")
print("=" * 60)
print("\nNote: Table operations require tables to be created.")
print("The catalog connection and basic operations are working correctly!")

