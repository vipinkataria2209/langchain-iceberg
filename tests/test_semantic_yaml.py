#!/usr/bin/env python3
"""Test semantic YAML loading and validation."""

import os
from langchain_iceberg import IcebergToolkit
from langchain_iceberg.loaders.semantic_loader import SemanticLoader

print("=" * 70)
print("SEMANTIC YAML TESTING - LangChain Iceberg Toolkit")
print("=" * 70)

# Test 1: Load YAML file
print("\n[1/5] Testing SemanticLoader - Load YAML file...")
yaml_path = "examples/semantic.yaml"
if os.path.exists(yaml_path):
    try:
        loader = SemanticLoader(yaml_path)
        config = loader.load()
        print("✓ YAML file loaded successfully")
        print(f"  - Version: {config.get('version')}")
        print(f"  - Catalog: {config.get('catalog')}")
        print(f"  - Warehouse: {config.get('warehouse')}")
        print(f"  - Tables: {len(config.get('tables', []))}")
        print(f"  - Metrics: {len(config.get('metrics', []))}")
        print(f"  - Dimensions: {len(config.get('dimensions', []))}")
    except Exception as e:
        print(f"✗ Error loading YAML: {e}")
        import traceback
        traceback.print_exc()
else:
    print(f"✗ YAML file not found: {yaml_path}")

# Test 2: Validate YAML schema
print("\n[2/5] Testing SemanticLoader - Schema validation...")
if os.path.exists(yaml_path):
    try:
        loader = SemanticLoader(yaml_path)
        config = loader.load()
        is_valid = loader.validate_schema(config)
        print(f"✓ Schema validation: {is_valid}")
    except Exception as e:
        print(f"✗ Validation error: {e}")

# Test 3: Get metrics
print("\n[3/5] Testing SemanticLoader - Get metrics...")
if os.path.exists(yaml_path):
    try:
        loader = SemanticLoader(yaml_path)
        metrics = loader.get_metrics()
        print(f"✓ Found {len(metrics)} metrics:")
        for metric in metrics:
            print(f"  - {metric.get('name')}: {metric.get('type')} ({metric.get('description', '')[:50]}...)")
    except Exception as e:
        print(f"✗ Error getting metrics: {e}")

# Test 4: Get dimensions
print("\n[4/5] Testing SemanticLoader - Get dimensions...")
if os.path.exists(yaml_path):
    try:
        loader = SemanticLoader(yaml_path)
        dimensions = loader.get_dimensions()
        print(f"✓ Found {len(dimensions)} dimensions:")
        for dim in dimensions:
            print(f"  - {dim.get('name')}: {dim.get('type')}")
    except Exception as e:
        print(f"✗ Error getting dimensions: {e}")

# Test 5: Toolkit with semantic YAML
print("\n[5/5] Testing IcebergToolkit with semantic YAML...")
if os.path.exists(yaml_path):
    try:
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
            semantic_yaml=yaml_path,
        )
        print("✓ Toolkit initialized with semantic YAML")
        
        tools = toolkit.get_tools()
        print(f"✓ Found {len(tools)} total tools")
        
        # Check for metric tools
        metric_tools = [t for t in tools if t.name.startswith("get_")]
        print(f"✓ Found {len(metric_tools)} auto-generated metric tools:")
        for tool in metric_tools:
            print(f"  - {tool.name}: {tool.description[:60]}...")
        
    except Exception as e:
        print(f"✗ Error initializing toolkit with YAML: {e}")
        import traceback
        traceback.print_exc()
else:
    print(f"✗ YAML file not found: {yaml_path}")

# Test 6: Date range parser
print("\n[6/5] Testing DateRangeParser...")
from langchain_iceberg.utils.date_parser import DateRangeParser

test_ranges = [
    "Q4_2024",
    "last_30_days",
    "2024-01-01:2024-12-31",
    "this_month",
    "last_month",
]

for date_range in test_ranges:
    try:
        start, end = DateRangeParser.parse(date_range)
        print(f"✓ '{date_range}' -> {start} to {end}")
    except Exception as e:
        print(f"✗ '{date_range}' -> Error: {e}")

print("\n" + "=" * 70)
print("✅ Semantic YAML testing complete!")
print("=" * 70)

