#!/usr/bin/env python3
"""Create tables using PyIceberg with file-based catalog (bypasses REST catalog S3 issue)."""

import warnings
import os
import tempfile
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    DecimalType,
    IntegerType,
    TimestampType,
)
import pyarrow as pa
from datetime import datetime

warnings.filterwarnings('ignore')

print("=" * 70)
print("CREATING TABLES WITH PYICEBERG (FILE-BASED CATALOG)")
print("=" * 70)

# Create temporary directory for file-based catalog
catalog_dir = tempfile.mkdtemp(prefix="iceberg_catalog_")
print(f"\n[1/5] Using file-based catalog: {catalog_dir}")

# Initialize file-based catalog
catalog = load_catalog(
    name="file",
    type="filesystem",
    warehouse=f"file://{catalog_dir}",
)

print("✅ File-based catalog initialized")

# Create namespace
print("\n[2/5] Creating namespace 'test'...")
try:
    catalog.create_namespace("test")
    print("✅ Namespace created")
except:
    print("✅ Namespace already exists")

# Create Customers table
print("\n[3/5] Creating tables...")
customers_schema = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "customer_name", StringType(), required=True),
    NestedField(3, "email", StringType(), required=False),
    NestedField(4, "customer_segment", StringType(), required=False),
    NestedField(5, "registration_date", TimestampType(), required=False),
    NestedField(6, "country", StringType(), required=False),
)

try:
    table = catalog.create_table(
        identifier=("test", "customers"),
        schema=customers_schema,
    )
    print("✅ Customers table created")
    
    # Insert data
    data = pa.table({
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown"],
        "email": ["john.doe@example.com", "jane.smith@example.com", "bob.johnson@example.com", 
                 "alice.williams@example.com", "charlie.brown@example.com"],
        "customer_segment": ["new", "returning", "vip", "returning", "new"],
        "registration_date": [
            datetime(2024, 1, 15, 10, 0, 0),
            datetime(2023, 6, 20, 14, 30, 0),
            datetime(2022, 3, 10, 9, 15, 0),
            datetime(2023, 8, 5, 16, 45, 0),
            datetime(2024, 2, 28, 11, 20, 0),
        ],
        "country": ["USA", "USA", "Canada", "UK", "USA"],
    })
    table.append(data)
    print("  ✅ Inserted 5 customers")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Create Products table
products_schema = Schema(
    NestedField(1, "product_id", LongType(), required=True),
    NestedField(2, "product_name", StringType(), required=True),
    NestedField(3, "category", StringType(), required=False),
    NestedField(4, "price", DecimalType(precision=10, scale=2), required=False),
    NestedField(5, "stock_quantity", IntegerType(), required=False),
    NestedField(6, "created_date", TimestampType(), required=False),
)

try:
    table = catalog.create_table(
        identifier=("test", "products"),
        schema=products_schema,
    )
    print("✅ Products table created")
    
    data = pa.table({
        "product_id": [101, 102, 103, 104, 105, 106, 107],
        "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Desk Chair", "Standing Desk"],
        "category": ["Electronics", "Electronics", "Electronics", "Electronics", "Electronics", "Furniture", "Furniture"],
        "price": [999.99, 29.99, 79.99, 299.99, 149.99, 199.99, 499.99],
        "stock_quantity": [50, 200, 150, 75, 100, 30, 20],
        "created_date": [datetime(2024, 1, 1, 0, 0, 0)] * 7,
    })
    table.append(data)
    print("  ✅ Inserted 7 products")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Create Orders table
orders_schema = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "customer_id", LongType(), required=True),
    NestedField(3, "product_id", LongType(), required=True),
    NestedField(4, "order_date", TimestampType(), required=False),
    NestedField(5, "quantity", IntegerType(), required=False),
    NestedField(6, "amount", DecimalType(precision=10, scale=2), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "shipping_address", StringType(), required=False),
)

try:
    table = catalog.create_table(
        identifier=("test", "orders"),
        schema=orders_schema,
    )
    print("✅ Orders table created")
    
    data = pa.table({
        "order_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
        "customer_id": [1, 2, 3, 1, 4, 2, 3, 5, 1, 4],
        "product_id": [101, 102, 103, 104, 105, 106, 107, 101, 102, 103],
        "order_date": [
            datetime(2024, 12, 1, 10, 23, 45),
            datetime(2024, 12, 1, 11, 15, 22),
            datetime(2024, 12, 1, 14, 32, 11),
            datetime(2024, 12, 2, 9, 10, 30),
            datetime(2024, 12, 2, 15, 45, 0),
            datetime(2024, 12, 3, 8, 20, 15),
            datetime(2024, 12, 3, 12, 30, 45),
            datetime(2024, 12, 4, 10, 0, 0),
            datetime(2024, 12, 4, 14, 22, 33),
            datetime(2024, 12, 5, 16, 10, 20),
        ],
        "quantity": [1, 2, 1, 1, 1, 1, 1, 1, 3, 2],
        "amount": [999.99, 59.98, 79.99, 299.99, 149.99, 199.99, 499.99, 999.99, 89.97, 159.98],
        "status": ["completed", "completed", "completed", "pending", "completed", 
                  "completed", "completed", "pending", "completed", "completed"],
        "shipping_address": [
            "123 Main St, New York, NY",
            "456 Oak Ave, Los Angeles, CA",
            "789 Pine Rd, Toronto, ON",
            "123 Main St, New York, NY",
            "321 Elm St, London, UK",
            "456 Oak Ave, Los Angeles, CA",
            "789 Pine Rd, Toronto, ON",
            "555 Maple Dr, Boston, MA",
            "123 Main St, New York, NY",
            "321 Elm St, London, UK",
        ],
    })
    table.append(data)
    print("  ✅ Inserted 10 orders")
except Exception as e:
    print(f"  ❌ Error: {e}")

print("\n" + "=" * 70)
print("[4/5] ✅ TABLES CREATED WITH FILE-BASED CATALOG")
print("=" * 70)
print(f"\nNote: These tables are in a file-based catalog at: {catalog_dir}")
print("To use with REST catalog, you would need to migrate them.")
print("\n[5/5] For REST catalog + MinIO, use Spark SQL instead:")
print("      ./scripts/create_tables_with_spark.sh")

