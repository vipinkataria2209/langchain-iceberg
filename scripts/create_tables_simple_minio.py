#!/usr/bin/env python3
"""Create simple Iceberg tables without partitions - easier for MinIO."""

import time
import warnings
from datetime import datetime
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

warnings.filterwarnings('ignore')

print("=" * 70)
print("CREATING SIMPLE TABLES (NO PARTITIONS) WITH MINIO")
print("=" * 70)

time.sleep(3)

print("\n[1/4] Connecting to catalog...")
catalog = load_catalog(
    name="rest",
    type="rest",
    uri="http://localhost:8181",
    warehouse="s3://warehouse/wh/",
    io_impl="org.apache.iceberg.aws.s3.S3FileIO",
    s3_endpoint="http://localhost:9000",
    s3_access_key_id="admin",
    s3_secret_access_key="password",
    s3_path_style_access="true",
    s3_region="us-east-1",
)
print("✅ Connected")

# Create namespace
print("\n[2/4] Ensuring namespace exists...")
try:
    catalog.create_namespace("test")
    print("✅ Namespace created")
except:
    print("✅ Namespace already exists")

# Create Customers table - NO PARTITIONS
print("\n[3/4] Creating tables (no partitions)...")
customers_schema = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "customer_name", StringType(), required=True),
    NestedField(3, "email", StringType(), required=False),
    NestedField(4, "customer_segment", StringType(), required=False),
    NestedField(5, "registration_date", TimestampType(), required=False),
    NestedField(6, "country", StringType(), required=False),
)

try:
    try:
        catalog.drop_table(("test", "customers"))
    except:
        pass
    
    # Create without partition spec
    table = catalog.create_table(
        identifier=("test", "customers"),
        schema=customers_schema,
    )
    print("✅ Customers table created (no partitions)")
    
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
    import traceback
    traceback.print_exc()

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
    try:
        catalog.drop_table(("test", "products"))
    except:
        pass
    
    table = catalog.create_table(
        identifier=("test", "products"),
        schema=products_schema,
    )
    print("✅ Products table created (no partitions)")
    
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
    try:
        catalog.drop_table(("test", "orders"))
    except:
        pass
    
    table = catalog.create_table(
        identifier=("test", "orders"),
        schema=orders_schema,
    )
    print("✅ Orders table created (no partitions)")
    
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
    print("  ✅ Inserted 10 orders (snapshot 1)")
    
    # Second snapshot
    time.sleep(2)
    more_data = pa.table({
        "order_id": [1011, 1012],
        "customer_id": [2, 3],
        "product_id": [104, 105],
        "order_date": [
            datetime(2024, 12, 6, 10, 0, 0),
            datetime(2024, 12, 6, 11, 0, 0),
        ],
        "quantity": [1, 1],
        "amount": [299.99, 149.99],
        "status": ["completed", "completed"],
        "shipping_address": [
            "456 Oak Ave, Los Angeles, CA",
            "789 Pine Rd, Toronto, ON",
        ],
    })
    table.append(more_data)
    print("  ✅ Inserted 2 more orders (snapshot 2)")
except Exception as e:
    print(f"  ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
print("[4/4] ✅ TABLE CREATION COMPLETE")
print("=" * 70)

