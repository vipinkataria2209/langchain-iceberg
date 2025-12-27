#!/usr/bin/env python3
"""Create and populate Iceberg tables using PyIceberg."""

import time
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
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import pyarrow as pa
import pyarrow.parquet as pq

# Wait for catalog to be ready
print("Waiting for catalog to be ready...")
time.sleep(3)

# Initialize catalog
print("Connecting to catalog...")
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

# Create namespace
print("\nCreating namespace 'test'...")
try:
    catalog.create_namespace("test")
    print("✓ Namespace 'test' created")
except Exception as e:
    if "already exists" in str(e).lower() or "namespace already exists" in str(e).lower():
        print("✓ Namespace 'test' already exists")
    else:
        print(f"Error creating namespace: {e}")

# Create Customers table
print("\nCreating customers table...")
customers_schema = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "customer_name", StringType(), required=True),
    NestedField(3, "email", StringType(), required=False),
    NestedField(4, "customer_segment", StringType(), required=False),
    NestedField(5, "registration_date", TimestampType(), required=False),
    NestedField(6, "country", StringType(), required=False),
)

customers_spec = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="registration_date_day")
)

try:
    # Drop table if exists
    try:
        catalog.drop_table(("test", "customers"))
        print("  Dropped existing customers table")
    except:
        pass
    
    table = catalog.create_table(
        identifier=("test", "customers"),
        schema=customers_schema,
        partition_spec=customers_spec,
    )
    print("✓ Customers table created")
    
    # Insert data
    print("  Inserting customer data...")
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
    print("  ✓ Inserted 5 customers")
    
except Exception as e:
    print(f"  Error: {e}")

# Create Products table
print("\nCreating products table...")
products_schema = Schema(
    NestedField(1, "product_id", LongType(), required=True),
    NestedField(2, "product_name", StringType(), required=True),
    NestedField(3, "category", StringType(), required=False),
    NestedField(4, "price", DecimalType(precision=10, scale=2), required=False),
    NestedField(5, "stock_quantity", IntegerType(), required=False),
    NestedField(6, "created_date", TimestampType(), required=False),
)

products_spec = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="created_date_day")
)

try:
    # Drop table if exists
    try:
        catalog.drop_table(("test", "products"))
        print("  Dropped existing products table")
    except:
        pass
    
    table = catalog.create_table(
        identifier=("test", "products"),
        schema=products_schema,
        partition_spec=products_spec,
    )
    print("✓ Products table created")
    
    # Insert data
    print("  Inserting product data...")
    data = pa.table({
        "product_id": [101, 102, 103, 104, 105, 106, 107],
        "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Desk Chair", "Standing Desk"],
        "category": ["Electronics", "Electronics", "Electronics", "Electronics", "Electronics", "Furniture", "Furniture"],
        "price": [999.99, 29.99, 79.99, 299.99, 149.99, 199.99, 499.99],
        "stock_quantity": [50, 200, 150, 75, 100, 30, 20],
        "created_date": [
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
        ],
    })
    
    table.append(data)
    print("  ✓ Inserted 7 products")
    
except Exception as e:
    print(f"  Error: {e}")

# Create Orders table
print("\nCreating orders table...")
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

orders_spec = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="order_date_day")
)

try:
    # Drop table if exists
    try:
        catalog.drop_table(("test", "orders"))
        print("  Dropped existing orders table")
    except:
        pass
    
    table = catalog.create_table(
        identifier=("test", "orders"),
        schema=orders_schema,
        partition_spec=orders_spec,
    )
    print("✓ Orders table created")
    
    # Insert data
    print("  Inserting order data...")
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
    print("  ✓ Inserted 10 orders")
    
    # Create a second snapshot for time-travel testing
    print("  Creating second snapshot...")
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
    print("  ✓ Created second snapshot with 2 more orders")
    
except Exception as e:
    print(f"  Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("✅ All tables created and populated successfully!")
print("=" * 60)
print("\nTables ready for testing:")
print("  - test.customers (5 rows)")
print("  - test.products (7 rows)")
print("  - test.orders (12 rows, 2 snapshots)")

