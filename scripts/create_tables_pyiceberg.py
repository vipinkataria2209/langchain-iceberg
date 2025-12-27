#!/usr/bin/env python3
"""Create Iceberg tables using PyIceberg directly."""

import time
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

# Wait for catalog to be ready
print("Waiting for catalog to be ready...")
time.sleep(5)

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
print("Creating namespace 'test'...")
try:
    catalog.create_namespace("test")
    print("Namespace 'test' created")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Namespace 'test' already exists")
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
    catalog.create_table(
        identifier=("test", "customers"),
        schema=customers_schema,
        partition_spec=customers_spec,
    )
    print("Customers table created")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Customers table already exists")
    else:
        print(f"Error creating customers table: {e}")

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
    catalog.create_table(
        identifier=("test", "products"),
        schema=products_schema,
        partition_spec=products_spec,
    )
    print("Products table created")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Products table already exists")
    else:
        print(f"Error creating products table: {e}")

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
    catalog.create_table(
        identifier=("test", "orders"),
        schema=orders_schema,
        partition_spec=orders_spec,
    )
    print("Orders table created")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Orders table already exists")
    else:
        print(f"Error creating orders table: {e}")

print("\n" + "=" * 60)
print("Tables created successfully!")
print("=" * 60)
print("\nNote: To insert data, you'll need to use Spark or another tool.")
print("For now, tables are empty but ready for testing schema operations.")

