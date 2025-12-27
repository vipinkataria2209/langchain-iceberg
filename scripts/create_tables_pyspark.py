#!/usr/bin/env python3
"""Create Iceberg tables using PySpark (works with MinIO)."""

import os
import sys
import warnings

warnings.filterwarnings('ignore')

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("❌ PySpark not installed. Installing...")
    print("   Run: pip install pyspark")
    sys.exit(1)

print("=" * 70)
print("CREATING ICEBERG TABLES WITH PYSPARK")
print("=" * 70)

# Initialize Spark with Iceberg and MinIO support
print("\n[1/4] Initializing Spark session...")

# Configure Spark with Iceberg and REST catalog
spark = SparkSession.builder \
    .appName("Iceberg with MinIO") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rest.type", "rest") \
    .config("spark.sql.catalog.rest.uri", "http://localhost:8181") \
    .config("spark.sql.catalog.rest.warehouse", "s3://warehouse/wh/") \
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.rest.s3.endpoint", "http://localhost:9000") \
    .config("spark.sql.catalog.rest.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.rest.s3.secret-access-key", "password") \
    .config("spark.sql.catalog.rest.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "rest") \
    .getOrCreate()

print("✅ Spark session created")

# Create namespace
print("\n[2/4] Creating namespace...")
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test")
    print("✅ Namespace 'test' created")
except Exception as e:
    print(f"⚠️  Namespace creation: {e}")

# Create tables using REST catalog
print("\n[3/4] Creating tables in REST catalog...")

# Customers table
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.test.customers (
            customer_id BIGINT,
            customer_name STRING,
            email STRING,
            customer_segment STRING,
            registration_date TIMESTAMP,
            country STRING
        ) USING iceberg
    """)
    print("✅ Customers table created in REST catalog")
except Exception as e:
    print(f"⚠️  Customers table: {e}")

# Products table
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.test.products (
            product_id BIGINT,
            product_name STRING,
            category STRING,
            price DECIMAL(10, 2),
            stock_quantity INT,
            created_date TIMESTAMP
        ) USING iceberg
    """)
    print("✅ Products table created in REST catalog")
except Exception as e:
    print(f"⚠️  Products table: {e}")

# Orders table
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.test.orders (
            order_id BIGINT,
            customer_id BIGINT,
            product_id BIGINT,
            order_date TIMESTAMP,
            quantity INT,
            amount DECIMAL(10, 2),
            status STRING,
            shipping_address STRING
        ) USING iceberg
    """)
    print("✅ Orders table created in REST catalog")
except Exception as e:
    print(f"⚠️  Orders table: {e}")

# Insert data
print("\n[4/4] Inserting sample data...")

# Insert customers
try:
    spark.sql("""
        INSERT INTO rest.test.customers VALUES
        (1, 'John Doe', 'john.doe@example.com', 'new', TIMESTAMP '2024-01-15 10:00:00', 'USA'),
        (2, 'Jane Smith', 'jane.smith@example.com', 'returning', TIMESTAMP '2023-06-20 14:30:00', 'USA'),
        (3, 'Bob Johnson', 'bob.johnson@example.com', 'vip', TIMESTAMP '2022-03-10 09:15:00', 'Canada'),
        (4, 'Alice Williams', 'alice.williams@example.com', 'returning', TIMESTAMP '2023-08-05 16:45:00', 'UK'),
        (5, 'Charlie Brown', 'charlie.brown@example.com', 'new', TIMESTAMP '2024-02-28 11:20:00', 'USA')
    """)
    print("✅ Inserted 5 customers")
except Exception as e:
    print(f"⚠️  Customers data: {e}")

# Insert products
try:
    spark.sql("""
        INSERT INTO rest.test.products VALUES
        (101, 'Laptop', 'Electronics', 999.99, 50, TIMESTAMP '2024-01-01 00:00:00'),
        (102, 'Mouse', 'Electronics', 29.99, 200, TIMESTAMP '2024-01-01 00:00:00'),
        (103, 'Keyboard', 'Electronics', 79.99, 150, TIMESTAMP '2024-01-01 00:00:00'),
        (104, 'Monitor', 'Electronics', 299.99, 75, TIMESTAMP '2024-01-01 00:00:00'),
        (105, 'Headphones', 'Electronics', 149.99, 100, TIMESTAMP '2024-01-01 00:00:00'),
        (106, 'Desk Chair', 'Furniture', 199.99, 30, TIMESTAMP '2024-01-01 00:00:00'),
        (107, 'Standing Desk', 'Furniture', 499.99, 20, TIMESTAMP '2024-01-01 00:00:00')
    """)
    print("✅ Inserted 7 products")
except Exception as e:
    print(f"⚠️  Products data: {e}")

# Insert orders
try:
    spark.sql("""
        INSERT INTO rest.test.orders VALUES
        (1001, 1, 101, TIMESTAMP '2024-12-01 10:23:45', 1, 999.99, 'completed', '123 Main St, New York, NY'),
        (1002, 2, 102, TIMESTAMP '2024-12-01 11:15:22', 2, 59.98, 'completed', '456 Oak Ave, Los Angeles, CA'),
        (1003, 3, 103, TIMESTAMP '2024-12-01 14:32:11', 1, 79.99, 'completed', '789 Pine Rd, Toronto, ON'),
        (1004, 1, 104, TIMESTAMP '2024-12-02 09:10:30', 1, 299.99, 'pending', '123 Main St, New York, NY'),
        (1005, 4, 105, TIMESTAMP '2024-12-02 15:45:00', 1, 149.99, 'completed', '321 Elm St, London, UK'),
        (1006, 2, 106, TIMESTAMP '2024-12-03 08:20:15', 1, 199.99, 'completed', '456 Oak Ave, Los Angeles, CA'),
        (1007, 3, 107, TIMESTAMP '2024-12-03 12:30:45', 1, 499.99, 'completed', '789 Pine Rd, Toronto, ON'),
        (1008, 5, 101, TIMESTAMP '2024-12-04 10:00:00', 1, 999.99, 'pending', '555 Maple Dr, Boston, MA'),
        (1009, 1, 102, TIMESTAMP '2024-12-04 14:22:33', 3, 89.97, 'completed', '123 Main St, New York, NY'),
        (1010, 4, 103, TIMESTAMP '2024-12-05 16:10:20', 2, 159.98, 'completed', '321 Elm St, London, UK')
    """)
    print("✅ Inserted 10 orders")
except Exception as e:
    print(f"⚠️  Orders data: {e}")

# Verify tables
print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

try:
    tables = spark.sql("SHOW TABLES IN rest.test").collect()
    print(f"\n✅ Tables in namespace 'test': {len(tables)}")
    for table in tables:
        print(f"   - {table['namespace']}.{table['tableName']}")
except Exception as e:
    print(f"⚠️  Could not list tables: {e}")

spark.stop()

print("\n" + "=" * 70)
print("✅ TABLE CREATION COMPLETE")
print("=" * 70)
print("\nYou can now test with:")
print("  python test_full_functionality.py")
print("  pytest tests/test_integration.py -v")

