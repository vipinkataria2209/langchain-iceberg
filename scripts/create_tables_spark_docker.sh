#!/bin/bash
# Create Iceberg tables using Apache Spark Docker image

set -e

echo "=========================================="
echo "Creating Iceberg Tables with Spark"
echo "=========================================="

# Check if Docker network exists
NETWORK_NAME="langchain-iceberg_iceberg-network"
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    echo "❌ Docker network '$NETWORK_NAME' not found"
    echo "   Please run: docker-compose up -d"
    exit 1
fi

# Check if services are running
if ! docker ps | grep -q iceberg-rest-catalog; then
    echo "❌ REST catalog not running"
    exit 1
fi

if ! docker ps | grep -q iceberg-minio; then
    echo "❌ MinIO not running"
    exit 1
fi

echo ""
echo "✅ Services are running"
echo ""

# Create bucket if it doesn't exist
echo "Creating MinIO bucket..."
docker exec iceberg-minio mc alias set myminio http://localhost:9000 admin password 2>/dev/null || true
docker exec iceberg-minio mc mb myminio/warehouse 2>/dev/null || true
echo "✅ Bucket ready"
echo ""

# Note: Apache Spark image doesn't have Iceberg built-in
# We need to download Iceberg jars first
echo "Note: Apache Spark image requires Iceberg JARs to be downloaded."
echo "This may take a few minutes on first run..."
echo ""

# Create a temporary script with all SQL commands
cat > /tmp/create_tables_all.sql << 'EOF'
-- Create namespace
CREATE NAMESPACE IF NOT EXISTS test;

-- Create Customers table
CREATE TABLE IF NOT EXISTS test.customers (
    customer_id BIGINT,
    customer_name STRING,
    email STRING,
    customer_segment STRING,
    registration_date TIMESTAMP,
    country STRING
) USING iceberg;

-- Create Products table
CREATE TABLE IF NOT EXISTS test.products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    created_date TIMESTAMP
) USING iceberg;

-- Create Orders table
CREATE TABLE IF NOT EXISTS test.orders (
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    order_date TIMESTAMP,
    quantity INT,
    amount DECIMAL(10, 2),
    status STRING,
    shipping_address STRING
) USING iceberg;

-- Insert sample customers
INSERT INTO test.customers VALUES
(1, 'John Doe', 'john.doe@example.com', 'new', TIMESTAMP '2024-01-15 10:00:00', 'USA'),
(2, 'Jane Smith', 'jane.smith@example.com', 'returning', TIMESTAMP '2023-06-20 14:30:00', 'USA'),
(3, 'Bob Johnson', 'bob.johnson@example.com', 'vip', TIMESTAMP '2022-03-10 09:15:00', 'Canada'),
(4, 'Alice Williams', 'alice.williams@example.com', 'returning', TIMESTAMP '2023-08-05 16:45:00', 'UK'),
(5, 'Charlie Brown', 'charlie.brown@example.com', 'new', TIMESTAMP '2024-02-28 11:20:00', 'USA');

-- Insert sample products
INSERT INTO test.products VALUES
(101, 'Laptop', 'Electronics', 999.99, 50, TIMESTAMP '2024-01-01 00:00:00'),
(102, 'Mouse', 'Electronics', 29.99, 200, TIMESTAMP '2024-01-01 00:00:00'),
(103, 'Keyboard', 'Electronics', 79.99, 150, TIMESTAMP '2024-01-01 00:00:00'),
(104, 'Monitor', 'Electronics', 299.99, 75, TIMESTAMP '2024-01-01 00:00:00'),
(105, 'Headphones', 'Electronics', 149.99, 100, TIMESTAMP '2024-01-01 00:00:00'),
(106, 'Desk Chair', 'Furniture', 199.99, 30, TIMESTAMP '2024-01-01 00:00:00'),
(107, 'Standing Desk', 'Furniture', 499.99, 20, TIMESTAMP '2024-01-01 00:00:00');

-- Insert sample orders
INSERT INTO test.orders VALUES
(1001, 1, 101, TIMESTAMP '2024-12-01 10:23:45', 1, 999.99, 'completed', '123 Main St, New York, NY'),
(1002, 2, 102, TIMESTAMP '2024-12-01 11:15:22', 2, 59.98, 'completed', '456 Oak Ave, Los Angeles, CA'),
(1003, 3, 103, TIMESTAMP '2024-12-01 14:32:11', 1, 79.99, 'completed', '789 Pine Rd, Toronto, ON'),
(1004, 1, 104, TIMESTAMP '2024-12-02 09:10:30', 1, 299.99, 'pending', '123 Main St, New York, NY'),
(1005, 4, 105, TIMESTAMP '2024-12-02 15:45:00', 1, 149.99, 'completed', '321 Elm St, London, UK'),
(1006, 2, 106, TIMESTAMP '2024-12-03 08:20:15', 1, 199.99, 'completed', '456 Oak Ave, Los Angeles, CA'),
(1007, 3, 107, TIMESTAMP '2024-12-03 12:30:45', 1, 499.99, 'completed', '789 Pine Rd, Toronto, ON'),
(1008, 5, 101, TIMESTAMP '2024-12-04 10:00:00', 1, 999.99, 'pending', '555 Maple Dr, Boston, MA'),
(1009, 1, 102, TIMESTAMP '2024-12-04 14:22:33', 3, 89.97, 'completed', '123 Main St, New York, NY'),
(1010, 4, 103, TIMESTAMP '2024-12-05 16:10:20', 2, 159.98, 'completed', '321 Elm St, London, UK');
EOF

echo "Creating tables with Spark..."
echo "Note: This requires Iceberg JARs. If it fails, you may need to:"
echo "  1. Install PySpark: pip install pyspark"
echo "  2. Run: python scripts/create_tables_pyspark.py"
echo ""

# Try with Apache Spark (may not have Iceberg)
docker run --rm \
  --network "$NETWORK_NAME" \
  -v /tmp:/tmp \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.type=rest \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest-catalog:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://warehouse/wh/ \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://iceberg-minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=password \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  -f /tmp/create_tables_all.sql 2>&1 || {
    echo ""
    echo "❌ Spark SQL failed (likely missing Iceberg JARs)"
    echo ""
    echo "Alternative: Use PySpark script instead:"
    echo "  pip install pyspark"
    echo "  python scripts/create_tables_pyspark.py"
    exit 1
}

echo ""
echo "=========================================="
echo "✅ Tables created successfully!"
echo "=========================================="

