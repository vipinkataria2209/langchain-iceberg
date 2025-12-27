#!/bin/bash
# Initialize Iceberg tables using Spark in a one-time container

set -e

echo "Initializing Iceberg tables..."

# Wait for services to be ready
echo "Waiting for services..."
sleep 15

# Create bucket in MinIO
echo "Creating MinIO bucket..."
docker exec iceberg-minio mc alias set myminio http://localhost:9000 admin password 2>/dev/null || true
docker exec iceberg-minio mc mb myminio/warehouse 2>/dev/null || true

# Run Spark SQL to create tables
echo "Creating tables..."
docker run --rm \
  --network iceberg-network \
  -v "$(pwd)/scripts:/scripts" \
  tabulario/iceberg-spark:latest \
  /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.type=rest \
  --conf spark.sql.catalog.rest.uri=http://rest-catalog:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://warehouse/wh/ \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=password \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  -f /scripts/create_tables.sql

echo "Tables created successfully!"
echo ""
echo "You can now test with:"
echo "  python tests/test_integration.py"

