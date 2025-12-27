#!/bin/bash
# Setup script to create tables and insert data

set -e

echo "Waiting for services to be ready..."
sleep 10

echo "Creating MinIO bucket..."
docker exec iceberg-minio mc alias set myminio http://localhost:9000 admin password
docker exec iceberg-minio mc mb myminio/warehouse || true

echo "Running Spark SQL to create tables..."
docker exec -i iceberg-spark /opt/spark/bin/spark-sql \
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
  < scripts/create_tables.sql

echo "Tables created successfully!"
echo "You can now test the toolkit with:"
echo "  python tests/test_integration.py"

