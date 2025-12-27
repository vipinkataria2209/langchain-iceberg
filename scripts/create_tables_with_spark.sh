#!/bin/bash
# Create Iceberg tables using Spark SQL (works with MinIO)

set -e

echo "=========================================="
echo "Creating Iceberg Tables with Spark SQL"
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
    echo "   Please run: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q iceberg-minio; then
    echo "❌ MinIO not running"
    echo "   Please run: docker-compose up -d"
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

# Create tables using Spark SQL
echo "Creating tables with Spark SQL..."
docker run --rm \
  --network "$NETWORK_NAME" \
  -v "$(pwd)/scripts:/scripts" \
  tabulario/iceberg-spark:latest \
  /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.type=rest \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest-catalog:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://warehouse/wh/ \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://iceberg-minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=password \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  -f /scripts/create_tables.sql

echo ""
echo "=========================================="
echo "✅ Tables created successfully!"
echo "=========================================="
echo ""
echo "You can now test with:"
echo "  python test_full_functionality.py"
echo "  pytest tests/test_integration.py -v"

