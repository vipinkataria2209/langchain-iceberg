# MinIO Setup and Testing Guide

## Current Status

### ✅ Working Components

1. **Docker Compose Setup**
   - REST Catalog Server: Running on port 8181
   - MinIO: Running on port 9000
   - Both services are healthy and connected

2. **Toolkit Configuration**
   - Successfully configured to use MinIO as S3-compatible storage
   - Catalog connection: ✅ Working
   - Namespace operations: ✅ Working
   - Tool initialization: ✅ All 6 tools available

3. **MinIO Configuration**
   - Endpoint: `http://localhost:9000`
   - Access Key: `admin`
   - Secret Key: `password`
   - Path-style access: Enabled (required for MinIO)
   - Bucket: `warehouse` (created)

### ⚠️ Known Issues

**Table Creation Issue**
- Problem: When PyIceberg tries to create tables, the REST catalog server (running in Docker) attempts to write metadata files to MinIO but encounters connectivity issues
- Error: `UnknownHostException` when REST catalog tries to access MinIO
- Root Cause: The REST catalog server is configured to use `minio:9000` (Docker service name), but there may be a network resolution issue

### Current Test Results

**Working Tests:**
- ✅ Toolkit initialization with MinIO config
- ✅ Namespace listing
- ✅ Table listing (returns empty, but works)
- ✅ Context retrieval
- ✅ All 6 tools initialized correctly

**Requires Tables:**
- ⚠️ Schema retrieval (needs tables)
- ⚠️ Query execution (needs tables)
- ⚠️ Snapshot operations (needs tables)
- ⚠️ Time-travel queries (needs tables)

## Configuration

### Docker Compose
The `docker-compose.yml` is correctly configured:
- REST catalog uses `http://minio:9000` (Docker network)
- MinIO is accessible on `localhost:9000` (host machine)

### Toolkit Configuration
```python
toolkit = IcebergToolkit(
    catalog_name="rest",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/wh/",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": "http://localhost:9000",  # MinIO
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",  # Required!
        "s3.region": "us-east-1",
    },
)
```

## Testing

### Run Basic Tests
```bash
python test_full_functionality.py
```

### Run Integration Tests
```bash
pytest tests/test_integration.py -v
```

### Run All Unit Tests
```bash
pytest tests/ --ignore=tests/test_integration.py -v
```

## Next Steps

1. **Fix Table Creation**
   - Option A: Use Spark SQL (requires Spark Docker image)
   - Option B: Fix REST catalog MinIO connectivity
   - Option C: Create tables manually via REST API

2. **Once Tables Exist**
   - Run full integration tests
   - Test query operations
   - Test time-travel functionality
   - Test snapshot operations

## Files Created

- `test_full_functionality.py` - Comprehensive functionality test
- `scripts/create_tables_minio.py` - Table creation script (has connectivity issue)
- `MINIO_SETUP.md` - This documentation

