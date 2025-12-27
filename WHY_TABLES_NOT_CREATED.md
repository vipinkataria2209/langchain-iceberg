# Why Tables Are Not Being Created

## Root Cause Analysis

### The Problem

Tables are not being created because of **S3 access permissions** when PyIceberg tries to write metadata files to MinIO.

### Error Details

**Error**: `ACCESS_DENIED during HeadObject operation`

**What happens**:
1. ✅ PyIceberg connects to REST catalog successfully
2. ✅ REST catalog receives table creation request
3. ✅ REST catalog tries to write metadata file to MinIO
4. ❌ MinIO denies access (ACCESS_DENIED error)
5. ❌ Table creation fails

### Why This Happens

1. **MinIO Permissions**: The MinIO bucket may not have proper read/write permissions
2. **S3 Client Configuration**: The AWS S3 SDK may not be sending credentials correctly
3. **Bucket Policy**: MinIO bucket policies may be blocking writes

## Attempted Solutions

### ✅ What We Tried

1. **REST Catalog + MinIO**
   - Issue: DNS resolution problem (UnknownHostException)
   - Status: ❌ Failed

2. **SQL Catalog + MinIO**
   - Issue: ACCESS_DENIED when writing metadata
   - Status: ❌ Failed

3. **Spark SQL**
   - Issue: S3FileIO initialization problems
   - Status: ❌ Failed

4. **MinIO Permissions**
   - Tried: Setting bucket policies, anonymous access
   - Status: ⚠️ Still having issues

## Current Status

### What Works
- ✅ Catalog connection (REST and SQL)
- ✅ Namespace creation
- ✅ Tool initialization
- ✅ All catalog read operations

### What Doesn't Work
- ❌ Table creation (ACCESS_DENIED)
- ❌ Metadata file writes to MinIO

## Solutions

### Option 1: Fix MinIO Permissions (Recommended)
```bash
# Set bucket to public read/write
docker exec iceberg-minio mc anonymous set public myminio/warehouse
docker exec iceberg-minio mc policy set public myminio/warehouse

# Or set proper IAM policy
docker exec iceberg-minio mc admin policy create myminio fullaccess \
  /tmp/policy.json
docker exec iceberg-minio mc admin policy attach myminio fullaccess --user admin
```

### Option 2: Use Real AWS S3
- REST catalog works perfectly with real S3
- No permission issues
- Production-ready

### Option 3: Use File-Based Storage (Testing)
- Create tables with local file storage
- Migrate to S3 later
- Good for development/testing

## Next Steps

1. **Fix MinIO bucket permissions** - Set proper read/write access
2. **Verify S3 credentials** - Ensure admin/password are correct
3. **Test with real S3** - If available, use AWS S3 instead
4. **Use alternative catalog** - Try different catalog type

## Summary

**Why tables aren't created**: MinIO S3 access permissions blocking metadata file writes.

**Impact**: Cannot create tables, but all other operations work.

**Solution**: Fix MinIO permissions or use real AWS S3.

