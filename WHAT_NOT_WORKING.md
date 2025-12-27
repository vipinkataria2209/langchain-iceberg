# What's Not Working - Current Issues

## 🔴 Critical Issues

### 1. Table Creation (Blocking Integration Tests)
**Status**: ❌ **NOT WORKING**

**Problem**: Cannot create tables via PyIceberg because REST catalog server cannot write to MinIO.

**Error**:
```
SdkClientException: Received an UnknownHostException when attempting to interact with a service.
```

**Root Cause**: 
- REST catalog server (in Docker) is configured to use `minio:9000` (Docker service name)
- When creating tables, the REST catalog tries to write metadata files to MinIO
- Network resolution fails between REST catalog container and MinIO container

**Impact**:
- ❌ Cannot create tables via PyIceberg
- ❌ Integration tests for table operations fail (7 tests)
- ❌ Cannot test query, schema, snapshot, or time-travel operations with real data

**Affected Tests**:
- `test_integration.py::test_list_tables` - Fails (no tables exist)
- `test_integration.py::test_get_schema` - Fails (table not found)
- `test_integration.py::test_query_customers` - Fails (table not found)
- `test_integration.py::test_query_orders_with_filter` - Fails (table not found)
- `test_integration.py::test_query_products` - Fails (table not found)
- `test_integration.py::test_snapshots` - Fails (table not found)
- `test_integration.py::test_time_travel` - Fails (table not found)

**Workarounds**:
1. Use Spark SQL to create tables (requires Spark Docker image)
2. Create tables via REST API directly
3. Fix Docker network configuration
4. Use pre-created tables from another source

**Workarounds**:
1. Use Spark SQL to create tables (requires Spark Docker image)
2. Create tables via REST API directly
3. Fix Docker network configuration
4. Use pre-created tables from another source

---

## 🟡 Test Failures (Some Unit Tests)

### 2. Snapshot Tool Test Issues
**Status**: ⚠️ **MOSTLY WORKING** (some mocking issues remain)

**Issues**:
- **Date assertion**: Test expects specific date format but gets different timezone
- Some mocking edge cases in snapshot tests

**Fix Applied**: ✅ Removed local imports that were causing UnboundLocalError

---

## 🟡 Dependency Issues (Non-Blocking)

### 3. NumPy Compatibility Warnings
**Status**: ⚠️ **WARNINGS** (code still works)

**Problem**: NumPy 2.x compatibility warnings with some dependencies

**Error**:
```
A module that was compiled using NumPy 1.x cannot be run in NumPy 2.4.0
```

**Impact**: 
- ⚠️ Warnings appear but code functions
- ⚠️ May cause issues in future NumPy versions

**Fix**: Downgrade to `numpy<2` or upgrade affected modules

---

## ✅ What IS Working

### Fully Functional
- ✅ Toolkit initialization with MinIO
- ✅ Catalog connection
- ✅ Namespace listing
- ✅ Table listing (returns empty correctly)
- ✅ Tool discovery (6 tools)
- ✅ Input validation
- ✅ Error handling
- ✅ 70/71 unit tests passing (98.6%)

### Ready (Needs Tables)
- ⚠️ Schema retrieval (needs tables)
- ⚠️ Query execution (needs tables)
- ⚠️ Snapshot operations (needs tables)
- ⚠️ Time-travel queries (needs tables)

---

## Summary

### Critical Blockers
1. ❌ **Table Creation** - Docker networking issue prevents table creation
   - Blocks: 7 integration tests
   - Impact: Cannot test table operations

### Minor Issues
2. ⚠️ **1 Unit Test Failure** - Date assertion (timezone issue)
   - Impact: Very Low (test issue, not code bug)
   - Fix: Update test to handle timezones

3. ⚠️ **NumPy Warnings** - Compatibility issues
   - Impact: Low (code works, just warnings)
   - Fix: Downgrade NumPy or upgrade dependencies

---

## Priority Fixes

### High Priority
1. **Fix table creation** - Resolve Docker networking or use alternative method
   - ✅ **FIXED**: UnboundLocalError in snapshot_tools.py (removed local imports)

### Medium Priority
3. Fix snapshot tool test mocking issues
4. Resolve NumPy compatibility warnings

### Low Priority
5. Add tests for QueryPlannerTool
6. Add tests for SemanticLoader
7. Improve DateRangeParser coverage

