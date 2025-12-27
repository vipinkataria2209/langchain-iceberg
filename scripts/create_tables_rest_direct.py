#!/usr/bin/env python3
"""Create tables directly via REST API using PyIceberg to generate metadata."""

import warnings
import json
import requests
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

warnings.filterwarnings('ignore')

REST_URL = "http://localhost:8181"

print("=" * 70)
print("CREATING TABLES VIA REST API")
print("=" * 70)

# Create namespace
print("\n[1/3] Creating namespace...")
try:
    response = requests.post(
        f"{REST_URL}/v1/namespaces",
        json={"namespace": ["test"]},
        headers={"Content-Type": "application/json"}
    )
    if response.status_code in [200, 201, 409]:
        print("✅ Namespace ready")
    else:
        print(f"⚠️  Namespace: {response.status_code}")
except Exception as e:
    print(f"⚠️  Namespace: {e}")

# Use PyIceberg to create schema, then use REST API
print("\n[2/3] Preparing table schemas...")

# Create a temporary file-based catalog to generate metadata
from tempfile import mkdtemp
import os

temp_dir = mkdtemp()
print(f"Using temp catalog: {temp_dir}")

try:
    temp_catalog = load_catalog(
        name="file",
        type="filesystem",
        warehouse=f"file://{temp_dir}",
    )
    
    # Create customers schema
    customers_schema = Schema(
        NestedField(1, "customer_id", LongType(), required=True),
        NestedField(2, "customer_name", StringType(), required=True),
        NestedField(3, "email", StringType(), required=False),
        NestedField(4, "customer_segment", StringType(), required=False),
        NestedField(5, "registration_date", TimestampType(), required=False),
        NestedField(6, "country", StringType(), required=False),
    )
    
    # Create table in temp catalog to get metadata
    temp_table = temp_catalog.create_table(
        identifier=("test", "customers"),
        schema=customers_schema,
    )
    
    # Get metadata location
    metadata_location = temp_table.metadata_location()
    print(f"✅ Generated metadata for customers table")
    print(f"   Metadata: {metadata_location[:80]}...")
    
    # Now register with REST catalog
    print("\n[3/3] Registering with REST catalog...")
    
    # Read metadata file
    metadata_path = metadata_location.replace("file://", "")
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Create table via REST API
    table_request = {
        "name": ["test", "customers"],
        "metadata": metadata
    }
    
    response = requests.post(
        f"{REST_URL}/v1/namespaces/test/tables",
        json=table_request,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code in [200, 201]:
        print("✅ Customers table registered in REST catalog")
    else:
        print(f"⚠️  Registration: {response.status_code} - {response.text[:200]}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
print("Note: This is a complex approach.")
print("For production, use real AWS S3 or wait for Spark JARs to load properly.")
print("=" * 70)

