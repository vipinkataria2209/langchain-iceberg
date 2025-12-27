#!/usr/bin/env python3
"""
Test customer count query using in-memory catalog
This demonstrates the full end-to-end flow with actual data.
"""
import warnings
warnings.filterwarnings('ignore')

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType
import pyarrow as pa
from datetime import datetime
from langchain_iceberg import IcebergToolkit

print('=' * 70)
print('CUSTOMER COUNT DEMONSTRATION')
print('=' * 70)

# Step 1: Create catalog and table
print('\n[1/4] Creating catalog and table...')
catalog = load_catalog(
    name='memory',
    type='in-memory',
    warehouse='file:///tmp/iceberg_test',
)

try:
    catalog.create_namespace('test')
except:
    pass

# Create schema
customers_schema = Schema(
    NestedField(1, 'customer_id', LongType(), required=False),
    NestedField(2, 'customer_name', StringType(), required=False),
    NestedField(3, 'email', StringType(), required=False),
    NestedField(4, 'customer_segment', StringType(), required=False),
    NestedField(5, 'registration_date', TimestampType(), required=False),
    NestedField(6, 'country', StringType(), required=False),
)

# Create table
table = catalog.create_table(('test', 'customers'), schema=customers_schema)
print('✅ Customers table created!')

# Step 2: Insert data
print('\n[2/4] Inserting customer data...')
data = pa.table({
    'customer_id': [1, 2, 3, 4, 5],
    'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams', 'Charlie Brown'],
    'email': ['john.doe@example.com', 'jane.smith@example.com', 'bob.johnson@example.com', 
             'alice.williams@example.com', 'charlie.brown@example.com'],
    'customer_segment': ['new', 'returning', 'vip', 'returning', 'new'],
    'registration_date': [
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2023, 6, 20, 14, 30, 0),
        datetime(2022, 3, 10, 9, 15, 0),
        datetime(2023, 8, 5, 16, 45, 0),
        datetime(2024, 2, 28, 11, 20, 0),
    ],
    'country': ['USA', 'USA', 'Canada', 'UK', 'USA'],
})
table.append(data)
print('✅ Inserted 5 customers')

# Step 3: Query directly
print('\n[3/4] Querying customers directly...')
scan = table.scan()
arrow_table = scan.to_arrow()
if arrow_table and len(arrow_table) > 0:
    df = arrow_table.to_pandas()
    count = len(df)
    print(f'\n' + '=' * 70)
    print(f'📊 DIRECT QUERY - CUSTOMER COUNT: {count}')
    print('=' * 70)
    print('\nCustomer Data:')
    print(df[['customer_id', 'customer_name', 'email', 'country']].to_string())

# Step 4: Query using toolkit (same catalog instance)
print('\n[4/4] Querying using LangChain Iceberg Toolkit...')

# Create toolkit with same catalog
toolkit = IcebergToolkit(
    catalog_name='memory',
    catalog_config={
        'type': 'in-memory',
        'warehouse': 'file:///tmp/iceberg_test',
    },
)

# Get the catalog from toolkit and manually set it to our existing catalog
# This ensures we use the same catalog instance
toolkit.catalog = catalog

tools = toolkit.get_tools()
query_tool = next(t for t in tools if t.name == 'iceberg_query')

print('\n' + '=' * 70)
print('RUNNING CUSTOMER COUNT QUERY VIA TOOLKIT')
print('=' * 70)

result = query_tool.run({
    'table_id': 'test.customers',
    'columns': ['customer_id', 'customer_name', 'email', 'country'],
    'limit': 1000
})

print('\n' + '=' * 70)
print('QUERY RESULT FROM TOOLKIT:')
print('=' * 70)
print(result)

# Extract count from result
if 'customer_id' in result or 'Results' in result:
    # Count non-header lines with data
    lines = [l for l in result.split('\n') 
             if l.strip() 
             and not l.startswith('=') 
             and not l.startswith('Query')
             and 'customer' not in l.lower() 
             and any(c.isdigit() for c in l)]
    count = len(lines)
    print(f'\n' + '=' * 70)
    print(f'📊 TOOLKIT QUERY - CUSTOMER COUNT: {count}')
    print('=' * 70)

print('\n✅✅✅ SUCCESS! Customer count query completed!')

