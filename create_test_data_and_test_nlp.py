#!/usr/bin/env python3
"""
Create test data and test NLP queries.
Uses PyIceberg to create tables and insert sample data, then tests NLP queries.
"""

import os
import sys
from pathlib import Path

# Suppress NumPy warnings
import warnings
warnings.filterwarnings('ignore')

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform
    import pyarrow as pa
    from datetime import datetime
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def create_test_tables():
    """Create test tables with sample data."""
    print("=" * 70)
    print("Creating Test Tables with Sample Data")
    print("=" * 70)
    
    try:
        catalog = load_catalog(
            name="rest",
            type="rest",
            uri="http://localhost:8181",
            warehouse="s3://warehouse/wh/",
            s3_endpoint="http://localhost:9000",
            s3_access_key_id="admin",
            s3_secret_access_key="password",
            s3_path_style_access="true",
        )
        
        # Create namespace if needed
        try:
            catalog.create_namespace("test")
            print("✅ Created 'test' namespace")
        except Exception:
            print("✅ 'test' namespace already exists")
        
        # Create orders table
        print("\n[1/3] Creating orders table...")
        try:
            orders_schema = Schema(
                NestedField(1, "order_id", LongType(), required=False),
                NestedField(2, "customer_id", LongType(), required=False),
                NestedField(3, "product_id", LongType(), required=False),
                NestedField(4, "order_date", TimestampType(), required=False),
                NestedField(5, "quantity", LongType(), required=False),
                NestedField(6, "amount", DoubleType(), required=False),
                NestedField(7, "status", StringType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="order_date_day")
            )
            
            try:
                catalog.drop_table(("test", "orders"))
            except Exception:
                pass
            
            table = catalog.create_table(
                identifier=("test", "orders"),
                schema=orders_schema,
                partition_spec=partition_spec,
            )
            
            # Insert sample data
            sample_data = pa.Table.from_arrays([
                pa.array([1001, 1002, 1003, 1004, 1005], type=pa.int64()),  # order_id
                pa.array([1, 2, 1, 3, 2], type=pa.int64()),  # customer_id
                pa.array([101, 102, 103, 101, 104], type=pa.int64()),  # product_id
                pa.array([
                    datetime(2024, 12, 1, 10, 0, 0),
                    datetime(2024, 12, 1, 11, 0, 0),
                    datetime(2024, 12, 2, 9, 0, 0),
                    datetime(2024, 12, 2, 14, 0, 0),
                    datetime(2024, 12, 3, 16, 0, 0),
                ], type=pa.timestamp('us')),  # order_date
                pa.array([1, 2, 1, 1, 3], type=pa.int64()),  # quantity
                pa.array([999.99, 59.98, 79.99, 999.99, 449.97], type=pa.float64()),  # amount
                pa.array(["completed", "completed", "pending", "completed", "completed"], type=pa.string()),  # status
            ], names=["order_id", "customer_id", "product_id", "order_date", "quantity", "amount", "status"])
            
            table.append(sample_data)
            print("✅ Created 'orders' table with 5 sample rows")
            
        except Exception as e:
            print(f"⚠️  Error creating orders table: {e}")
            import traceback
            traceback.print_exc()
        
        # Create customers table
        print("\n[2/3] Creating customers table...")
        try:
            customers_schema = Schema(
                NestedField(1, "customer_id", LongType(), required=False),
                NestedField(2, "customer_name", StringType(), required=False),
                NestedField(3, "email", StringType(), required=False),
                NestedField(4, "customer_segment", StringType(), required=False),
            )
            
            try:
                catalog.drop_table(("test", "customers"))
            except Exception:
                pass
            
            table = catalog.create_table(
                identifier=("test", "customers"),
                schema=customers_schema,
            )
            
            # Insert sample data
            sample_data = pa.Table.from_arrays([
                pa.array([1, 2, 3], type=pa.int64()),  # customer_id
                pa.array(["John Doe", "Jane Smith", "Bob Johnson"], type=pa.string()),  # customer_name
                pa.array(["john@example.com", "jane@example.com", "bob@example.com"], type=pa.string()),  # email
                pa.array(["new", "returning", "vip"], type=pa.string()),  # customer_segment
            ], names=["customer_id", "customer_name", "email", "customer_segment"])
            
            table.append(sample_data)
            print("✅ Created 'customers' table with 3 sample rows")
            
        except Exception as e:
            print(f"⚠️  Error creating customers table: {e}")
        
        print("\n[3/3] Verifying tables...")
        tables = list(catalog.list_tables(("test",)))
        print(f"✅ Tables in 'test' namespace: {[t[-1] for t in tables]}")
        
        return catalog
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_nlp_queries():
    """Test NLP queries with the created data."""
    print("\n" + "=" * 70)
    print("Testing NLP Queries")
    print("=" * 70)
    
    try:
        from langchain_iceberg import IcebergToolkit
        
        toolkit = IcebergToolkit(
            catalog_name="rest",
            catalog_config={
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://warehouse/wh/",
                "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            }
        )
        
        tools = toolkit.get_tools()
        print(f"✅ Toolkit initialized with {len(tools)} tools")
        
        # Test direct tool usage first
        print("\n[1/2] Testing Direct Tool Usage:")
        print("-" * 70)
        
        query_tool = next((t for t in tools if t.name == "iceberg_query"), None)
        if query_tool:
            print("\n❓ Query: Get all orders")
            result = query_tool.run({
                "table_id": "test.orders",
                "limit": 10
            })
            print(f"✅ Result:\n{result[:400]}...")
            
            print("\n❓ Query: Get orders with status='completed'")
            result = query_tool.run({
                "table_id": "test.orders",
                "filters": "status = 'completed'",
                "limit": 10
            })
            print(f"✅ Result:\n{result[:400]}...")
        
        # Test with LLM agent if API key available
        if os.getenv("OPENAI_API_KEY"):
            print("\n[2/2] Testing LLM Agent (NLP Queries):")
            print("-" * 70)
            
            try:
                from langchain_openai import ChatOpenAI
                from langchain.agents import create_react_agent, AgentExecutor
                from langchain import hub
                
                llm = ChatOpenAI(model="gpt-4", temperature=0)
                prompt = hub.pull("hwchase17/react")
                agent = create_react_agent(llm, tools, prompt=prompt)
                agent_executor = AgentExecutor(
                    agent=agent,
                    tools=tools,
                    verbose=False,
                    handle_parsing_errors=True,
                    max_iterations=5
                )
                
                test_queries = [
                    "What namespaces are available?",
                    "List all tables in the test namespace",
                    "Show me all orders",
                    "How many completed orders are there?",
                    "What is the total amount of all orders?",
                ]
                
                for query in test_queries:
                    print(f"\n❓ NLP Query: {query}")
                    try:
                        result = agent_executor.invoke({"input": query})
                        print(f"✅ Answer: {result['output'][:300]}...")
                    except Exception as e:
                        print(f"❌ Error: {str(e)[:200]}")
                        
            except Exception as e:
                print(f"⚠️  LLM agent test failed: {e}")
        else:
            print("\n[2/2] LLM Agent Test:")
            print("⚠️  OPENAI_API_KEY not set - skipping LLM agent test")
            print("   Set it to test full NLP queries:")
            print("   export OPENAI_API_KEY='your-key-here'")
        
    except Exception as e:
        print(f"❌ Error testing NLP queries: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function."""
    catalog = create_test_tables()
    if catalog:
        test_nlp_queries()
    
    print("\n" + "=" * 70)
    print("✅ Test Complete")
    print("=" * 70)


if __name__ == "__main__":
    main()

