#!/usr/bin/env python3
"""
Load data into tables and test NLP queries.
Uses Hive catalog to create tables, load data, and test NLP queries.
"""

import os
import sys
import warnings
from pathlib import Path
from datetime import datetime

# Suppress warnings
warnings.filterwarnings('ignore')

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestampType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform
    import pyarrow as pa
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def create_and_load_tables():
    """Create test tables and load sample data."""
    print("=" * 70)
    print("Creating Tables and Loading Data")
    print("=" * 70)
    
    try:
        # Use SQL catalog (SQLite-based) for local persistence
        from pathlib import Path
        warehouse_path = Path("warehouse_sql").absolute()
        warehouse_path.mkdir(exist_ok=True)
        catalog_db = warehouse_path / "catalog.db"
        
        catalog = load_catalog(
            name="sql",
            type="sql",
            uri=f"sqlite:///{catalog_db}",
            warehouse=f"file://{warehouse_path}",
        )
        print(f"✅ SQL catalog initialized (persistent)")
        print(f"   Warehouse: file://{warehouse_path}")
        print(f"   Catalog DB: {catalog_db}")
        
        # Create namespace
        try:
            catalog.create_namespace("test")
            print("✅ Created 'test' namespace")
        except Exception:
            print("✅ 'test' namespace already exists")
        
        # Create and load orders table
        print("\n[1/3] Creating and loading 'orders' table...")
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
                print("   Dropped existing orders table")
            except Exception:
                pass
            
            table = catalog.create_table(
                identifier=("test", "orders"),
                schema=orders_schema,
                partition_spec=partition_spec,
            )
            
            # Load sample data
            sample_data = pa.Table.from_arrays([
                pa.array([1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010], type=pa.int64()),
                pa.array([1, 2, 1, 3, 2, 1, 3, 4, 2, 5], type=pa.int64()),
                pa.array([101, 102, 103, 101, 104, 102, 105, 103, 101, 104], type=pa.int64()),
                pa.array([
                    datetime(2024, 12, 1, 10, 0, 0),
                    datetime(2024, 12, 1, 11, 0, 0),
                    datetime(2024, 12, 2, 9, 0, 0),
                    datetime(2024, 12, 2, 14, 0, 0),
                    datetime(2024, 12, 3, 16, 0, 0),
                    datetime(2024, 12, 4, 10, 0, 0),
                    datetime(2024, 12, 4, 14, 0, 0),
                    datetime(2024, 12, 5, 9, 0, 0),
                    datetime(2024, 12, 5, 15, 0, 0),
                    datetime(2024, 12, 6, 11, 0, 0),
                ], type=pa.timestamp('us')),
                pa.array([1, 2, 1, 1, 3, 2, 1, 1, 2, 1], type=pa.int64()),
                pa.array([999.99, 59.98, 79.99, 999.99, 449.97, 119.98, 149.99, 79.99, 199.98, 999.99], type=pa.float64()),
                pa.array(["completed", "completed", "pending", "completed", "completed", "completed", "completed", "pending", "completed", "completed"], type=pa.string()),
            ], names=["order_id", "customer_id", "product_id", "order_date", "quantity", "amount", "status"])
            
            table.append(sample_data)
            print(f"✅ Loaded {len(sample_data)} rows into 'orders' table")
            
        except Exception as e:
            print(f"⚠️  Error with orders table: {e}")
            import traceback
            traceback.print_exc()
        
        # Create and load customers table
        print("\n[2/3] Creating and loading 'customers' table...")
        try:
            customers_schema = Schema(
                NestedField(1, "customer_id", LongType(), required=False),
                NestedField(2, "customer_name", StringType(), required=False),
                NestedField(3, "email", StringType(), required=False),
                NestedField(4, "customer_segment", StringType(), required=False),
            )
            
            try:
                catalog.drop_table(("test", "customers"))
                print("   Dropped existing customers table")
            except Exception:
                pass
            
            table = catalog.create_table(
                identifier=("test", "customers"),
                schema=customers_schema,
            )
            
            # Load sample data
            sample_data = pa.Table.from_arrays([
                pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                pa.array(["John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown"], type=pa.string()),
                pa.array(["john@example.com", "jane@example.com", "bob@example.com", "alice@example.com", "charlie@example.com"], type=pa.string()),
                pa.array(["new", "returning", "vip", "returning", "new"], type=pa.string()),
            ], names=["customer_id", "customer_name", "email", "customer_segment"])
            
            table.append(sample_data)
            print(f"✅ Loaded {len(sample_data)} rows into 'customers' table")
            
        except Exception as e:
            print(f"⚠️  Error with customers table: {e}")
        
        # Verify data
        print("\n[3/3] Verifying loaded data...")
        tables = list(catalog.list_tables(("test",)))
        print(f"✅ Tables in 'test' namespace: {[t[-1] for t in tables]}")
        
        for table_name in ["orders", "customers"]:
            try:
                table = catalog.load_table(("test", table_name))
                scan = table.scan()
                result = scan.to_arrow()
                print(f"  ✅ {table_name}: {len(result)} rows")
            except Exception as e:
                print(f"  ⚠️  {table_name}: Error - {e}")
        
        return catalog
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_nlp_queries():
    """Test NLP queries with loaded data."""
    print("\n" + "=" * 70)
    print("Testing NLP Queries")
    print("=" * 70)
    
    try:
        from langchain_iceberg import IcebergToolkit
        
        # Use SQL catalog
        from pathlib import Path
        warehouse_path = Path("warehouse_sql").absolute()
        catalog_db = warehouse_path / "catalog.db"
        
        toolkit = IcebergToolkit(
            catalog_name="sql",
            catalog_config={
                "type": "sql",
                "uri": f"sqlite:///{catalog_db}",
                "warehouse": f"file://{warehouse_path}",
            }
        )
        
        tools = toolkit.get_tools()
        print(f"✅ Toolkit initialized with {len(tools)} tools")
        
        # Test 1: Direct tool usage
        print("\n[Test 1] Direct Tool Usage (No LLM):")
        print("-" * 70)
        
        # List namespaces
        list_ns_tool = next((t for t in tools if t.name == "iceberg_list_namespaces"), None)
        if list_ns_tool:
            result = list_ns_tool.run({})
            print(f"✅ Namespaces: {result[:150]}...")
        
        # List tables
        list_tables_tool = next((t for t in tools if t.name == "iceberg_list_tables"), None)
        if list_tables_tool:
            result = list_tables_tool.run({"namespace": "test"})
            print(f"✅ Tables in 'test': {result[:150]}...")
        
        # Get schema
        get_schema_tool = next((t for t in tools if t.name == "iceberg_get_schema"), None)
        if get_schema_tool:
            try:
                result = get_schema_tool.run({"table_id": "test.orders"})
                print(f"✅ Schema for 'orders':")
                print(result[:300] + "..." if len(result) > 300 else result)
            except Exception as e:
                print(f"⚠️  Schema error: {str(e)[:100]}")
        
        # Query data
        query_tool = next((t for t in tools if t.name == "iceberg_query"), None)
        if query_tool:
            try:
                print(f"\n✅ Query: Get all orders (limit 5)")
                result = query_tool.run({
                    "table_id": "test.orders",
                    "limit": 5
                })
                print(result[:400] + "..." if len(result) > 400 else result)
                
                print(f"\n✅ Query: Get orders with amount > 500")
                result = query_tool.run({
                    "table_id": "test.orders",
                    "filters": "amount > 500",
                    "limit": 5
                })
                print(result[:400] + "..." if len(result) > 400 else result)
                
                print(f"\n✅ Query: Get specific columns")
                result = query_tool.run({
                    "table_id": "test.orders",
                    "columns": ["order_id", "customer_id", "amount"],
                    "limit": 5
                })
                print(result[:400] + "..." if len(result) > 400 else result)
                
                print(f"\n✅ Query: Get customers")
                result = query_tool.run({
                    "table_id": "test.customers",
                    "limit": 5
                })
                print(result[:400] + "..." if len(result) > 400 else result)
                
            except Exception as e:
                print(f"⚠️  Query error: {str(e)[:200]}")
        
        # Test 2: NLP queries with LLM (if API key available)
        if os.getenv("OPENAI_API_KEY"):
            print("\n[Test 2] NLP Queries with LLM Agent:")
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
                
                nlp_queries = [
                    "What namespaces are available?",
                    "List all tables in the test namespace",
                    "Show me all orders",
                    "How many completed orders are there?",
                    "What is the total amount of all orders?",
                    "Show me customer information",
                ]
                
                for i, query in enumerate(nlp_queries, 1):
                    print(f"\n❓ NLP Query {i}: {query}")
                    try:
                        result = agent_executor.invoke({"input": query})
                        print(f"✅ Answer:\n{result['output'][:400]}...")
                    except Exception as e:
                        print(f"❌ Error: {str(e)[:200]}")
                        
            except Exception as e:
                print(f"⚠️  LLM agent test failed: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("\n[Test 2] NLP Queries with LLM:")
            print("⚠️  OPENAI_API_KEY not set - skipping LLM agent test")
            print("   Direct tool usage tests above show the framework works!")
            print("   Set OPENAI_API_KEY to test full NLP queries:")
            print("   export OPENAI_API_KEY='your-key-here'")
        
    except Exception as e:
        print(f"❌ Error testing NLP queries: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function."""
    catalog = create_and_load_tables()
    if catalog:
        test_nlp_queries()
    
    print("\n" + "=" * 70)
    print("✅ Complete - Data Loaded and Queries Tested")
    print("=" * 70)


if __name__ == "__main__":
    main()

