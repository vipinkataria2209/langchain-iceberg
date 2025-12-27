"""
Complete example showing how to use langchain-iceberg package.

This demonstrates:
1. Basic toolkit setup
2. Using tools directly
3. Using with LangChain agents
4. Semantic layer integration
5. Time-travel queries
"""

from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor

# ============================================================================
# STEP 1: Initialize Toolkit
# ============================================================================

print("=" * 70)
print("STEP 1: Initializing Iceberg Toolkit")
print("=" * 70)

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
    },
    # Optional: Add semantic layer
    # semantic_yaml="examples/semantic.yaml",
)

print("✓ Toolkit initialized")

# ============================================================================
# STEP 2: Get Tools
# ============================================================================

print("\n" + "=" * 70)
print("STEP 2: Getting Available Tools")
print("=" * 70)

tools = toolkit.get_tools()
print(f"✓ Found {len(tools)} tools:")
for i, tool in enumerate(tools, 1):
    print(f"  {i}. {tool.name}")

# ============================================================================
# STEP 3: Use Tools Directly
# ============================================================================

print("\n" + "=" * 70)
print("STEP 3: Using Tools Directly")
print("=" * 70)

# List namespaces
print("\n3.1 Listing namespaces...")
list_ns = next(t for t in tools if t.name == "iceberg_list_namespaces")
result = list_ns.run({})
print(result)

# List tables
print("\n3.2 Listing tables...")
list_tables = next(t for t in tools if t.name == "iceberg_list_tables")
result = list_tables.run({"namespace": "test"})
print(result)

# Get schema (if tables exist)
print("\n3.3 Getting schema...")
try:
    get_schema = next(t for t in tools if t.name == "iceberg_get_schema")
    result = get_schema.run({"table_id": "test.customers"})
    print(result)
except Exception as e:
    print(f"  (Table may not exist: {e})")

# Query data (if tables exist)
print("\n3.4 Querying data...")
try:
    query = next(t for t in tools if t.name == "iceberg_query")
    result = query.run({
        "table_id": "test.orders",
        "columns": ["order_id", "customer_id", "amount"],
        "filters": "status = 'completed'",
        "limit": 5
    })
    print(result)
except Exception as e:
    print(f"  (Table may not exist: {e})")

# ============================================================================
# STEP 4: Use with LangChain Agent
# ============================================================================

print("\n" + "=" * 70)
print("STEP 4: Using with LangChain Agent")
print("=" * 70)

# Note: Requires OPENAI_API_KEY environment variable
try:
    llm = ChatOpenAI(model="gpt-4", temperature=0)
    
    agent = create_react_agent(llm, tools)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True
    )
    
    print("\n4.1 Asking agent a question...")
    result = agent_executor.invoke({
        "input": "What tables are available in the test namespace?"
    })
    print(f"\nAgent Response:\n{result['output']}")
    
    print("\n4.2 Complex query...")
    result = agent_executor.invoke({
        "input": "Show me the schema for the customers table if it exists"
    })
    print(f"\nAgent Response:\n{result['output']}")
    
except Exception as e:
    print(f"  (LangChain agent setup failed: {e})")
    print("  Note: Set OPENAI_API_KEY environment variable to use agents")

# ============================================================================
# STEP 5: Semantic Layer (if YAML provided)
# ============================================================================

print("\n" + "=" * 70)
print("STEP 5: Semantic Layer Example")
print("=" * 70)

try:
    # Initialize with semantic YAML
    semantic_toolkit = IcebergToolkit(
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
        },
        semantic_yaml="examples/semantic.yaml",
    )
    
    semantic_tools = semantic_toolkit.get_tools()
    metric_tools = [t for t in semantic_tools if t.name.startswith("get_")]
    
    print(f"✓ Semantic toolkit initialized")
    print(f"✓ Found {len(metric_tools)} metric tools:")
    for tool in metric_tools:
        print(f"  - {tool.name}")
    
    # Use metric tool
    if metric_tools:
        print(f"\n5.1 Using metric tool: {metric_tools[0].name}")
        # Note: This will fail if tables don't exist, but shows the API
        print("  (Tool ready to use when tables exist)")
        
except Exception as e:
    print(f"  (Semantic layer setup: {e})")

# ============================================================================
# STEP 6: Time-Travel Example
# ============================================================================

print("\n" + "=" * 70)
print("STEP 6: Time-Travel Queries")
print("=" * 70)

try:
    snapshots = next(t for t in tools if t.name == "iceberg_snapshots")
    time_travel = next(t for t in tools if t.name == "iceberg_time_travel")
    
    print("6.1 Listing snapshots...")
    result = snapshots.run({"table_id": "test.orders"})
    print(result)
    
    print("\n6.2 Time-travel query...")
    result = time_travel.run({
        "table_id": "test.orders",
        "timestamp": "2024-12-01T00:00:00",
        "limit": 5
    })
    print(result)
    
except Exception as e:
    print(f"  (Time-travel requires tables: {e})")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

print("""
✅ Toolkit Usage:

1. Initialize toolkit with catalog configuration
2. Get tools using toolkit.get_tools()
3. Use tools directly or with LangChain agents
4. Optional: Add semantic YAML for business metrics
5. Optional: Use time-travel for historical queries

Key Features:
- ✅ Native PyIceberg integration (no SQL strings)
- ✅ 6 core tools for catalog and query operations
- ✅ Auto-generated metric tools from YAML
- ✅ Time-travel and snapshot support
- ✅ Full LangChain agent compatibility

Next Steps:
- Create tables in your Iceberg catalog
- Define semantic YAML for your metrics
- Integrate with your LangChain applications
""")

