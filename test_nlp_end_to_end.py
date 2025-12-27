#!/usr/bin/env python3
"""Test end-to-end NLP agent flow with OpenAI."""

import os
import warnings
warnings.filterwarnings('ignore')

# Set OpenAI API key
OPENAI_KEY = "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"
os.environ["OPENAI_API_KEY"] = OPENAI_KEY

print("=" * 70)
print("END-TO-END NLP AGENT TESTING")
print("=" * 70)

from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub

# Initialize toolkit
print("\n[1/4] Initializing Iceberg Toolkit...")
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
)
print("✅ Toolkit initialized")

# Get tools
print("\n[2/4] Getting tools...")
tools = toolkit.get_tools()
print(f"✅ Found {len(tools)} tools:")
for i, tool in enumerate(tools, 1):
    print(f"  {i}. {tool.name}")

# Check if tables exist
print("\n[3/4] Checking for tables...")
list_tables = next(t for t in tools if t.name == "iceberg_list_tables")
tables_result = list_tables.run({"namespace": "test"})
print(f"Tables status: {tables_result[:100]}...")

if "empty" in tables_result.lower() or len(tables_result) < 50:
    print("⚠️  No tables found - will test catalog operations only")
    has_tables = False
else:
    print("✅ Tables found - will test full query flow")
    has_tables = True

# Initialize LLM
print("\n[4/4] Initializing OpenAI agent...")
try:
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    print("✅ OpenAI LLM initialized")
    
    # Get prompt template
    try:
        prompt = hub.pull("hwchase17/react")
        print("✅ Prompt template loaded")
    except:
        print("⚠️  Using default prompt")
        from langchain.prompts import PromptTemplate
        prompt = PromptTemplate.from_template("""
You are a helpful assistant that can query Iceberg tables using the available tools.

Use the following tools to answer questions:
{tools}

Question: {input}
{agent_scratchpad}
""")
    
    # Create agent
    agent = create_react_agent(llm, tools, prompt)
    executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
    print("✅ Agent created")
    
    # Test queries
    print("\n" + "=" * 70)
    print("TESTING NLP QUERIES")
    print("=" * 70)
    
    # Test 1: List namespaces
    print("\n[Query 1] What namespaces are available?")
    try:
        result = executor.invoke({"input": "What namespaces are available in the catalog?"})
        print(f"✅ Result: {result['output'][:200]}...")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 2: List tables (if namespace exists)
    print("\n[Query 2] What tables are in the test namespace?")
    try:
        result = executor.invoke({"input": "What tables are in the test namespace?"})
        print(f"✅ Result: {result['output'][:200]}...")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 3: Query table (if tables exist)
    if has_tables:
        print("\n[Query 3] Show me some customer data")
        try:
            result = executor.invoke({"input": "Show me the first 5 customers from the customers table"})
            print(f"✅ Result: {result['output'][:300]}...")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print("\n[Query 4] Filter query")
        try:
            result = executor.invoke({"input": "How many orders have status 'completed'?"})
            print(f"✅ Result: {result['output'][:300]}...")
        except Exception as e:
            print(f"❌ Error: {e}")
    else:
        print("\n[Query 3-4] Skipped - no tables available")
    
    print("\n" + "=" * 70)
    print("✅ END-TO-END NLP TESTING COMPLETE")
    print("=" * 70)
    
except Exception as e:
    print(f"❌ Failed to initialize agent: {e}")
    import traceback
    traceback.print_exc()

