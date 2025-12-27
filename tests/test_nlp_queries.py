#!/usr/bin/env python3
"""
Test natural language queries with LangChain agents.

This demonstrates the full end-to-end flow:
1. User asks question in natural language
2. Agent selects appropriate tools
3. Tools execute queries
4. Agent formats response
"""

import os
from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub

print("=" * 70)
print("NATURAL LANGUAGE QUERY TESTING")
print("=" * 70)

# Check if OpenAI API key is available
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("\n⚠️  OPENAI_API_KEY not set. Skipping agent tests.")
    print("   Set OPENAI_API_KEY to test natural language queries.")
    print("\n   Testing direct tool usage instead...")
    
    # Test direct tool usage as fallback
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
    print(f"\n✅ Toolkit initialized with {len(tools)} tools")
    
    # Simulate what an agent would do
    print("\n[Simulated Agent Behavior]")
    print("Question: 'What namespaces are available?'")
    
    list_ns = next(t for t in tools if t.name == "iceberg_list_namespaces")
    result = list_ns.run({})
    print(f"Tool: {list_ns.name}")
    print(f"Response: {result[:200]}...")
    
    print("\n✅ Direct tool usage works - agent would use this same way")
    exit(0)

# Initialize toolkit
print("\n[1/5] Initializing toolkit...")
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

# Initialize LLM
print("\n[2/5] Initializing LLM...")
try:
    llm = ChatOpenAI(model="gpt-4", temperature=0)
    print("✅ LLM initialized (GPT-4)")
except Exception as e:
    print(f"❌ LLM initialization failed: {e}")
    exit(1)

# Create agent
print("\n[3/5] Creating LangChain agent...")
try:
    # Get the ReAct prompt
    prompt = hub.pull("hwchase17/react")
    agent = create_react_agent(llm, tools, prompt=prompt)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=5
    )
    print("✅ Agent created successfully")
except Exception as e:
    print(f"❌ Agent creation failed: {e}")
    exit(1)

# Test natural language queries
print("\n[4/5] Testing natural language queries...")
print("=" * 70)

test_questions = [
    "What namespaces are available in the catalog?",
    "List all tables in the test namespace",
    "Show me the schema for the customers table if it exists",
]

for i, question in enumerate(test_questions, 1):
    print(f"\n--- Test {i}: {question} ---")
    try:
        result = agent_executor.invoke({"input": question})
        print(f"\n✅ Question answered successfully")
        print(f"Response: {result['output'][:300]}...")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Test with semantic layer
print("\n[5/5] Testing with semantic layer...")
try:
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
    semantic_agent = create_react_agent(llm, semantic_tools, prompt=prompt)
    semantic_executor = AgentExecutor(
        agent=semantic_agent,
        tools=semantic_tools,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=5
    )
    
    print("✅ Semantic toolkit initialized")
    print(f"✅ Found {len(semantic_tools)} tools (including metric tools)")
    
    # Test business question
    business_question = "What was the total revenue for Q4 2024?"
    print(f"\nBusiness Question: {business_question}")
    result = semantic_executor.invoke({"input": business_question})
    print(f"✅ Business question answered")
    print(f"Response: {result['output'][:300]}...")
    
except Exception as e:
    print(f"⚠️  Semantic layer test: {e}")

print("\n" + "=" * 70)
print("✅ NATURAL LANGUAGE QUERY TESTING COMPLETE")
print("=" * 70)

