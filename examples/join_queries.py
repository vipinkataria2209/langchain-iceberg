#!/usr/bin/env python3
"""
Example: Using DuckDB for JOIN queries with LangChain Iceberg Toolkit

This demonstrates how to use the new DuckDB integration to perform
multi-table JOINs that aren't possible with single-table PyIceberg scans.
"""

import os
from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub

def main():
    print("=" * 70)
    print("DuckDB JOIN Queries Example")
    print("=" * 70)
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("\n⚠️  OPENAI_API_KEY not set.")
        print("   Set it to run this example:")
        print("   export OPENAI_API_KEY='your-key-here'")
        return
    
    # 1. Initialize toolkit with DuckDB enabled
    print("\n[1/3] Setting up toolkit with DuckDB...")
    toolkit = IcebergToolkit(
        catalog_name="sql",
        catalog_config={
            "type": "sql",
            "uri": "sqlite:///warehouse_sql/catalog.db",
            "warehouse": "file://warehouse_sql",
        },
        enable_sql_queries=True,  # Enable DuckDB for JOINs
    )
    
    tools = toolkit.get_tools()
    print(f"✅ Toolkit ready with {len(tools)} tools")
    
    # Check if DuckDB tool is available
    sql_tool = next((t for t in tools if t.name == "iceberg_sql_query"), None)
    if sql_tool:
        print("✅ DuckDB SQL query tool available")
    else:
        print("⚠️  DuckDB tool not available (DuckDB may not be installed)")
    
    # 2. Create agent
    print("\n[2/3] Creating LangChain agent...")
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
    print("✅ Agent ready")
    
    # 3. Test JOIN queries
    print("\n[3/3] Testing JOIN queries:")
    print("=" * 70)
    
    # Example 1: Direct SQL query
    if sql_tool:
        print("\n[Example 1] Direct SQL JOIN query:")
        print("-" * 70)
        try:
            result = sql_tool.run({
                "sql_query": """
                    SELECT o.order_id, c.customer_name, o.amount, o.status
                    FROM test.orders o
                    JOIN test.customers c ON o.customer_id = c.customer_id
                    WHERE o.amount > 500
                    LIMIT 10
                """
            })
            print(result)
        except Exception as e:
            print(f"❌ Error: {str(e)[:300]}")
    
    # Example 2: NLP query that requires JOIN
    print("\n[Example 2] NLP query requiring JOIN:")
    print("-" * 70)
    questions = [
        "Show me orders with customer names",
        "What is the total amount per customer?",
        "List all customers and their order counts",
    ]
    
    for question in questions:
        print(f"\n❓ Question: {question}")
        try:
            result = agent_executor.invoke({"input": question})
            print(f"✅ Answer:\n{result['output']}\n")
        except Exception as e:
            print(f"❌ Error: {str(e)[:300]}\n")
    
    print("=" * 70)
    print("✅ Examples Complete")
    print("=" * 70)

if __name__ == "__main__":
    main()

