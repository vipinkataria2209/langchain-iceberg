#!/usr/bin/env python3
"""
Example: Natural Language Queries with LangChain Iceberg Toolkit

This demonstrates how users can ask questions in natural language
and get answers from their Iceberg data.
"""

import os
from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub

def main():
    print("=" * 70)
    print("Natural Language Query Example")
    print("=" * 70)
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("\n⚠️  OPENAI_API_KEY not set.")
        print("   Set it to run this example:")
        print("   export OPENAI_API_KEY='your-key-here'")
        return
    
    # 1. Initialize toolkit
    print("\n[1/3] Setting up toolkit...")
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
    
    tools = toolkit.get_tools()
    print(f"✅ Toolkit ready with {len(tools)} tools")
    
    # 2. Create agent
    print("\n[2/3] Creating LangChain agent...")
    llm = ChatOpenAI(model="gpt-4", temperature=0)
    prompt = hub.pull("hwchase17/react")
    agent = create_react_agent(llm, tools, prompt=prompt)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True,
    )
    print("✅ Agent ready")
    
    # 3. Ask questions!
    print("\n[3/3] Ask questions in natural language:")
    print("=" * 70)
    
    questions = [
        "What namespaces are available?",
        "List all tables in the test namespace",
        "Show me the schema for the orders table",
        "What are the top 10 orders by amount?",
        "How many orders are there?",
    ]
    
    for question in questions:
        print(f"\n❓ Question: {question}")
        print("-" * 70)
        try:
            result = agent_executor.invoke({"input": question})
            print(f"✅ Answer: {result['output']}")
        except Exception as e:
            print(f"❌ Error: {e}")
        print()
    
    # Interactive mode
    print("\n" + "=" * 70)
    print("Interactive Mode - Ask your own questions!")
    print("Type 'exit' to quit")
    print("=" * 70)
    
    while True:
        try:
            question = input("\n❓ Your question: ").strip()
            if question.lower() in ['exit', 'quit', 'q']:
                break
            if not question:
                continue
            
            result = agent_executor.invoke({"input": question})
            print(f"\n✅ Answer: {result['output']}")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

