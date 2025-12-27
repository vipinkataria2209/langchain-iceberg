"""Basic usage example for LangChain Iceberg Toolkit."""

from langchain_iceberg import IcebergToolkit

# Example 1: Initialize toolkit with REST catalog
toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://my-warehouse",
    },
)

# Get available tools
tools = toolkit.get_tools()
print(f"Available tools: {[tool.name for tool in tools]}")

# Example 2: Use tools directly
list_namespaces_tool = tools[0]  # iceberg_list_namespaces
result = list_namespaces_tool.run({})
print(f"\nNamespaces:\n{result}")

# Example 3: With LangChain agent
# from langchain_openai import ChatOpenAI
# from langchain.agents import create_react_agent
#
# llm = ChatOpenAI(model="gpt-4")
# agent = create_react_agent(llm, tools)
#
# result = agent.invoke({
#     "input": "Show me the top 10 orders by amount from the sales.orders table"
# })
# print(result)

