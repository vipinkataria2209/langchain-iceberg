"""Example using semantic layer with YAML configuration."""

from langchain_iceberg import IcebergToolkit

# Initialize toolkit with semantic YAML
toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://my-warehouse",
    },
    semantic_yaml="examples/semantic.yaml",  # Path to YAML file
)

# Get tools (now includes auto-generated metric tools)
tools = toolkit.get_tools()
print(f"Available tools: {[tool.name for tool in tools]}")

# Find metric tools
metric_tools = [t for t in tools if t.name.startswith("get_")]
print(f"\nMetric tools: {[t.name for t in metric_tools]}")

# Use a metric tool directly
if metric_tools:
    revenue_tool = next(t for t in metric_tools if "revenue" in t.name)
    
    # Get total revenue
    result = revenue_tool.run({"date_range": "Q4_2024"})
    print(f"\n{result}")
    
    # Get revenue by date range
    result = revenue_tool.run({"date_range": "last_30_days"})
    print(f"\n{result}")

# Example with LangChain agent
# from langchain_openai import ChatOpenAI
# from langchain.agents import create_react_agent
#
# llm = ChatOpenAI(model="gpt-4")
# agent = create_react_agent(llm, tools)
#
# # Business question (no SQL needed!)
# result = agent.invoke({
#     "input": "What was Q4 2024 revenue by customer segment?"
# })
# print(result)

