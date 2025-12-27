"""Example using time-travel features."""

from langchain_iceberg import IcebergToolkit

# Initialize toolkit
toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://my-warehouse",
    },
)

tools = toolkit.get_tools()

# Find time-travel tools
snapshot_tool = next(t for t in tools if t.name == "iceberg_snapshots")
time_travel_tool = next(t for t in tools if t.name == "iceberg_time_travel")

# List snapshots for a table
print("Getting snapshots...")
snapshots = snapshot_tool.run({"table_id": "sales.orders"})
print(snapshots)

# Query data at a specific timestamp
print("\nQuerying historical data...")
historical_data = time_travel_tool.run({
    "table_id": "sales.orders",
    "timestamp": "2024-12-01T00:00:00",
    "limit": 10,
})
print(historical_data)

# Query data at a specific snapshot
print("\nQuerying by snapshot ID...")
snapshot_data = time_travel_tool.run({
    "table_id": "sales.orders",
    "snapshot_id": 1234567890,  # Replace with actual snapshot ID
    "limit": 10,
})
print(snapshot_data)

# Example with LangChain agent for comparison queries
# from langchain_openai import ChatOpenAI
# from langchain.agents import create_react_agent
#
# llm = ChatOpenAI(model="gpt-4")
# agent = create_react_agent(llm, tools)
#
# result = agent.invoke({
#     "input": "Compare this month's revenue to the same period last year using time-travel"
# })
# print(result)

