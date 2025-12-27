"""Example using query planner tool with LLM."""

from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI

# Initialize toolkit with LLM for query planning
llm = ChatOpenAI(model="gpt-4")

toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://my-warehouse",
    },
    llm=llm,  # Provide LLM for query planning
)

tools = toolkit.get_tools()

# Find query planner tool
planner_tool = next(t for t in tools if t.name == "iceberg_plan_query")

# Plan a complex query
print("Planning query...")
plan = planner_tool.run({
    "question": "Find high-value orders from last quarter where the customer is a VIP"
})
print(plan)

# Use the plan to execute the query
# The agent would use this plan to construct the actual query

