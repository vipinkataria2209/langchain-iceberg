# LangChain Iceberg Toolkit

[![PyPI version](https://badge.fury.io/py/langchain-iceberg.svg)](https://badge.fury.io/py/langchain-iceberg)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A native LangChain integration for Apache Iceberg that enables AI-powered natural language queries over your data lakes. Built with PyIceberg for direct API access (not SQL strings) and featuring Iceberg-specific capabilities like time-travel, snapshots, and partition-aware queries.

## Features

- 🚀 **Native PyIceberg Integration** - Direct API access, not SQL strings
- 🔍 **Iceberg-Specific Tools** - Snapshots, time-travel, partition-aware queries
- 📊 **Optional Semantic Layer** - YAML-driven metrics and dimensions
- 💬 **Zero SQL Required** - Natural language to Iceberg queries
- 🏢 **Enterprise-Ready** - Access control, PII protection, audit logging

## Installation

### Using pip (standard)

```bash
pip install langchain-iceberg
```

For semantic layer support:

```bash
pip install langchain-iceberg[semantic]
```

### Using uv (recommended by LangChain)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer recommended by LangChain:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install package
uv pip install langchain-iceberg

# With semantic layer
uv pip install "langchain-iceberg[semantic]"
```

See [INSTALL_WITH_UV.md](INSTALL_WITH_UV.md) for more details.

## Quick Start

### Basic Usage

```python
from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent

# Initialize toolkit
toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://my-warehouse"
    }
)

# Get tools
tools = toolkit.get_tools()

# Create agent
llm = ChatOpenAI(model="gpt-4")
agent = create_react_agent(llm, tools)

# Query with natural language
result = agent.invoke({
    "input": "Show me the top 10 orders by amount from the sales.orders table"
})

print(result)
```

### Direct Tool Usage

```python
from langchain_iceberg import IcebergToolkit

toolkit = IcebergToolkit(
    catalog_name="rest",
    catalog_config={
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/wh/"
    }
)

tools = toolkit.get_tools()

# Use tools directly
list_ns = next(t for t in tools if t.name == "iceberg_list_namespaces")
namespaces = list_ns.run({})
print(namespaces)

query = next(t for t in tools if t.name == "iceberg_query")
results = query.run({
    "table_id": "test.orders",
    "filters": "status = 'completed'",
    "limit": 10
})
print(results)
```

### With Semantic Layer

```python
# Load semantic YAML for business-friendly metrics
toolkit = IcebergToolkit(
    catalog_name="prod",
    catalog_config={...},
    semantic_yaml="s3://bucket/semantic.yaml"
)

tools = toolkit.get_tools()
# Now includes auto-generated metric tools like get_total_revenue, get_order_count, etc.

agent = create_react_agent(llm, tools)

# Business question (no SQL needed!)
result = agent.invoke({
    "input": "What was Q4 2024 revenue by customer segment?"
})
```

### Time-Travel Queries

```python
# Query historical data
result = agent.invoke({
    "input": "Compare this month's revenue to the same period last year using time-travel"
})
```

## Available Tools

The toolkit provides the following tools:

### Catalog Exploration
- `iceberg_list_namespaces` - List all namespaces in the catalog
- `iceberg_list_tables` - List tables in a namespace
- `iceberg_get_schema` - Get table schema with sample data

### Query Execution
- `iceberg_query` - Execute queries with filters and column selection
- `iceberg_plan_query` - LLM-assisted query planning

### Time-Travel (Iceberg-Specific)
- `iceberg_snapshots` - List table snapshots
- `iceberg_time_travel` - Query data at a specific point in time

### Semantic Layer (Auto-Generated)
- `get_{metric_name}` - Auto-generated tools from YAML metrics

## Documentation

- [Installation Guide](docs/installation.md)
- [Quick Start Guide](docs/quickstart.md)
- [Semantic Layer Guide](docs/semantic_layer.md)
- [API Reference](docs/api_reference.md)

## Requirements

- Python 3.10+
- Apache Iceberg catalog (REST, Hive, Glue, or Nessie)
- Cloud storage (S3, ADLS, or GCS)

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache 2.0 License - see [LICENSE](LICENSE) file for details.

## Support

- GitHub Issues: [Report a bug or request a feature](https://github.com/langchain-ai/langchain-iceberg/issues)
- Documentation: [Full documentation](https://github.com/langchain-ai/langchain-iceberg#readme)

## Roadmap

- [x] Core toolkit with catalog exploration
- [x] Query execution tools
- [ ] Time-travel and snapshot tools
- [ ] Semantic layer with YAML support
- [ ] Governance features (access control, PII protection)
- [ ] Query planner tool

