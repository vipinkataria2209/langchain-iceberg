# Examples Directory

This directory contains example scripts showing how to use the LangChain Iceberg Toolkit.

## Examples

### 1. `basic_usage.py`
Basic toolkit initialization and tool usage without agents.

**Run**: `python examples/basic_usage.py`

### 2. `semantic_layer.py`
Demonstrates semantic layer with YAML configuration and auto-generated metric tools.

**Run**: `python examples/semantic_layer.py`

**Requirements**: `examples/semantic.yaml` file

### 3. `time_travel.py`
Shows how to use time-travel features for historical data queries.

**Run**: `python examples/time_travel.py`

### 4. `query_planner.py`
Demonstrates LLM-assisted query planning.

**Run**: `python examples/query_planner.py`

**Requirements**: OpenAI API key

### 5. `complete_example.py`
Comprehensive example showing all features together.

**Run**: `python examples/complete_example.py`

## Semantic YAML

### `semantic.yaml`
Example semantic layer configuration file showing:
- Table definitions
- Metric definitions (total_revenue, order_count, avg_order_value)
- Dimension definitions (customer_segment, order_date)
- Governance configuration

**Use**: Reference this file to create your own semantic layer configuration.

## Running Examples

All examples assume you have:
1. Docker services running (see `README_TESTING.md`)
2. Iceberg catalog accessible
3. Python package installed: `pip install -e .`

For examples using LangChain agents, you'll also need:
- `OPENAI_API_KEY` environment variable set
- Or use a different LLM provider

## Quick Test

```bash
# Start services
docker-compose up -d

# Run basic example
python examples/basic_usage.py

# Run complete example
python examples/complete_example.py
```

