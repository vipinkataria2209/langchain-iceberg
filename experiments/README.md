# EPA Air Quality Data Experiments

This directory contains experiments evaluating the LangChain Iceberg Toolkit with EPA air quality data, similar to the experiments described in the Frontiers paper.

## Overview

The experiments compare:
1. **LLM Accuracy**: With semantic layer vs without semantic layer
2. **Query Performance**: Execution time and efficiency
3. **Natural Language Understanding**: Success rate of complex queries

## Files

- `epa_data_loader.py`: Loads EPA daily summary CSV files into Iceberg tables
- `epa_semantic.yaml`: Semantic layer configuration for air quality metrics
- `evaluation_framework.py`: Evaluation framework comparing semantic vs non-semantic approaches
- `run_experiments.py`: Main script to run all experiments

## Setup

### 1. Download EPA Data

Download EPA daily summary files from:
https://aqs.epa.gov/aqsweb/airdata/download_files.html

Example files:
- `daily_44201_2023.zip` (Ozone data)
- `daily_88101_2023.zip` (PM2.5 data)

Extract and place in `data/epa/` directory.

### 2. Load Data into Iceberg

```python
from experiments.epa_data_loader import EPADataLoader
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    name="rest",
    type="rest",
    uri="http://localhost:8181",
    warehouse="s3://warehouse/wh/",
)

loader = EPADataLoader(catalog, namespace="epa")
table = loader.create_table()
loader.load_directory("data/epa/", pattern="*.csv*")
```

### 3. Run Experiments

```bash
# Set OpenAI API key
export OPENAI_API_KEY="your-key-here"

# Run evaluation
python experiments/evaluation_framework.py
```

Or use the main experiment runner:

```bash
python experiments/run_experiments.py
```

## Experiment Design

### Test Queries

The evaluation uses a set of natural language queries covering:
- Basic aggregation queries
- Time-based queries
- Geographic queries
- Complex analytical queries
- Semantic layer-specific queries

### Metrics Collected

1. **Success Rate**: Percentage of queries that return correct results
2. **Execution Time**: Average time to execute queries
3. **Tool Calls**: Number of tool invocations per query
4. **Accuracy Improvement**: Improvement when using semantic layer

## Expected Results

Based on the Frontiers paper, we expect:
- **Semantic layer improves LLM accuracy** by providing structured metric definitions
- **Native API integration** provides better performance than SQL string generation
- **Time-travel queries** enable historical analysis without complex SQL

## Results Format

Results are saved as JSON with:
- Aggregate metrics (success rates, avg times)
- Detailed per-query results
- Comparison metrics (improvements)

## References

- EPA AirData Documentation: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html
- Frontiers Paper: "From SQL to Natural Language: LLM-Powered Analytics with Semantic Layers for Apache Iceberg"

