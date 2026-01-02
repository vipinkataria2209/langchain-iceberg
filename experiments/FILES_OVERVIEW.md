# Experiments Folder - Files Overview

## Table of Contents
- [Python Scripts](#python-scripts)
- [Configuration Files](#configuration-files)
- [Query Files](#query-files)
- [Documentation](#documentation)

---

## Python Scripts

| File Name | Purpose | Key Functionality | When to Use |
|-----------|---------|-------------------|-------------|
| `count_epa_rows.py` | **Data Verification** | Unzips EPA ZIP files and counts total rows in CSV files. Categorizes by: sites, monitors, and individual gas files (PM2.5, Ozone, SO2, CO, NO2). | After downloading EPA data to verify completeness and count records |
| `download_epa_data.py` | **Core Downloader** | Core implementation for downloading EPA air quality data files. Supports parallel downloads with ThreadPoolExecutor, progress tracking, error handling, and ZIP extraction. Handles individual file downloads. | Used internally by `download_all_epa_data.py` or for custom download scenarios |
| `download_all_epa_data.py` | **User-Friendly Downloader** | Wrapper script that uses `download_epa_data.py` to download all EPA data (5 pollutants × 11 years = 55 files). Provides confirmation prompts and command-line arguments for parallel workers. | Primary script for downloading all EPA data (2014-2024) |
| `epa_data_loader.py` | **Main Data Loader** | Comprehensive loader for all 3 EPA tables (`sites`, `monitors`, `daily_summary`). Creates Iceberg schemas, handles type casting, loads CSV data into Iceberg tables using PyIceberg. Supports REST catalog. | Use to load EPA CSV files into Iceberg tables after downloading |
| `test_load_and_query.py` | **Load & Verify Script** | Orchestrates data loading and runs basic verification queries. Includes `--verify` flag to check existing tables without loading. Merged functionality from deleted `verify_tables.py`. | Use to load data and verify it's working, or just verify existing tables |
| `test_queries_direct.py` | **Direct Query Tests** | Runs comprehensive query tests directly against Iceberg tables without LLM. Tests 10+ queries including simple, aggregation, JOIN, and complex queries. Uses PyArrow compute for aggregations. | Use to test queries directly on loaded data without LLM involvement |
| `evaluation_framework.py` | **Evaluation Framework** | Framework for comparing LLM performance with/without semantic layer. Creates LangChain agents, runs queries, measures success rates, execution times, and accuracy improvements. Similar to Frontiers paper experiments. | Use for systematic evaluation of semantic layer impact on LLM accuracy |
| `run_experiments.py` | **Main Experiment Runner** | Orchestrates complete evaluation: loads EPA data (if needed), runs evaluation with/without semantic layer, generates results and reports. Uses `evaluation_framework.py` internally. | Use to run full evaluation experiments comparing semantic vs non-semantic approaches |
| `run_query_experiments.py` | **Query Category Experiments** | Tests queries across 5 categories: Simple (PyIceberg), Medium (DuckDB), Hard (DuckDB), Join (DuckDB), Semantic (Semantic tools). Runs 50 queries and generates detailed results JSON. | Use to test queries across different complexity levels and engines |

---

## Configuration Files

| File Name | Purpose | Key Functionality | When to Use |
|-----------|---------|-------------------|-------------|
| `epa_complete_semantic.yaml` | **Semantic Layer Config** | Complete semantic layer configuration defining all 3 EPA tables (`sites`, `monitors`, `daily_summary`), their relationships, dimensions, and metrics. Includes 5 pollutants, relationships for JOINs, and 20+ metrics. | Required for semantic layer queries. Loaded by LangChain Iceberg toolkit for natural language querying |

---

## Query Files

| File Name | Purpose | Key Functionality | When to Use |
|-----------|---------|-------------------|-------------|
| `queries/01_simple_queries.md` | **Simple Queries** | 10 basic queries on single table: count operations, basic aggregations (AVG, MAX, MIN), simple GROUP BY, basic WHERE filters | Testing basic query understanding and SQL generation |
| `queries/02_medium_queries.md` | **Medium Queries** | 10 queries with multiple conditions: date ranges, multiple aggregations, HAVING clauses, CASE statements, substring operations | Testing intermediate SQL complexity |
| `queries/03_hard_queries.md` | **Hard Queries** | 10 complex queries: window functions (ROW_NUMBER, PERCENT_RANK), CTEs, statistical calculations (std dev, z-scores), complex aggregations | Testing advanced SQL features and complex logic |
| `queries/04_join_queries.md` | **Join Queries** | 10 queries requiring JOINs: JOINs between `daily_summary`, `sites`, `monitors`, composite key joins, multi-table aggregations | Testing JOIN handling and composite key relationships |
| `queries/05_semantic_queries.md` | **Semantic Queries** | 10 queries using semantic layer: semantic metrics (avg_pm25_concentration, etc.), semantic dimensions, natural language examples | Testing semantic layer integration and natural language understanding |
| `queries/README.md` | **Query Documentation** | Documentation for all 50 queries: format, usage, prerequisites, table relationships, parameter codes reference | Reference guide for understanding and using the query collection |

---

## Documentation

| File Name | Purpose | Key Functionality | When to Use |
|-----------|---------|-------------------|-------------|
| `EPA_SETUP_GUIDE.md` | **Setup Instructions** | Step-by-step guide for EPA data setup: download, verification (row counting), and loading steps for reproducibility | Follow this guide when setting up EPA data for the first time |
| `EPA_SCHEMA_DOCUMENTATION.md` | **Schema Reference** | Documentation of EPA table schemas: `sites`, `monitors`, `daily_summary` tables with sample data, relationship definitions, parameter codes, and AQI scale | Reference when understanding data structure and relationships |

---

## Workflow Summary

### Typical Workflow:

1. **Download Data**: `download_all_epa_data.py` → Downloads 55 ZIP files
2. **Verify Data**: `count_epa_rows.py` → Counts rows to verify completeness
3. **Load Data**: `test_load_and_query.py` → Loads all 3 tables into Iceberg
4. **Test Queries**: `test_queries_direct.py` → Tests queries without LLM
5. **Run Experiments**: `run_experiments.py` or `run_query_experiments.py` → Full evaluation

### Quick Verification:

- `test_load_and_query.py --verify` → Quick check of existing tables

---

## File Dependencies

```
download_all_epa_data.py
    └──> download_epa_data.py (core downloader)

test_load_and_query.py
    └──> epa_data_loader.py (loads data)

run_experiments.py
    └──> epa_data_loader.py (loads data)
    └──> evaluation_framework.py (runs evaluation)

run_query_experiments.py
    └──> epa_complete_semantic.yaml (semantic config)
    └──> queries/*.md (test queries)

test_queries_direct.py
    └──> (uses loaded Iceberg tables directly)
```

---

**Last Updated**: 2026-01-02

