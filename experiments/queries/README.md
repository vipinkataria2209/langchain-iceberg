# EPA Air Quality Query Collection

This directory contains 50 test queries organized by complexity and type.

## Query Categories

### 1. Simple Queries (10 queries)
**File**: `01_simple_queries.md`

Basic queries on a single table with simple filters and aggregations.
- Count operations
- Basic aggregations (AVG, MAX, MIN)
- Simple GROUP BY
- Basic WHERE filters

**Example**: "How many total air quality measurements are in the database?"

---

### 2. Medium Queries (10 queries)
**File**: `02_medium_queries.md

Queries with multiple conditions, date ranges, and more complex aggregations.
- Date range filtering
- Multiple aggregations
- HAVING clauses
- CASE statements
- Substring operations for dates

**Example**: "What is the average PM2.5 concentration by month for 2023?"

---

### 3. Hard Queries (10 queries)
**File**: `03_hard_queries.md`

Complex queries with advanced SQL features.
- Window functions (ROW_NUMBER, PERCENT_RANK, OVER)
- CTEs (Common Table Expressions)
- Statistical calculations (standard deviation, z-scores)
- Complex aggregations
- Multi-step logic

**Example**: "Find days where PM2.5 concentration was more than 2 standard deviations above the mean."

---

### 4. Join Queries (10 queries)
**File**: `04_join_queries.md`

Queries requiring JOINs between multiple tables.
- JOINs between `daily_summary`, `sites`, and `monitors`
- Composite key joins
- Multi-table aggregations
- Enriching measurements with metadata

**Example**: "Show daily PM2.5 measurements with site location and land use information."

---

### 5. Semantic Queries (10 queries)
**File**: `05_semantic_queries.md`

Queries using semantic layer metrics and dimensions from `epa_complete_semantic.yaml`.
- Using semantic metrics (avg_pm25_concentration, max_ozone_concentration, etc.)
- Using semantic dimensions (state, county, pollutant, measurement_date)
- Natural language query examples
- Expected SQL translations

**Example**: "What is the average PM2.5 concentration?" (uses `avg_pm25_concentration` metric)

---

## Usage

### For Testing LLM Agents

These queries can be used to test:
1. **Query Understanding**: Can the LLM understand the natural language question?
2. **SQL Generation**: Can the LLM generate correct SQL?
3. **Semantic Layer**: Can the LLM use semantic metrics and dimensions?
4. **Join Handling**: Can the LLM construct proper JOINs with composite keys?

### For Manual Testing

1. Load data using `test_load_and_query.py` or `epa_data_loader.py`
2. Execute queries directly against the Iceberg tables
3. Compare results with expected outputs

### For Evaluation

Use these queries in evaluation frameworks to:
- Measure query accuracy
- Test semantic layer integration
- Benchmark join performance
- Evaluate natural language understanding

---

## Query Format

Each query includes:
- **Question**: Natural language question
- **SQL**: Complete SQL query
- **Semantic Metrics/Dimensions** (for semantic queries): Which semantic elements are used
- **Natural Language** (for semantic queries): Example natural language query

---

## Prerequisites

Before running queries:

1. **Load Data**: Run `test_load_and_query.py` to load all 3 tables
   ```bash
   python experiments/test_load_and_query.py
   ```
   
   Or use the loader directly:
   ```python
   from experiments.epa_data_loader import EPADataLoader
   from pyiceberg.catalog import load_catalog
   
   catalog = load_catalog("rest_epa")
   loader = EPADataLoader(catalog, namespace="epa")
   loader.load_all_tables("data/epa")
   ```

2. **Verify Tables**: Ensure these tables exist:
   - `epa.sites`
   - `epa.monitors`
   - `epa.daily_summary`

3. **Semantic Layer** (for semantic queries): Load `epa_complete_semantic.yaml`
   ```python
   from langchain_iceberg import SemanticLoader
   loader = SemanticLoader.from_yaml("experiments/epa_complete_semantic.yaml")
   ```

---

## Table Relationships

Understanding relationships is crucial for JOIN queries:

### daily_summary → sites
```sql
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
```

### daily_summary → monitors
```sql
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
    AND d.poc = m.poc
```

---

## Parameter Codes Reference

| Code | Pollutant | Units |
|------|-----------|-------|
| 88101 | PM2.5 | μg/m³ |
| 44201 | Ozone | ppm |
| 42401 | SO2 | ppm |
| 42101 | CO | ppm |
| 42602 | NO2 | ppm |

---

## Notes

- All queries use the `epa` namespace
- Date fields are stored as strings (format: 'YYYY-MM-DD')
- Some queries may need adjustment based on actual data availability
- JOIN queries require all 3 tables to be loaded
- Semantic queries require the semantic layer YAML to be configured

---

**Total Queries**: 50  
**Last Updated**: 2026-01-02

