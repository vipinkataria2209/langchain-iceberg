# Semantic EPA Queries (10 queries)

Queries that use semantic layer metrics and dimensions from the YAML configuration.

## Query 1: Average PM2.5 using semantic metric
**Question**: What is the average PM2.5 concentration across all measurements?

**Semantic Metric**: `avg_pm25_concentration`

**Natural Language**: "What is the average PM2.5 concentration?"

**Expected SQL** (using semantic layer):
```sql
SELECT AVG(arithmetic_mean) as avg_pm25_concentration
FROM epa.daily_summary
WHERE parameter_code = '88101';
```

---

## Query 2: Maximum ozone using semantic metric
**Question**: What is the maximum ozone concentration ever recorded?

**Semantic Metric**: `max_ozone_concentration`

**Natural Language**: "What is the maximum ozone concentration?"

**Expected SQL**:
```sql
SELECT MAX(first_max_value) as max_ozone_concentration
FROM epa.daily_summary
WHERE parameter_code = '44201';
```

---

## Query 3: Unhealthy days count
**Question**: How many days had unhealthy air quality (AQI > 100)?

**Semantic Metric**: `unhealthy_days_count`

**Natural Language**: "How many unhealthy days were there?"

**Expected SQL**:
```sql
SELECT COUNT(*) as unhealthy_days_count
FROM epa.daily_summary
WHERE aqi > 100;
```

---

## Query 4: Average PM2.5 by state dimension
**Question**: What is the average PM2.5 concentration for each state?

**Semantic Metric**: `avg_pm25_concentration`  
**Semantic Dimension**: `state`

**Natural Language**: "Show me average PM2.5 by state"

**Expected SQL**:
```sql
SELECT 
    state_name as state,
    AVG(arithmetic_mean) as avg_pm25_concentration
FROM epa.daily_summary
WHERE parameter_code = '88101'
  AND state_name IS NOT NULL
GROUP BY state_name
ORDER BY avg_pm25_concentration DESC;
```

---

## Query 5: PM2.5 measurement count
**Question**: How many PM2.5 measurements are in the database?

**Semantic Metric**: `pm25_measurement_count`

**Natural Language**: "How many PM2.5 measurements do we have?"

**Expected SQL**:
```sql
SELECT COUNT(*) as pm25_measurement_count
FROM epa.daily_summary
WHERE parameter_code = '88101'
  AND arithmetic_mean IS NOT NULL;
```

---

## Query 6: Average AQI by pollutant
**Question**: What is the average Air Quality Index for each pollutant type?

**Semantic Metric**: `avg_aqi`  
**Semantic Dimension**: `pollutant`

**Natural Language**: "Show average AQI by pollutant"

**Expected SQL**:
```sql
SELECT 
    parameter_name as pollutant,
    AVG(aqi) as avg_aqi
FROM epa.daily_summary
WHERE aqi IS NOT NULL
GROUP BY parameter_name
ORDER BY avg_aqi DESC;
```

---

## Query 7: Maximum SO2 concentration
**Question**: What is the maximum SO2 concentration recorded?

**Semantic Metric**: `max_so2_concentration`

**Natural Language**: "What is the maximum SO2 concentration?"

**Expected SQL**:
```sql
SELECT MAX(first_max_value) as max_so2_concentration
FROM epa.daily_summary
WHERE parameter_code = '42401';
```

---

## Query 8: Average CO by measurement date
**Question**: What is the average CO concentration by month in 2024?

**Semantic Metric**: `avg_co_concentration`  
**Semantic Dimension**: `measurement_date` (monthly granularity)

**Natural Language**: "Show average CO by month in 2024"

**Expected SQL**:
```sql
SELECT 
    SUBSTRING(date_local, 1, 7) as measurement_date,
    AVG(arithmetic_mean) as avg_co_concentration
FROM epa.daily_summary
WHERE parameter_code = '42101'
  AND date_local LIKE '2024%'
GROUP BY SUBSTRING(date_local, 1, 7)
ORDER BY measurement_date;
```

---

## Query 9: Very unhealthy days count
**Question**: How many days had very unhealthy air quality (AQI > 200)?

**Semantic Metric**: `very_unhealthy_days_count`

**Natural Language**: "How many very unhealthy days were there?"

**Expected SQL**:
```sql
SELECT COUNT(*) as very_unhealthy_days_count
FROM epa.daily_summary
WHERE aqi > 200;
```

---

## Query 10: Average NO2 by county
**Question**: What is the average NO2 concentration for each county?

**Semantic Metric**: `avg_no2_concentration`  
**Semantic Dimension**: `county`

**Natural Language**: "Show average NO2 by county"

**Expected SQL**:
```sql
SELECT 
    county_name as county,
    AVG(arithmetic_mean) as avg_no2_concentration
FROM epa.daily_summary
WHERE parameter_code = '42602'
  AND county_name IS NOT NULL
GROUP BY county_name
HAVING COUNT(*) >= 10
ORDER BY avg_no2_concentration DESC
LIMIT 50;
```

---

## Additional Semantic Query Examples

### Using Multiple Metrics
**Question**: Compare average and maximum PM2.5 concentrations by state.

**Semantic Metrics**: `avg_pm25_concentration`, `max_pm25_concentration`  
**Semantic Dimension**: `state`

**Natural Language**: "Compare average and maximum PM2.5 by state"

---

### Using Time Travel
**Question**: What was the average PM2.5 concentration as of June 2023?

**Semantic Metric**: `avg_pm25_concentration`  
**Semantic Dimension**: `measurement_date` (with time_travel: true)

**Natural Language**: "What was the average PM2.5 in June 2023?"

---

### Combining Metrics and Dimensions
**Question**: Show unhealthy days count by state and pollutant.

**Semantic Metric**: `unhealthy_days_count`  
**Semantic Dimensions**: `state`, `pollutant`

**Natural Language**: "Show unhealthy days by state and pollutant"

