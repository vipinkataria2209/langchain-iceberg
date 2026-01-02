# Medium EPA Queries (10 queries)

Medium complexity queries with multiple conditions, date ranges, and aggregations.

## Query 1: Monthly PM2.5 trends
**Question**: What is the average PM2.5 concentration by month for 2023?

**SQL**:
```sql
SELECT 
    SUBSTRING(date_local, 1, 7) as month,
    AVG(arithmetic_mean) as avg_pm25
FROM epa.daily_summary
WHERE parameter_code = '88101' 
  AND date_local LIKE '2023%'
GROUP BY SUBSTRING(date_local, 1, 7)
ORDER BY month;
```

---

## Query 2: Top 5 states with highest ozone
**Question**: Which 5 states have the highest average ozone concentrations in 2024?

**SQL**:
```sql
SELECT 
    state_name,
    AVG(arithmetic_mean) as avg_ozone,
    MAX(first_max_value) as max_ozone
FROM epa.daily_summary
WHERE parameter_code = '44201' 
  AND date_local LIKE '2024%'
  AND state_name IS NOT NULL
GROUP BY state_name
ORDER BY avg_ozone DESC
LIMIT 5;
```

---

## Query 3: Pollutant comparison by state
**Question**: Compare average concentrations of PM2.5, Ozone, and SO2 for each state.

**SQL**:
```sql
SELECT 
    state_name,
    AVG(CASE WHEN parameter_code = '88101' THEN arithmetic_mean END) as avg_pm25,
    AVG(CASE WHEN parameter_code = '44201' THEN arithmetic_mean END) as avg_ozone,
    AVG(CASE WHEN parameter_code = '42401' THEN arithmetic_mean END) as avg_so2
FROM epa.daily_summary
WHERE state_name IS NOT NULL
GROUP BY state_name
ORDER BY state_name;
```

---

## Query 4: Days exceeding AQI 150
**Question**: How many days exceeded AQI 150 (unhealthy) in each state in 2023?

**SQL**:
```sql
SELECT 
    state_name,
    COUNT(*) as unhealthy_days
FROM epa.daily_summary
WHERE aqi > 150 
  AND date_local LIKE '2023%'
  AND state_name IS NOT NULL
GROUP BY state_name
ORDER BY unhealthy_days DESC;
```

---

## Query 5: Year-over-year PM2.5 change
**Question**: Compare average PM2.5 concentrations between 2023 and 2024.

**SQL**:
```sql
SELECT 
    SUBSTRING(date_local, 1, 4) as year,
    AVG(arithmetic_mean) as avg_pm25,
    COUNT(*) as measurement_count
FROM epa.daily_summary
WHERE parameter_code = '88101'
  AND (date_local LIKE '2023%' OR date_local LIKE '2024%')
GROUP BY SUBSTRING(date_local, 1, 4)
ORDER BY year;
```

---

## Query 6: Cities with multiple pollutants
**Question**: Which cities have measurements for all 5 pollutants (PM2.5, Ozone, SO2, CO, NO2)?

**SQL**:
```sql
SELECT 
    city_name,
    COUNT(DISTINCT parameter_code) as pollutant_count
FROM epa.daily_summary
WHERE city_name IS NOT NULL
GROUP BY city_name
HAVING COUNT(DISTINCT parameter_code) = 5
ORDER BY city_name;
```

---

## Query 7: Peak pollution hours
**Question**: What hour of the day has the highest average pollution (based on first_max_hour)?

**SQL**:
```sql
SELECT 
    first_max_hour,
    COUNT(*) as occurrences,
    AVG(first_max_value) as avg_max_value
FROM epa.daily_summary
WHERE first_max_hour IS NOT NULL
  AND parameter_code = '44201'  -- Ozone
GROUP BY first_max_hour
ORDER BY avg_max_value DESC
LIMIT 1;
```

---

## Query 8: Seasonal patterns
**Question**: What is the average PM2.5 concentration by season (Q1=Winter, Q2=Spring, Q3=Summer, Q4=Fall)?

**SQL**:
```sql
SELECT 
    CASE 
        WHEN SUBSTRING(date_local, 6, 2) IN ('12', '01', '02') THEN 'Winter'
        WHEN SUBSTRING(date_local, 6, 2) IN ('03', '04', '05') THEN 'Spring'
        WHEN SUBSTRING(date_local, 6, 2) IN ('06', '07', '08') THEN 'Summer'
        ELSE 'Fall'
    END as season,
    AVG(arithmetic_mean) as avg_pm25
FROM epa.daily_summary
WHERE parameter_code = '88101'
  AND date_local LIKE '2023%'
GROUP BY season
ORDER BY 
    CASE season
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        WHEN 'Fall' THEN 4
    END;
```

---

## Query 9: Measurement quality by observation count
**Question**: What percentage of measurements have observation_count >= 20 (high quality)?

**SQL**:
```sql
SELECT 
    COUNT(*) as total_measurements,
    SUM(CASE WHEN observation_count >= 20 THEN 1 ELSE 0 END) as high_quality,
    ROUND(100.0 * SUM(CASE WHEN observation_count >= 20 THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_percentage
FROM epa.daily_summary
WHERE observation_count IS NOT NULL;
```

---

## Query 10: CBSA pollution ranking
**Question**: Rank Core Based Statistical Areas (metropolitan areas) by average PM2.5 concentration.

**SQL**:
```sql
SELECT 
    cbsa_name,
    AVG(arithmetic_mean) as avg_pm25,
    COUNT(*) as measurement_count
FROM epa.daily_summary
WHERE parameter_code = '88101'
  AND cbsa_name IS NOT NULL
GROUP BY cbsa_name
HAVING COUNT(*) >= 100  -- At least 100 measurements
ORDER BY avg_pm25 DESC
LIMIT 20;
```

