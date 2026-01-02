# Simple EPA Queries (10 queries)

Simple queries that work on a single table with basic filters and aggregations.

## Query 1: Count total measurements
**Question**: How many total air quality measurements are in the database?

**SQL**:
```sql
SELECT COUNT(*) as total_measurements
FROM epa.daily_summary;
```

---

## Query 2: Average PM2.5 by state
**Question**: What is the average PM2.5 concentration for each state?

**SQL**:
```sql
SELECT state_name, AVG(arithmetic_mean) as avg_pm25
FROM epa.daily_summary
WHERE parameter_code = '88101'
GROUP BY state_name
ORDER BY avg_pm25 DESC;
```

---

## Query 3: Maximum ozone value
**Question**: What is the maximum ozone concentration ever recorded?

**SQL**:
```sql
SELECT MAX(first_max_value) as max_ozone
FROM epa.daily_summary
WHERE parameter_code = '44201';
```

---

## Query 4: Measurements in 2024
**Question**: How many measurements were taken in 2024?

**SQL**:
```sql
SELECT COUNT(*) as measurements_2024
FROM epa.daily_summary
WHERE date_local LIKE '2024%';
```

---

## Query 5: Top 10 cities by PM2.5
**Question**: Which 10 cities have the highest average PM2.5 concentrations?

**SQL**:
```sql
SELECT city_name, AVG(arithmetic_mean) as avg_pm25
FROM epa.daily_summary
WHERE parameter_code = '88101' AND city_name IS NOT NULL
GROUP BY city_name
ORDER BY avg_pm25 DESC
LIMIT 10;
```

---

## Query 6: Unhealthy days count
**Question**: How many days had AQI greater than 100 (unhealthy)?

**SQL**:
```sql
SELECT COUNT(*) as unhealthy_days
FROM epa.daily_summary
WHERE aqi > 100;
```

---

## Query 7: SO2 measurements by year
**Question**: How many SO2 measurements were taken each year?

**SQL**:
```sql
SELECT 
    SUBSTRING(date_local, 1, 4) as year,
    COUNT(*) as so2_measurements
FROM epa.daily_summary
WHERE parameter_code = '42401'
GROUP BY SUBSTRING(date_local, 1, 4)
ORDER BY year;
```

---

## Query 8: Average CO in California
**Question**: What is the average carbon monoxide concentration in California?

**SQL**:
```sql
SELECT AVG(arithmetic_mean) as avg_co
FROM epa.daily_summary
WHERE parameter_code = '42101' AND state_name = 'California';
```

---

## Query 9: NO2 measurement count
**Question**: How many NO2 measurements are in the database?

**SQL**:
```sql
SELECT COUNT(*) as no2_count
FROM epa.daily_summary
WHERE parameter_code = '42602';
```

---

## Query 10: Distinct monitoring sites
**Question**: How many distinct monitoring sites are there?

**SQL**:
```sql
SELECT COUNT(DISTINCT CONCAT(state_code, county_code, site_num)) as distinct_sites
FROM epa.daily_summary;
```

