# Hard EPA Queries (10 queries)

Complex queries with advanced SQL features, window functions, and multi-step logic.

## Query 1: Running average PM2.5 by month
**Question**: Calculate the 3-month running average of PM2.5 concentrations.

**SQL**:
```sql
WITH monthly_avg AS (
    SELECT 
        SUBSTRING(date_local, 1, 7) as month,
        AVG(arithmetic_mean) as avg_pm25
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
    GROUP BY SUBSTRING(date_local, 1, 7)
)
SELECT 
    month,
    avg_pm25,
    AVG(avg_pm25) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as running_avg_3mo
FROM monthly_avg
ORDER BY month;
```

---

## Query 2: Pollution spikes detection
**Question**: Find days where PM2.5 concentration was more than 2 standard deviations above the mean.

**SQL**:
```sql
WITH stats AS (
    SELECT 
        AVG(arithmetic_mean) as mean_pm25,
        STDDEV(arithmetic_mean) as stddev_pm25
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
)
SELECT 
    d.date_local,
    d.city_name,
    d.arithmetic_mean,
    s.mean_pm25,
    s.stddev_pm25,
    (d.arithmetic_mean - s.mean_pm25) / s.stddev_pm25 as z_score
FROM epa.daily_summary d
CROSS JOIN stats s
WHERE d.parameter_code = '88101'
  AND d.arithmetic_mean > (s.mean_pm25 + 2 * s.stddev_pm25)
ORDER BY d.arithmetic_mean DESC
LIMIT 50;
```

---

## Query 3: Year-over-year growth rate
**Question**: Calculate the year-over-year percentage change in average PM2.5 for each state.

**SQL**:
```sql
WITH yearly_avg AS (
    SELECT 
        state_name,
        SUBSTRING(date_local, 1, 4) as year,
        AVG(arithmetic_mean) as avg_pm25
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
      AND state_name IS NOT NULL
      AND (date_local LIKE '2023%' OR date_local LIKE '2024%')
    GROUP BY state_name, SUBSTRING(date_local, 1, 4)
),
year_comparison AS (
    SELECT 
        state_name,
        MAX(CASE WHEN year = '2023' THEN avg_pm25 END) as pm25_2023,
        MAX(CASE WHEN year = '2024' THEN avg_pm25 END) as pm25_2024
    FROM yearly_avg
    GROUP BY state_name
)
SELECT 
    state_name,
    pm25_2023,
    pm25_2024,
    ROUND(100.0 * (pm25_2024 - pm25_2023) / pm25_2023, 2) as yoy_change_pct
FROM year_comparison
WHERE pm25_2023 IS NOT NULL AND pm25_2024 IS NOT NULL
ORDER BY yoy_change_pct DESC;
```

---

## Query 4: Top percentile analysis
**Question**: Find the top 1% of days with highest PM2.5 concentrations.

**SQL**:
```sql
WITH ranked_days AS (
    SELECT 
        date_local,
        city_name,
        state_name,
        arithmetic_mean,
        PERCENT_RANK() OVER (ORDER BY arithmetic_mean DESC) as percentile_rank
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
      AND arithmetic_mean IS NOT NULL
)
SELECT 
    date_local,
    city_name,
    state_name,
    arithmetic_mean,
    ROUND(percentile_rank * 100, 2) as percentile
FROM ranked_days
WHERE percentile_rank <= 0.01
ORDER BY arithmetic_mean DESC;
```

---

## Query 5: Correlation between pollutants
**Question**: Find cities where PM2.5 and Ozone are both above average (indicating correlation).

**SQL**:
```sql
WITH pollutant_avg AS (
    SELECT 
        parameter_code,
        AVG(arithmetic_mean) as overall_avg
    FROM epa.daily_summary
    WHERE parameter_code IN ('88101', '44201')
    GROUP BY parameter_code
),
city_daily AS (
    SELECT 
        city_name,
        date_local,
        MAX(CASE WHEN parameter_code = '88101' THEN arithmetic_mean END) as pm25,
        MAX(CASE WHEN parameter_code = '44201' THEN arithmetic_mean END) as ozone
    FROM epa.daily_summary
    WHERE city_name IS NOT NULL
      AND parameter_code IN ('88101', '44201')
    GROUP BY city_name, date_local
)
SELECT 
    cd.city_name,
    COUNT(*) as days_both_high,
    AVG(cd.pm25) as avg_pm25,
    AVG(cd.ozone) as avg_ozone
FROM city_daily cd
CROSS JOIN pollutant_avg pa_pm25
CROSS JOIN pollutant_avg pa_ozone
WHERE cd.pm25 > pa_pm25.overall_avg
  AND cd.ozone > pa_ozone.overall_avg
  AND pa_pm25.parameter_code = '88101'
  AND pa_ozone.parameter_code = '44201'
GROUP BY cd.city_name
HAVING COUNT(*) >= 10
ORDER BY days_both_high DESC;
```

---

## Query 6: Consecutive unhealthy days
**Question**: Find the longest streak of consecutive days with AQI > 100 for each city.

**SQL**:
```sql
WITH daily_aqi AS (
    SELECT 
        city_name,
        date_local,
        aqi,
        CASE WHEN aqi > 100 THEN 1 ELSE 0 END as is_unhealthy,
        ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY date_local) as day_num
    FROM epa.daily_summary
    WHERE city_name IS NOT NULL
      AND aqi IS NOT NULL
),
streaks AS (
    SELECT 
        city_name,
        date_local,
        is_unhealthy,
        SUM(is_unhealthy) OVER (
            PARTITION BY city_name 
            ORDER BY date_local 
            ROWS UNBOUNDED PRECEDING
        ) - 
        SUM(is_unhealthy) OVER (
            PARTITION BY city_name, is_unhealthy
            ORDER BY date_local
            ROWS UNBOUNDED PRECEDING
        ) as streak_id
    FROM daily_aqi
)
SELECT 
    city_name,
    COUNT(*) as consecutive_unhealthy_days,
    MIN(date_local) as streak_start,
    MAX(date_local) as streak_end
FROM streaks
WHERE is_unhealthy = 1
GROUP BY city_name, streak_id
ORDER BY consecutive_unhealthy_days DESC
LIMIT 20;
```

---

## Query 7: Pollutant diversity index
**Question**: Calculate a diversity index showing how many different pollutants each site measures.

**SQL**:
```sql
WITH site_pollutants AS (
    SELECT 
        CONCAT(state_code, county_code, site_num) as site_id,
        COUNT(DISTINCT parameter_code) as pollutant_count,
        COUNT(*) as total_measurements
    FROM epa.daily_summary
    GROUP BY CONCAT(state_code, county_code, site_num)
)
SELECT 
    pollutant_count,
    COUNT(*) as site_count,
    AVG(total_measurements) as avg_measurements_per_site
FROM site_pollutants
GROUP BY pollutant_count
ORDER BY pollutant_count DESC;
```

---

## Query 8: Temporal clustering
**Question**: Identify months with unusually high pollution (more than 1.5x the annual average).

**SQL**:
```sql
WITH monthly_stats AS (
    SELECT 
        SUBSTRING(date_local, 1, 4) as year,
        SUBSTRING(date_local, 1, 7) as month,
        AVG(arithmetic_mean) as monthly_avg
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
    GROUP BY SUBSTRING(date_local, 1, 4), SUBSTRING(date_local, 1, 7)
),
annual_avg AS (
    SELECT 
        year,
        AVG(monthly_avg) as yearly_avg
    FROM monthly_stats
    GROUP BY year
)
SELECT 
    ms.month,
    ms.monthly_avg,
    aa.yearly_avg,
    ROUND(100.0 * (ms.monthly_avg - aa.yearly_avg) / aa.yearly_avg, 2) as deviation_pct
FROM monthly_stats ms
JOIN annual_avg aa ON ms.year = aa.year
WHERE ms.monthly_avg > 1.5 * aa.yearly_avg
ORDER BY deviation_pct DESC;
```

---

## Query 9: Geographic pollution gradient
**Question**: Analyze how pollution varies by latitude (north-south gradient).

**SQL**:
```sql
WITH lat_buckets AS (
    SELECT 
        CASE 
            WHEN latitude < 30 THEN 'South (<30°)'
            WHEN latitude < 35 THEN 'South-Central (30-35°)'
            WHEN latitude < 40 THEN 'Central (35-40°)'
            WHEN latitude < 45 THEN 'North-Central (40-45°)'
            ELSE 'North (>45°)'
        END as lat_zone,
        arithmetic_mean
    FROM epa.daily_summary
    WHERE parameter_code = '88101'
      AND latitude IS NOT NULL
)
SELECT 
    lat_zone,
    COUNT(*) as measurements,
    AVG(arithmetic_mean) as avg_pm25,
    MIN(arithmetic_mean) as min_pm25,
    MAX(arithmetic_mean) as max_pm25
FROM lat_buckets
GROUP BY lat_zone
ORDER BY 
    CASE lat_zone
        WHEN 'South (<30°)' THEN 1
        WHEN 'South-Central (30-35°)' THEN 2
        WHEN 'Central (35-40°)' THEN 3
        WHEN 'North-Central (40-45°)' THEN 4
        WHEN 'North (>45°)' THEN 5
    END;
```

---

## Query 10: Complex multi-pollutant health risk score
**Question**: Calculate a composite health risk score combining PM2.5, Ozone, and SO2 levels.

**SQL**:
```sql
WITH daily_pollutants AS (
    SELECT 
        date_local,
        city_name,
        state_name,
        MAX(CASE WHEN parameter_code = '88101' THEN arithmetic_mean END) as pm25,
        MAX(CASE WHEN parameter_code = '44201' THEN arithmetic_mean END) as ozone,
        MAX(CASE WHEN parameter_code = '42401' THEN arithmetic_mean END) as so2
    FROM epa.daily_summary
    WHERE city_name IS NOT NULL
      AND parameter_code IN ('88101', '44201', '42401')
    GROUP BY date_local, city_name, state_name
),
normalized AS (
    SELECT 
        date_local,
        city_name,
        state_name,
        pm25,
        ozone,
        so2,
        -- Normalize to 0-100 scale (assuming typical ranges)
        LEAST(100, (pm25 / 50.0) * 100) as pm25_score,
        LEAST(100, (ozone / 0.1) * 100) as ozone_score,
        LEAST(100, (so2 / 0.1) * 100) as so2_score
    FROM daily_pollutants
    WHERE pm25 IS NOT NULL OR ozone IS NOT NULL OR so2 IS NOT NULL
)
SELECT 
    city_name,
    state_name,
    COUNT(*) as days,
    AVG((COALESCE(pm25_score, 0) + COALESCE(ozone_score, 0) + COALESCE(so2_score, 0)) / 3) as avg_risk_score,
    MAX((COALESCE(pm25_score, 0) + COALESCE(ozone_score, 0) + COALESCE(so2_score, 0)) / 3) as max_risk_score
FROM normalized
GROUP BY city_name, state_name
HAVING COUNT(*) >= 30
ORDER BY avg_risk_score DESC
LIMIT 20;
```

