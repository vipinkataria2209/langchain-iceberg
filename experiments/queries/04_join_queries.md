# Join EPA Queries (10 queries)

Queries that require JOINs between multiple tables (sites, monitors, daily_summary).

## Query 1: Site details with measurements
**Question**: Show daily PM2.5 measurements with site location and land use information.

**SQL**:
```sql
SELECT 
    d.date_local,
    d.arithmetic_mean as pm25,
    s.local_site_name,
    s.city_name,
    s.state_name,
    s.land_use,
    s.location_setting
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
WHERE d.parameter_code = '88101'
  AND d.date_local LIKE '2024%'
ORDER BY d.arithmetic_mean DESC
LIMIT 100;
```

---

## Query 2: Monitor method analysis
**Question**: Compare PM2.5 measurements by monitoring method used.

**SQL**:
```sql
SELECT 
    m.last_method,
    m.monitor_type,
    COUNT(*) as measurement_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    MAX(d.first_max_value) as max_pm25
FROM epa.daily_summary d
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
    AND d.poc = m.poc
WHERE d.parameter_code = '88101'
GROUP BY m.last_method, m.monitor_type
ORDER BY measurement_count DESC;
```

---

## Query 3: Urban vs rural pollution comparison
**Question**: Compare average PM2.5 levels between urban and rural monitoring sites.

**SQL**:
```sql
SELECT 
    s.location_setting,
    COUNT(DISTINCT CONCAT(s.state_code, s.county_code, s.site_number)) as site_count,
    COUNT(*) as measurement_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    MAX(d.first_max_value) as max_pm25
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
WHERE d.parameter_code = '88101'
  AND s.location_setting IN ('URBAN', 'RURAL', 'SUBURBAN')
GROUP BY s.location_setting
ORDER BY avg_pm25 DESC;
```

---

## Query 4: Site establishment impact
**Question**: Compare pollution levels at sites established before and after 2000.

**SQL**:
```sql
SELECT 
    CASE 
        WHEN CAST(SUBSTRING(s.site_established_date, 1, 4) AS INT) < 2000 THEN 'Pre-2000'
        ELSE 'Post-2000'
    END as establishment_period,
    COUNT(DISTINCT CONCAT(s.state_code, s.county_code, s.site_number)) as site_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    COUNT(*) as measurement_count
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
WHERE d.parameter_code = '88101'
  AND s.site_established_date IS NOT NULL
GROUP BY establishment_period;
```

---

## Query 5: Monitor network analysis
**Question**: Analyze pollution levels by monitoring network type (SLAMS, NAMS, PAMS).

**SQL**:
```sql
SELECT 
    m.monitor_type,
    m.networks,
    COUNT(DISTINCT CONCAT(m.state_code, m.county_code, m.site_number, m.parameter_code)) as monitor_count,
    COUNT(*) as measurement_count,
    AVG(d.arithmetic_mean) as avg_concentration,
    AVG(d.observation_count) as avg_observations
FROM epa.daily_summary d
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
    AND d.poc = m.poc
WHERE d.parameter_code = '44201'  -- Ozone
  AND m.monitor_type IS NOT NULL
GROUP BY m.monitor_type, m.networks
ORDER BY measurement_count DESC;
```

---

## Query 6: Site elevation impact
**Question**: Analyze how elevation affects air quality measurements.

**SQL**:
```sql
SELECT 
    CASE 
        WHEN s.elevation < 100 THEN 'Low (<100m)'
        WHEN s.elevation < 500 THEN 'Medium (100-500m)'
        WHEN s.elevation < 1000 THEN 'High (500-1000m)'
        ELSE 'Very High (>1000m)'
    END as elevation_zone,
    COUNT(DISTINCT CONCAT(s.state_code, s.county_code, s.site_number)) as site_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    MIN(d.arithmetic_mean) as min_pm25,
    MAX(d.arithmetic_mean) as max_pm25
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
WHERE d.parameter_code = '88101'
  AND s.elevation IS NOT NULL
GROUP BY elevation_zone
ORDER BY 
    CASE elevation_zone
        WHEN 'Low (<100m)' THEN 1
        WHEN 'Medium (100-500m)' THEN 2
        WHEN 'High (500-1000m)' THEN 3
        WHEN 'Very High (>1000m)' THEN 4
    END;
```

---

## Query 7: Complete site-monitor-measurement view
**Question**: Create a comprehensive view showing measurements with full site and monitor details.

**SQL**:
```sql
SELECT 
    d.date_local,
    d.parameter_name,
    d.arithmetic_mean,
    d.first_max_value,
    d.aqi,
    s.local_site_name,
    s.city_name,
    s.state_name,
    s.land_use,
    s.location_setting,
    m.monitor_type,
    m.last_method,
    m.monitoring_objective,
    m.measurement_scale
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
    AND d.poc = m.poc
WHERE d.date_local LIKE '2024-06%'  -- June 2024
  AND d.parameter_code = '88101'
ORDER BY d.arithmetic_mean DESC
LIMIT 50;
```

---

## Query 8: Agency comparison
**Question**: Compare air quality measurements by reporting agency.

**SQL**:
```sql
SELECT 
    m.reporting_agency,
    COUNT(DISTINCT CONCAT(m.state_code, m.county_code, m.site_number)) as site_count,
    COUNT(*) as measurement_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    AVG(d.observation_count) as avg_observation_count,
    AVG(d.observation_percent) as avg_observation_percent
FROM epa.daily_summary d
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
    AND d.poc = m.poc
WHERE d.parameter_code = '88101'
  AND m.reporting_agency IS NOT NULL
GROUP BY m.reporting_agency
HAVING COUNT(*) >= 1000
ORDER BY measurement_count DESC;
```

---

## Query 9: Active vs closed sites
**Question**: Compare measurements from active sites vs closed sites.

**SQL**:
```sql
SELECT 
    CASE 
        WHEN s.site_closed_date IS NULL THEN 'Active'
        ELSE 'Closed'
    END as site_status,
    COUNT(DISTINCT CONCAT(s.state_code, s.county_code, s.site_number)) as site_count,
    COUNT(*) as measurement_count,
    AVG(d.arithmetic_mean) as avg_pm25,
    MIN(d.date_local) as earliest_measurement,
    MAX(d.date_local) as latest_measurement
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
WHERE d.parameter_code = '88101'
GROUP BY site_status;
```

---

## Query 10: Multi-table aggregation with site context
**Question**: Find the top monitoring sites by average pollution, including site and monitor metadata.

**SQL**:
```sql
SELECT 
    s.local_site_name,
    s.city_name,
    s.state_name,
    s.land_use,
    s.location_setting,
    COUNT(DISTINCT m.parameter_code) as pollutants_measured,
    COUNT(*) as total_measurements,
    AVG(d.arithmetic_mean) as avg_concentration,
    MAX(d.first_max_value) as max_concentration,
    AVG(d.aqi) as avg_aqi
FROM epa.daily_summary d
JOIN epa.sites s ON 
    d.state_code = s.state_code 
    AND d.county_code = s.county_code 
    AND d.site_num = s.site_number
JOIN epa.monitors m ON 
    d.state_code = m.state_code 
    AND d.county_code = m.county_code 
    AND d.site_num = m.site_number
    AND d.parameter_code = m.parameter_code
WHERE d.parameter_code = '88101'
  AND s.local_site_name IS NOT NULL
GROUP BY 
    s.local_site_name,
    s.city_name,
    s.state_name,
    s.land_use,
    s.location_setting
HAVING COUNT(*) >= 100
ORDER BY avg_concentration DESC
LIMIT 20;
```

