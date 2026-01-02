# EPA Air Quality Data Analysis

## Data Overview

### File Structure

The EPA data folder contains three main types of files:

1. **Daily Summary Files**: Hourly/daily air quality measurements
   - `daily_44201_*.csv` - Ozone (O₃) data (Parameter Code: 44201)
   - `daily_88101_*.csv` - PM2.5 data (Parameter Code: 88101)
   - Years: 2019-2024
   - Total: ~2.3M rows for Ozone, ~2.7M rows for PM2.5

2. **Sites File**: `aqs_sites.csv`
   - 20,953 site records
   - 5.8 MB
   - Contains location and metadata for monitoring sites

3. **Monitors File**: `aqs_monitors.csv`
   - 368,481 monitor records
   - 153 MB
   - Contains monitor-specific information (parameters measured, methods, etc.)

---

## 1. Sites Table (aqs_sites.csv)

### Schema

| Column | Description | Example |
|--------|-------------|---------|
| State Code | 2-digit state code | "01" (Alabama) |
| County Code | 3-digit county code | "001" |
| Site Number | 4-digit site identifier | "0001" |
| Latitude | Site latitude | 32.437458 |
| Longitude | Site longitude | -86.472891 |
| Datum | Coordinate system | "WGS84" |
| Elevation | Elevation in meters | 64 |
| Land Use | Land use category | "RESIDENTIAL", "AGRICULTURAL" |
| Location Setting | Location type | "SUBURBAN", "RURAL" |
| Site Established Date | When site was established | "1974-05-01" |
| Site Closed Date | When site was closed (if applicable) | "1976-12-31" |
| Local Site Name | Human-readable site name | "KING ARTHUR TRAILER COURT" |
| Address | Physical address | "KING ARTHUR TRAILER COURT, PRATTVILLE,AL" |
| State Name | Full state name | "Alabama" |
| County Name | Full county name | "Autauga" |
| City Name | City name | "Prattville" |
| CBSA Name | Metropolitan area | "Montgomery, AL" |

### Key Characteristics

- **Primary Key**: (State Code, County Code, Site Number)
- **Total Sites**: 20,953 unique sites
- **Geographic Coverage**: All 50 US states + territories
- **Temporal Coverage**: Sites established from 1970s to present
- **Site Status**: Includes both active and closed sites

### Relationships

- **One-to-Many with Monitors**: One site can have multiple monitors (different parameters)
- **One-to-Many with Daily Summary**: One site can have many daily measurements

---

## 2. Monitors Table (aqs_monitors.csv)

### Schema

| Column | Description | Example |
|--------|-------------|---------|
| State Code | 2-digit state code | "01" |
| County Code | 3-digit county code | "001" |
| Site Number | 4-digit site identifier | "0001" |
| Parameter Code | Parameter being measured | "44201" (Ozone), "88101" (PM2.5) |
| Parameter Name | Full parameter name | "Ozone", "PM2.5 - Local Conditions" |
| POC | Parameter Occurrence Code (multiple monitors for same parameter) | 1, 2, 3... |
| Latitude | Monitor latitude | 32.437458 |
| Longitude | Monitor longitude | -86.472891 |
| First Year of Data | First year monitor collected data | 1974 |
| Last Sample Date | Last date monitor collected data | "1974-06-10" |
| Monitor Type | Type of monitor | "OTHER", "SLAMS", "NAMS" |
| Last Method Code | Method used for measurement | "087" |
| Last Method | Method description | "INSTRUMENTAL - ULTRA VIOLET ABSORPTION" |
| Measurement Scale | Scale of measurement | "NEIGHBORHOOD", "URBAN SCALE" |
| NAAQS Primary Monitor | Is this a primary NAAQS monitor? | "Y" or "N" |
| QA Primary Monitor | Is this a QA primary monitor? | "Y" or "N" |

### Key Characteristics

- **Primary Key**: (State Code, County Code, Site Number, Parameter Code, POC)
- **Total Records**: 368,481 monitor configurations
- **Unique Monitors**: ~368,481 (each record is a unique monitor configuration)
- **Parameters**: Multiple parameter types (Ozone, PM2.5, PM10, CO, NO2, SO2, etc.)
- **Temporal Coverage**: Monitors from 1970s to present

### Relationships

- **Many-to-One with Sites**: Multiple monitors can be at the same site (different parameters or POCs)
- **One-to-Many with Daily Summary**: One monitor configuration produces many daily measurements

### Parameter Codes

Common parameter codes in the data:
- **44201**: Ozone (O₃)
- **88101**: PM2.5 - Local Conditions
- **42401**: Sulfur dioxide (SO₂)
- **42101**: Carbon monoxide (CO)
- **42602**: Nitrogen dioxide (NO₂)
- **81102**: PM10

---

## 3. Daily Summary Table (daily_44201_*.csv, daily_88101_*.csv)

### Schema

| Column | Description | Example |
|--------|-------------|---------|
| State Code | 2-digit state code | "01" |
| County Code | 3-digit county code | "003" |
| Site Num | 4-digit site identifier | "0010" |
| Parameter Code | Parameter being measured | "44201" (Ozone) |
| POC | Parameter Occurrence Code | 1 |
| Latitude | Site latitude | 30.497478 |
| Longitude | Site longitude | -87.880258 |
| Parameter Name | Full parameter name | "Ozone" |
| Sample Duration | Sampling duration | "8-HR RUN AVG BEGIN HOUR" |
| Pollutant Standard | Standard being measured against | "Ozone 8-hour 2015" |
| Date Local | Measurement date (local time) | "2020-02-29" |
| Units of Measure | Measurement units | "Parts per million" |
| Event Type | Special event indicator | "None" |
| Observation Count | Number of observations in period | 1, 17, 24 |
| Observation Percent | Percentage of valid observations | 6.0, 100.0 |
| Arithmetic Mean | Average concentration | 0.005, 0.046941 |
| 1st Max Value | Maximum value | 0.005, 0.051 |
| 1st Max Hour | Hour of maximum value | 23, 10 |
| AQI | Air Quality Index | 5, 47 |
| Method Code | Measurement method code | "087" |
| Method Name | Measurement method | "INSTRUMENTAL - ULTRA VIOLET ABSORPTION" |
| Local Site Name | Site name | "FAIRHOPE, Alabama" |
| Address | Site address | "FAIRHOPE HIGH SCHOOL..." |
| State Name | Full state name | "Alabama" |
| County Name | Full county name | "Baldwin" |
| City Name | City name | "Fairhope" |
| CBSA Name | Metropolitan area | "Daphne-Fairhope-Foley, AL" |

### Key Characteristics

**Ozone Data (44201)**:
- Years: 2019-2024
- Total Records: ~2.3M rows
- File Sizes: ~128-131 MB per year
- Date Range: Daily measurements
- Sample Duration: Typically "8-HR RUN AVG BEGIN HOUR"

**PM2.5 Data (88101)**:
- Years: 2019-2024
- Total Records: ~2.7M rows
- File Sizes: ~233-271 MB per year
- Date Range: Daily measurements
- Sample Duration: Typically "24-HR BLK AVG"

### Data Quality Indicators

- **Observation Count**: Number of valid observations in the measurement period
- **Observation Percent**: Percentage of expected observations that were valid
- **Event Type**: Indicates special events (e.g., "None", "Excluded", "Included")

---

## 4. Data Relationships

### Entity Relationship Diagram

```
Sites (aqs_sites.csv)
├── Primary Key: (State Code, County Code, Site Number)
├── Attributes: Location, Address, Land Use, etc.
│
├── 1:N ──→ Monitors (aqs_monitors.csv)
│           ├── Foreign Key: (State Code, County Code, Site Number)
│           ├── Attributes: Parameter Code, POC, Method, etc.
│           │
│           └── 1:N ──→ Daily Summary (daily_*.csv)
│                       ├── Foreign Key: (State Code, County Code, Site Num, Parameter Code, POC)
│                       └── Attributes: Date, Measurements, AQI, etc.
│
└── 1:N ──→ Daily Summary (daily_*.csv)
            └── Foreign Key: (State Code, County Code, Site Num)
```

### Join Relationships

1. **Sites to Daily Summary**:
   - Join Key: `(State Code, County Code, Site Number)`
   - Relationship: One site has many daily measurements
   - Use Case: Get site location/metadata for daily measurements

2. **Monitors to Daily Summary**:
   - Join Key: `(State Code, County Code, Site Number, Parameter Code, POC)`
   - Relationship: One monitor configuration produces many daily measurements
   - Use Case: Get monitor method/type information for measurements

3. **Sites to Monitors**:
   - Join Key: `(State Code, County Code, Site Number)`
   - Relationship: One site can have multiple monitors (different parameters)
   - Use Case: List all parameters measured at a site

### Example Queries Enabled by Relationships

1. **Site Information for Measurements**:
   ```sql
   SELECT d.*, s.Local_Site_Name, s.Address, s.Land_Use
   FROM daily_summary d
   JOIN sites s ON d.State_Code = s.State_Code 
                AND d.County_Code = s.County_Code 
                AND d.Site_Num = s.Site_Number
   WHERE d.Parameter_Code = '44201'
   ```

2. **Monitor Details for Measurements**:
   ```sql
   SELECT d.*, m.Monitor_Type, m.Last_Method, m.Measurement_Scale
   FROM daily_summary d
   JOIN monitors m ON d.State_Code = m.State_Code 
                   AND d.County_Code = m.County_Code 
                   AND d.Site_Num = m.Site_Number
                   AND d.Parameter_Code = m.Parameter_Code
                   AND d.POC = m.POC
   ```

3. **Complete Site-Monitor-Measurement View**:
   ```sql
   SELECT 
       d.Date_Local,
       d.Arithmetic_Mean,
       d.AQI,
       s.Local_Site_Name,
       s.State_Name,
       s.County_Name,
       m.Monitor_Type,
       m.Last_Method
   FROM daily_summary d
   JOIN sites s ON d.State_Code = s.State_Code 
                AND d.County_Code = s.County_Code 
                AND d.Site_Num = s.Site_Number
   JOIN monitors m ON d.State_Code = m.State_Code 
                AND d.County_Code = m.County_Code 
                AND d.Site_Num = m.Site_Number
                AND d.Parameter_Code = m.Parameter_Code
                AND d.POC = m.POC
   WHERE d.Parameter_Code = '44201'
   ```

---

## 5. Data Quality and Characteristics

### Completeness

- **Sites**: 20,953 sites across all states
- **Monitors**: 368,481 monitor configurations
- **Daily Measurements**: 
  - Ozone: ~2.3M records (2019-2024)
  - PM2.5: ~2.7M records (2019-2024)

### Temporal Coverage

- **Sites**: Established from 1970s to present (includes closed sites)
- **Monitors**: Active from 1970s to present
- **Daily Summary**: 2019-2024 (in our dataset)

### Geographic Coverage

- All 50 US states
- Territories (Puerto Rico, etc.)
- Metropolitan areas (CBSA)

### Data Quality Indicators

1. **Observation Percent**: Indicates data completeness for each measurement period
2. **Event Type**: Flags special events or data exclusions
3. **Site Status**: Active vs. closed sites
4. **Monitor Status**: Active vs. inactive monitors

---

## 6. Use Cases for JOIN Queries

### 1. Site-Enriched Measurements
**Query**: "Show me PM2.5 measurements with site location and land use information"
**JOIN**: Daily Summary ↔ Sites
**Benefit**: Adds geographic and environmental context to measurements

### 2. Monitor-Method Analysis
**Query**: "Compare measurements from different monitor types"
**JOIN**: Daily Summary ↔ Monitors
**Benefit**: Analyzes measurement methods and their impact on results

### 3. Complete Site Profile
**Query**: "Get all information about a monitoring site including its monitors and recent measurements"
**JOIN**: Sites ↔ Monitors ↔ Daily Summary
**Benefit**: Comprehensive site analysis

### 4. Geographic Aggregations
**Query**: "Average PM2.5 by metropolitan area (CBSA)"
**JOIN**: Daily Summary ↔ Sites (for CBSA grouping)
**Benefit**: Regional air quality analysis

### 5. Method Comparison
**Query**: "Compare AQI values from different measurement methods"
**JOIN**: Daily Summary ↔ Monitors (for method information)
**Benefit**: Quality assurance and method validation

---

## 7. Recommendations for Iceberg Schema Design

### Tables to Create

1. **epa.sites**
   - Primary Key: (state_code, county_code, site_number)
   - Partition: None (small table, ~21K rows)
   - Use for: Site location and metadata lookups

2. **epa.monitors**
   - Primary Key: (state_code, county_code, site_number, parameter_code, poc)
   - Partition: None or by parameter_code (if filtering by parameter)
   - Use for: Monitor configuration and method information

3. **epa.daily_summary**
   - Primary Key: (state_code, county_code, site_num, parameter_code, poc, date_local)
   - Partition: By date_local (day) and parameter_code
   - Use for: Daily air quality measurements
   - **Note**: This is the main fact table

### Relationships for Semantic Layer

```yaml
relationships:
  - name: daily_to_site
    type: many_to_one
    from_table: epa.daily_summary
    from_column: site_num
    to_table: epa.sites
    to_column: site_number
    join_keys:
      - state_code
      - county_code
    
  - name: daily_to_monitor
    type: many_to_one
    from_table: epa.daily_summary
    to_table: epa.monitors
    join_keys:
      - state_code
      - county_code
      - site_num -> site_number
      - parameter_code
      - poc
```

---

## 8. Summary

### Data Volume
- **Sites**: 20,953 records (5.8 MB)
- **Monitors**: 368,481 records (153 MB)
- **Daily Summary**: ~5M records (Ozone + PM2.5, ~1.5 GB)

### Key Insights

1. **Hierarchical Structure**: Sites → Monitors → Daily Measurements
2. **Multiple Parameters**: Same site can measure multiple pollutants
3. **Temporal Dimension**: Daily measurements over multiple years
4. **Geographic Dimension**: Nationwide coverage with state/county/city/CBSA
5. **Quality Indicators**: Observation counts, percentages, event types

### JOIN Opportunities

The data structure provides rich opportunities for JOIN queries:
- Enriching measurements with site metadata
- Analyzing monitor methods and types
- Geographic aggregations (state, county, CBSA)
- Temporal analysis with site/monitor context
- Quality assurance across measurement methods

This structure is ideal for demonstrating the DuckDB JOIN capabilities in the LangChain-Iceberg framework!

