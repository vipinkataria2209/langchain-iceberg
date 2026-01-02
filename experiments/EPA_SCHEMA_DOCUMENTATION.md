# EPA Air Quality Data Schema Documentation

**Source**: [EPA AirData File Formats Documentation](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html)  
**Version**: 3.0.0 (December 01, 2015)  
**Last Updated**: 2026-01-02

This document describes the complete schema for all EPA Air Quality tables used in this project:
- **Sites**: Monitoring site locations and metadata
- **Monitors**: Monitor equipment and configuration details
- **Daily Summary**: Daily air quality measurements for all pollutants

---

## Table of Contents

1. [Sites Table](#1-sites-table)
2. [Monitors Table](#2-monitors-table)
3. [Daily Summary Table](#3-daily-summary-table)
4. [Parameter Codes Reference](#4-parameter-codes-reference)
5. [Table Relationships](#5-table-relationships)
6. [Data Types](#6-data-types)

---

## 1. Sites Table

### Overview

Each unique geographic location that contains monitors is called a "site" in AQS. Information about the geographic setting is stored in the site record. A unique site is identified by the combination of **state code**, **county code**, and **site number** (within county).

**Table Name**: `epa.sites`  
**Source File**: `aqs_sites.csv` (from `aqs_sites.zip`)  
**Estimated Rows**: ~20,952 sites

### Schema

| Field # | Column Name | Data Type | Required | Description |
|---------|-------------|-----------|----------|-------------|
| 1 | `state_code` | string | No | The FIPS code of the state in which the monitor resides (2 digits) |
| 2 | `county_code` | string | No | The FIPS code of the county in which the monitor resides (3 digits) |
| 3 | `site_number` | string | No | A unique number within the county identifying the site (4 digits) |
| 4 | `latitude` | double | No | The monitoring site's angular distance north of the equator measured in decimal degrees |
| 5 | `longitude` | double | No | The monitoring site's angular distance east of the prime meridian measured in decimal degrees |
| 6 | `datum` | string | No | The Datum associated with the Latitude and Longitude measures (e.g., WGS84, NAD83) |
| 7 | `elevation` | long | No | The elevation of the ground at the site in meters above mean sea level |
| 8 | `land_use` | string | No | A category describing the predominant land use within a 1/4 mile radius of the site (RESIDENTIAL, AGRICULTURAL, COMMERCIAL, etc.) |
| 9 | `location_setting` | string | No | A description of the setting within which the monitoring site is located (URBAN, SUBURBAN, RURAL, etc.) |
| 10 | `site_established_date` | string | No | The date when the site began operating (YYYY-MM-DD format) |
| 11 | `site_closed_date` | string | No | The date on which the operating agency indicated that all operations ceased at this site (YYYY-MM-DD format) |
| 12 | `met_site_state_code` | string | No | Where sites are required to collect meteorological data, they may list a surrogate site. This contains the AQS State Code identifier for that site |
| 13 | `met_site_county_code` | string | No | AQS County Code identifier for the meteorological surrogate site |
| 14 | `met_site_site_number` | string | No | AQS Site Number for the meteorological surrogate site |
| 15 | `met_site_type` | string | No | Type of surrogate meteorological site (e.g., AQS site, National Weather Service site) |
| 16 | `met_site_distance` | string | No | Distance from this site to the met site in meters |
| 17 | `met_site_direction` | string | No | Direction from this site to the met site (true, not magnetic, direction) |
| 18 | `gmt_offset` | long | No | The time difference (in hours) between local standard time at this site and GMT |
| 19 | `owning_agency` | string | No | The name of the agency that owns or controls the land at the site |
| 20 | `local_site_name` | string | No | The name of the site (if any) given by the State, local, or tribal air pollution control agency that operates it |
| 21 | `address` | string | No | The approximate street address of the monitoring site |
| 22 | `zip_code` | string | No | The postal zip code in which the monitoring site resides |
| 23 | `state_name` | string | No | The name of the state where the monitoring site is located |
| 24 | `county_name` | string | No | The name of the county where the monitoring site is located |
| 25 | `city_name` | string | No | The name of the city where the monitoring site is located (represents legal incorporated boundaries, not urban areas) |
| 26 | `cbsa_name` | string | No | The name of the core based statistical area (metropolitan area) where the monitoring site is located |
| 27 | `tribe_name` | string | No | If this site resides on tribal lands and the tribe has chosen to identify the site with tribal identifiers, this is the name of the tribe that owns the site |
| 28 | `extraction_date` | string | No | The date on which this data was retrieved from the AQS Data Mart (YYYY-MM-DD format) |

### Primary Key

The primary key for the sites table is a composite key:
- `state_code` + `county_code` + `site_number`

### Example Site ID

Site IDs are written as: `SS-CCC-NNNN`

Where:
- `SS` = State FIPS code (2 digits)
- `CCC` = County FIPS code (3 digits)
- `NNNN` = Site Number within county (4 digits, with leading zeros)

Example: `01-089-0014` = Alabama, Madison County, Site Number 14

---

## 2. Monitors Table

### Overview

Each parameter that is measured at a site is considered a "monitor" in AQS. A "monitor" does not necessarily correspond to a physical instrument/sampler. AQS tracks administrative information about monitors including who operates them, the methods being used, the networks they belong to, etc.

A unique monitor is identified by the combination of:
- **state code** + **county code** + **site number** (within county)
- **parameter code** (the pollutant being measured)
- **POC** (Parameter Occurrence Code, used to differentiate when a parameter is measured more than once at a site)

**Table Name**: `epa.monitors`  
**Source File**: `aqs_monitors.csv` (from `aqs_monitors.zip`)  
**Estimated Rows**: ~368,480 monitors

### Schema

| Field # | Column Name | Data Type | Required | Description |
|---------|-------------|-----------|----------|-------------|
| 1 | `state_code` | string | No | The FIPS code of the state in which the monitor resides |
| 2 | `county_code` | string | No | The FIPS code of the county in which the monitor resides |
| 3 | `site_number` | string | No | A unique number within the county identifying the site |
| 4 | `parameter_code` | string | No | The AQS 5-digit parameter code (e.g., 88101 for PM2.5, 44201 for Ozone) |
| 5 | `parameter_name` | string | No | The name or description assigned in AQS to the parameter measured by the monitor |
| 6 | `poc` | long | No | Parameter Occurrence Code - used to distinguish different instruments that measure the same parameter at the same site |
| 7 | `latitude` | double | No | The monitoring site's angular distance north of the equator measured in decimal degrees |
| 8 | `longitude` | double | No | The monitoring site's angular distance east of the prime meridian measured in decimal degrees |
| 9 | `datum` | string | No | The Datum associated with the Latitude and Longitude measures |
| 10 | `first_year_of_data` | long | No | The first year this monitor collected data |
| 11 | `last_sample_date` | string | No | The last date this monitor collected data (YYYY-MM-DD format) |
| 12 | `monitor_type` | string | No | Type of monitor: SLAMS (State/Local Air Monitoring Stations), NAMS (National Air Monitoring Stations), PAMS (Photochemical Assessment Monitoring Stations), OTHER |
| 13 | `networks` | string | No | Monitoring networks this monitor belongs to (comma-separated list) |
| 14 | `reporting_agency` | string | No | The name of the agency that reports data from this monitor |
| 15 | `pqao` | string | No | Primary Quality Assurance Organization |
| 16 | `collecting_agency` | string | No | The name of the agency that collects data from this monitor |
| 17 | `exclusions` | string | No | Any exclusions that apply to this monitor |
| 18 | `monitoring_objective` | string | No | Objective of monitoring (e.g., HIGHEST CONCENTRATION, POPULATION EXPOSURE, BACKGROUND, SECONDARY MAXIMUM, etc.) |
| 19 | `last_method_code` | string | No | The last method code used by this monitor |
| 20 | `last_method` | string | No | A short description of the processes, equipment, and protocols used in gathering and measuring the sample |
| 21 | `measurement_scale` | string | No | Measurement scale (e.g., NEIGHBORHOOD, URBAN SCALE, REGIONAL SCALE, etc.) |
| 22 | `measurement_scale_definition` | string | No | Measurement scale definition (e.g., "500 M TO 4KM") |
| 23 | `naaqs_primary_monitor` | string | No | Whether this is a NAAQS (National Ambient Air Quality Standards) primary monitor (Y/N) |
| 24 | `qa_primary_monitor` | string | No | Whether this is a QA (Quality Assurance) primary monitor (Y/N) |
| 25 | `local_site_name` | string | No | The name of the site given by the operating agency |
| 26 | `address` | string | No | The approximate street address of the monitoring site |
| 27 | `state_name` | string | No | The name of the state where the monitoring site is located |
| 28 | `county_name` | string | No | The name of the county where the monitoring site is located |
| 29 | `city_name` | string | No | The name of the city where the monitoring site is located |
| 30 | `cbsa_name` | string | No | The name of the core based statistical area (metropolitan area) where the monitoring site is located |
| 31 | `tribe_name` | string | No | If this site resides on tribal lands, this is the name of the tribe that owns the site |
| 32 | `extraction_date` | string | No | The date on which this data was retrieved from the AQS Data Mart |

### Primary Key

The primary key for the monitors table is a composite key:
- `state_code` + `county_code` + `site_number` + `parameter_code` + `poc`

### Example Monitor ID

Monitor IDs are written as: `SS-CCC-NNNN-PPPPP-Q`

Where:
- `SS` = State FIPS code (2 digits)
- `CCC` = County FIPS code (3 digits)
- `NNNN` = Site Number within county (4 digits)
- `PPPPP` = AQS 5-digit parameter code
- `Q` = POC (Parameter Occurrence Code)

Example: `01-089-0014-44201-2` = Alabama, Madison County, Site 14, Ozone monitor, POC 2

---

## 3. Daily Summary Table

### Overview

The daily summary table contains daily aggregated air quality measurements for all pollutants. Each record represents one day's measurements for a specific monitor (site + parameter + POC combination).

**Table Name**: `epa.daily_summary`  
**Source Files**: `daily_88101_YYYY.csv` (PM2.5), `daily_44201_YYYY.csv` (Ozone), `daily_42401_YYYY.csv` (SO2), `daily_42101_YYYY.csv` (CO), `daily_42602_YYYY.csv` (NO2)  
**Time Range**: 2014-2024 (11 years)  
**Estimated Rows**: Millions of daily measurements

### Schema

| Field # | Column Name | Data Type | Required | Description |
|---------|-------------|-----------|----------|-------------|
| 1 | `state_code` | string | No | The FIPS code of the state in which the monitor resides |
| 2 | `county_code` | string | No | The FIPS code of the county in which the monitor resides |
| 3 | `site_num` | string | No | A unique number within the county identifying the site |
| 4 | `parameter_code` | string | No | The AQS 5-digit parameter code (e.g., 88101, 44201, 42401, 42101, 42602) |
| 5 | `poc` | long | No | Parameter Occurrence Code - used to distinguish different instruments measuring the same parameter at the same site |
| 6 | `latitude` | double | Yes | The monitoring site's angular distance north of the equator measured in decimal degrees |
| 7 | `longitude` | double | Yes | The monitoring site's angular distance east of the prime meridian measured in decimal degrees |
| 8 | `datum` | string | Yes | The Datum associated with the Latitude and Longitude measures |
| 9 | `parameter_name` | string | Yes | The name or description assigned in AQS to the parameter measured |
| 10 | `sample_duration` | string | Yes | The averaging period for the sample (e.g., "1 HOUR", "24 HOUR", "8-HR RUN AVG BEGIN HOUR") |
| 11 | `pollutant_standard` | string | Yes | A description of the ambient air quality standard rules used to aggregate statistics |
| 12 | `date_local` | string | No | The calendar date of the measurement in Local Standard Time at the monitor (YYYY-MM-DD format) |
| 13 | `units_of_measure` | string | Yes | The unit of measure for the parameter (e.g., "Micrograms/cubic meter (LC)", "Parts per million") |
| 14 | `event_type` | string | Yes | Indicates whether the day had exceptional events (e.g., "None", "Excluded", "Concurred Excluded") |
| 15 | `observation_count` | long | Yes | The total number of observations (samples) taken during the day |
| 16 | `observation_percent` | double | Yes | The percentage of possible observations that were made during the day |
| 17 | `arithmetic_mean` | double | Yes | The average (arithmetic mean) value for the day |
| 18 | `first_max_value` | double | Yes | The maximum value observed during the day |
| 19 | `first_max_hour` | long | Yes | The hour (0-23) when the first maximum value was observed |
| 20 | `aqi` | long | Yes | The Air Quality Index value for the day (calculated for Criteria Gases and PM) |
| 21 | `method_code` | string | Yes | The method code used to collect the data |
| 22 | `method_name` | string | Yes | A short description of the processes, equipment, and protocols used in gathering and measuring the sample |
| 23 | `local_site_name` | string | Yes | The name of the site given by the operating agency |
| 24 | `address` | string | Yes | The approximate street address of the monitoring site |
| 25 | `state_name` | string | Yes | The name of the state where the monitoring site is located |
| 26 | `county_name` | string | Yes | The name of the county where the monitoring site is located |
| 27 | `city_name` | string | Yes | The name of the city where the monitoring site is located |
| 28 | `cbsa_name` | string | Yes | The name of the core based statistical area (metropolitan area) where the monitoring site is located |
| 29 | `date_of_last_change` | string | Yes | The date the last time any numeric values in this record were updated in the AQS data system |

### Primary Key

The primary key for the daily_summary table is a composite key:
- `state_code` + `county_code` + `site_num` + `parameter_code` + `poc` + `date_local`

### Partitioning

The daily_summary table is partitioned by `date_local` (day) for efficient time-based queries.

---

## 4. Parameter Codes Reference

The following parameter codes are used in this project:

| Parameter Code | Parameter Name | Units | Description |
|---------------|----------------|-------|-------------|
| **88101** | PM2.5 - Local Conditions | μg/m³ | Fine particulate matter (2.5 micrometers or smaller) |
| **44201** | Ozone | ppm | Ground-level ozone |
| **42401** | Sulfur Dioxide (SO2) | ppm | Sulfur dioxide |
| **42101** | Carbon Monoxide (CO) | ppm | Carbon monoxide |
| **42602** | Nitrogen Dioxide (NO2) | ppm | Nitrogen dioxide |

### Air Quality Index (AQI) Scale

The AQI values in the daily_summary table follow the EPA AQI scale:

| AQI Range | Category | Health Concern |
|-----------|----------|---------------|
| 0-50 | Good | Air quality is satisfactory |
| 51-100 | Moderate | Acceptable for most people |
| 101-150 | Unhealthy for Sensitive Groups | Members of sensitive groups may experience health effects |
| 151-200 | Unhealthy | Everyone may begin to experience health effects |
| 201-300 | Very Unhealthy | Health alert: everyone may experience more serious health effects |
| 301-500 | Hazardous | Health warning of emergency conditions |

---

## 5. Table Relationships

### Relationship: daily_summary → sites

**Type**: Many-to-One  
**Join Condition**:
```sql
daily_summary.state_code = sites.state_code
AND daily_summary.county_code = sites.county_code
AND daily_summary.site_num = sites.site_number
```

**Purpose**: Enrich daily measurements with site location and metadata (elevation, land use, location setting, etc.)

### Relationship: daily_summary → monitors

**Type**: Many-to-One  
**Join Condition**:
```sql
daily_summary.state_code = monitors.state_code
AND daily_summary.county_code = monitors.county_code
AND daily_summary.site_num = monitors.site_number
AND daily_summary.parameter_code = monitors.parameter_code
AND daily_summary.poc = monitors.poc
```

**Purpose**: Enrich daily measurements with monitor configuration details (monitor type, networks, methods, etc.)

### Relationship: sites → monitors

**Type**: One-to-Many  
**Join Condition**:
```sql
sites.state_code = monitors.state_code
AND sites.county_code = monitors.county_code
AND sites.site_number = monitors.site_number
```

**Purpose**: Find all monitors at a given site

---

## 6. Data Types

### Iceberg Schema Types

| Iceberg Type | Python Type | Description |
|--------------|-------------|-------------|
| `StringType` | `str` | Variable-length string |
| `LongType` | `int` | 64-bit signed integer |
| `DoubleType` | `float` | 64-bit floating point |

### Date Handling

Dates are stored as **strings** in `YYYY-MM-DD` format:
- `site_established_date`
- `site_closed_date`
- `last_sample_date`
- `date_local`
- `extraction_date`
- `date_of_last_change`

This allows for:
- Easy string-based filtering and parsing
- Handling of NULL/empty dates
- Compatibility with CSV source files

### Null Handling

All fields are marked as `required=False` to handle:
- Missing data in source CSV files
- Historical data gaps
- Optional fields in EPA data

Null values are represented as:
- Empty strings: `""`
- Special values: `"NA"`, `"N/A"`, `"NULL"`, `"null"`

---

## References

- **EPA AirData File Formats**: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html
- **EPA AirData Download Page**: https://aqs.epa.gov/aqsweb/airdata/download_files.html
- **EPA AQS Code Tables**: https://aqs.epa.gov/aqsweb/code_lookup.html
- **NAAQS Standards**: https://www.epa.gov/criteria-air-pollutants/naaqs-table

---

**Document Version**: 1.0  
**Last Updated**: 2026-01-02  
**Maintained By**: LangChain Iceberg Project

