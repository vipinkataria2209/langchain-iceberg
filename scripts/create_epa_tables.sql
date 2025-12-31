-- Create EPA Air Quality tables in Iceberg
-- Based on EPA AirData daily summary file format
-- Documentation: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html

-- Create namespace
CREATE NAMESPACE IF NOT EXISTS epa;

-- Create Daily Summary table
-- This matches the EPA daily summary CSV format
CREATE TABLE IF NOT EXISTS epa.daily_summary (
    state_code STRING,
    county_code STRING,
    site_num STRING,
    parameter_code STRING,
    poc INT,
    latitude DOUBLE,
    longitude DOUBLE,
    datum STRING,
    parameter_name STRING,
    date_local TIMESTAMP,
    date_gmt TIMESTAMP,
    sample_duration STRING,
    pollutant_standard STRING,
    units_of_measure STRING,
    event_type STRING,
    observation_count INT,
    observations_with_events INT,
    null_observations INT,
    arithmetic_mean DOUBLE,
    first_max_value DOUBLE,
    first_max_hour INT,
    aqi INT,
    method_code STRING,
    method_name STRING,
    state_name STRING,
    county_name STRING,
    date_of_last_change TIMESTAMP
) USING iceberg
PARTITIONED BY (days(date_local));

-- Create index-like table for quick parameter lookups
-- (Iceberg doesn't support indexes, but we can create a smaller summary table)
CREATE TABLE IF NOT EXISTS epa.parameter_summary (
    parameter_code STRING,
    parameter_name STRING,
    units_of_measure STRING,
    sample_duration STRING
) USING iceberg;

-- Insert common parameter codes
INSERT INTO epa.parameter_summary VALUES
('88101', 'PM2.5 - Local Conditions', 'Micrograms/cubic meter (LC)', '24-HR BLK AVG'),
('44201', 'Ozone', 'Parts per million', '8-HR RUN AVG BEGIN HOUR'),
('81102', 'PM10 Total 0-10um STP', 'Micrograms/cubic meter (25 C)', '24-HR BLK AVG'),
('42101', 'Carbon monoxide', 'Parts per million', '1-HR'),
('42602', 'Nitrogen dioxide (NO2)', 'Parts per million', '1-HR'),
('42401', 'Sulfur dioxide', 'Parts per million', '1-HR');

-- Note: Actual EPA data should be loaded using the epa_data_loader.py script
-- or by importing CSV files directly into the daily_summary table

