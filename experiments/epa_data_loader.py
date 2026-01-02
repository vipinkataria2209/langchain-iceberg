#!/usr/bin/env python3
"""
EPA Air Quality Data Loader

Complete loader for all EPA Air Quality tables:
- sites: Monitoring site locations and metadata
- monitors: Monitor equipment and configuration details  
- daily_summary: Daily air quality measurements for all pollutants

Based on EPA AirData file format: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html
"""

import csv
import gzip
import os
from pathlib import Path
from typing import Optional, Dict, Any
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
import pyarrow as pa
import pyarrow.csv as csv_arrow
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, DoubleType, TimestampType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform


class EPADataLoader:
    """
    Complete loader for all EPA Air Quality tables.
    
    Handles:
    - sites: Monitoring site locations and metadata
    - monitors: Monitor equipment and configuration details
    - daily_summary: Daily air quality measurements for all pollutants
    """
    
    # EPA Daily Summary file column mapping - actual CSV headers
    # Based on actual file structure from EPA downloads
    COLUMN_MAPPING = {
        'State Code': 'state_code',
        'County Code': 'county_code', 
        'Site Num': 'site_num',
        'Parameter Code': 'parameter_code',
        'POC': 'poc',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'Datum': 'datum',
        'Parameter Name': 'parameter_name',
        'Sample Duration': 'sample_duration',
        'Pollutant Standard': 'pollutant_standard',
        'Date Local': 'date_local',
        'Units of Measure': 'units_of_measure',
        'Event Type': 'event_type',
        'Observation Count': 'observation_count',
        'Observation Percent': 'observation_percent',
        'Arithmetic Mean': 'arithmetic_mean',
        '1st Max Value': 'first_max_value',
        '1st Max Hour': 'first_max_hour',
        'AQI': 'aqi',
        'Method Code': 'method_code',
        'Method Name': 'method_name',
        'Local Site Name': 'local_site_name',
        'Address': 'address',
        'State Name': 'state_name',
        'County Name': 'county_name',
        'City Name': 'city_name',
        'CBSA Name': 'cbsa_name',
        'Date of Last Change': 'date_of_last_change',
    }
    
    def __init__(self, catalog: Catalog, namespace: str = "epa"):
        """
        Initialize EPA data loader.
        
        Args:
            catalog: PyIceberg catalog instance
            namespace: Namespace for EPA tables (default: "epa")
        """
        self.catalog = catalog
        self.namespace = namespace
        
    def create_table_schema(self) -> Schema:
        """Create Iceberg schema for EPA daily summary data."""
        from pyiceberg.types import LongType
        return Schema(
            NestedField(1, "state_code", StringType(), required=False),  # Make optional for CSV compatibility
            NestedField(2, "county_code", StringType(), required=False),
            NestedField(3, "site_num", StringType(), required=False),
            NestedField(4, "parameter_code", StringType(), required=False),
            NestedField(5, "poc", LongType(), required=False),  # PyArrow infers as long
            NestedField(6, "latitude", DoubleType()),
            NestedField(7, "longitude", DoubleType()),
            NestedField(8, "datum", StringType()),
            NestedField(9, "parameter_name", StringType()),
            NestedField(10, "sample_duration", StringType()),
            NestedField(11, "pollutant_standard", StringType()),
            NestedField(12, "date_local", StringType(), required=False),  # Keep as string for partitioning
            NestedField(13, "units_of_measure", StringType()),
            NestedField(14, "event_type", StringType()),
            NestedField(15, "observation_count", LongType()),  # PyArrow infers as long
            NestedField(16, "observation_percent", DoubleType()),
            NestedField(17, "arithmetic_mean", DoubleType()),
            NestedField(18, "first_max_value", DoubleType()),
            NestedField(19, "first_max_hour", LongType()),  # PyArrow infers as long
            NestedField(20, "aqi", LongType()),  # PyArrow infers as long
            NestedField(21, "method_code", StringType()),  # Keep as string (some values might be text)
            NestedField(22, "method_name", StringType()),
            NestedField(23, "local_site_name", StringType()),
            NestedField(24, "address", StringType()),
            NestedField(25, "state_name", StringType()),
            NestedField(26, "county_name", StringType()),
            NestedField(27, "city_name", StringType()),
            NestedField(28, "cbsa_name", StringType()),
            NestedField(29, "date_of_last_change", StringType()),  # Keep as string
        )
    
    def create_table(self, table_name: str = "daily_summary") -> Table:
        """
        Create EPA daily summary table in Iceberg.
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            Created Iceberg table
        """
        # Create namespace if it doesn't exist
        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass  # Namespace already exists
        
        # Check if table already exists
        try:
            table = self.catalog.load_table((self.namespace, table_name))
            print(f"✅ Table already exists: {self.namespace}.{table_name}")
            return table
        except Exception:
            pass  # Table doesn't exist, create it
        
        schema = self.create_table_schema()
        
        # Note: Partitioning by date requires converting date_local to timestamp
        # For now, create table without partition to avoid DayTransform issues with string dates
        # TODO: Convert date_local to timestamp and add partition back
        partition_spec = PartitionSpec()  # No partition for now
        
        # Create table
        try:
            table = self.catalog.create_table(
                identifier=(self.namespace, table_name),
                schema=schema,
                partition_spec=partition_spec,
            )
            print(f"✅ Created table: {self.namespace}.{table_name}")
            return table
        except Exception as e:
            print(f"⚠️  Error creating table (may already exist): {e}")
            # Try to load it
            try:
                table = self.catalog.load_table((self.namespace, table_name))
                return table
            except Exception as e2:
                raise Exception(f"Failed to create or load table: {e2}")
    
    def load_csv_file(self, csv_path: str, table: Optional[Table] = None) -> int:
        """
        Load EPA daily summary CSV file into Iceberg table.
        
        Args:
            csv_path: Path to EPA CSV file (can be gzipped)
            table: Iceberg table (will create if not provided)
            
        Returns:
            Number of rows loaded
        """
        if table is None:
            table = self.catalog.load_table((self.namespace, "daily_summary"))
        
        print(f"Loading EPA data from: {csv_path}")
        
        # Read CSV using PyArrow (faster and avoids pandas/NumPy issues)
        read_options = csv_arrow.ReadOptions(use_threads=True)
        parse_options = csv_arrow.ParseOptions(delimiter=',')
        convert_options = csv_arrow.ConvertOptions(
            strings_can_be_null=True,
            null_values=['', 'NA', 'N/A', 'NULL', 'null']
        )
        
        # Read CSV directly to PyArrow table
        if csv_path.endswith('.gz'):
            # PyArrow can read gzipped files
            arrow_table = csv_arrow.read_csv(
                csv_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            )
        else:
            arrow_table = csv_arrow.read_csv(
                csv_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            )
        
        # Convert to pandas for easier column renaming (if available)
        if HAS_PANDAS:
            df = arrow_table.to_pandas()
        else:
            # Use PyArrow directly
            df = None
        
        # Rename columns to match schema using PyArrow
        column_mapping = {k: v for k, v in self.COLUMN_MAPPING.items()}
        
        # Get current column names
        current_cols = arrow_table.column_names
        new_cols = []
        rename_map = {}
        
        for old_name in current_cols:
            if old_name in column_mapping:
                new_name = column_mapping[old_name]
                rename_map[old_name] = new_name
                new_cols.append(new_name)
            else:
                new_cols.append(old_name)
        
        # Rename columns in PyArrow table
        if rename_map:
            arrow_table = arrow_table.rename_columns([
                rename_map.get(col, col) for col in current_cols
            ])
        
        # Select only columns that exist in schema
        schema_cols = [field.name for field in table.schema().fields]
        existing_cols = [col for col in schema_cols if col in arrow_table.column_names]
        
        # Reorder and select columns
        arrow_table = arrow_table.select(existing_cols)
        
        # Cast columns to match schema types
        schema = table.schema()
        cast_columns = []
        column_names = []
        
        for field in schema.fields:
            column_names.append(field.name)
            if field.name in arrow_table.column_names:
                col = arrow_table[field.name]
                # Cast to match schema type
                if isinstance(field.field_type, StringType) and col.type != pa.string():
                    # Convert numeric codes to strings
                    cast_columns.append(col.cast(pa.string()))
                elif isinstance(field.field_type, LongType) and col.type != pa.int64():
                    # Convert to long
                    cast_columns.append(col.cast(pa.int64()))
                elif isinstance(field.field_type, DoubleType) and col.type != pa.float64():
                    # Convert to double
                    cast_columns.append(col.cast(pa.float64()))
                else:
                    cast_columns.append(col)
            else:
                # Missing column - add null column with appropriate type
                arrow_type = field.field_type.to_arrow()
                cast_columns.append(pa.nulls(len(arrow_table), arrow_type))
        
        # Rebuild table with cast columns
        arrow_table = pa.Table.from_arrays(cast_columns, names=column_names)
        
        rows_to_load = len(arrow_table)
        print(f"   Prepared {rows_to_load:,} rows for loading...")
        
        # Append to Iceberg table using PyIceberg native API
        print(f"   Appending to Iceberg table (this may take a moment)...")
        try:
            table.append(arrow_table)
            print(f"✅ Loaded {rows_to_load:,} rows into {self.namespace}.daily_summary")
        except Exception as e:
            print(f"❌ Error appending data: {e}")
            import traceback
            traceback.print_exc()
            raise
        
        return rows_to_load
    
    def load_directory(self, directory: str, pattern: str = "*.csv*") -> Dict[str, int]:
        """
        Load all EPA CSV files from a directory.
        
        Args:
            directory: Directory containing EPA CSV files
            pattern: File pattern to match (default: "*.csv*")
            
        Returns:
            Dictionary mapping file paths to row counts
        """
        results = {}
        path = Path(directory)
        
        # Create table if it doesn't exist
        try:
            table = self.catalog.load_table((self.namespace, "daily_summary"))
        except Exception:
            table = self.create_table()
        
        # Find all matching files
        files = list(path.glob(pattern))
        print(f"Found {len(files)} files to load")
        
        for file_path in files:
            try:
                rows = self.load_csv_file(str(file_path), table)
                results[str(file_path)] = rows
            except Exception as e:
                print(f"❌ Error loading {file_path}: {e}")
                results[str(file_path)] = 0
        
        return results
    
    # ============================================
    # SITES TABLE METHODS
    # ============================================
    
    def create_sites_schema(self) -> Schema:
        """Create Iceberg schema for EPA sites table."""
        return Schema(
            NestedField(1, "state_code", StringType(), required=False),
            NestedField(2, "county_code", StringType(), required=False),
            NestedField(3, "site_number", StringType(), required=False),
            NestedField(4, "latitude", DoubleType(), required=False),
            NestedField(5, "longitude", DoubleType(), required=False),
            NestedField(6, "datum", StringType(), required=False),
            NestedField(7, "elevation", LongType(), required=False),
            NestedField(8, "land_use", StringType(), required=False),
            NestedField(9, "location_setting", StringType(), required=False),
            NestedField(10, "site_established_date", StringType(), required=False),
            NestedField(11, "site_closed_date", StringType(), required=False),
            NestedField(12, "met_site_state_code", StringType(), required=False),
            NestedField(13, "met_site_county_code", StringType(), required=False),
            NestedField(14, "met_site_site_number", StringType(), required=False),
            NestedField(15, "met_site_type", StringType(), required=False),
            NestedField(16, "met_site_distance", StringType(), required=False),
            NestedField(17, "met_site_direction", StringType(), required=False),
            NestedField(18, "gmt_offset", LongType(), required=False),
            NestedField(19, "owning_agency", StringType(), required=False),
            NestedField(20, "local_site_name", StringType(), required=False),
            NestedField(21, "address", StringType(), required=False),
            NestedField(22, "zip_code", StringType(), required=False),
            NestedField(23, "state_name", StringType(), required=False),
            NestedField(24, "county_name", StringType(), required=False),
            NestedField(25, "city_name", StringType(), required=False),
            NestedField(26, "cbsa_name", StringType(), required=False),
            NestedField(27, "tribe_name", StringType(), required=False),
            NestedField(28, "extraction_date", StringType(), required=False),
        )
    
    def create_sites_table(self) -> Table:
        """Create EPA sites table."""
        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass
        
        try:
            table = self.catalog.load_table((self.namespace, "sites"))
            print(f"✅ Table already exists: {self.namespace}.sites")
            return table
        except Exception:
            pass
        
        schema = self.create_sites_schema()
        partition_spec = PartitionSpec()
        
        table = self.catalog.create_table(
            identifier=(self.namespace, "sites"),
            schema=schema,
            partition_spec=partition_spec,
        )
        print(f"✅ Created table: {self.namespace}.sites")
        return table
    
    def load_sites_csv(self, csv_path: str, table: Optional[Table] = None) -> int:
        """Load sites CSV file."""
        if table is None:
            table = self.create_sites_table()
        
        print(f"Loading sites data from: {csv_path}")
        
        column_mapping = {
            'State Code': 'state_code',
            'County Code': 'county_code',
            'Site Number': 'site_number',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Datum': 'datum',
            'Elevation': 'elevation',
            'Land Use': 'land_use',
            'Location Setting': 'location_setting',
            'Site Established Date': 'site_established_date',
            'Site Closed Date': 'site_closed_date',
            'Met Site State Code': 'met_site_state_code',
            'Met Site County Code': 'met_site_county_code',
            'Met Site Site Number': 'met_site_site_number',
            'Met Site Type': 'met_site_type',
            'Met Site Distance': 'met_site_distance',
            'Met Site Direction': 'met_site_direction',
            'GMT Offset': 'gmt_offset',
            'Owning Agency': 'owning_agency',
            'Local Site Name': 'local_site_name',
            'Address': 'address',
            'Zip Code': 'zip_code',
            'State Name': 'state_name',
            'County Name': 'county_name',
            'City Name': 'city_name',
            'CBSA Name': 'cbsa_name',
            'Tribe Name': 'tribe_name',
            'Extraction Date': 'extraction_date',
        }
        
        read_options = csv_arrow.ReadOptions(use_threads=True)
        parse_options = csv_arrow.ParseOptions(delimiter=',')
        convert_options = csv_arrow.ConvertOptions(
            strings_can_be_null=True,
            null_values=['', 'NA', 'N/A', 'NULL', 'null']
        )
        
        arrow_table = csv_arrow.read_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        
        current_cols = arrow_table.column_names
        rename_map = {old: column_mapping.get(old, old) for old in current_cols}
        arrow_table = arrow_table.rename_columns([
            rename_map.get(col, col) for col in current_cols
        ])
        
        schema_cols = [field.name for field in table.schema().fields]
        existing_cols = [col for col in schema_cols if col in arrow_table.column_names]
        arrow_table = arrow_table.select(existing_cols)
        
        schema = table.schema()
        cast_columns = []
        column_names = []
        
        for field in schema.fields:
            column_names.append(field.name)
            if field.name in arrow_table.column_names:
                col = arrow_table[field.name]
                if isinstance(field.field_type, StringType) and col.type != pa.string():
                    cast_columns.append(col.cast(pa.string()))
                elif isinstance(field.field_type, LongType) and col.type != pa.int64():
                    if col.type in (pa.float64(), pa.float32()):
                        cast_columns.append(col.cast(pa.int64(), safe=False))
                    else:
                        cast_columns.append(col.cast(pa.int64()))
                elif isinstance(field.field_type, DoubleType) and col.type != pa.float64():
                    cast_columns.append(col.cast(pa.float64()))
                else:
                    cast_columns.append(col)
            else:
                arrow_type = field.field_type.to_arrow()
                cast_columns.append(pa.nulls(len(arrow_table), arrow_type))
        
        arrow_table = pa.Table.from_arrays(cast_columns, names=column_names)
        
        rows_to_load = len(arrow_table)
        print(f"   Prepared {rows_to_load:,} rows for loading...")
        
        table.append(arrow_table)
        print(f"✅ Loaded {rows_to_load:,} rows into {self.namespace}.sites")
        
        return rows_to_load
    
    # ============================================
    # MONITORS TABLE METHODS
    # ============================================
    
    def create_monitors_schema(self) -> Schema:
        """Create Iceberg schema for EPA monitors table."""
        return Schema(
            NestedField(1, "state_code", StringType(), required=False),
            NestedField(2, "county_code", StringType(), required=False),
            NestedField(3, "site_number", StringType(), required=False),
            NestedField(4, "parameter_code", StringType(), required=False),
            NestedField(5, "parameter_name", StringType(), required=False),
            NestedField(6, "poc", LongType(), required=False),
            NestedField(7, "latitude", DoubleType(), required=False),
            NestedField(8, "longitude", DoubleType(), required=False),
            NestedField(9, "datum", StringType(), required=False),
            NestedField(10, "first_year_of_data", LongType(), required=False),
            NestedField(11, "last_sample_date", StringType(), required=False),
            NestedField(12, "monitor_type", StringType(), required=False),
            NestedField(13, "networks", StringType(), required=False),
            NestedField(14, "reporting_agency", StringType(), required=False),
            NestedField(15, "pqao", StringType(), required=False),
            NestedField(16, "collecting_agency", StringType(), required=False),
            NestedField(17, "exclusions", StringType(), required=False),
            NestedField(18, "monitoring_objective", StringType(), required=False),
            NestedField(19, "last_method_code", StringType(), required=False),
            NestedField(20, "last_method", StringType(), required=False),
            NestedField(21, "measurement_scale", StringType(), required=False),
            NestedField(22, "measurement_scale_definition", StringType(), required=False),
            NestedField(23, "naaqs_primary_monitor", StringType(), required=False),
            NestedField(24, "qa_primary_monitor", StringType(), required=False),
            NestedField(25, "local_site_name", StringType(), required=False),
            NestedField(26, "address", StringType(), required=False),
            NestedField(27, "state_name", StringType(), required=False),
            NestedField(28, "county_name", StringType(), required=False),
            NestedField(29, "city_name", StringType(), required=False),
            NestedField(30, "cbsa_name", StringType(), required=False),
            NestedField(31, "tribe_name", StringType(), required=False),
            NestedField(32, "extraction_date", StringType(), required=False),
        )
    
    def create_monitors_table(self) -> Table:
        """Create EPA monitors table."""
        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass
        
        try:
            table = self.catalog.load_table((self.namespace, "monitors"))
            print(f"✅ Table already exists: {self.namespace}.monitors")
            return table
        except Exception:
            pass
        
        schema = self.create_monitors_schema()
        partition_spec = PartitionSpec()
        
        table = self.catalog.create_table(
            identifier=(self.namespace, "monitors"),
            schema=schema,
            partition_spec=partition_spec,
        )
        print(f"✅ Created table: {self.namespace}.monitors")
        return table
    
    def load_monitors_csv(self, csv_path: str, table: Optional[Table] = None) -> int:
        """Load monitors CSV file."""
        if table is None:
            table = self.create_monitors_table()
        
        print(f"Loading monitors data from: {csv_path}")
        
        column_mapping = {
            'State Code': 'state_code',
            'County Code': 'county_code',
            'Site Number': 'site_number',
            'Parameter Code': 'parameter_code',
            'Parameter Name': 'parameter_name',
            'POC': 'poc',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Datum': 'datum',
            'First Year of Data': 'first_year_of_data',
            'Last Sample Date': 'last_sample_date',
            'Monitor Type': 'monitor_type',
            'Networks': 'networks',
            'Reporting Agency': 'reporting_agency',
            'PQAO': 'pqao',
            'Collecting Agency': 'collecting_agency',
            'Exclusions': 'exclusions',
            'Monitoring Objective': 'monitoring_objective',
            'Last Method Code': 'last_method_code',
            'Last Method': 'last_method',
            'Measurement Scale': 'measurement_scale',
            'Measurement Scale Definition': 'measurement_scale_definition',
            'NAAQS Primary Monitor': 'naaqs_primary_monitor',
            'QA Primary Monitor': 'qa_primary_monitor',
            'Local Site Name': 'local_site_name',
            'Address': 'address',
            'State Name': 'state_name',
            'County Name': 'county_name',
            'City Name': 'city_name',
            'CBSA Name': 'cbsa_name',
            'Tribe Name': 'tribe_name',
            'Extraction Date': 'extraction_date',
        }
        
        read_options = csv_arrow.ReadOptions(use_threads=True)
        parse_options = csv_arrow.ParseOptions(delimiter=',')
        convert_options = csv_arrow.ConvertOptions(
            strings_can_be_null=True,
            null_values=['', 'NA', 'N/A', 'NULL', 'null']
        )
        
        arrow_table = csv_arrow.read_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        
        current_cols = arrow_table.column_names
        rename_map = {old: column_mapping.get(old, old) for old in current_cols}
        arrow_table = arrow_table.rename_columns([
            rename_map.get(col, col) for col in current_cols
        ])
        
        schema_cols = [field.name for field in table.schema().fields]
        existing_cols = [col for col in schema_cols if col in arrow_table.column_names]
        arrow_table = arrow_table.select(existing_cols)
        
        schema = table.schema()
        cast_columns = []
        column_names = []
        
        for field in schema.fields:
            column_names.append(field.name)
            if field.name in arrow_table.column_names:
                col = arrow_table[field.name]
                if isinstance(field.field_type, StringType) and col.type != pa.string():
                    cast_columns.append(col.cast(pa.string()))
                elif isinstance(field.field_type, LongType) and col.type != pa.int64():
                    if col.type in (pa.float64(), pa.float32()):
                        cast_columns.append(col.cast(pa.int64(), safe=False))
                    else:
                        cast_columns.append(col.cast(pa.int64()))
                elif isinstance(field.field_type, DoubleType) and col.type != pa.float64():
                    cast_columns.append(col.cast(pa.float64()))
                else:
                    cast_columns.append(col)
            else:
                arrow_type = field.field_type.to_arrow()
                cast_columns.append(pa.nulls(len(arrow_table), arrow_type))
        
        arrow_table = pa.Table.from_arrays(cast_columns, names=column_names)
        
        rows_to_load = len(arrow_table)
        print(f"   Prepared {rows_to_load:,} rows for loading...")
        
        table.append(arrow_table)
        print(f"✅ Loaded {rows_to_load:,} rows into {self.namespace}.monitors")
        
        return rows_to_load
    
    # ============================================
    # MAIN LOAD FUNCTION - ALL TABLES
    # ============================================
    
    def load_all_tables(self, data_dir: str = "data/epa"):
        """
        Load all EPA tables from data directory.
        
        Loads:
        - sites: From aqs_sites*.csv
        - monitors: From aqs_monitors*.csv
        - daily_summary: From daily_*.csv files
        """
        print("=" * 70)
        print("EPA COMPLETE DATA LOADER")
        print("=" * 70)
        
        data_path = Path(data_dir)
        
        # Load sites
        sites_file = list(data_path.glob("aqs_sites*.csv"))
        if sites_file:
            table = self.create_sites_table()
            self.load_sites_csv(str(sites_file[0]), table)
        else:
            print("⚠️  No sites CSV file found")
        
        # Load monitors
        monitors_file = list(data_path.glob("aqs_monitors*.csv"))
        if monitors_file:
            table = self.create_monitors_table()
            self.load_monitors_csv(str(monitors_file[0]), table)
        else:
            print("⚠️  No monitors CSV file found")
        
        # Load daily summary
        print("\n" + "=" * 70)
        print("Loading Daily Summary Files")
        print("=" * 70)
        self.load_directory(data_dir, pattern="daily_*.csv")
        
        print("\n" + "=" * 70)
        print("✅ DATA LOAD COMPLETE")
        print("=" * 70)


if __name__ == "__main__":
    # Main entry point - load all tables
    from pyiceberg.catalog import load_catalog
    import os
    
    catalog_name = os.getenv("ICEBERG_CATALOG", "rest_epa")
    
    try:
        catalog = load_catalog(catalog_name)
    except Exception:
        catalog = load_catalog(
            name="rest_epa",
            type="rest",
            uri="http://localhost:8181",
            warehouse="s3://warehouse/",
            **{
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            }
        )
    
    loader = EPADataLoader(catalog, namespace="epa")
    loader.load_all_tables("data/epa")

