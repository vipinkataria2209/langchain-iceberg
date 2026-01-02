#!/usr/bin/env python3
"""
EPA Air Quality Data Loader and Setup Script

Complete loader for all EPA Air Quality tables:
- sites: Monitoring site locations and metadata
- monitors: Monitor equipment and configuration details
- daily_summary: Daily air quality measurements for all pollutants

This script can be used as:
1. Library: Import EPADataLoader class
2. Standalone: Run as main script to set up catalog, create tables, and load data

Based on EPA AirData file format: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html

Usage:
    python experiments/epa_data_loader.py  # Clean start (drops existing tables by default)
    python experiments/epa_data_loader.py --data-dir /path/to/epa/data
    python experiments/epa_data_loader.py --no-clean  # Preserve existing tables
"""

import os
from pathlib import Path
from typing import Optional
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


# ============================================
# SETUP AND ORCHESTRATION FUNCTIONS
# ============================================

def setup_rest_catalog(
    catalog_name: str = "rest_epa",
    uri: str = "http://localhost:8181",
    warehouse: str = "s3://warehouse/",
    s3_endpoint: str = "http://localhost:9000",
    s3_access_key: str = "admin",
    s3_secret_key: str = "password",
    s3_region: str = "us-east-1"
) -> Catalog:
    """
    Set up and return REST catalog connection.

    Args:
        catalog_name: Name of the catalog
        uri: REST catalog server URI
        warehouse: S3 warehouse path
        s3_endpoint: MinIO/S3 endpoint
        s3_access_key: S3 access key
        s3_secret_key: S3 secret key
        s3_region: S3 region

    Returns:
        PyIceberg Catalog instance
    """
    print("=" * 70)
    print("STEP 1: Setting Up REST Catalog")
    print("=" * 70)

    from pyiceberg.catalog import load_catalog

    # Try to load from environment/config first
    catalog_env = os.getenv("ICEBERG_CATALOG")
    if catalog_env:
        try:
            catalog = load_catalog(catalog_env)
            print(f"✅ Loaded catalog from environment: {catalog_env}")
            return catalog
        except Exception as e:
            print(f"⚠️  Failed to load catalog from environment: {e}")
            print("   Using explicit configuration...")

    # Load catalog with explicit configuration
    try:
        catalog = load_catalog(
            name=catalog_name,
            type="rest",
            uri=uri,
            warehouse=warehouse,
            **{
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
                "s3.path-style-access": "true",
                "s3.region": s3_region,
            }
        )
        print(f"✅ REST catalog initialized: {catalog_name}")
        print(f"   URI: {uri}")
        print(f"   Warehouse: {warehouse}")
        print(f"   S3 Endpoint: {s3_endpoint}")
        return catalog
    except Exception as e:
        print(f"❌ Failed to connect to REST catalog: {e}")
        print("\n⚠️  Make sure Docker services are running:")
        print("   docker-compose up -d")
        raise


def drop_existing_tables(catalog: Catalog, namespace: str = "epa", clean: bool = False) -> dict:
    """
    Drop existing EPA tables if they exist.

    Args:
        catalog: PyIceberg Catalog instance
        namespace: Namespace for tables (default: "epa")
        clean: If True, drop tables; if False, skip

    Returns:
        Dictionary with drop status for each table
    """
    if not clean:
        return {
            "sites": "skipped",
            "monitors": "skipped",
            "daily_summary": "skipped",
        }

    print("\n" + "=" * 70)
    print("STEP 1.5: Dropping Existing Tables (Clean Start)")
    print("=" * 70)

    results = {
        "sites": "not_found",
        "monitors": "not_found",
        "daily_summary": "not_found",
    }

    tables_to_drop = ["sites", "monitors", "daily_summary"]

    for table_name in tables_to_drop:
        try:
            table_identifier = (namespace, table_name)
            table = catalog.load_table(table_identifier)

            # Drop the table
            print(f"\n🗑️  Dropping table: {namespace}.{table_name}")
            catalog.drop_table(table_identifier)
            results[table_name] = "dropped"
            print(f"   ✅ Dropped {namespace}.{table_name}")

        except Exception:
            # Table doesn't exist, which is fine
            results[table_name] = "not_found"
            print(f"   ℹ️  Table {namespace}.{table_name} does not exist (skipping)")

    # Summary
    print("\n" + "-" * 70)
    print("Table Drop Summary:")
    for table_name, status in results.items():
        if status == "dropped":
            print(f"   ✅ {table_name} - dropped")
        elif status == "not_found":
            print(f"   ℹ️  {table_name} - not found (already clean)")
        else:
            print(f"   ⏭️  {table_name} - skipped")
    print("-" * 70)

    return results


def create_all_tables(loader: EPADataLoader) -> dict:
    """
    Create all EPA tables in Iceberg.

    Args:
        loader: EPADataLoader instance

    Returns:
        Dictionary with table creation status
    """
    print("\n" + "=" * 70)
    print("STEP 2: Creating All Iceberg Tables")
    print("=" * 70)

    results = {
        "sites": False,
        "monitors": False,
        "daily_summary": False,
    }

    # Create sites table
    try:
        print("\n📊 Creating sites table...")
        loader.create_sites_table()
        results["sites"] = True
        print("   ✅ Sites table ready")
    except Exception as e:
        print(f"   ❌ Error creating sites table: {e}")
        results["sites"] = False

    # Create monitors table
    try:
        print("\n📊 Creating monitors table...")
        loader.create_monitors_table()
        results["monitors"] = True
        print("   ✅ Monitors table ready")
    except Exception as e:
        print(f"   ❌ Error creating monitors table: {e}")
        results["monitors"] = False

    # Create daily_summary table
    try:
        print("\n📊 Creating daily_summary table...")
        loader.create_table(table_name="daily_summary")
        results["daily_summary"] = True
        print("   ✅ Daily summary table ready")
    except Exception as e:
        print(f"   ❌ Error creating daily_summary table: {e}")
        results["daily_summary"] = False

    # Summary
    print("\n" + "-" * 70)
    print("Table Creation Summary:")
    for table_name, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {table_name}")
    print("-" * 70)

    return results


def verify_data_files(data_dir: str) -> dict:
    """
    Verify that required data files exist.

    Args:
        data_dir: Directory containing EPA data files

    Returns:
        Dictionary with file verification status
    """
    data_path = Path(data_dir)

    if not data_path.exists():
        return {
            "directory_exists": False,
            "sites_file": False,
            "monitors_file": False,
            "daily_files": 0,
        }

    # Check for sites file
    sites_files = list(data_path.glob("aqs_sites*.csv"))
    sites_exists = len(sites_files) > 0

    # Check for monitors file
    monitors_files = list(data_path.glob("aqs_monitors*.csv"))
    monitors_exists = len(monitors_files) > 0

    # Check for daily summary files
    daily_files = list(data_path.glob("daily_*.csv"))
    daily_count = len(daily_files)

    return {
        "directory_exists": True,
        "sites_file": sites_exists,
        "monitors_file": monitors_exists,
        "daily_files": daily_count,
        "sites_path": str(sites_files[0]) if sites_files else None,
        "monitors_path": str(monitors_files[0]) if monitors_files else None,
    }


def load_all_epa_data(loader: EPADataLoader, data_dir: str = "data/epa") -> bool:
    """
    Load all EPA data into Iceberg tables.

    Args:
        loader: EPADataLoader instance
        data_dir: Directory containing EPA CSV files

    Returns:
        True if successful, False otherwise
    """
    print("\n" + "=" * 70)
    print("STEP 3: Loading EPA Data")
    print("=" * 70)

    # Verify data files exist
    print("\n📋 Verifying data files...")
    file_status = verify_data_files(data_dir)

    if not file_status["directory_exists"]:
        print(f"❌ Data directory not found: {data_dir}")
        print("\n⚠️  Please download EPA data first:")
        print("   python experiments/download_all_epa_data.py")
        return False

    print(f"   ✅ Directory exists: {data_dir}")

    if file_status["sites_file"]:
        print(f"   ✅ Sites file found: {Path(file_status['sites_path']).name}")
    else:
        print(f"   ⚠️  Sites file not found (aqs_sites*.csv)")

    if file_status["monitors_file"]:
        print(f"   ✅ Monitors file found: {Path(file_status['monitors_path']).name}")
    else:
        print(f"   ⚠️  Monitors file not found (aqs_monitors*.csv)")

    if file_status["daily_files"] > 0:
        print(f"   ✅ Daily summary files found: {file_status['daily_files']} files")
    else:
        print(f"   ⚠️  No daily summary files found (daily_*.csv)")

    if not (file_status["sites_file"] or file_status["monitors_file"] or file_status["daily_files"] > 0):
        print("\n❌ No EPA data files found!")
        print("   Please download data first:")
        print("   python experiments/download_all_epa_data.py")
        return False

    # Load all tables
    try:
        print("\n📥 Loading data into Iceberg tables...")
        loader.load_all_tables(data_dir)
        print("\n✅ Data loading complete!")
        return True
    except Exception as e:
        print(f"\n❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return False


def main(data_dir: Optional[str] = None, clean: bool = True):
    """
    Main function to set up catalog, create tables, and load data.

    Args:
        data_dir: Optional path to EPA data directory (default: "data/epa")
        clean: If True, drop existing tables before creating new ones
    """
    print("=" * 70)
    print("EPA AIR QUALITY DATA - COMPLETE SETUP AND LOAD")
    print("=" * 70)
    print("\nThis script will:")
    print("  1. Connect to REST catalog")
    if clean:
        print("  2. Drop existing tables (clean start)")
        print("  3. Create all Iceberg tables (sites, monitors, daily_summary)")
        print("  4. Load all EPA data from CSV files")
    else:
        print("  2. Create all Iceberg tables (sites, monitors, daily_summary)")
        print("  3. Load all EPA data from CSV files")
        print("  (Existing tables will be preserved)")
    print("=" * 70)

    # Set data directory
    if data_dir is None:
        data_dir = os.getenv("EPA_DATA_DIR", "data/epa")

    # Step 1: Setup REST catalog
    try:
        catalog = setup_rest_catalog()
    except Exception as e:
        print(f"\n❌ Failed to set up catalog: {e}")
        import sys
        sys.exit(1)

    # Step 2: Initialize loader
    try:
        loader = EPADataLoader(catalog, namespace="epa")
        print(f"✅ EPADataLoader initialized (namespace: epa)")
    except Exception as e:
        print(f"\n❌ Failed to initialize loader: {e}")
        import sys
        sys.exit(1)

    # Step 2.5: Drop existing tables if clean flag is set
    if clean:
        try:
            drop_existing_tables(catalog, namespace="epa", clean=clean)
        except Exception as e:
            print(f"\n⚠️  Warning: Error dropping tables: {e}")
            print("   Continuing with table creation...")

    # Step 3: Create all tables
    try:
        table_results = create_all_tables(loader)
        if not all(table_results.values()):
            print("\n⚠️  Some tables failed to create. Continuing anyway...")
    except Exception as e:
        print(f"\n❌ Failed to create tables: {e}")
        import sys
        sys.exit(1)

    # Step 4: Load all data
    try:
        success = load_all_epa_data(loader, data_dir)
        if not success:
            print("\n⚠️  Data loading had issues. Check errors above.")
            import sys
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Failed to load data: {e}")
        import sys
        sys.exit(1)

    # Final summary
    print("\n" + "=" * 70)
    print("✅ SETUP AND LOAD COMPLETE!")
    print("=" * 70)
    print("\n📊 Tables created and loaded:")
    print("   ✅ epa.sites")
    print("   ✅ epa.monitors")
    print("   ✅ epa.daily_summary")
    print("\n📝 Next steps:")
    print("   1. Verify tables: Check row counts via REST API or queries")
    print("   2. Run test queries: python experiments/test_queries_direct.py")
    print("   3. Run experiments: python experiments/run_experiments.py")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    from typing import Optional

    parser = argparse.ArgumentParser(
        description="Set up REST catalog, create tables, and load EPA data"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to EPA data directory (default: data/epa)"
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Preserve existing tables (do not drop before creating)"
    )

    args = parser.parse_args()

    # Clean is default, unless --no-clean is specified
    clean = not args.no_clean

    main(data_dir=args.data_dir, clean=clean)

