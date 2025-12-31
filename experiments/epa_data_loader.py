#!/usr/bin/env python3
"""
EPA Air Quality Data Loader

Loads EPA daily summary CSV files into Apache Iceberg tables.
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
    """Loader for EPA Air Quality daily summary data into Iceberg."""
    
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


if __name__ == "__main__":
    # Example usage
    from pyiceberg.catalog import load_catalog
    
    catalog = load_catalog(
        name="rest",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://warehouse/wh/",
    )
    
    loader = EPADataLoader(catalog, namespace="epa")
    
    # Create table
    table = loader.create_table()
    
    # Load data (example - user should provide actual EPA file path)
    # loader.load_csv_file("path/to/epa_daily_summary.csv.gz")
    
    print("\n✅ EPA data loader ready!")
    print("Usage:")
    print("  loader.load_csv_file('path/to/epa_daily_summary.csv.gz')")
    print("  loader.load_directory('path/to/epa/files/')")

