#!/usr/bin/env python3
"""
Load EPA data using local catalog and PyIceberg native API.

Uses in-memory or SQL catalog with local file system storage.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from experiments.epa_data_loader import EPADataLoader
from pyiceberg.catalog import load_catalog
import os


def main():
    print("=" * 70)
    print("EPA Data Loader - Local Catalog")
    print("=" * 70)
    
    # Use local file system for warehouse
    warehouse_path = Path("warehouse").absolute()
    warehouse_path.mkdir(exist_ok=True)
    warehouse_uri = f"file://{warehouse_path}"
    
    print(f"\n[1/4] Initializing local catalog...")
    print(f"   Warehouse: {warehouse_uri}")
    
    # Use SQL catalog for persistence (SQLite-based)
    catalog_db = warehouse_path / "catalog.db"
    try:
        catalog = load_catalog(
            name="local",
            type="sql",
            uri=f"sqlite:///{catalog_db}",
            warehouse=warehouse_uri,
        )
        print("✅ SQL catalog initialized (persistent)")
    except Exception as e:
        print(f"❌ Error initializing SQL catalog: {e}")
        return
    
    # Create loader
    print("\n[2/4] Creating EPA data loader...")
    loader = EPADataLoader(catalog, namespace="epa")
    
    # Create table (drop if exists to ensure correct schema)
    print("\n[3/4] Creating table...")
    try:
        # Drop table if exists
        try:
            catalog.drop_table(("epa", "daily_summary"))
            print("   Dropped existing table")
        except Exception:
            pass  # Table doesn't exist
        
        table = loader.create_table()
        print("✅ Table ready")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Load sample file
    print("\n[4/4] Loading sample data...")
    data_dir = Path("data/epa")
    csv_files = list(data_dir.glob("daily_*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data/epa/")
        return
    
    # Load first file as test
    test_file = csv_files[0]
    print(f"   Loading: {test_file.name}")
    print(f"   This may take a few minutes for large files...")
    
    try:
        rows = loader.load_csv_file(str(test_file), table)
        print(f"\n✅ Successfully loaded {rows:,} rows!")
        print(f"\n✅ Table is ready at: epa.daily_summary")
        print(f"\nNext steps:")
        print(f"  1. Load more files: loader.load_directory('data/epa/')")
        print(f"  2. Run experiments: python experiments/run_experiments.py")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

