#!/usr/bin/env python3
"""Quick test to load sample EPA data and verify setup."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from experiments.epa_data_loader import EPADataLoader
from pyiceberg.catalog import load_catalog

def main():
    print("=" * 70)
    print("Quick EPA Data Load Test")
    print("=" * 70)
    
    # Wait for services
    import time
    print("\nWaiting for services to be ready...")
    time.sleep(5)
    
    # Initialize catalog
    print("\n[1/3] Connecting to Iceberg catalog...")
    try:
        catalog = load_catalog(
            name="rest",
            type="rest",
            uri="http://localhost:8181",
            warehouse="s3://warehouse/wh/",
        )
        print("✅ Catalog connected")
    except Exception as e:
        print(f"❌ Error connecting to catalog: {e}")
        return
    
    # Create loader
    print("\n[2/3] Creating EPA data loader...")
    loader = EPADataLoader(catalog, namespace="epa")
    
    # Create table
    try:
        table = loader.create_table()
        print("✅ Table created/verified")
    except Exception as e:
        print(f"⚠️  Table creation: {e}")
        try:
            table = catalog.load_table(("epa", "daily_summary"))
            print("✅ Table already exists")
        except Exception as e2:
            print(f"❌ Error: {e2}")
            return
    
    # Load one sample file
    print("\n[3/3] Loading sample data file...")
    data_dir = Path("data/epa")
    csv_files = list(data_dir.glob("daily_*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data/epa/")
        return
    
    # Load first file as test
    test_file = csv_files[0]
    print(f"   Loading: {test_file.name}")
    
    try:
        rows = loader.load_csv_file(str(test_file), table)
        print(f"✅ Loaded {rows:,} rows successfully!")
        print(f"\n✅ Quick test complete! Table is ready for experiments.")
        print(f"\nNext: Run full experiment with:")
        print(f"  python experiments/run_experiments.py")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

