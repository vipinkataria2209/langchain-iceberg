#!/usr/bin/env python3
"""
Unzip EPA files and count total rows by category.
"""

import zipfile
from pathlib import Path
import csv

# Parameter codes mapping
PARAMETERS = {
    "42101": "CO (Carbon Monoxide)",
    "42401": "SO2 (Sulfur Dioxide)",
    "42602": "NO2 (Nitrogen Dioxide)",
    "44201": "Ozone",
    "88101": "PM2.5",
}

def unzip_file(zip_path: Path, output_dir: Path) -> bool:
    """Unzip a file if it hasn't been extracted yet."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Check if CSV already exists
            csv_name = zip_path.stem + ".csv"
            csv_path = output_dir / csv_name

            # If CSV doesn't exist, extract
            if not csv_path.exists():
                zip_ref.extractall(output_dir)
                return True
        return False
    except Exception as e:
        print(f"  ❌ Error extracting {zip_path.name}: {e}")
        return False

def count_csv_rows(csv_path: Path) -> int:
    """Count rows in a CSV file (excluding header)."""
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Skip header
            next(reader, None)
            return sum(1 for _ in reader)
    except Exception as e:
        print(f"  ⚠️  Error counting {csv_path.name}: {e}")
        return 0

def find_csv_files(directory: Path, pattern: str = None):
    """Find CSV files, handling both direct files and subdirectories."""
    csv_files = []
    for item in directory.iterdir():
        if item.is_dir():
            # Check for CSV in subdirectory
            for subitem in item.iterdir():
                if subitem.suffix == '.csv':
                    if pattern is None or pattern in subitem.name:
                        csv_files.append(subitem)
        elif item.suffix == '.csv':
            if pattern is None or pattern in item.name:
                csv_files.append(item)
    return csv_files

def main():
    data_dir = Path("data/epa")

    print("=" * 70)
    print("EPA DATA ANALYSIS - UNZIP AND COUNT ROWS")
    print("=" * 70)

    # Step 1: Unzip remaining files
    print("\n📦 Step 1: Extracting ZIP files...")
    zip_files = list(data_dir.glob("*.zip"))
    extracted_count = 0

    for zip_file in zip_files:
        csv_name = zip_file.stem + ".csv"
        csv_path = data_dir / csv_name

        # Check if already extracted (or in subdirectory for some PM2.5 files)
        if csv_path.exists():
            continue

        # Check subdirectories for PM2.5 files
        subdir = data_dir / zip_file.stem
        if subdir.exists() and (subdir / csv_name).exists():
            continue

        print(f"  📥 Extracting: {zip_file.name}")
        if unzip_file(zip_file, data_dir):
            extracted_count += 1

    print(f"  ✅ Extracted {extracted_count} new files")

    # Step 2: Count rows by category
    print("\n📊 Step 2: Counting rows...")

    # Count site and monitor files
    print("\n📍 Site and Monitor Files:")
    site_files = list(data_dir.glob("*sites*.csv"))
    monitor_files = list(data_dir.glob("*monitors*.csv"))

    site_rows = 0
    for site_file in site_files:
        rows = count_csv_rows(site_file)
        site_rows += rows
        print(f"  {site_file.name}: {rows:,} rows")

    monitor_rows = 0
    for monitor_file in monitor_files:
        rows = count_csv_rows(monitor_file)
        monitor_rows += rows
        print(f"  {monitor_file.name}: {rows:,} rows")

    # Count daily summary files by gas
    print("\n🌬️  Daily Summary Files by Gas:")
    gas_totals = {}
    all_daily_files = []

    for param_code, gas_name in PARAMETERS.items():
        # Find all CSV files for this parameter
        csv_files = find_csv_files(data_dir, f"daily_{param_code}_")
        total_rows = 0

        for csv_file in csv_files:
            rows = count_csv_rows(csv_file)
            total_rows += rows
            all_daily_files.append((csv_file, rows))

        gas_totals[gas_name] = total_rows
        print(f"  {gas_name} ({param_code}): {total_rows:,} rows ({len(csv_files)} files)")

    # Total across all daily files
    total_daily_rows = sum(gas_totals.values())

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"📍 Site Files:        {site_rows:,} rows")
    print(f"📍 Monitor Files:     {monitor_rows:,} rows")
    print(f"\n🌬️  Daily Summary by Gas:")
    for gas_name, rows in sorted(gas_totals.items(), key=lambda x: x[1], reverse=True):
        print(f"   {gas_name:30s}: {rows:>15,} rows")
    print(f"\n📊 TOTAL Daily Summary Rows: {total_daily_rows:,} rows")
    print(f"📁 Total Daily Summary Files: {len(all_daily_files)} files")
    print("=" * 70)

    # Detailed breakdown
    print("\n📋 Detailed File Breakdown:")
    print(f"   Site files: {len(site_files)}")
    print(f"   Monitor files: {len(monitor_files)}")
    print(f"   Daily summary files: {len(all_daily_files)}")
    print(f"   Total CSV files: {len(site_files) + len(monitor_files) + len(all_daily_files)}")

if __name__ == "__main__":
    main()

