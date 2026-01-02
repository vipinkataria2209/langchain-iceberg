#!/usr/bin/env python3
"""
Download All EPA Air Quality Data (2014-2024)

Downloads daily summary files for all 5 pollutants:
- PM2.5 (88101)
- Ozone (44201)
- SO2 (42401)
- CO (42101)
- NO2 (42602)

Years: 2014-2024 (11 years)
Total files: 5 parameters × 11 years = 55 files

Estimated size: ~2-5 GB compressed.
"""

import sys
from pathlib import Path

# Import the downloader
sys.path.insert(0, str(Path(__file__).parent.parent))
from experiments.download_epa_data import EPADataDownloader

def main():
    """Download all EPA data from 2014-2024."""
    import sys

    # Allow user to specify number of parallel workers
    max_workers = 5  # Default
    if "--workers" in sys.argv:
        try:
            idx = sys.argv.index("--workers")
            max_workers = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            print("⚠️  Invalid --workers value, using default: 5")

    downloader = EPADataDownloader(output_dir="data/epa", max_workers=max_workers)

    print("=" * 70)
    print("EPA AIR QUALITY DATA DOWNLOAD - COMPLETE DATASET")
    print("=" * 70)
    print("Parameters: PM2.5, Ozone, SO2, CO, NO2")
    print("Years: 2014-2024 (11 years)")
    print("Total files: 55 ZIP files")
    print("Estimated size: ~2-5 GB compressed")
    print(f"Parallel downloads: {max_workers} workers")
    print("Estimated time: 20-40 minutes (with parallel downloads)")
    print("=" * 70)

    # Skip prompt if running non-interactively or with --yes flag
    if "--yes" not in sys.argv:
        try:
            response = input("\n⚠️  This is a large download. Continue? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Download cancelled.")
                return
        except EOFError:
            # Non-interactive mode, proceed automatically
            print("\n⚠️  Non-interactive mode detected. Proceeding with download...")

    # Download Site and Monitor listings
    print("\n" + "=" * 70)
    print("STEP 1: Downloading Site and Monitor Information")
    print("=" * 70)
    site_monitor_results = downloader.download_site_monitor_files()

    # Download 2014-2024 data for all 5 pollutants
    print("\n" + "=" * 70)
    print("STEP 2: Downloading Daily Summary Data (2014-2024)")
    print("=" * 70)
    print("This will download 55 files. Please be patient...")
    print("=" * 70)

    years = list(range(2014, 2025))  # 2014-2024
    parameters = ["pm25", "ozone", "so2", "co", "no2"]

    daily_results = downloader.download_years(
        years=years,
        parameters=parameters,
        extract=True,
        parallel=True  # Enable parallel downloads
    )

    # Summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"✅ Site/Monitor files: {len(site_monitor_results['downloaded'])} downloaded")
    print(f"✅ Daily summary files: {len(daily_results['downloaded'])} downloaded")
    print(f"📦 Extracted: {len(daily_results['extracted'])} files")
    print(f"❌ Failed: {len(site_monitor_results['failed']) + len(daily_results['failed'])} files")
    print("=" * 70)

    # Calculate expected vs actual
    expected_files = len(parameters) * len(years)
    actual_files = len(daily_results['downloaded'])

    print(f"\nExpected: {expected_files} files")
    print(f"Downloaded: {actual_files} files")
    print(f"Success rate: {(actual_files / expected_files * 100):.1f}%")

    if actual_files < expected_files:
        print("\n⚠️  Some files may have failed to download.")
        print("   You can re-run this script - it will skip already downloaded files.")

    print("\n✅ Download complete!")
    print(f"\nFiles are in: {downloader.output_dir}")
    print("\nNext steps:")
    print("  1. Verify files in data/epa/")
    print("  2. Load data into Iceberg tables")
    print("  3. Run experiments and queries")
    print("\n💡 Tip: Use --workers N to adjust parallel downloads (default: 5)")
    print("   Example: python experiments/download_all_epa_data.py --workers 10")

if __name__ == "__main__":
    main()

