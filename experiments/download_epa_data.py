#!/usr/bin/env python3
"""
Download EPA Air Quality Data

Downloads daily summary files for 5 pollutants (PM2.5, Ozone, SO2, CO, NO2) 
from 2014-2024 (11 years of data).

Also downloads Site and Monitor listings from the metadata page.

Based on:
- Daily data: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Daily
- Site/Monitor metadata: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Meta

Parameter Codes:
- 88101: PM2.5 - Local Conditions
- 44201: Ozone
- 42401: Sulfur dioxide (SO2)
- 42101: Carbon monoxide (CO)
- 42602: Nitrogen dioxide (NO2)
"""

import os
import requests
import zipfile
from pathlib import Path
from typing import List, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class EPADataDownloader:
    """Downloader for EPA air quality data files."""
    
    BASE_URL = "https://aqs.epa.gov/aqsweb/airdata"
    
    # Parameter codes for key pollutants
    PARAMETERS = {
        "pm25": "88101",   # PM2.5 - Local Conditions
        "ozone": "44201",  # Ozone
        "so2": "42401",   # Sulfur dioxide (SO2)
        "co": "42101",    # Carbon monoxide (CO)
        "no2": "42602",   # Nitrogen dioxide (NO2)
    }
    
    def __init__(self, output_dir: str = "data/epa", max_workers: int = 5):
        """
        Initialize downloader.
        
        Args:
            output_dir: Directory to save downloaded files
            max_workers: Number of parallel download threads (default: 5)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.print_lock = Lock()  # For thread-safe printing
    
    def download_site_monitor_files(self) -> dict:
        """
        Download Site Listing and Monitor Listing files from EPA metadata page.
        
        Downloads from: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Meta
        
        Files downloaded:
        - aqs_sites.zip → extracts to aqs_sites.csv (20,952 rows, 993 KB)
        - aqs_monitors.zip → extracts to aqs_monitors.csv (368,480 rows, 6,727 KB)
        
        URLs:
        - https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip
        - https://aqs.epa.gov/aqsweb/airdata/aqs_monitors.zip
        
        Returns:
            Dictionary with download results
        """
        results = {
            "downloaded": [],
            "failed": [],
            "extracted": [],
        }
        
        # Files from EPA metadata page (Site and Monitor Descriptions section)
        # Source: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Meta
        # ZIP files extract to: aqs_sites.csv and aqs_monitors.csv
        files = [
            ("aqs_sites.zip", "Site Listing", "aqs_sites.csv"),
            ("aqs_monitors.zip", "Monitor Listing", "aqs_monitors.csv"),
        ]
        
        print("=" * 70)
        print("Downloading Site and Monitor Information")
        print("Source: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Meta")
        print("=" * 70)
        
        for filename, description, extracted_csv in files:
            url = f"{self.BASE_URL}/{filename}"
            output_path = self.output_dir / filename
            
            # Check if CSV already extracted (skip download if CSV exists)
            csv_path = self.output_dir / extracted_csv
            if csv_path.exists():
                print(f"  ⏭️  Already extracted: {extracted_csv} ({description})")
                results["downloaded"].append(str(output_path))
                results["extracted"].append(str(csv_path))
                continue
            
            # Skip if ZIP already downloaded
            if output_path.exists():
                print(f"  ⏭️  Already exists: {filename} ({description})")
                results["downloaded"].append(str(output_path))
                if self.extract_file(str(output_path)):
                    results["extracted"].append(str(output_path))
                continue
            
            print(f"  📥 Downloading: {filename} ({description})")
            print(f"     Will extract to: {extracted_csv}")
            print(f"     Connecting to {self.BASE_URL}...", flush=True)
            try:
                response = requests.get(url, stream=True, timeout=(10, 120))  # (connect timeout, read timeout)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                chunk_count = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            chunk_count += 1
                            
                            # Show progress every 100 chunks or if we have total_size
                            if chunk_count % 100 == 0 or total_size > 0:
                                if total_size > 0:
                                    percent = (downloaded / total_size) * 100
                                    print(f"     Progress: {percent:.1f}% ({downloaded / 1024 / 1024:.1f} MB)", end='\r', flush=True)
                                else:
                                    print(f"     Progress: {downloaded / 1024 / 1024:.1f} MB downloaded...", end='\r', flush=True)
                
                print()  # New line after progress
                print(f"  ✅ Downloaded: {filename} ({downloaded / 1024 / 1024:.1f} MB)")
                results["downloaded"].append(str(output_path))
                
                if self.extract_file(str(output_path)):
                    results["extracted"].append(str(output_path))
                
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Error downloading {filename}: {e}")
                results["failed"].append(filename)
                if output_path.exists():
                    output_path.unlink()
        
        return results
    
    def download_file(self, parameter_code: str, year: int) -> Tuple[bool, str]:
        """
        Download a single EPA daily summary file.
        
        Args:
            parameter_code: EPA parameter code (e.g., "88101" for PM2.5)
            year: Year of data
            
        Returns:
            Tuple of (success, file_path)
        """
        filename = f"daily_{parameter_code}_{year}.zip"
        url = f"{self.BASE_URL}/{filename}"
        output_path = self.output_dir / filename
        
        # Skip if already downloaded
        if output_path.exists():
            print(f"  ⏭️  Already exists: {filename}")
            return True, str(output_path)
        
        # Thread-safe download (no progress for parallel downloads)
        try:
            response = requests.get(url, stream=True, timeout=(10, 120))  # (connect timeout, read timeout)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            return True, str(output_path)
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Error downloading {filename}: {e}")
            if output_path.exists():
                output_path.unlink()  # Remove partial file
            return False, ""
    
    def extract_file(self, zip_path: str) -> bool:
        """
        Extract ZIP file to CSV.
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            True if successful
        """
        zip_file = Path(zip_path)
        if not zip_file.exists():
            return False
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Extract to same directory
                zip_ref.extractall(zip_file.parent)
                return True
        except Exception as e:
            # Use print_lock if available (for parallel downloads)
            if hasattr(self, 'print_lock'):
                with self.print_lock:
                    print(f"  ❌ Error extracting {zip_file.name}: {e}")
            else:
                print(f"  ❌ Error extracting {zip_file.name}: {e}")
            return False
    
    def download_years(
        self,
        years: List[int],
        parameters: List[str] = None,
        extract: bool = True,
        parallel: bool = True
    ) -> dict:
        """
        Download data for multiple years and parameters.
        
        Args:
            years: List of years to download
            parameters: List of parameter names (default: ["pm25", "ozone", "so2", "co", "no2"])
            extract: Whether to extract ZIP files after downloading
            parallel: Whether to download files in parallel (default: True)
            
        Returns:
            Dictionary with download results
        """
        if parameters is None:
            parameters = ["pm25", "ozone", "so2", "co", "no2"]
        
        results = {
            "downloaded": [],
            "failed": [],
            "extracted": [],
        }
        
        print("=" * 70)
        print("EPA Air Quality Data Downloader")
        print("=" * 70)
        print(f"Years: {years}")
        print(f"Parameters: {parameters}")
        print(f"Output directory: {self.output_dir}")
        print(f"Parallel downloads: {parallel} (max_workers: {self.max_workers})")
        print("=" * 70)
        
        # Build list of all files to download
        download_tasks = []
        for year in years:
            for param_name in parameters:
                param_code = self.PARAMETERS.get(param_name)
                if not param_code:
                    continue
                download_tasks.append((param_code, year, param_name))
        
        total_files = len(download_tasks)
        print(f"\n📥 Total files to download: {total_files}")
        
        if parallel and total_files > 1:
            # Parallel download
            print(f"🚀 Using parallel downloads ({self.max_workers} workers)...")
            results = self._download_parallel(download_tasks, extract)
        else:
            # Sequential download
            print("📥 Using sequential downloads...")
            for param_code, year, param_name in download_tasks:
                with self.print_lock:
                    print(f"\n📅 {param_name.upper()} - {year}")
                
                success, file_path = self.download_file(param_code, year)
                
                if success:
                    results["downloaded"].append(file_path)
                    if extract:
                        if self.extract_file(file_path):
                            results["extracted"].append(file_path)
                else:
                    results["failed"].append(f"{param_name}_{year}")
                
                time.sleep(0.2)  # Small delay
        
        # Summary
        print("\n" + "=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)
        print(f"✅ Downloaded: {len(results['downloaded'])} files")
        print(f"❌ Failed: {len(results['failed'])} files")
        print(f"📦 Extracted: {len(results['extracted'])} files")
        print("=" * 70)
        
        return results
    
    def _download_parallel(self, download_tasks: List[Tuple[str, int, str]], extract: bool) -> dict:
        """Download files in parallel using ThreadPoolExecutor."""
        results = {
            "downloaded": [],
            "failed": [],
            "extracted": [],
        }
        
        # Thread-safe counters and lists
        downloaded_count = [0]
        failed_count = [0]
        results_lock = Lock()
        
        def download_and_extract(task):
            """Download a single file and optionally extract it."""
            param_code, year, param_name = task
            filename = f"daily_{param_code}_{year}.zip"
            
            try:
                success, file_path = self.download_file(param_code, year)
                
                if success:
                    with results_lock:
                        downloaded_count[0] += 1
                        results["downloaded"].append(file_path)
                    
                    with self.print_lock:
                        print(f"✅ [{downloaded_count[0]}/{len(download_tasks)}] {filename}")
                    
                    if extract:
                        if self.extract_file(file_path):
                            with results_lock:
                                results["extracted"].append(file_path)
                    return True
                else:
                    with results_lock:
                        failed_count[0] += 1
                        results["failed"].append(f"{param_name}_{year}")
                    
                    with self.print_lock:
                        print(f"❌ [{failed_count[0]} failed] {filename}")
                    return False
            except Exception as e:
                with results_lock:
                    failed_count[0] += 1
                    results["failed"].append(f"{param_name}_{year}")
                
                with self.print_lock:
                    print(f"❌ [{failed_count[0]} failed] {filename}: {str(e)[:50]}")
                return False
        
        # Execute downloads in parallel
        print(f"Starting {len(download_tasks)} downloads with {self.max_workers} workers...")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(download_and_extract, task): task for task in download_tasks}
            
            # Wait for all downloads to complete
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    future.result()
                except Exception as e:
                    task = futures[future]
                    with results_lock:
                        results["failed"].append(f"{task[2]}_{task[1]}")
                    with self.print_lock:
                        print(f"❌ Exception downloading {task[2]}_{task[1]}: {e}")
                
                # Progress update every 5 files
                if completed % 5 == 0:
                    with self.print_lock:
                        print(f"📊 Progress: {completed}/{len(download_tasks)} files processed")
        
        return results


def main():
    """Main download function."""
    downloader = EPADataDownloader(output_dir="data/epa")
    
    # Download Site and Monitor listings
    print("\n" + "=" * 70)
    print("STEP 1: Downloading Site and Monitor Information")
    print("=" * 70)
    site_monitor_results = downloader.download_site_monitor_files()
    
    # Download 2000-2024 data for all 5 pollutants
    print("\n" + "=" * 70)
    print("STEP 2: Downloading Daily Summary Data")
    print("=" * 70)
    years = list(range(2014, 2025))  # 2014-2024 (11 years)
    parameters = ["pm25", "ozone", "so2", "co", "no2"]  # All 5 pollutants
    
    daily_results = downloader.download_years(
        years=years,
        parameters=parameters,
        extract=True
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"✅ Site/Monitor files: {len(site_monitor_results['downloaded'])} downloaded")
    print(f"✅ Daily summary files: {len(daily_results['downloaded'])} downloaded")
    print(f"❌ Failed: {len(site_monitor_results['failed']) + len(daily_results['failed'])} files")
    print("=" * 70)
    
    print("\n✅ Download complete!")
    print(f"\nFiles are in: {downloader.output_dir}")
    print("\nNext steps:")
    print("  1. Verify files in data/epa/")
    print("  2. Load data: python load_epa_and_test_nlp.py")
    print("  3. Run NLP queries: python test_epa_nlp.py")


if __name__ == "__main__":
    main()

