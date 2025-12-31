#!/usr/bin/env python3
"""
Download EPA Air Quality Data

Downloads daily summary files for PM2.5 and Ozone from 2019-2024.
Based on: https://aqs.epa.gov/aqsweb/airdata/download_files.html#Daily
"""

import os
import requests
import zipfile
from pathlib import Path
from typing import List, Tuple
import time


class EPADataDownloader:
    """Downloader for EPA air quality data files."""
    
    BASE_URL = "https://aqs.epa.gov/aqsweb/airdata"
    
    # Parameter codes for key pollutants
    PARAMETERS = {
        "pm25": "88101",  # PM2.5 - Local Conditions
        "ozone": "44201",  # Ozone
    }
    
    def __init__(self, output_dir: str = "data/epa"):
        """
        Initialize downloader.
        
        Args:
            output_dir: Directory to save downloaded files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_site_monitor_files(self) -> dict:
        """
        Download Site Listing and Monitor Listing files.
        
        Returns:
            Dictionary with download results
        """
        results = {
            "downloaded": [],
            "failed": [],
            "extracted": [],
        }
        
        files = [
            ("sites.zip", "Site Listing"),
            ("monitors.zip", "Monitor Listing"),
        ]
        
        print("=" * 70)
        print("Downloading Site and Monitor Information")
        print("=" * 70)
        
        for filename, description in files:
            url = f"{self.BASE_URL}/{filename}"
            output_path = self.output_dir / filename
            
            # Skip if already downloaded
            if output_path.exists():
                print(f"  ⏭️  Already exists: {filename} ({description})")
                results["downloaded"].append(str(output_path))
                if self.extract_file(str(output_path)):
                    results["extracted"].append(str(output_path))
                continue
            
            print(f"  📥 Downloading: {filename} ({description})")
            try:
                response = requests.get(url, stream=True, timeout=120)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(f"     Progress: {percent:.1f}%", end='\r')
                
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
        
        print(f"  📥 Downloading: {filename}")
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"     Progress: {percent:.1f}%", end='\r')
            
            print(f"  ✅ Downloaded: {filename} ({downloaded / 1024 / 1024:.1f} MB)")
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
        
        print(f"  📦 Extracting: {zip_file.name}")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Extract to same directory
                zip_ref.extractall(zip_file.parent)
                print(f"  ✅ Extracted: {zip_file.name}")
                return True
        except Exception as e:
            print(f"  ❌ Error extracting {zip_file.name}: {e}")
            return False
    
    def download_years(
        self,
        years: List[int],
        parameters: List[str] = None,
        extract: bool = True
    ) -> dict:
        """
        Download data for multiple years and parameters.
        
        Args:
            years: List of years to download
            parameters: List of parameter names (default: ["pm25", "ozone"])
            extract: Whether to extract ZIP files after downloading
            
        Returns:
            Dictionary with download results
        """
        if parameters is None:
            parameters = ["pm25", "ozone"]
        
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
        print("=" * 70)
        
        for year in years:
            print(f"\n📅 Year: {year}")
            for param_name in parameters:
                param_code = self.PARAMETERS.get(param_name)
                if not param_code:
                    print(f"  ⚠️  Unknown parameter: {param_name}")
                    continue
                
                success, file_path = self.download_file(param_code, year)
                
                if success:
                    results["downloaded"].append(file_path)
                    
                    if extract:
                        if self.extract_file(file_path):
                            results["extracted"].append(file_path)
                else:
                    results["failed"].append(f"{param_name}_{year}")
                
                # Small delay to be respectful
                time.sleep(1)
        
        # Summary
        print("\n" + "=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)
        print(f"✅ Downloaded: {len(results['downloaded'])} files")
        print(f"❌ Failed: {len(results['failed'])} files")
        print(f"📦 Extracted: {len(results['extracted'])} files")
        print("=" * 70)
        
        return results


def main():
    """Main download function."""
    downloader = EPADataDownloader(output_dir="data/epa")
    
    # Download Site and Monitor listings
    print("\n" + "=" * 70)
    print("STEP 1: Downloading Site and Monitor Information")
    print("=" * 70)
    site_monitor_results = downloader.download_site_monitor_files()
    
    # Download 2019-2024 data for PM2.5 and Ozone
    print("\n" + "=" * 70)
    print("STEP 2: Downloading Daily Summary Data")
    print("=" * 70)
    years = list(range(2019, 2025))  # 2019-2024
    parameters = ["pm25", "ozone"]
    
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

