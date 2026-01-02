#!/usr/bin/env python3
"""
Main script to run EPA air quality data experiments.

This script orchestrates the complete evaluation:
1. Loads EPA data (if needed)
2. Runs evaluation with/without semantic layer
3. Generates results and reports
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from experiments.epa_data_loader import EPADataLoader
from experiments.evaluation_framework import EPAAirQualityEvaluator, EPA_TEST_QUERIES
from pyiceberg.catalog import load_catalog


def check_environment():
    """Check if required environment is set up."""
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ ERROR: OPENAI_API_KEY environment variable not set")
        print("   Set it with: export OPENAI_API_KEY='your-key-here'")
        return False
    return True


def load_epa_data(data_dir: str = "data/epa", catalog_config: dict = None):
    """
    Load EPA data into Iceberg if data directory exists.
    
    Args:
        data_dir: Directory containing EPA CSV files
        catalog_config: Catalog configuration
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"⚠️  Data directory not found: {data_dir}")
        print("   Skipping data loading. Place EPA CSV files in this directory to load data.")
        return False
    
    csv_files = list(data_path.glob("*.csv*"))
    if not csv_files:
        print(f"⚠️  No CSV files found in {data_dir}")
        return False
    
    print(f"📊 Loading EPA data from {data_dir}...")
    print(f"   Found {len(csv_files)} files")
    
    try:
        catalog = load_catalog(
            name="rest",
            type="rest",
            uri=catalog_config.get("uri", "http://localhost:8181"),
            warehouse=catalog_config.get("warehouse", "s3://warehouse/wh/"),
        )
        
        loader = EPADataLoader(catalog, namespace="epa")
        
        # Check if table exists
        try:
            table = catalog.load_table(("epa", "daily_summary"))
            print("✅ Table already exists")
        except Exception:
            table = loader.create_table()
        
        # Load files
        results = loader.load_directory(str(data_path))
        total_rows = sum(results.values())
        print(f"✅ Loaded {total_rows} total rows from {len(results)} files")
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False


def run_evaluation(
    catalog_config: dict,
    semantic_yaml: str = "experiments/epa_complete_semantic.yaml",
    output_dir: str = "experiments/results",
    num_queries: int = None,
):
    """
    Run the evaluation framework.
    
    Args:
        catalog_config: Catalog configuration
        semantic_yaml: Path to semantic layer YAML
        output_dir: Directory to save results
        num_queries: Number of queries to test (None = all)
    """
    print("\n" + "=" * 70)
    print("RUNNING EVALUATION")
    print("=" * 70)
    
    # Select queries
    queries = EPA_TEST_QUERIES
    if num_queries:
        queries = queries[:num_queries]
        print(f"Testing with {len(queries)} queries (limited for testing)")
    else:
        print(f"Testing with all {len(queries)} queries")
    
    # Check if semantic YAML exists
    semantic_path = Path(semantic_yaml)
    if not semantic_path.exists():
        print(f"⚠️  Semantic YAML not found: {semantic_yaml}")
        print("   Running evaluation without semantic layer")
        semantic_yaml = None
    
    # Create evaluator
    evaluator = EPAAirQualityEvaluator(
        catalog_name="rest",
        catalog_config=catalog_config,
        semantic_yaml=semantic_yaml,
        llm_model="gpt-4",
        temperature=0.0,
    )
    
    # Run evaluation
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    results = evaluator.run_evaluation(
        test_queries=queries,
        output_file=str(output_path / "evaluation_results.json"),
    )
    
    return results


def main():
    """Main experiment runner."""
    print("=" * 70)
    print("EPA Air Quality Data - Experiment Runner")
    print("=" * 70)
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Catalog configuration
    catalog_config = {
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/wh/",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
    
    # Step 1: Load data (optional)
    print("\n[Step 1/2] Data Loading")
    print("-" * 70)
    load_epa_data(data_dir="data/epa", catalog_config=catalog_config)
    
    # Step 2: Run evaluation
    print("\n[Step 2/2] Evaluation")
    print("-" * 70)
    
    # For testing, use fewer queries. Set to None for full evaluation
    results = run_evaluation(
        catalog_config=catalog_config,
        semantic_yaml="experiments/epa_complete_semantic.yaml",
        output_dir="experiments/results",
        num_queries=5,  # Change to None for full evaluation
    )
    
    print("\n" + "=" * 70)
    print("EXPERIMENTS COMPLETE")
    print("=" * 70)
    print(f"Results saved to: experiments/results/evaluation_results.json")
    print("\nNext steps:")
    print("  1. Review results in experiments/results/")
    print("  2. Compare with paper results")
    print("  3. Adjust queries or semantic layer as needed")


if __name__ == "__main__":
    main()

