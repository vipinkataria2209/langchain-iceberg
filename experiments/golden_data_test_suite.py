#!/usr/bin/env python3
"""
Golden Data Test Suite for LangChain Iceberg Framework Validation

This script:
1. Runs queries directly (ground truth) to get expected results
2. Stores results in JSON format
3. Can be used to validate LLM query results

Usage:
    python golden_data_test_suite.py --generate  # Generate golden data
    python golden_data_test_suite.py --validate  # Validate LLM results
"""

import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class GoldenTestResult:
    """Result of a golden data test."""
    query_id: str
    query_type: str  # "catalog", "simple", "aggregation", "join"
    natural_language_query: str
    expected_result: Any
    expected_result_type: str  # "count", "value", "list", "schema", "dataframe"
    metadata: Dict[str, Any]  # Additional info (table, filters, etc.)


class GoldenDataGenerator:
    """Generate golden data by running queries directly."""

    def __init__(
        self,
        catalog_name: str = "rest_epa",
        catalog_config: Dict[str, Any] = None
    ):
        """Initialize with catalog."""
        self.catalog_name = catalog_name

        # Default catalog config (REST catalog)
        if catalog_config is None:
            catalog_config = {
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://warehouse/wh/",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            }

        # Try environment variable first
        catalog_env = os.getenv("ICEBERG_CATALOG")
        if catalog_env:
            try:
                self.catalog = load_catalog(catalog_env)
                print(f"✅ Loaded catalog from environment: {catalog_env}")
            except Exception:
                # Fall back to explicit config
                self.catalog = load_catalog(name=catalog_name, **catalog_config)
        else:
            # Use explicit configuration
            self.catalog = load_catalog(name=catalog_name, **catalog_config)

        self.results: List[GoldenTestResult] = []

    def generate_all(self) -> List[GoldenTestResult]:
        """Generate all golden data tests."""
        print("=" * 70)
        print("GENERATING GOLDEN DATA TEST SUITE")
        print("=" * 70)

        # 1. Basic Catalog Queries
        print("\n[1/4] Generating catalog queries...")
        self._generate_catalog_queries()

        # 2. Simple Data Queries
        print("\n[2/4] Generating simple data queries...")
        self._generate_simple_queries()

        # 3. Aggregation Queries
        print("\n[3/4] Generating aggregation queries...")
        self._generate_aggregation_queries()

        # 4. Schema Queries
        print("\n[4/4] Generating schema queries...")
        self._generate_schema_queries()

        print(f"\n✅ Generated {len(self.results)} golden data tests")
        return self.results

    def _generate_catalog_queries(self):
        """Generate catalog exploration queries."""
        # Query 1: Number of namespaces
        try:
            namespaces = list(self.catalog.list_namespaces())
            namespace_count = len(namespaces)
            namespace_list = [".".join(ns) if isinstance(ns, tuple) else str(ns) for ns in namespaces]

            self.results.append(GoldenTestResult(
                query_id="catalog_001",
                query_type="catalog",
                natural_language_query="How many namespaces are in the catalog?",
                expected_result=namespace_count,
                expected_result_type="count",
                metadata={
                    "namespaces": namespace_list,
                    "tool": "iceberg_list_namespaces"
                }
            ))
            print(f"  ✅ catalog_001: {namespace_count} namespaces")
        except Exception as e:
            print(f"  ❌ catalog_001 failed: {e}")

        # Query 2: Number of tables in epa namespace
        try:
            namespace_tuple = ("epa",)
            tables = list(self.catalog.list_tables(namespace_tuple))
            table_count = len(tables)
            table_list = [f"epa.{t[-1]}" if isinstance(t, tuple) else f"epa.{t}" for t in tables]

            self.results.append(GoldenTestResult(
                query_id="catalog_002",
                query_type="catalog",
                natural_language_query="How many tables are in the epa namespace?",
                expected_result=table_count,
                expected_result_type="count",
                metadata={
                    "namespace": "epa",
                    "tables": table_list,
                    "tool": "iceberg_list_tables"
                }
            ))
            print(f"  ✅ catalog_002: {table_count} tables in epa namespace")
        except Exception as e:
            print(f"  ❌ catalog_002 failed: {e}")

        # Query 3: List all table names
        try:
            namespace_tuple = ("epa",)
            tables = list(self.catalog.list_tables(namespace_tuple))
            table_names = [t[-1] if isinstance(t, tuple) else str(t) for t in tables]

            self.results.append(GoldenTestResult(
                query_id="catalog_003",
                query_type="catalog",
                natural_language_query="List all table names in the epa namespace",
                expected_result=table_names,
                expected_result_type="list",
                metadata={
                    "namespace": "epa",
                    "tool": "iceberg_list_tables"
                }
            ))
            print(f"  ✅ catalog_003: {len(table_names)} table names")
        except Exception as e:
            print(f"  ❌ catalog_003 failed: {e}")

    def _generate_simple_queries(self):
        """Generate simple data queries."""
        try:
            table = self.catalog.load_table(("epa", "daily_summary"))
            scan = table.scan()
            arrow_table = scan.to_arrow()

            # Query 1: Total row count
            total_rows = len(arrow_table)
            self.results.append(GoldenTestResult(
                query_id="simple_001",
                query_type="simple",
                natural_language_query="How many total records are in the daily_summary table?",
                expected_result=total_rows,
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "tool": "iceberg_query"
                }
            ))
            print(f"  ✅ simple_001: {total_rows:,} total records")

            # Query 2: Count PM2.5 records
            pm25_data = arrow_table.filter(pc.equal(arrow_table['parameter_code'], '88101'))
            pm25_count = len(pm25_data)
            self.results.append(GoldenTestResult(
                query_id="simple_002",
                query_type="simple",
                natural_language_query="How many PM2.5 measurements are in the database?",
                expected_result=pm25_count,
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "filter": "parameter_code = '88101'",
                    "tool": "iceberg_query"
                }
            ))
            print(f"  ✅ simple_002: {pm25_count:,} PM2.5 records")

            # Query 3: Count Ozone records
            ozone_data = arrow_table.filter(pc.equal(arrow_table['parameter_code'], '44201'))
            ozone_count = len(ozone_data)
            self.results.append(GoldenTestResult(
                query_id="simple_003",
                query_type="simple",
                natural_language_query="How many Ozone measurements are in the database?",
                expected_result=ozone_count,
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "filter": "parameter_code = '44201'",
                    "tool": "iceberg_query"
                }
            ))
            print(f"  ✅ simple_003: {ozone_count:,} Ozone records")

            # Query 4: Count records for California (state_code = '6')
            ca_data = arrow_table.filter(pc.equal(arrow_table['state_code'], '6'))
            ca_count = len(ca_data)
            self.results.append(GoldenTestResult(
                query_id="simple_004",
                query_type="simple",
                natural_language_query="How many air quality records are there for California?",
                expected_result=ca_count,
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "filter": "state_code = '6'",
                    "state": "California",
                    "tool": "iceberg_query"
                }
            ))
            print(f"  ✅ simple_004: {ca_count:,} California records")

        except Exception as e:
            print(f"  ❌ Simple queries failed: {e}")

    def _generate_aggregation_queries(self):
        """Generate aggregation queries."""
        try:
            table = self.catalog.load_table(("epa", "daily_summary"))
            scan = table.scan()
            arrow_table = scan.to_arrow()
            df = arrow_table.to_pandas()

            # Query 1: Average PM2.5 overall
            pm25_df = df[df['parameter_code'] == '88101']
            if len(pm25_df) > 0:
                avg_pm25 = float(pm25_df['arithmetic_mean'].mean())
                self.results.append(GoldenTestResult(
                    query_id="agg_001",
                    query_type="aggregation",
                    natural_language_query="What is the average PM2.5 concentration across all measurements?",
                    expected_result=round(avg_pm25, 3),
                    expected_result_type="value",
                    metadata={
                        "table": "epa.daily_summary",
                        "filter": "parameter_code = '88101'",
                        "aggregation": "mean(arithmetic_mean)",
                        "tool": "iceberg_query"
                    }
                ))
                print(f"  ✅ agg_001: Average PM2.5 = {avg_pm25:.3f} μg/m³")

            # Query 2: Average PM2.5 in California
            ca_pm25_df = df[(df['parameter_code'] == '88101') & (df['state_code'] == '6')]
            if len(ca_pm25_df) > 0:
                avg_ca_pm25 = float(ca_pm25_df['arithmetic_mean'].mean())
                self.results.append(GoldenTestResult(
                    query_id="agg_002",
                    query_type="aggregation",
                    natural_language_query="What is the average PM2.5 concentration in California?",
                    expected_result=round(avg_ca_pm25, 3),
                    expected_result_type="value",
                    metadata={
                        "table": "epa.daily_summary",
                        "filter": "parameter_code = '88101' AND state_code = '6'",
                        "aggregation": "mean(arithmetic_mean)",
                        "state": "California",
                        "tool": "iceberg_query"
                    }
                ))
                print(f"  ✅ agg_002: Average PM2.5 in CA = {avg_ca_pm25:.3f} μg/m³")

            # Query 3: Maximum Ozone value
            ozone_df = df[df['parameter_code'] == '44201']
            if len(ozone_df) > 0:
                max_ozone = float(ozone_df['first_max_value'].max())
                self.results.append(GoldenTestResult(
                    query_id="agg_003",
                    query_type="aggregation",
                    natural_language_query="What is the maximum Ozone concentration recorded?",
                    expected_result=round(max_ozone, 3),
                    expected_result_type="value",
                    metadata={
                        "table": "epa.daily_summary",
                        "filter": "parameter_code = '44201'",
                        "aggregation": "max(first_max_value)",
                        "tool": "iceberg_query"
                    }
                ))
                print(f"  ✅ agg_003: Max Ozone = {max_ozone:.3f} ppm")

            # Query 4: Number of unique states
            unique_states = df['state_code'].nunique()
            self.results.append(GoldenTestResult(
                query_id="agg_004",
                query_type="aggregation",
                natural_language_query="How many different states have air quality data?",
                expected_result=int(unique_states),
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "aggregation": "count(distinct state_code)",
                    "tool": "iceberg_query"
                }
            ))
            print(f"  ✅ agg_004: {unique_states} unique states")

            # Query 5: Top 3 states by PM2.5 average
            pm25_df = df[df['parameter_code'] == '88101']
            if len(pm25_df) > 0:
                state_avg = pm25_df.groupby('state_code')['arithmetic_mean'].mean().sort_values(ascending=False)
                top_3_states = state_avg.head(3).to_dict()
                top_3_formatted = {k: round(v, 3) for k, v in top_3_states.items()}

                self.results.append(GoldenTestResult(
                    query_id="agg_005",
                    query_type="aggregation",
                    natural_language_query="What are the top 3 states by average PM2.5 concentration?",
                    expected_result=top_3_formatted,
                    expected_result_type="dict",
                    metadata={
                        "table": "epa.daily_summary",
                        "filter": "parameter_code = '88101'",
                        "aggregation": "group by state_code, mean(arithmetic_mean), order by desc, limit 3",
                        "tool": "iceberg_query"
                    }
                ))
                print(f"  ✅ agg_005: Top 3 states by PM2.5")

        except Exception as e:
            print(f"  ❌ Aggregation queries failed: {e}")

    def _generate_schema_queries(self):
        """Generate schema-related queries."""
        try:
            table = self.catalog.load_table(("epa", "daily_summary"))
            schema = table.schema()

            # Query 1: Number of columns
            column_count = len(schema.fields)
            column_names = [field.name for field in schema.fields]

            self.results.append(GoldenTestResult(
                query_id="schema_001",
                query_type="schema",
                natural_language_query="How many columns are in the daily_summary table?",
                expected_result=column_count,
                expected_result_type="count",
                metadata={
                    "table": "epa.daily_summary",
                    "columns": column_names,
                    "tool": "iceberg_get_schema"
                }
            ))
            print(f"  ✅ schema_001: {column_count} columns")

            # Query 2: Column names list
            self.results.append(GoldenTestResult(
                query_id="schema_002",
                query_type="schema",
                natural_language_query="What are the column names in the daily_summary table?",
                expected_result=column_names,
                expected_result_type="list",
                metadata={
                    "table": "epa.daily_summary",
                    "tool": "iceberg_get_schema"
                }
            ))
            print(f"  ✅ schema_002: {len(column_names)} column names")

            # Query 3: Check if specific column exists
            has_parameter_code = any(field.name == 'parameter_code' for field in schema.fields)
            self.results.append(GoldenTestResult(
                query_id="schema_003",
                query_type="schema",
                natural_language_query="Does the daily_summary table have a parameter_code column?",
                expected_result=has_parameter_code,
                expected_result_type="boolean",
                metadata={
                    "table": "epa.daily_summary",
                    "column": "parameter_code",
                    "tool": "iceberg_get_schema"
                }
            ))
            print(f"  ✅ schema_003: parameter_code exists = {has_parameter_code}")

        except Exception as e:
            print(f"  ❌ Schema queries failed: {e}")

    def save_to_file(self, output_file: str = "experiments/golden_data.json"):
        """Save golden data to JSON file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to dict for JSON serialization
        results_dict = {
            "catalog_name": self.catalog_name,
            "total_tests": len(self.results),
            "tests": [asdict(result) for result in self.results]
        }

        with open(output_path, 'w') as f:
            json.dump(results_dict, f, indent=2)

        print(f"\n✅ Golden data saved to: {output_file}")
        return output_path


class GoldenDataValidator:
    """Validate LLM query results against golden data."""

    def __init__(self, golden_data_file: str = "experiments/golden_data.json"):
        """Load golden data."""
        with open(golden_data_file, 'r') as f:
            self.golden_data = json.load(f)
        self.tests = self.golden_data["tests"]

    def validate_result(
        self,
        query_id: str,
        llm_result: Any,
        tolerance: float = 0.01
    ) -> Dict[str, Any]:
        """
        Validate LLM result against golden data.

        Args:
            query_id: ID of the test query
            llm_result: Result from LLM query
            tolerance: Tolerance for numeric comparisons

        Returns:
            Validation result with match status and details
        """
        # Find test
        test = next((t for t in self.tests if t["query_id"] == query_id), None)
        if not test:
            return {
                "query_id": query_id,
                "match": False,
                "error": f"Test {query_id} not found in golden data"
            }

        expected = test["expected_result"]
        result_type = test["expected_result_type"]

        # Compare based on result type
        if result_type == "count":
            match = self._compare_count(llm_result, expected, tolerance)
        elif result_type == "value":
            match = self._compare_value(llm_result, expected, tolerance)
        elif result_type == "list":
            match = self._compare_list(llm_result, expected)
        elif result_type == "dict":
            match = self._compare_dict(llm_result, expected, tolerance)
        elif result_type == "boolean":
            match = self._compare_boolean(llm_result, expected)
        else:
            match = str(llm_result) == str(expected)

        return {
            "query_id": query_id,
            "query": test["natural_language_query"],
            "expected": expected,
            "actual": llm_result,
            "match": match,
            "result_type": result_type
        }

    def _compare_count(self, actual: Any, expected: int, tolerance: float) -> bool:
        """Compare count values."""
        try:
            # Extract number from string (handles "3,539,697" or "There are 3,539,697...")
            import re
            actual_str = str(actual).replace(',', '').strip()
            # Find all numbers and use the largest one (likely the count)
            numbers = re.findall(r'\d+', actual_str)
            if numbers:
                actual_int = int(max(numbers, key=lambda x: int(x)))
            else:
                actual_int = int(float(actual_str))
            return abs(actual_int - expected) <= (expected * tolerance)
        except (ValueError, TypeError):
            return False

    def _compare_value(self, actual: Any, expected: float, tolerance: float) -> bool:
        """Compare numeric values."""
        try:
            # Extract numeric value from string (handles cases like "7.723 µg/m³")
            import re
            actual_str = str(actual).strip()
            # Find all numbers (integers or floats) in the string
            numbers = re.findall(r'\d+\.\d+|\d+', actual_str)
            if numbers:
                # Convert to floats
                float_numbers = [float(n) for n in numbers]
                # If we have an expected value, pick the number closest to it
                # Otherwise, use the largest number (likely the result)
                if expected is not None:
                    actual_float = min(float_numbers, key=lambda x: abs(x - expected))
                else:
                    actual_float = max(float_numbers)
            else:
                # Fallback: try direct conversion
                actual_float = float(actual_str)
            return abs(actual_float - expected) <= (abs(expected) * tolerance + tolerance)
        except (ValueError, TypeError):
            return False

    def _compare_list(self, actual: Any, expected: List) -> bool:
        """Compare lists."""
        if not isinstance(actual, (list, str)):
            return False
        if isinstance(actual, str):
            # Try to parse as list or check if it contains expected items
            actual_lower = actual.lower()
            return all(str(item).lower() in actual_lower for item in expected)
        return set(actual) == set(expected)

    def _compare_dict(self, actual: Any, expected: Dict, tolerance: float) -> bool:
        """Compare dictionaries."""
        if not isinstance(actual, dict):
            return False
        if set(actual.keys()) != set(expected.keys()):
            return False
        for key in expected.keys():
            if not self._compare_value(actual.get(key), expected[key], tolerance):
                return False
        return True

    def _compare_boolean(self, actual: Any, expected: bool) -> bool:
        """Compare boolean values."""
        actual_str = str(actual).lower().strip()
        expected_str = str(expected).lower()
        return expected_str in actual_str or actual_str == expected_str


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Golden Data Test Suite")
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate golden data by running queries directly"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate LLM results (not yet implemented)"
    )
    parser.add_argument(
        "--output",
        default="golden_data.json",
        help="Output file for golden data (relative to experiments directory)"
    )
    parser.add_argument(
        "--catalog",
        default="rest_epa",
        help="Catalog name to use"
    )

    args = parser.parse_args()

    if args.generate:
        # Default catalog config
        catalog_config = {
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/wh/",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        }

        generator = GoldenDataGenerator(
            catalog_name=args.catalog,
            catalog_config=catalog_config
        )
        generator.generate_all()
        generator.save_to_file(args.output)
    elif args.validate:
        print("=" * 70)
        print("VALIDATION MODE")
        print("=" * 70)
        print("\n⚠️  Note: Use 'validate_llm_results.py' for full LLM validation")
        print("   This script only provides the validation logic class.")
        print("\nTo validate LLM results, run:")
        print("   python experiments/validate_llm_results.py --both")
        print("\nOr use the GoldenDataValidator class programmatically:")
        print("   from experiments.golden_data_test_suite import GoldenDataValidator")
        print("   validator = GoldenDataValidator('golden_data.json')")
        print("   result = validator.validate_result(query_id, llm_result)")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

