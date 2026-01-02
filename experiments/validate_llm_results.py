#!/usr/bin/env python3
"""
Validate LLM Query Results Against Golden Data

This script:
1. Runs queries through LLM (using evaluation framework)
2. Compares results with golden data
3. Reports accuracy and mismatches

Usage:
    python validate_llm_results.py --with-semantic
    python validate_llm_results.py --without-semantic
    python validate_llm_results.py --both
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from experiments.golden_data_test_suite import GoldenDataValidator
from experiments.evaluation_framework import EPAAirQualityEvaluator, QueryResult


class LLMResultValidator:
    """Validate LLM results against golden data."""

    def __init__(
        self,
        golden_data_file: str = "experiments/golden_data.json",
        catalog_name: str = "rest",
        catalog_config: Dict[str, Any] = None,
        semantic_yaml: str = None,
    ):
        """Initialize validator."""
        self.validator = GoldenDataValidator(golden_data_file)
        self.golden_tests = self.validator.tests

        # Default catalog config
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

        # Initialize evaluator
        self.evaluator = EPAAirQualityEvaluator(
            catalog_name=catalog_name,
            catalog_config=catalog_config,
            semantic_yaml=semantic_yaml,
        )

    def validate_all(
        self,
        use_semantic: bool = False,
        output_file: str = None
    ) -> Dict[str, Any]:
        """
        Validate all golden data tests through LLM.

        Args:
            use_semantic: Whether to use semantic layer
            output_file: Optional file to save validation results

        Returns:
            Validation summary
        """
        print("=" * 70)
        print(f"VALIDATING LLM RESULTS {'WITH' if use_semantic else 'WITHOUT'} SEMANTIC LAYER")
        print("=" * 70)

        results = []
        passed = 0
        failed = 0

        for i, test in enumerate(self.golden_tests, 1):
            query_id = test["query_id"]
            nl_query = test["natural_language_query"]

            print(f"\n[{i}/{len(self.golden_tests)}] {query_id}: {nl_query}")

            # Run query through LLM
            try:
                llm_result = self.evaluator.evaluate_query(
                    nl_query,
                    use_semantic=use_semantic
                )

                # Extract result from LLM response
                llm_value = self._extract_result_from_response(
                    llm_result.response,
                    test["expected_result_type"]
                )

                # Validate against golden data
                validation = self.validator.validate_result(
                    query_id=query_id,
                    llm_result=llm_value
                )

                if validation["match"]:
                    print(f"  ✅ PASSED")
                    passed += 1
                else:
                    print(f"  ❌ FAILED")
                    print(f"     Expected: {validation['expected']}")
                    print(f"     Got: {validation['actual']}")
                    failed += 1

                results.append({
                    **validation,
                    "llm_response": llm_result.response,
                    "execution_time": llm_result.execution_time,
                    "success": llm_result.success,
                    "error": llm_result.error,
                })

            except Exception as e:
                print(f"  ❌ ERROR: {e}")
                failed += 1
                results.append({
                    "query_id": query_id,
                    "query": nl_query,
                    "match": False,
                    "error": str(e)
                })

        # Summary
        total = len(self.golden_tests)
        accuracy = (passed / total * 100) if total > 0 else 0

        summary = {
            "total_tests": total,
            "passed": passed,
            "failed": failed,
            "accuracy": accuracy,
            "use_semantic": use_semantic,
            "results": results
        }

        # Print summary
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ({accuracy:.1f}%)")
        print(f"Failed: {failed}")
        print("=" * 70)

        # Save results
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"\n✅ Results saved to: {output_file}")

        return summary

    def _extract_result_from_response(
        self,
        response: str,
        result_type: str
    ) -> Any:
        """
        Extract structured result from LLM text response.

        This is a simple extraction - can be improved with better parsing.
        """
        response_lower = response.lower()

        if result_type == "count":
            # Look for numbers in response
            import re
            numbers = re.findall(r'\d{1,3}(?:,\d{3})*', response)
            if numbers:
                return int(numbers[0].replace(',', ''))
            # Try to find number after keywords
            for keyword in ["is", "are", "has", "have", "total", "count"]:
                pattern = f"{keyword}[^0-9]*([0-9,]+)"
                match = re.search(pattern, response_lower)
                if match:
                    return int(match.group(1).replace(',', ''))
            return response

        elif result_type == "value":
            # Look for decimal numbers
            import re
            numbers = re.findall(r'\d+\.\d+', response)
            if numbers:
                return float(numbers[0])
            return response

        elif result_type == "list":
            # Try to extract list items
            import re
            # Look for bullet points or numbered lists
            items = re.findall(r'[-•]\s*([^\n]+)', response)
            if items:
                return [item.strip() for item in items]
            # Look for comma-separated items
            if ',' in response:
                items = [item.strip() for item in response.split(',')]
                return items
            return response

        elif result_type == "boolean":
            # Look for yes/no, true/false
            if any(word in response_lower for word in ["yes", "true", "exists", "has"]):
                return True
            if any(word in response_lower for word in ["no", "false", "does not exist", "doesn't have"]):
                return False
            return response

        elif result_type == "dict":
            # Try to extract key-value pairs
            import re
            # Look for patterns like "state: value" or "state = value"
            pairs = re.findall(r'([a-zA-Z_]+)[\s:=]+([0-9.]+)', response)
            if pairs:
                return {k: float(v) for k, v in pairs}
            return response

        return response


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Validate LLM Results Against Golden Data")
    parser.add_argument(
        "--with-semantic",
        action="store_true",
        help="Validate with semantic layer"
    )
    parser.add_argument(
        "--without-semantic",
        action="store_true",
        help="Validate without semantic layer"
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="Validate both with and without semantic layer"
    )
    parser.add_argument(
        "--golden-data",
        default="experiments/golden_data.json",
        help="Path to golden data JSON file"
    )
    parser.add_argument(
        "--output",
        help="Output file for validation results"
    )
    parser.add_argument(
        "--semantic-yaml",
        default="experiments/epa_complete_semantic.yaml",
        help="Path to semantic YAML file"
    )

    args = parser.parse_args()

    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ ERROR: OPENAI_API_KEY environment variable not set")
        print("   Set it with: export OPENAI_API_KEY='your-key-here'")
        return

    # Determine what to run
    if args.both:
        run_with = True
        run_without = True
    elif args.with_semantic:
        run_with = True
        run_without = False
    elif args.without_semantic:
        run_with = False
        run_without = True
    else:
        # Default: run both
        run_with = True
        run_without = True

    # Run validations
    if run_without:
        print("\n" + "=" * 70)
        validator = LLMResultValidator(
            golden_data_file=args.golden_data,
            semantic_yaml=None
        )
        output_file = args.output or "experiments/validation_results_without_semantic.json"
        validator.validate_all(use_semantic=False, output_file=output_file)

    if run_with:
        print("\n" + "=" * 70)
        validator = LLMResultValidator(
            golden_data_file=args.golden_data,
            semantic_yaml=args.semantic_yaml
        )
        output_file = args.output or "experiments/validation_results_with_semantic.json"
        validator.validate_all(use_semantic=True, output_file=output_file)


if __name__ == "__main__":
    main()

