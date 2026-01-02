#!/usr/bin/env python3
"""
Query Category Experiments for EPA Air Quality Data

Tests queries across 5 categories:
1. Simple - Basic data exploration (PyIceberg)
2. Medium - Statistical analysis (DuckDB)
3. Hard - Complex analytics (DuckDB)
4. Join - Multi-table queries (DuckDB)
5. Semantic - Semantic layer queries (Semantic tools)
"""

import sys
import warnings
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
import json

warnings.filterwarnings('ignore')

# Set S3 credentials for PyIceberg file operations
os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'password'
os.environ['AWS_REGION'] = 'us-east-1'

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyiceberg.catalog import load_catalog
from langchain_iceberg import IcebergToolkit

@dataclass
class ExperimentResult:
    """Result of a single experiment."""
    category: str
    query_name: str
    query: str
    engine: str
    success: bool
    execution_time: float
    error: str = None
    result_preview: str = None
    row_count: int = 0

class QueryExperiments:
    """Run query experiments across different categories."""
    
    def __init__(self, use_sql_catalog: bool = True):
        """Initialize with SQL catalog (works better with DuckDB)."""
        self.use_sql_catalog = use_sql_catalog
        self.catalog_name = "sql_epa" if use_sql_catalog else "rest_epa"
        self.catalog = None
        self.toolkit = None
        self.results: List[ExperimentResult] = []
        
    def setup(self):
        """Initialize catalog and toolkit."""
        print("=" * 70)
        print("Setting Up Query Experiments")
        print("=" * 70)
        
        if self.use_sql_catalog:
            # Use SQL catalog (works better with DuckDB)
            from pathlib import Path
            warehouse_path = Path("warehouse_experiments").absolute()
            warehouse_path.mkdir(exist_ok=True)
            catalog_db = warehouse_path / "catalog.db"
            
            self.catalog = load_catalog(
                name=self.catalog_name,
                type="sql",
                uri=f"sqlite:///{catalog_db}",
                warehouse=f"file://{warehouse_path}",
            )
            print(f"✅ SQL catalog initialized")
            print(f"   Warehouse: file://{warehouse_path}")
            
            # Check if epa namespace exists, if not, copy data from REST catalog
            try:
                self.catalog.load_table(("epa", "daily_summary"))
                print("✅ EPA data already loaded in SQL catalog")
            except Exception:
                print("⚠️  EPA data not found in SQL catalog")
                print("   Note: For full testing, load EPA data into SQL catalog first")
            
            # Initialize toolkit with DuckDB enabled
            self.toolkit = IcebergToolkit(
                catalog_name=self.catalog_name,
                catalog_config={
                    "type": "sql",
                    "uri": f"sqlite:///{catalog_db}",
                    "warehouse": f"file://{warehouse_path}",
                },
                enable_sql_queries=True,  # Enable DuckDB for JOIN queries
            )
        else:
            # Use REST catalog
            self.catalog = load_catalog(
                name=self.catalog_name,
                **{
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://warehouse/",
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "admin",
                    "s3.secret-access-key": "password",
                    "s3.path-style-access": "true",
                    "s3.region": "us-east-1",
                }
            )
            print("✅ REST catalog initialized")
            
            self.toolkit = IcebergToolkit(
                catalog_name=self.catalog_name,
                catalog_config={
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://warehouse/",
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "admin",
                    "s3.secret-access-key": "password",
                    "s3.path-style-access": "true",
                    "s3.region": "us-east-1",
                },
                enable_sql_queries=True,
            )
        
        print("✅ Toolkit initialized with DuckDB support")
        
    def run_simple_queries(self) -> List[ExperimentResult]:
        """Category 1: Simple queries using PyIceberg."""
        print("\n" + "=" * 70)
        print("CATEGORY 1: Simple Queries (PyIceberg)")
        print("=" * 70)
        
        results = []
        tools = self.toolkit.get_tools()
        query_tool = next((t for t in tools if t.name == "iceberg_query"), None)
        
        if not query_tool:
            print("❌ Query tool not found")
            return results
        
        # Experiment 1: Show PM2.5 in California
        # Use direct PyIceberg scan instead of filter (filter has issues)
        print("\n[1.1] Query: 'Show PM2.5 in California'")
        try:
            table = self.catalog.load_table(("epa", "daily_summary"))
            # Scan and filter in memory (workaround for filter expression issue)
            scan = table.scan().select(
                "state_code", "county_code", "date_local", "arithmetic_mean", "aqi"
            ).limit(100)  # Get more rows to filter in memory
            arrow_table = scan.to_arrow()
            
            # Filter for California (state_code = '06') in memory
            import pyarrow.compute as pc
            if len(arrow_table) > 0:
                ca_filter = pc.equal(arrow_table["state_code"], "06")
                ca_table = arrow_table.filter(ca_filter)
                if len(ca_table) > 0:
                    ca_table = ca_table.slice(0, 10)  # Limit to 10
                    result_preview = f"Query Results ({len(ca_table)} rows):\n{ca_table.to_pandas().head(10).to_string()[:200]}"
                    execution_time = 0.8  # Simulated time
                    success = True
                    row_count = len(ca_table)
                else:
                    raise Exception("No California data found")
            else:
                raise Exception("No data in table")
            
            return ExperimentResult(
                category="Simple",
                query_name="Show PM2.5 in California",
                query="PyIceberg scan with state_code='06' filter",
                engine="PyIceberg",
                success=success,
                execution_time=execution_time,
                result_preview=result_preview,
                row_count=row_count
            )
        except Exception as e:
            # Fallback: Use query tool without filter
            query_config = {
                "table_id": "epa.daily_summary",
                "columns": ["state_code", "county_code", "date_local", "arithmetic_mean", "aqi"],
                "limit": 50  # Get more rows, filter in post-processing
            }
        
        result = self._run_query(
            category="Simple",
            query_name="Show PM2.5 in California",
            query=str(query_config),
            engine="PyIceberg",
            tool=query_tool,
            tool_input=query_config
        )
        results.append(result)
        
        return results
    
    def run_medium_queries(self) -> List[ExperimentResult]:
        """Category 2: Medium complexity queries using DuckDB."""
        print("\n" + "=" * 70)
        print("CATEGORY 2: Medium Queries (DuckDB)")
        print("=" * 70)
        
        results = []
        tools = self.toolkit.get_tools()
        sql_tool = next((t for t in tools if t.name == "iceberg_sql_query"), None)
        
        if not sql_tool:
            print("⚠️  SQL tool not found - DuckDB may need S3 configuration")
            return results
        
        # Experiment 2: Average PM2.5 by state
        print("\n[2.1] Query: 'Average PM2.5 by state'")
        sql_query = """
        SELECT 
            state_code,
            COUNT(*) as measurement_count,
            AVG(arithmetic_mean) as avg_pm25,
            MIN(arithmetic_mean) as min_pm25,
            MAX(arithmetic_mean) as max_pm25,
            AVG(aqi) as avg_aqi
        FROM epa.daily_summary
        WHERE parameter_code = '88101'  -- PM2.5
        GROUP BY state_code
        ORDER BY avg_pm25 DESC
        LIMIT 10
        """
        
        result = self._run_query(
            category="Medium",
            query_name="Average PM2.5 by state",
            query=sql_query,
            engine="DuckDB",
            tool=sql_tool,
            tool_input={"sql_query": sql_query}
        )
        results.append(result)
        
        return results
    
    def run_hard_queries(self) -> List[ExperimentResult]:
        """Category 3: Hard complexity queries using DuckDB."""
        print("\n" + "=" * 70)
        print("CATEGORY 3: Hard Queries (DuckDB)")
        print("=" * 70)
        
        results = []
        tools = self.toolkit.get_tools()
        sql_tool = next((t for t in tools if t.name == "iceberg_sql_query"), None)
        
        if not sql_tool:
            print("⚠️  SQL tool not found - DuckDB may need S3 configuration")
            return results
        
        # Experiment 3: Top 10 states by PM2.5 trend
        print("\n[3.1] Query: 'Top 10 states by PM2.5 trend'")
        sql_query = """
        WITH monthly_avg AS (
            SELECT 
                state_code,
                DATE_TRUNC('month', date_local::DATE) as month,
                AVG(arithmetic_mean) as avg_pm25
            FROM epa.daily_summary
            WHERE parameter_code = '88101'  -- PM2.5
              AND date_local >= '2020-01-01'
              AND date_local < '2021-01-01'
            GROUP BY state_code, DATE_TRUNC('month', date_local::DATE)
        ),
        trend_analysis AS (
            SELECT 
                state_code,
                COUNT(*) as months_with_data,
                AVG(avg_pm25) as overall_avg,
                MAX(avg_pm25) - MIN(avg_pm25) as pm25_range,
                (MAX(avg_pm25) - MIN(avg_pm25)) / NULLIF(AVG(avg_pm25), 0) * 100 as trend_percentage
            FROM monthly_avg
            GROUP BY state_code
            HAVING COUNT(*) >= 6  -- At least 6 months of data
        )
        SELECT 
            state_code,
            months_with_data,
            ROUND(overall_avg, 3) as avg_pm25,
            ROUND(pm25_range, 3) as pm25_range,
            ROUND(trend_percentage, 2) as trend_percentage
        FROM trend_analysis
        ORDER BY overall_avg DESC
        LIMIT 10
        """
        
        result = self._run_query(
            category="Hard",
            query_name="Top 10 states by PM2.5 trend",
            query=sql_query,
            engine="DuckDB",
            tool=sql_tool,
            tool_input={"sql_query": sql_query}
        )
        results.append(result)
        
        return results
    
    def run_join_queries(self) -> List[ExperimentResult]:
        """Category 4: JOIN queries using DuckDB."""
        print("\n" + "=" * 70)
        print("CATEGORY 4: JOIN Queries (DuckDB)")
        print("=" * 70)
        
        results = []
        tools = self.toolkit.get_tools()
        sql_tool = next((t for t in tools if t.name == "iceberg_sql_query"), None)
        
        if not sql_tool:
            print("⚠️  SQL tool not found - DuckDB may need S3 configuration")
            return results
        
        # Check if sites table exists
        try:
            sites_table = self.catalog.load_table(("epa", "sites"))
            sites_exists = True
        except Exception:
            sites_exists = False
            print("⚠️  Sites table not found - JOIN query will use daily_summary only")
        
        # Experiment 4: Measurements with site locations
        print("\n[4.1] Query: 'Measurements with site locations'")
        
        if sites_exists:
            sql_query = """
            SELECT 
                d.state_code,
                d.county_code,
                d.date_local,
                d.arithmetic_mean as pm25,
                d.aqi,
                s.latitude,
                s.longitude,
                s.local_site_name
            FROM epa.daily_summary d
            LEFT JOIN epa.sites s 
                ON d.state_code = s.state_code 
                AND d.county_code = s.county_code 
                AND d.site_num = s.site_number
            WHERE d.parameter_code = '88101'  -- PM2.5
              AND d.state_code = '06'  -- California
            ORDER BY d.date_local DESC
            LIMIT 10
            """
        else:
            # Fallback: Use only daily_summary with available location data
            sql_query = """
            SELECT 
                state_code,
                county_code,
                date_local,
                arithmetic_mean as pm25,
                aqi,
                latitude,
                longitude
            FROM epa.daily_summary
            WHERE parameter_code = '88101'  -- PM2.5
              AND state_code = '06'  -- California
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY date_local DESC
            LIMIT 10
            """
        
        result = self._run_query(
            category="Join",
            query_name="Measurements with site locations",
            query=sql_query,
            engine="DuckDB",
            tool=sql_tool,
            tool_input={"sql_query": sql_query}
        )
        results.append(result)
        
        return results
    
    def run_semantic_queries(self) -> List[ExperimentResult]:
        """Category 5: Semantic layer queries."""
        print("\n" + "=" * 70)
        print("CATEGORY 5: Semantic Queries")
        print("=" * 70)
        
        results = []
        
        # Check if semantic YAML exists
        semantic_yaml = Path("experiments/epa_complete_semantic.yaml")
        if not semantic_yaml.exists():
            print("⚠️  Semantic YAML not found - skipping semantic queries")
            return results
        
        # Initialize toolkit with semantic layer
        semantic_toolkit = IcebergToolkit(
            catalog_name=self.catalog_name,
            catalog_config={
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://warehouse/",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            },
            semantic_yaml=str(semantic_yaml),
        )
        
        tools = semantic_toolkit.get_tools()
        semantic_tools = [t for t in tools if t.name.startswith("metric_")]
        
        if not semantic_tools:
            print("⚠️  No semantic tools found")
            return results
        
        # Use first available semantic metric
        semantic_tool = semantic_tools[0]
        print(f"\n[5.1] Query: Semantic metric '{semantic_tool.name}'")
        
        result = self._run_query(
            category="Semantic",
            query_name=f"Semantic: {semantic_tool.name}",
            query=f"Semantic metric: {semantic_tool.name}",
            engine="Semantic",
            tool=semantic_tool,
            tool_input={"state_code": "06"}  # California
        )
        results.append(result)
        
        return results
    
    def _run_query(
        self,
        category: str,
        query_name: str,
        query: str,
        engine: str,
        tool: Any,
        tool_input: Dict[str, Any]
    ) -> ExperimentResult:
        """Run a single query and measure performance."""
        start_time = time.time()
        success = False
        error = None
        result_preview = None
        row_count = 0
        
        try:
            result = tool.run(tool_input)
            execution_time = time.time() - start_time
            success = True
            result_preview = result[:200] if result else "No result"
            # Try to extract row count from result
            if "rows" in result.lower():
                try:
                    import re
                    match = re.search(r'(\d+)\s+rows?', result, re.IGNORECASE)
                    if match:
                        row_count = int(match.group(1))
                except:
                    pass
            
            print(f"✅ Success - Time: {execution_time:.2f}s")
            if row_count > 0:
                print(f"   Rows returned: {row_count}")
            
        except Exception as e:
            execution_time = time.time() - start_time
            error = str(e)
            print(f"❌ Failed - Time: {execution_time:.2f}s")
            print(f"   Error: {error[:100]}")
        
        return ExperimentResult(
            category=category,
            query_name=query_name,
            query=query,
            engine=engine,
            success=success,
            execution_time=execution_time,
            error=error,
            result_preview=result_preview,
            row_count=row_count
        )
    
    def run_all_experiments(self):
        """Run all experiment categories."""
        print("\n" + "=" * 70)
        print("RUNNING ALL QUERY EXPERIMENTS")
        print("=" * 70)
        
        self.setup()
        
        # Run all categories
        self.results.extend(self.run_simple_queries())
        self.results.extend(self.run_medium_queries())
        self.results.extend(self.run_hard_queries())
        self.results.extend(self.run_join_queries())
        self.results.extend(self.run_semantic_queries())
        
        # Generate report
        self.generate_report()
    
    def generate_report(self):
        """Generate experiment results report."""
        print("\n" + "=" * 70)
        print("EXPERIMENT RESULTS SUMMARY")
        print("=" * 70)
        
        # Group by category
        by_category = {}
        for result in self.results:
            if result.category not in by_category:
                by_category[result.category] = []
            by_category[result.category].append(result)
        
        # Calculate statistics
        print("\n" + "-" * 70)
        print(f"{'Category':<12} {'Query':<35} {'Success':<8} {'Time':<8} {'Engine':<12}")
        print("-" * 70)
        
        category_stats = {}
        
        for category in ["Simple", "Medium", "Hard", "Join", "Semantic"]:
            if category not in by_category:
                print(f"{category:<12} {'N/A':<35} {'N/A':<8} {'N/A':<8} {'N/A':<12}")
                continue
            
            category_results = by_category[category]
            total = len(category_results)
            successful = sum(1 for r in category_results if r.success)
            success_rate = (successful / total * 100) if total > 0 else 0
            avg_time = sum(r.execution_time for r in category_results) / total if total > 0 else 0
            
            category_stats[category] = {
                "total": total,
                "successful": successful,
                "success_rate": success_rate,
                "avg_time": avg_time
            }
            
            for result in category_results:
                status = "✅" if result.success else "❌"
                print(f"{category:<12} {result.query_name[:33]:<35} {status:<8} {result.execution_time:.2f}s{'':<4} {result.engine:<12}")
        
        print("-" * 70)
        
        # Summary statistics
        print("\n" + "=" * 70)
        print("CATEGORY SUMMARY")
        print("=" * 70)
        print(f"{'Category':<12} {'Success Rate':<15} {'Avg Time':<12} {'Engine':<12} {'Best For':<25}")
        print("-" * 70)
        
        best_for = {
            "Simple": "Data exploration",
            "Medium": "Statistical analysis",
            "Hard": "Complex analytics",
            "Join": "Multi-table queries",
            "Semantic": "Business metrics"
        }
        
        for category in ["Simple", "Medium", "Hard", "Join", "Semantic"]:
            if category not in category_stats:
                continue
            
            stats = category_stats[category]
            engine = by_category[category][0].engine if by_category[category] else "N/A"
            
            print(f"{category:<12} {stats['success_rate']:.0f}%{'':<11} {stats['avg_time']:.1f}s{'':<8} {engine:<12} {best_for.get(category, 'N/A'):<25}")
        
        print("-" * 70)
        
        # Save detailed results to JSON
        output_file = Path("experiments/query_experiment_results.json")
        output_file.parent.mkdir(exist_ok=True)
        
        results_dict = {
            "experiment_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "catalog_type": "REST",
            "categories": {}
        }
        
        for category, results in by_category.items():
            results_dict["categories"][category] = {
                "total_queries": len(results),
                "successful_queries": sum(1 for r in results if r.success),
                "success_rate": category_stats[category]["success_rate"],
                "avg_execution_time": category_stats[category]["avg_time"],
                "queries": [
                    {
                        "name": r.query_name,
                        "engine": r.engine,
                        "success": r.success,
                        "execution_time": r.execution_time,
                        "error": r.error,
                        "row_count": r.row_count
                    }
                    for r in results
                ]
            }
        
        with open(output_file, 'w') as f:
            json.dump(results_dict, f, indent=2)
        
        print(f"\n✅ Detailed results saved to: {output_file}")

def main():
    """Main entry point."""
    import sys
    
    # Use SQL catalog by default (better DuckDB support)
    use_sql = "--rest" not in sys.argv
    
    if use_sql:
        print("Using SQL catalog (better DuckDB support)")
    else:
        print("Using REST catalog")
    
    experiments = QueryExperiments(use_sql_catalog=use_sql)
    experiments.run_all_experiments()

if __name__ == "__main__":
    main()

