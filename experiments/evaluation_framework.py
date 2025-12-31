#!/usr/bin/env python3
"""
Evaluation Framework for LangChain Iceberg Toolkit

Compares performance and accuracy with/without semantic layer,
similar to experiments described in the Frontiers paper.
"""

import time
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

from langchain_iceberg import IcebergToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub


@dataclass
class QueryResult:
    """Result of a single query evaluation."""
    query: str
    with_semantic: bool
    success: bool
    execution_time: float
    response: str
    error: Optional[str] = None
    tool_calls: int = 0
    tokens_used: Optional[int] = None


@dataclass
class EvaluationResults:
    """Complete evaluation results."""
    total_queries: int
    with_semantic_success: int
    without_semantic_success: int
    with_semantic_avg_time: float
    without_semantic_avg_time: float
    accuracy_improvement: float
    performance_improvement: float
    detailed_results: List[QueryResult]


class EPAAirQualityEvaluator:
    """
    Evaluator for EPA air quality data queries.
    
    Compares:
    1. LLM accuracy with semantic layer vs without
    2. Query execution performance
    3. Natural language understanding
    """
    
    def __init__(
        self,
        catalog_name: str,
        catalog_config: Dict[str, Any],
        semantic_yaml: Optional[str] = None,
        llm_model: str = "gpt-4",
        temperature: float = 0.0,
    ):
        """
        Initialize evaluator.
        
        Args:
            catalog_name: Iceberg catalog name
            catalog_config: Catalog configuration
            semantic_yaml: Path to semantic layer YAML
            llm_model: LLM model to use
            temperature: LLM temperature
        """
        self.catalog_name = catalog_name
        self.catalog_config = catalog_config
        self.semantic_yaml = semantic_yaml
        self.llm_model = llm_model
        self.temperature = temperature
        
        # Initialize LLM
        self.llm = ChatOpenAI(model=llm_model, temperature=temperature)
        
    def create_toolkit(self, use_semantic: bool = False) -> IcebergToolkit:
        """Create toolkit with or without semantic layer."""
        return IcebergToolkit(
            catalog_name=self.catalog_name,
            catalog_config=self.catalog_config,
            semantic_yaml=self.semantic_yaml if use_semantic else None,
        )
    
    def create_agent(self, toolkit: IcebergToolkit) -> AgentExecutor:
        """Create LangChain agent with toolkit."""
        tools = toolkit.get_tools()
        prompt = hub.pull("hwchase17/react")
        agent = create_react_agent(self.llm, tools, prompt=prompt)
        return AgentExecutor(
            agent=agent,
            tools=tools,
            verbose=False,
            handle_parsing_errors=True,
            max_iterations=10,
        )
    
    def evaluate_query(
        self,
        query: str,
        use_semantic: bool = False,
    ) -> QueryResult:
        """
        Evaluate a single query.
        
        Args:
            query: Natural language query
            use_semantic: Whether to use semantic layer
            
        Returns:
            QueryResult with evaluation metrics
        """
        start_time = time.time()
        toolkit = self.create_toolkit(use_semantic=use_semantic)
        agent = self.create_agent(toolkit)
        
        try:
            result = agent.invoke({"input": query})
            execution_time = time.time() - start_time
            
            return QueryResult(
                query=query,
                with_semantic=use_semantic,
                success=True,
                execution_time=execution_time,
                response=result.get("output", ""),
                tool_calls=len(result.get("intermediate_steps", [])),
            )
        except Exception as e:
            execution_time = time.time() - start_time
            return QueryResult(
                query=query,
                with_semantic=use_semantic,
                success=False,
                execution_time=execution_time,
                response="",
                error=str(e),
            )
    
    def evaluate_query_pair(self, query: str) -> Dict[str, QueryResult]:
        """
        Evaluate same query with and without semantic layer.
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary with 'with_semantic' and 'without_semantic' results
        """
        print(f"\n📊 Evaluating: {query}")
        
        # Without semantic layer
        print("  Testing without semantic layer...")
        result_without = self.evaluate_query(query, use_semantic=False)
        
        # With semantic layer
        if self.semantic_yaml:
            print("  Testing with semantic layer...")
            result_with = self.evaluate_query(query, use_semantic=True)
        else:
            result_with = None
        
        return {
            "without_semantic": result_without,
            "with_semantic": result_with,
        }
    
    def run_evaluation(
        self,
        test_queries: List[str],
        output_file: Optional[str] = None,
    ) -> EvaluationResults:
        """
        Run complete evaluation suite.
        
        Args:
            test_queries: List of natural language queries to test
            output_file: Optional file to save results
            
        Returns:
            EvaluationResults with aggregated metrics
        """
        print("=" * 70)
        print("EPA Air Quality Data - Evaluation Framework")
        print("=" * 70)
        print(f"Total queries: {len(test_queries)}")
        print(f"Semantic layer: {self.semantic_yaml or 'Not used'}")
        print("=" * 70)
        
        detailed_results = []
        with_semantic_results = []
        without_semantic_results = []
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n[{i}/{len(test_queries)}] Query: {query}")
            
            # Evaluate without semantic layer
            result_without = self.evaluate_query(query, use_semantic=False)
            without_semantic_results.append(result_without)
            detailed_results.append(result_without)
            
            print(f"  Without semantic: {'✅' if result_without.success else '❌'} "
                  f"({result_without.execution_time:.2f}s)")
            
            # Evaluate with semantic layer if available
            if self.semantic_yaml:
                result_with = self.evaluate_query(query, use_semantic=True)
                with_semantic_results.append(result_with)
                detailed_results.append(result_with)
                
                print(f"  With semantic: {'✅' if result_with.success else '❌'} "
                      f"({result_with.execution_time:.2f}s)")
        
        # Calculate aggregate metrics
        without_success = sum(1 for r in without_semantic_results if r.success)
        without_avg_time = (
            sum(r.execution_time for r in without_semantic_results) / len(without_semantic_results)
            if without_semantic_results else 0
        )
        
        with_success = sum(1 for r in with_semantic_results if r.success) if with_semantic_results else 0
        with_avg_time = (
            sum(r.execution_time for r in with_semantic_results) / len(with_semantic_results)
            if with_semantic_results else 0
        )
        
        accuracy_improvement = 0
        if without_semantic_results and with_semantic_results:
            without_rate = without_success / len(without_semantic_results)
            with_rate = with_success / len(with_semantic_results)
            accuracy_improvement = ((with_rate - without_rate) / without_rate * 100) if without_rate > 0 else 0
        
        performance_improvement = 0
        if with_avg_time > 0 and without_avg_time > 0:
            performance_improvement = ((without_avg_time - with_avg_time) / without_avg_time * 100)
        
        results = EvaluationResults(
            total_queries=len(test_queries),
            with_semantic_success=with_success,
            without_semantic_success=without_success,
            with_semantic_avg_time=with_avg_time,
            without_semantic_avg_time=without_avg_time,
            accuracy_improvement=accuracy_improvement,
            performance_improvement=performance_improvement,
            detailed_results=detailed_results,
        )
        
        # Print summary
        self._print_summary(results)
        
        # Save results
        if output_file:
            self._save_results(results, output_file)
        
        return results
    
    def _print_summary(self, results: EvaluationResults):
        """Print evaluation summary."""
        print("\n" + "=" * 70)
        print("EVALUATION SUMMARY")
        print("=" * 70)
        print(f"Total Queries: {results.total_queries}")
        print(f"\nWithout Semantic Layer:")
        print(f"  Success Rate: {results.without_semantic_success}/{results.total_queries} "
              f"({results.without_semantic_success/results.total_queries*100:.1f}%)")
        print(f"  Avg Execution Time: {results.without_semantic_avg_time:.2f}s")
        
        if results.with_semantic_avg_time > 0:
            print(f"\nWith Semantic Layer:")
            print(f"  Success Rate: {results.with_semantic_success}/{results.total_queries} "
                  f"({results.with_semantic_success/results.total_queries*100:.1f}%)")
            print(f"  Avg Execution Time: {results.with_semantic_avg_time:.2f}s")
            print(f"\nImprovements:")
            print(f"  Accuracy: {results.accuracy_improvement:+.1f}%")
            print(f"  Performance: {results.performance_improvement:+.1f}%")
        print("=" * 70)
    
    def _save_results(self, results: EvaluationResults, output_file: str):
        """Save results to JSON file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to dict for JSON serialization
        results_dict = {
            "total_queries": results.total_queries,
            "with_semantic_success": results.with_semantic_success,
            "without_semantic_success": results.without_semantic_success,
            "with_semantic_avg_time": results.with_semantic_avg_time,
            "without_semantic_avg_time": results.without_semantic_avg_time,
            "accuracy_improvement": results.accuracy_improvement,
            "performance_improvement": results.performance_improvement,
            "detailed_results": [asdict(r) for r in results.detailed_results],
        }
        
        with open(output_path, 'w') as f:
            json.dump(results_dict, f, indent=2)
        
        print(f"\n✅ Results saved to: {output_file}")


# EPA-specific test queries
EPA_TEST_QUERIES = [
    # Basic queries
    "What is the average PM2.5 concentration in California?",
    "Show me the maximum ozone levels in New York",
    "How many air quality measurements are in the database?",
    
    # Time-based queries
    "What was the average AQI in 2023?",
    "Compare PM2.5 levels in January vs July 2023",
    "Show me the worst air quality day in 2023",
    
    # Geographic queries
    "Which state has the highest average PM2.5?",
    "List all counties in California with air quality data",
    "What is the air quality in Los Angeles County?",
    
    # Complex queries
    "How many days had unhealthy air quality (AQI > 100) in 2023?",
    "What is the trend of PM2.5 concentrations over the last year?",
    "Which pollutant has the highest average concentration?",
    
    # Semantic layer specific (if using semantic YAML)
    "Get the average PM2.5 concentration",
    "What is the maximum ozone concentration?",
    "How many unhealthy days were there?",
]


if __name__ == "__main__":
    import os
    
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️  OPENAI_API_KEY not set")
        exit(1)
    
    # Example evaluation
    evaluator = EPAAirQualityEvaluator(
        catalog_name="rest",
        catalog_config={
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/wh/",
        },
        semantic_yaml="experiments/epa_semantic.yaml",
    )
    
    # Run evaluation with subset of queries for testing
    results = evaluator.run_evaluation(
        test_queries=EPA_TEST_QUERIES[:5],  # Test with first 5 queries
        output_file="experiments/results/evaluation_results.json",
    )

