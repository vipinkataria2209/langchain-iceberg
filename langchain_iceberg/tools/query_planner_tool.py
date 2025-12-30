"""Query planner tool with LLM assistance."""

from typing import Any, Dict, Optional

from langchain_iceberg.tools.base import IcebergBaseTool


class QueryPlannerTool(IcebergBaseTool):
    """Tool for planning queries using LLM assistance."""

    name: str = "iceberg_plan_query"
    description: str = """
    Before executing a complex query, use this to plan the best approach.
    The LLM will suggest:
    - Which table to query
    - Which columns to select
    - What filters to apply
    - Whether to use partitions
    - Estimated result size

    Input: question (string) - The data question to answer
    Output: Query plan and suggested parameters

    Example usage: 
    - iceberg_plan_query(question="Find high-value orders from last quarter")
    """

    def __init__(self, catalog: Any, llm: Optional[Any] = None, **kwargs: Any):
        """Initialize query planner with optional LLM."""
        super().__init__(catalog=catalog, **kwargs)
        self.llm = llm

    def _run(self, question: str, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            # If LLM is available, use it for planning
            if self.llm:
                return self._plan_with_llm(question)
            else:
                # Fallback to rule-based planning
                return self._plan_without_llm(question)
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError
            raise IcebergConnectionError(
                f"Failed to plan query: {str(e)}"
            ) from e

    def _plan_with_llm(self, question: str) -> str:
        """Plan query using LLM."""
        # Get available tables for context
        try:
            namespaces = list(self.catalog.list_namespaces())
            tables_info = []

            for ns in namespaces[:5]:  # Limit to first 5 namespaces
                ns_tuple = ns if isinstance(ns, tuple) else (ns,)
                try:
                    tables = list(self.catalog.list_tables(ns_tuple))
                    for table in tables[:3]:  # Limit to first 3 tables per namespace
                        table_id = f"{'.'.join(ns_tuple)}.{table}"
                        tables_info.append(table_id)
                except Exception:
                    continue
        except Exception:
            tables_info = []

        # Build prompt for LLM
        prompt = f"""You are a query planner for Apache Iceberg tables.

Available tables: {', '.join(tables_info) if tables_info else 'Unknown'}

User question: {question}

Suggest a query plan with:
1. Recommended table_id
2. Recommended columns to select
3. Recommended filters
4. Estimated result size
5. Performance considerations

Format your response as a structured plan."""

        try:
            # Call LLM
            if hasattr(self.llm, "invoke"):
                response = self.llm.invoke(prompt)
                if hasattr(response, "content"):
                    return response.content
                return str(response)
            elif hasattr(self.llm, "predict"):
                return self.llm.predict(prompt)
            else:
                return f"LLM planning not available. Question: {question}"
        except Exception as e:
            return f"LLM planning failed: {str(e)}. Falling back to basic planning."

    def _plan_without_llm(self, question: str) -> str:
        """Plan query without LLM (rule-based)."""
        question_lower = question.lower()

        plan = {
            "question": question,
            "suggestions": [],
        }

        # Basic keyword matching
        if "order" in question_lower:
            plan["suggestions"].append("Consider querying tables with 'order' in the name")
            plan["suggestions"].append("Common columns: order_id, customer_id, amount, order_date")

        if "revenue" in question_lower or "sales" in question_lower:
            plan["suggestions"].append("Consider aggregating amount or price columns")
            plan["suggestions"].append("Use SUM() aggregation for revenue calculations")

        if "date" in question_lower or "time" in question_lower or "quarter" in question_lower:
            plan["suggestions"].append("Filter by date columns to narrow results")
            plan["suggestions"].append("Consider using time-travel for historical comparisons")

        if "top" in question_lower or "highest" in question_lower:
            plan["suggestions"].append("Use ORDER BY with LIMIT for top N queries")

        # Format output
        output = [f"Query Plan for: {question}\n"]
        output.append("\nSuggestions:")
        for i, suggestion in enumerate(plan["suggestions"], 1):
            output.append(f"{i}. {suggestion}")

        if not plan["suggestions"]:
            output.append("No specific suggestions. Use iceberg_get_schema to explore table structure.")

        output.append("\nNext steps:")
        output.append("1. Use iceberg_list_tables to find relevant tables")
        output.append("2. Use iceberg_get_schema to understand table structure")
        output.append("3. Use iceberg_query to execute the query")

        return "\n".join(output)

