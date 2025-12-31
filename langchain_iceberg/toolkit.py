"""Main IcebergToolkit class for LangChain integration."""

from typing import Any, Dict, List, Optional

from langchain_core.tools import BaseTool
from pyiceberg.catalog import Catalog, load_catalog

from langchain_iceberg.exceptions import IcebergConnectionError
from langchain_iceberg.loaders.semantic_loader import SemanticLoader
from langchain_iceberg.tools.catalog_tools import (
    GetSchemaTool,
    ListNamespacesTool,
    ListTablesTool,
)
from langchain_iceberg.tools.query_planner_tool import QueryPlannerTool
from langchain_iceberg.tools.query_tools import QueryTool
from langchain_iceberg.tools.semantic_tools import MetricToolGenerator
from langchain_iceberg.tools.snapshot_tools import SnapshotTool, TimeTravelTool


class IcebergToolkit:
    """
    Main toolkit class for LangChain Iceberg integration.

    This toolkit provides tools for interacting with Apache Iceberg tables
    through the PyIceberg API, enabling natural language queries via LLM agents.

    Example:
        >>> from langchain_iceberg import IcebergToolkit
        >>> toolkit = IcebergToolkit(
        ...     catalog_name="prod",
        ...     catalog_config={
        ...         "type": "rest",
        ...         "uri": "http://localhost:8181",
        ...         "warehouse": "s3://my-warehouse"
        ...     }
        ... )
        >>> tools = toolkit.get_tools()
    """

    def __init__(
        self,
        catalog_name: str,
        catalog_config: Dict[str, Any],
        semantic_yaml: Optional[str] = None,
        llm: Optional[Any] = None,
        enable_time_travel: bool = True,
        enable_snapshots: bool = True,
        enable_sql_queries: bool = False,
    ) -> None:
        """
        Initialize the Iceberg toolkit.

        Args:
            catalog_name: Name of the catalog to use
            catalog_config: Configuration dictionary for the catalog.
                Must include:
                - "type": Catalog type ("rest", "hive", "glue", "nessie")
                - "uri": Catalog URI (for REST catalogs)
                - "warehouse": Warehouse location (S3, ADLS, GCS path)
                Additional config keys depend on catalog type.
            semantic_yaml: Optional path to semantic YAML file (local or S3/ADLS/GCS)
            llm: Optional LLM instance for query planning (not yet implemented)
            enable_time_travel: Enable time-travel tools (default: True)
            enable_snapshots: Enable snapshot tools (default: True)
            enable_sql_queries: Enable DuckDB SQL query tool for JOINs (default: False)

        Raises:
            IcebergConnectionError: If catalog connection fails
        """
        self.catalog_name = catalog_name
        self.catalog_config = catalog_config
        self.semantic_yaml = semantic_yaml
        self.llm = llm
        self.enable_time_travel = enable_time_travel
        self.enable_snapshots = enable_snapshots
        self.enable_sql_queries = enable_sql_queries

        # Initialize catalog connection
        try:
            self.catalog = self._initialize_catalog()
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to connect to Iceberg catalog '{catalog_name}': {str(e)}"
            ) from e

        # Validate connection by listing namespaces
        try:
            _ = list(self.catalog.list_namespaces())
        except Exception as e:
            raise IcebergConnectionError(
                f"Catalog connection validation failed: {str(e)}"
            ) from e

        # Load semantic layer if provided
        self.semantic_config = None
        if semantic_yaml:
            try:
                loader = SemanticLoader(semantic_yaml)
                self.semantic_config = loader.load()
            except Exception as e:
                from langchain_iceberg.exceptions import SemanticYAMLError
                raise SemanticYAMLError(
                    f"Failed to load semantic YAML: {str(e)}"
                ) from e

        # Set default query limits (can be overridden in catalog_config)
        self.query_timeout_seconds = self.catalog_config.get("query_timeout_seconds", 60)
        self.max_rows_per_query = self.catalog_config.get("max_rows_per_query", 10000)

    def _initialize_catalog(self) -> Catalog:
        """
        Initialize PyIceberg catalog from configuration.

        Returns:
            PyIceberg Catalog instance

        Raises:
            IcebergConnectionError: If catalog initialization fails
        """
        catalog_type = self.catalog_config.get("type")
        if not catalog_type:
            raise IcebergConnectionError(
                "catalog_config must include 'type' field"
            )

        # Extract warehouse location
        warehouse = self.catalog_config.get("warehouse")
        if not warehouse:
            raise IcebergConnectionError(
                "catalog_config must include 'warehouse' field"
            )

        # Build catalog properties
        properties = {
            "warehouse": warehouse,
        }

        # Add catalog-specific properties
        if catalog_type == "rest":
            uri = self.catalog_config.get("uri")
            if not uri:
                raise IcebergConnectionError(
                    "REST catalog requires 'uri' in catalog_config"
                )
            properties["uri"] = uri
        elif catalog_type == "hive":
            # Hive catalog may need additional config
            if "uri" in self.catalog_config:
                properties["uri"] = self.catalog_config["uri"]
        elif catalog_type == "glue":
            # Glue catalog may need AWS credentials
            if "region" in self.catalog_config:
                properties["region"] = self.catalog_config["region"]
        elif catalog_type == "nessie":
            uri = self.catalog_config.get("uri")
            if not uri:
                raise IcebergConnectionError(
                    "Nessie catalog requires 'uri' in catalog_config"
                )
            properties["uri"] = uri
        elif catalog_type == "sql":
            uri = self.catalog_config.get("uri")
            if not uri:
                raise IcebergConnectionError(
                    "SQL catalog requires 'uri' in catalog_config"
                )
            properties["uri"] = uri
        elif catalog_type == "in-memory":
            # In-memory catalog doesn't need special config
            pass

        # Add any additional properties from config
        for key, value in self.catalog_config.items():
            if key not in ("type", "warehouse", "uri", "region"):
                properties[key] = value

        try:
            catalog = load_catalog(
                name=self.catalog_name,
                type=catalog_type,
                **properties,
            )
            return catalog
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to load catalog '{self.catalog_name}' of type '{catalog_type}': {str(e)}"
            ) from e

    def get_tools(self) -> List[BaseTool]:
        """
        Get all available tools for the toolkit.

        Returns:
            List of BaseTool instances
        """
        tools: List[BaseTool] = []

        # Core catalog tools
        tools.append(ListNamespacesTool(catalog=self.catalog))
        tools.append(ListTablesTool(catalog=self.catalog))
        tools.append(GetSchemaTool(catalog=self.catalog))

        # Query tools
        tools.append(
            QueryTool(
                catalog=self.catalog,
                query_timeout_seconds=self.query_timeout_seconds,
                max_rows_per_query=self.max_rows_per_query,
            )
        )

        # DuckDB SQL query tool (if enabled)
        if self.enable_sql_queries:
            try:
                from langchain_iceberg.tools.duckdb_tool import DuckDBQueryTool
                tools.append(
                    DuckDBQueryTool(
                        catalog=self.catalog,
                        catalog_config=self.catalog_config,
                        query_timeout_seconds=self.query_timeout_seconds,
                        max_rows_per_query=self.max_rows_per_query,
                    )
                )
            except ImportError:
                # DuckDB not installed - skip this tool
                pass

        # Query planner tool (if LLM provided)
        if self.llm:
            tools.append(QueryPlannerTool(catalog=self.catalog, llm=self.llm))

        # Time-travel tools (if enabled)
        if self.enable_snapshots:
            tools.append(SnapshotTool(catalog=self.catalog))
        if self.enable_time_travel:
            tools.append(TimeTravelTool(catalog=self.catalog))

        # Semantic layer tools (if YAML provided)
        if self.semantic_config:
            semantic_tools = MetricToolGenerator.generate_tools(
                catalog=self.catalog,
                semantic_config=self.semantic_config,
                enable_sql=self.enable_sql_queries,
                catalog_config=self.catalog_config,
            )
            tools.extend(semantic_tools)

        return tools

    def get_context(self) -> Dict[str, Any]:
        """
        Get context information for prompts.

        Returns:
            Dictionary with context information
        """
        context = {
            "catalog_name": self.catalog_name,
            "catalog_type": self.catalog_config.get("type"),
            "warehouse": self.catalog_config.get("warehouse"),
            "has_semantic_layer": self.semantic_config is not None,
        }

        # Add namespace and table information if available
        try:
            namespaces = list(self.catalog.list_namespaces())
            context["namespaces"] = [
                ".".join(ns) if isinstance(ns, tuple) else str(ns)
                for ns in namespaces
            ]
        except Exception:
            context["namespaces"] = []

        return context

