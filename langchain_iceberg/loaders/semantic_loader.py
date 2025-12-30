"""Semantic layer YAML loader and parser."""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from langchain_iceberg.exceptions import SemanticYAMLError


class SemanticLoader:
    """Loads and parses semantic YAML configuration."""

    def __init__(self, yaml_path: str):
        """
        Initialize semantic loader.

        Args:
            yaml_path: Path to YAML file (local file path or S3/ADLS/GCS URI)
        """
        self.yaml_path = yaml_path
        self.config: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        """
        Load and parse YAML configuration.

        Returns:
            Parsed configuration dictionary

        Raises:
            SemanticYAMLError: If YAML is invalid or cannot be loaded
        """
        try:
            # Load YAML file
            if self.yaml_path.startswith(("s3://", "s3a://", "s3n://")):
                content = self._load_from_s3()
            elif self.yaml_path.startswith(("adl://", "adls://", "abfss://")):
                content = self._load_from_adls()
            elif self.yaml_path.startswith("gs://"):
                content = self._load_from_gcs()
            else:
                # Local file
                with open(self.yaml_path, "r", encoding="utf-8") as f:
                    content = f.read()

            # Parse YAML
            try:
                config = yaml.safe_load(content)
            except yaml.YAMLError as e:
                raise SemanticYAMLError(f"Invalid YAML syntax: {str(e)}") from e

            if not isinstance(config, dict):
                raise SemanticYAMLError("YAML must contain a dictionary/object at root level")

            # Validate schema
            self.validate_schema(config)

            self.config = config
            return config

        except FileNotFoundError:
            raise SemanticYAMLError(f"YAML file not found: {self.yaml_path}")
        except Exception as e:
            if isinstance(e, SemanticYAMLError):
                raise
            raise SemanticYAMLError(f"Failed to load YAML: {str(e)}") from e

    def _load_from_s3(self) -> str:
        """Load YAML from S3 (placeholder - requires boto3)."""
        # TODO: Implement S3 loading with boto3
        raise SemanticYAMLError("S3 loading not yet implemented. Use local file path.")

    def _load_from_adls(self) -> str:
        """Load YAML from Azure Data Lake Storage (placeholder)."""
        # TODO: Implement ADLS loading
        raise SemanticYAMLError("ADLS loading not yet implemented. Use local file path.")

    def _load_from_gcs(self) -> str:
        """Load YAML from Google Cloud Storage (placeholder)."""
        # TODO: Implement GCS loading
        raise SemanticYAMLError("GCS loading not yet implemented. Use local file path.")

    def validate_schema(self, config: Dict[str, Any]) -> bool:
        """
        Validate YAML schema against expected structure.

        Args:
            config: Configuration dictionary to validate

        Returns:
            True if valid

        Raises:
            SemanticYAMLError: If schema is invalid
        """
        # Check required top-level fields
        if "version" not in config:
            raise SemanticYAMLError("Missing required field: version")
        version = config.get("version")
        # Handle both string and numeric versions
        if str(version) != "1.0":
            raise SemanticYAMLError(f"Unsupported version: {version}. Expected: 1.0")

        if "catalog" not in config:
            raise SemanticYAMLError("Missing required field: catalog")
        if "warehouse" not in config:
            raise SemanticYAMLError("Missing required field: warehouse")

        # Validate tables
        if "tables" in config:
            if not isinstance(config["tables"], list):
                raise SemanticYAMLError("'tables' must be a list")
            for i, table in enumerate(config["tables"]):
                if not isinstance(table, dict):
                    raise SemanticYAMLError(f"Table at index {i} must be a dictionary")
                if "name" not in table:
                    raise SemanticYAMLError(f"Table at index {i} missing 'name' field")
                if "namespace" not in table:
                    raise SemanticYAMLError(f"Table at index {i} missing 'namespace' field")

        # Validate metrics
        if "metrics" in config:
            if not isinstance(config["metrics"], list):
                raise SemanticYAMLError("'metrics' must be a list")

            metric_names = []
            for i, metric in enumerate(config["metrics"]):
                if not isinstance(metric, dict):
                    raise SemanticYAMLError(f"Metric at index {i} must be a dictionary")
                if "name" not in metric:
                    raise SemanticYAMLError(f"Metric at index {i} missing 'name' field")
                if "description" not in metric:
                    raise SemanticYAMLError(f"Metric at index {i} missing 'description' field")
                if "type" not in metric:
                    raise SemanticYAMLError(f"Metric at index {i} missing 'type' field")

                metric_name = metric["name"]
                if metric_name in metric_names:
                    raise SemanticYAMLError(f"Duplicate metric name: {metric_name}")
                metric_names.append(metric_name)

                # Validate metric type
                valid_types = ["sum", "count", "avg", "min", "max", "calculated"]
                if metric["type"] not in valid_types:
                    raise SemanticYAMLError(
                        f"Invalid metric type '{metric['type']}' for metric '{metric_name}'. "
                        f"Valid types: {valid_types}"
                    )

                # Validate expression or formula
                if "expression" not in metric and "formula" not in metric:
                    raise SemanticYAMLError(
                        f"Metric '{metric_name}' must have either 'expression' or 'formula'"
                    )

                if "expression" in metric:
                    expr = metric["expression"]
                    if not isinstance(expr, dict):
                        raise SemanticYAMLError(f"Metric '{metric_name}' expression must be a dictionary")
                    if "table" not in expr:
                        raise SemanticYAMLError(f"Metric '{metric_name}' expression missing 'table'")
                    if "aggregation" not in expr:
                        raise SemanticYAMLError(f"Metric '{metric_name}' expression missing 'aggregation'")
                    if "column" not in expr and expr.get("aggregation") != "count":
                        raise SemanticYAMLError(f"Metric '{metric_name}' expression missing 'column'")

        # Validate dimensions
        if "dimensions" in config:
            if not isinstance(config["dimensions"], list):
                raise SemanticYAMLError("'dimensions' must be a list")

            dimension_names = []
            for i, dimension in enumerate(config["dimensions"]):
                if not isinstance(dimension, dict):
                    raise SemanticYAMLError(f"Dimension at index {i} must be a dictionary")
                if "name" not in dimension:
                    raise SemanticYAMLError(f"Dimension at index {i} missing 'name' field")

                dimension_name = dimension["name"]
                if dimension_name in dimension_names:
                    raise SemanticYAMLError(f"Duplicate dimension name: {dimension_name}")
                dimension_names.append(dimension_name)

                # Validate dimension type
                valid_types = ["categorical", "datetime", "numeric"]
                if "type" in dimension:
                    if dimension["type"] not in valid_types:
                        raise SemanticYAMLError(
                            f"Invalid dimension type '{dimension['type']}' for dimension '{dimension_name}'. "
                            f"Valid types: {valid_types}"
                        )

        return True

    def get_metrics(self) -> List[Dict[str, Any]]:
        """
        Get all metric definitions.

        Returns:
            List of metric dictionaries
        """
        if self.config is None:
            self.load()
        return self.config.get("metrics", [])

    def get_dimensions(self) -> List[Dict[str, Any]]:
        """
        Get all dimension definitions.

        Returns:
            List of dimension dictionaries
        """
        if self.config is None:
            self.load()
        return self.config.get("dimensions", [])

    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Get all table definitions.

        Returns:
            List of table dictionaries
        """
        if self.config is None:
            self.load()
        return self.config.get("tables", [])

    def get_governance(self) -> Dict[str, Any]:
        """
        Get governance configuration.

        Returns:
            Governance configuration dictionary
        """
        if self.config is None:
            self.load()
        return self.config.get("governance", {})

