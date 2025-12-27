"""Governance utilities: access control, PII masking, audit logging."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class AccessControlValidator:
    """Validates access control based on roles and permissions."""

    def __init__(self, governance_config: Dict[str, Any]):
        """
        Initialize access control validator.
        
        Args:
            governance_config: Governance configuration from semantic YAML
        """
        self.governance_config = governance_config
        self.roles_config = governance_config.get("roles", [])

    def can_access_metric(self, metric_name: str, user_role: Optional[str] = None) -> bool:
        """
        Check if user can access a metric.
        
        Args:
            metric_name: Name of the metric
            user_role: User's role (optional)
            
        Returns:
            True if access is allowed
        """
        # If no roles configured, allow all
        if not self.roles_config:
            return True

        # If no user role, check default permissions
        if not user_role:
            # Check if metric is explicitly hidden
            for role_config in self.roles_config:
                cannot_access = role_config.get("cannot_access_metrics", [])
                if metric_name in cannot_access:
                    return False
            return True

        # Check role-specific permissions
        for role_config in self.roles_config:
            if role_config.get("name") == user_role:
                can_access = role_config.get("can_access_metrics", [])
                cannot_access = role_config.get("cannot_access_metrics", [])

                # Explicit deny takes precedence
                if metric_name in cannot_access:
                    return False

                # If can_access is specified, only allow those
                if can_access:
                    return metric_name in can_access

                # Otherwise allow (unless in cannot_access)
                return True

        # Role not found - default deny
        return False

    def can_access_table(self, table_id: str, user_role: Optional[str] = None) -> bool:
        """
        Check if user can access a table.
        
        Args:
            table_id: Table identifier
            user_role: User's role (optional)
            
        Returns:
            True if access is allowed
        """
        # For now, allow all table access
        # Can be extended with table-level permissions
        return True


class PIIMasker:
    """Masks PII columns in query results."""

    def __init__(self, governance_config: Dict[str, Any]):
        """
        Initialize PII masker.
        
        Args:
            governance_config: Governance configuration from semantic YAML
        """
        self.governance_config = governance_config
        self.pii_policy = governance_config.get("pii_policy", {})
        self.mask_by_default = self.pii_policy.get("mask_by_default", True)
        self.allowed_roles = self.pii_policy.get("allowed_roles", [])
        self.mask_char = self.pii_policy.get("mask_char", "*")

    def should_mask_column(self, column_name: str, user_role: Optional[str] = None) -> bool:
        """
        Check if a column should be masked.
        
        Args:
            column_name: Name of the column
            user_role: User's role (optional)
            
        Returns:
            True if column should be masked
        """
        # Check if user role is allowed to see PII
        if user_role and user_role in self.allowed_roles:
            return False

        # Check table configuration for PII columns
        # This would be populated from semantic YAML table definitions
        # For now, use mask_by_default
        return self.mask_by_default

    def mask_value(self, value: Any) -> str:
        """
        Mask a PII value.
        
        Args:
            value: Value to mask
            
        Returns:
            Masked string
        """
        if value is None:
            return "***"

        value_str = str(value)
        if len(value_str) <= 3:
            return self.mask_char * len(value_str)

        # Mask middle characters, keep first and last
        if len(value_str) > 4:
            return value_str[0] + self.mask_char * (len(value_str) - 2) + value_str[-1]
        else:
            return self.mask_char * len(value_str)

    def mask_dataframe(self, df: Any, pii_columns: List[str], user_role: Optional[str] = None) -> Any:
        """
        Mask PII columns in a DataFrame.
        
        Args:
            df: Pandas DataFrame
            pii_columns: List of column names that contain PII
            user_role: User's role (optional)
            
        Returns:
            DataFrame with masked PII columns
        """
        import pandas as pd

        if df is None or df.empty:
            return df

        masked_df = df.copy()

        for col in pii_columns:
            if col in masked_df.columns:
                if self.should_mask_column(col, user_role):
                    masked_df[col] = masked_df[col].apply(self.mask_value)

        return masked_df


class AuditLogger:
    """Logs queries and operations for audit purposes."""

    def __init__(self, governance_config: Dict[str, Any]):
        """
        Initialize audit logger.
        
        Args:
            governance_config: Governance configuration from semantic YAML
        """
        self.governance_config = governance_config
        self.audit_config = governance_config.get("audit", {})
        self.enabled = self.audit_config.get("enabled", False)
        self.log_location = self.audit_config.get("log_location", None)

        # Set up logging
        if self.enabled:
            self._setup_logging()

    def _setup_logging(self):
        """Set up audit logging."""
        audit_logger = logging.getLogger("langchain_iceberg.audit")
        audit_logger.setLevel(logging.INFO)

        # Add file handler if log_location specified
        if self.log_location:
            # For S3/cloud storage, would need specific handlers
            # For now, use local file logging
            if not self.log_location.startswith(("s3://", "adl://", "gs://")):
                handler = logging.FileHandler(self.log_location)
                handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                    )
                )
                audit_logger.addHandler(handler)

    def log_query(
        self,
        operation: str,
        table_id: Optional[str] = None,
        user: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        result_count: Optional[int] = None,
        execution_time_ms: Optional[float] = None,
    ):
        """
        Log a query operation.
        
        Args:
            operation: Operation name (e.g., "query", "get_schema", "time_travel")
            table_id: Table identifier
            user: User identifier
            query_params: Query parameters
            result_count: Number of results returned
            execution_time_ms: Execution time in milliseconds
        """
        if not self.enabled:
            return

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "operation": operation,
            "table_id": table_id,
            "user": user,
            "query_params": query_params,
            "result_count": result_count,
            "execution_time_ms": execution_time_ms,
        }

        audit_logger = logging.getLogger("langchain_iceberg.audit")
        audit_logger.info(json.dumps(log_entry))

