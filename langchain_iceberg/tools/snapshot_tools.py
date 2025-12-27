"""Time-travel and snapshot tools for Iceberg."""

from typing import Any, List, Optional

import pandas as pd

from langchain_iceberg.exceptions import (
    IcebergSnapshotNotFoundError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.base import IcebergBaseTool
from langchain_iceberg.utils.formatters import ResultFormatter
from langchain_iceberg.utils.validators import validate_table_id


class SnapshotTool(IcebergBaseTool):
    """Tool for listing table snapshots."""

    name: str = "iceberg_snapshots"
    description: str = """
    Get the snapshot history of an Iceberg table.
    This enables time-travel queries to see data as it was in the past.
    
    Input: 
        table_id (required): Format "namespace.table_name"
        limit (optional): Maximum number of snapshots to return (default: 20)
    
    Output: List of snapshots with timestamps and operations
    
    Example usage:
    - iceberg_snapshots(table_id="sales.orders")
    - iceberg_snapshots(table_id="sales.orders", limit=10)
    """

    def _run(self, table_id: str, limit: int = 20, **kwargs: Any) -> str:
        """Execute the tool."""
        try:
            namespace, table_name = validate_table_id(table_id)
            
            # Load table
            try:
                namespace_tuple = tuple(namespace.split("."))
                table = self.catalog.load_table((*namespace_tuple, table_name))
            except Exception as e:
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    raise IcebergTableNotFoundError(
                        f"Table '{table_id}' not found: {str(e)}"
                    ) from e
                raise
            
            # Get snapshots
            try:
                snapshots = []
                current_snapshot = table.metadata.current_snapshot()
                
                if current_snapshot is None:
                    return "No snapshots found for this table."
                
                # Walk through snapshot history
                snapshot = current_snapshot
                count = 0
                
                while snapshot is not None and count < limit:
                    snapshot_info = {
                        "id": snapshot.snapshot_id,
                        "timestamp": snapshot.timestamp_ms,
                        "operation": snapshot.operation if hasattr(snapshot, 'operation') else "unknown",
                    }
                    
                    # Format timestamp
                    from datetime import datetime
                    timestamp_dt = datetime.fromtimestamp(snapshot.timestamp_ms / 1000.0)
                    snapshot_info["timestamp_formatted"] = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
                    
                    snapshots.append(snapshot_info)
                    
                    # Get parent snapshot if available
                    if hasattr(snapshot, 'parent_snapshot_id') and snapshot.parent_snapshot_id:
                        # For now, we'll just get the current snapshot
                        # Full history traversal would require metadata access
                        break
                    
                    count += 1
                    if count >= limit:
                        break
                
                # If we have metadata access, get all snapshots
                try:
                    metadata = table.metadata
                    if hasattr(metadata, 'snapshots'):
                        all_snapshots = metadata.snapshots
                        snapshots = []
                        for snap in all_snapshots[-limit:]:  # Get most recent
                            from datetime import datetime
                            timestamp_dt = datetime.fromtimestamp(snap.timestamp_ms / 1000.0)
                            snapshots.append({
                                "id": snap.snapshot_id,
                                "timestamp": snap.timestamp_ms,
                                "timestamp_formatted": timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "operation": snap.operation if hasattr(snap, 'operation') else "unknown",
                            })
                except Exception:
                    # Fallback to current snapshot only
                    pass
                
                if not snapshots:
                    return "No snapshots found for this table."
                
                # Format output
                output = [f"Snapshots for {table_id}:\n"]
                for snap in snapshots:
                    output.append(f"\nSnapshot ID: {snap['id']}")
                    output.append(f"Timestamp: {snap['timestamp_formatted']}")
                    output.append(f"Operation: {snap['operation']}")
                
                if len(snapshots) >= limit:
                    output.append(f"\n(Showing {limit} most recent snapshots)")
                
                return "\n".join(output)
                
            except Exception as e:
                raise IcebergSnapshotNotFoundError(
                    f"Failed to retrieve snapshots for table '{table_id}': {str(e)}"
                ) from e
                
        except (IcebergTableNotFoundError, IcebergSnapshotNotFoundError):
            raise
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError
            raise IcebergConnectionError(
                f"Failed to get snapshots for table '{table_id}': {str(e)}"
            ) from e


class TimeTravelTool(IcebergBaseTool):
    """Tool for querying historical data using time-travel."""

    name: str = "iceberg_time_travel"
    description: str = """
    Query an Iceberg table as it existed at a specific snapshot or timestamp.
    
    Inputs:
        table_id (required): Format "namespace.table_name"
        snapshot_id (optional): Specific snapshot ID to query
        timestamp (optional): ISO 8601 timestamp (e.g., "2024-12-01T00:00:00")
        columns (optional): List of columns to select (default: all)
        filters (optional): Filter expression
        limit (optional): Max rows to return (default: 100)
        
    Note: Must provide either snapshot_id OR timestamp (not both)
    
    Example usage:
    - iceberg_time_travel(table_id="sales.orders", timestamp="2024-12-01T00:00:00")
    - iceberg_time_travel(table_id="sales.orders", snapshot_id=1234567890, limit=50)
    """

    def _run(
        self,
        table_id: str,
        snapshot_id: Optional[int] = None,
        timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> str:
        """Execute the tool."""
        try:
            namespace, table_name = validate_table_id(table_id)
            
            # Validate that exactly one of snapshot_id or timestamp is provided
            if snapshot_id is None and timestamp is None:
                raise IcebergSnapshotNotFoundError(
                    "Must provide either snapshot_id or timestamp"
                )
            if snapshot_id is not None and timestamp is not None:
                raise IcebergSnapshotNotFoundError(
                    "Cannot provide both snapshot_id and timestamp. Choose one."
                )
            
            # Load table
            try:
                namespace_tuple = tuple(namespace.split("."))
                table = self.catalog.load_table((*namespace_tuple, table_name))
            except Exception as e:
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    raise IcebergTableNotFoundError(
                        f"Table '{table_id}' not found: {str(e)}"
                    ) from e
                raise
            
            # Build scan with time-travel
            scan_builder = table.scan()
            
            # Apply time-travel
            if snapshot_id is not None:
                # Query at specific snapshot
                try:
                    scan_builder = scan_builder.use_snapshot(snapshot_id)
                except Exception as e:
                    raise IcebergSnapshotNotFoundError(
                        f"Snapshot {snapshot_id} not found: {str(e)}"
                    ) from e
            elif timestamp is not None:
                # Query at specific timestamp
                try:
                    from datetime import datetime
                    # Parse ISO 8601 timestamp
                    if 'T' in timestamp:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    else:
                        dt = datetime.fromisoformat(timestamp)
                    timestamp_ms = int(dt.timestamp() * 1000)
                    scan_builder = scan_builder.as_of_time(timestamp_ms)
                except ValueError as e:
                    raise IcebergSnapshotNotFoundError(
                        f"Invalid timestamp format '{timestamp}': {str(e)}. Use ISO 8601 format (e.g., '2024-12-01T00:00:00')"
                    ) from e
                except Exception as e:
                    raise IcebergSnapshotNotFoundError(
                        f"Failed to query at timestamp '{timestamp}': {str(e)}"
                    ) from e
            
            # Apply column selection
            if columns:
                schema = table.schema()
                schema_columns = {field.name for field in schema.fields}
                invalid_columns = set(columns) - schema_columns
                if invalid_columns:
                    from langchain_iceberg.exceptions import IcebergInvalidQueryError
                    raise IcebergInvalidQueryError(
                        f"Invalid columns: {invalid_columns}. "
                        f"Available columns: {sorted(schema_columns)}"
                    )
                scan_builder = scan_builder.select(columns)
            
            # Apply filters
            if filters:
                from langchain_iceberg.utils.filters import FilterBuilder
                try:
                    filter_expr = FilterBuilder.parse_filter(filters, table.schema())
                    if filter_expr is not None:
                        scan_builder = scan_builder.filter(filter_expr)
                except Exception as e:
                    from langchain_iceberg.exceptions import IcebergInvalidFilterError
                    raise IcebergInvalidFilterError(
                        f"Failed to apply filter '{filters}': {str(e)}"
                    ) from e
            
            # Apply limit
            scan_builder = scan_builder.limit(limit)
            
            # Execute scan
            scan = scan_builder
            arrow_table = scan.to_arrow()
            
            # Convert to pandas
            if arrow_table and len(arrow_table) > 0:
                df = arrow_table.to_pandas()
            else:
                df = pd.DataFrame()
            
            # Format results with time-travel context
            time_context = ""
            if snapshot_id:
                time_context = f" (snapshot {snapshot_id})"
            elif timestamp:
                time_context = f" (as of {timestamp})"
            
            result = ResultFormatter.format_table(df, limit=limit)
            return f"Time Travel Results{time_context}:\n{result}"
            
        except (
            IcebergTableNotFoundError,
            IcebergSnapshotNotFoundError,
            IcebergInvalidQueryError,
            IcebergInvalidFilterError,
        ):
            raise
        except Exception as e:
            from langchain_iceberg.exceptions import IcebergConnectionError
            raise IcebergConnectionError(
                f"Failed to execute time-travel query on table '{table_id}': {str(e)}"
            ) from e

