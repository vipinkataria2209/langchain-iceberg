"""Tests for snapshot and time-travel tools."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import pyarrow as pa

from langchain_iceberg.exceptions import (
    IcebergSnapshotNotFoundError,
    IcebergTableNotFoundError,
)
from langchain_iceberg.tools.snapshot_tools import SnapshotTool, TimeTravelTool


class TestSnapshotTool:
    """Test cases for SnapshotTool."""

    def test_list_snapshots_success(self):
        """Test successful snapshot listing."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_metadata = MagicMock()
        
        # Mock snapshot
        mock_snapshot = MagicMock()
        mock_snapshot.snapshot_id = 1234567890
        mock_snapshot.timestamp_ms = 1701388800000  # 2023-12-01 00:00:00
        mock_snapshot.operation = "append"
        mock_snapshot.parent_snapshot_id = None
        
        mock_metadata.current_snapshot.return_value = mock_snapshot
        mock_metadata.snapshots = [mock_snapshot]
        mock_table.metadata = mock_metadata
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = SnapshotTool(catalog=mock_catalog)
        result = tool.run({"table_id": "sales.orders"})
        
        assert "Snapshot ID: 1234567890" in result
        assert "2023-12-01" in result
        assert "append" in result

    def test_list_snapshots_no_snapshots(self):
        """Test snapshot listing when no snapshots exist."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_metadata = MagicMock()
        
        mock_metadata.current_snapshot.return_value = None
        mock_table.metadata = mock_metadata
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = SnapshotTool(catalog=mock_catalog)
        result = tool.run({"table_id": "sales.orders"})
        
        assert "No snapshots found" in result

    def test_list_snapshots_with_limit(self):
        """Test snapshot listing with custom limit."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_metadata = MagicMock()
        
        # Create multiple mock snapshots
        snapshots = []
        for i in range(5):
            mock_snap = MagicMock()
            mock_snap.snapshot_id = 1000 + i
            mock_snap.timestamp_ms = 1701388800000 + (i * 1000)
            mock_snap.operation = "append"
            snapshots.append(mock_snap)
        
        mock_metadata.current_snapshot.return_value = snapshots[-1]
        mock_metadata.snapshots = snapshots
        mock_table.metadata = mock_metadata
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = SnapshotTool(catalog=mock_catalog)
        result = tool.run({"table_id": "sales.orders", "limit": 3})
        
        assert "Snapshot ID: 1004" in result
        assert "(Showing 3 most recent snapshots)" in result

    def test_list_snapshots_table_not_found(self):
        """Test snapshot listing with non-existent table."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("Table not found")
        
        tool = SnapshotTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergTableNotFoundError):
            tool.run({"table_id": "sales.nonexistent"})

    def test_list_snapshots_invalid_table_id(self):
        """Test snapshot listing with invalid table_id format."""
        mock_catalog = MagicMock()
        tool = SnapshotTool(catalog=mock_catalog)
        
        with pytest.raises(Exception):  # Should raise validation error
            tool.run({"table_id": "invalid"})


class TestTimeTravelTool:
    """Test cases for TimeTravelTool."""

    def test_time_travel_by_timestamp(self):
        """Test time-travel query by timestamp."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        
        # Mock as_of_time (correct method name)
        mock_scan_builder.as_of_time.return_value = mock_scan_builder
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({"order_id": [1, 2]})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = TimeTravelTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "timestamp": "2024-12-01T00:00:00",
            "limit": 10
        })
        
        assert "order_id" in result
        # Verify as_of_time was called
        mock_scan_builder.as_of_time.assert_called_once()

    def test_time_travel_by_snapshot_id(self):
        """Test time-travel query by snapshot ID."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        
        # Mock use_snapshot (correct method name)
        mock_scan_builder.use_snapshot.return_value = mock_scan_builder
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({"order_id": [1, 2]})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = TimeTravelTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "snapshot_id": 1234567890,
            "limit": 10
        })
        
        assert "order_id" in result
        # Verify use_snapshot was called
        mock_scan_builder.use_snapshot.assert_called_once_with(1234567890)

    def test_time_travel_both_snapshot_and_timestamp(self):
        """Test time-travel with both snapshot_id and timestamp raises error."""
        mock_catalog = MagicMock()
        tool = TimeTravelTool(catalog=mock_catalog)
        
        from langchain_iceberg.exceptions import IcebergSnapshotNotFoundError
        with pytest.raises(IcebergSnapshotNotFoundError):
            tool.run({
                "table_id": "sales.orders",
                "snapshot_id": 1234567890,
                "timestamp": "2024-12-01T00:00:00"
            })

    def test_time_travel_neither_snapshot_nor_timestamp(self):
        """Test time-travel without snapshot_id or timestamp raises error."""
        mock_catalog = MagicMock()
        tool = TimeTravelTool(catalog=mock_catalog)
        
        from langchain_iceberg.exceptions import IcebergSnapshotNotFoundError
        with pytest.raises(IcebergSnapshotNotFoundError):
            tool.run({
                "table_id": "sales.orders",
                "limit": 10
            })

    def test_time_travel_with_columns(self):
        """Test time-travel query with column selection."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, StringType
        schema = Schema(
            NestedField(1, "order_id", LongType(), required=True),
            NestedField(2, "status", StringType(), required=False),
        )
        mock_table.schema.return_value = schema
        
        mock_scan = MagicMock()
        mock_scan_builder = MagicMock()
        mock_scan_builder.select.return_value = mock_scan_builder
        mock_scan_builder.filter.return_value = mock_scan_builder
        mock_scan_builder.limit.return_value = mock_scan
        mock_scan_builder.as_of_time.return_value = mock_scan_builder
        mock_table.scan.return_value = mock_scan_builder
        
        arrow_table = pa.table({"order_id": [1], "status": ["completed"]})
        mock_scan.to_arrow.return_value = arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        tool = TimeTravelTool(catalog=mock_catalog)
        result = tool.run({
            "table_id": "sales.orders",
            "timestamp": "2024-12-01T00:00:00",
            "columns": ["order_id", "status"],
            "limit": 10
        })
        
        assert "order_id" in result
        assert "status" in result
        mock_scan_builder.select.assert_called_once_with(["order_id", "status"])

    def test_time_travel_table_not_found(self):
        """Test time-travel with non-existent table."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("Table not found")
        
        tool = TimeTravelTool(catalog=mock_catalog)
        
        with pytest.raises(IcebergTableNotFoundError):
            tool.run({
                "table_id": "sales.nonexistent",
                "timestamp": "2024-12-01T00:00:00"
            })

    def test_time_travel_invalid_table_id(self):
        """Test time-travel with invalid table_id format."""
        mock_catalog = MagicMock()
        tool = TimeTravelTool(catalog=mock_catalog)
        
        with pytest.raises(Exception):  # Should raise validation error
            tool.run({
                "table_id": "invalid",
                "timestamp": "2024-12-01T00:00:00"
            })

