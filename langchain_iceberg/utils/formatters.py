"""Result formatting utilities."""

from typing import Any, Optional

import pandas as pd


class ResultFormatter:
    """Formats query results for display."""

    @staticmethod
    def format_table(
        df: pd.DataFrame,
        limit: Optional[int] = None,
        max_rows_display: int = 100,
    ) -> str:
        """
        Format a DataFrame as a readable table string.

        Args:
            df: DataFrame to format
            limit: Optional limit that was applied
            max_rows_display: Maximum rows to display in output

        Returns:
            Formatted string representation
        """
        if df.empty:
            return "No results found."

        # Truncate if too many rows
        display_df = df.head(max_rows_display)

        # Format the DataFrame
        output = []
        output.append(f"Query Results ({len(df)} rows):\n")

        # Convert to string representation
        table_str = display_df.to_string(index=False, max_rows=max_rows_display)
        output.append(table_str)

        # Add note if truncated
        if len(df) > max_rows_display:
            output.append(
                f"\n(Showing first {max_rows_display} rows. "
                f"Use limit parameter to see more.)"
            )
        elif limit and len(df) >= limit:
            output.append(f"\n(Limited to {limit} rows. Use limit parameter for more.)")

        return "\n".join(output)

    @staticmethod
    def format_schema(
        columns: list[dict],
        partitions: Optional[list[str]] = None,
        sample_data: Optional[pd.DataFrame] = None,
    ) -> str:
        """
        Format schema information for display.

        Args:
            columns: List of column definitions with 'name' and 'type'
            partitions: Optional list of partition column names
            sample_data: Optional sample DataFrame

        Returns:
            Formatted schema string
        """
        output = []
        output.append("Schema:\n")
        output.append("\nColumns:")

        for col in columns:
            col_name = col.get("name", "unknown")
            col_type = col.get("type", "unknown")
            output.append(f"  - {col_name}: {col_type}")

        if partitions:
            output.append("\nPartition Columns:")
            for part in partitions:
                output.append(f"  - {part}")

        if sample_data is not None and not sample_data.empty:
            output.append("\nSample Rows (3):")
            sample_str = sample_data.head(3).to_string(index=False)
            output.append(sample_str)

        return "\n".join(output)

    @staticmethod
    def format_list(items: list[str], title: str = "Items") -> str:
        """
        Format a list of items for display.

        Args:
            items: List of item strings
            title: Title for the list

        Returns:
            Formatted list string
        """
        if not items:
            return f"{title}: (empty)"

        output = [f"{title}:"]
        for item in items:
            output.append(f"  - {item}")

        return "\n".join(output)

    @staticmethod
    def format_snapshots(snapshots: list[dict]) -> str:
        """
        Format snapshot list for display.

        Args:
            snapshots: List of snapshot dictionaries with 'id', 'timestamp', 'operation'

        Returns:
            Formatted snapshots string
        """
        if not snapshots:
            return "No snapshots found."

        output = ["Snapshots:"]
        for snap in snapshots:
            snap_id = snap.get("id", "unknown")
            timestamp = snap.get("timestamp", "unknown")
            operation = snap.get("operation", "unknown")
            output.append(f"\nSnapshot ID: {snap_id}")
            output.append(f"Timestamp: {timestamp}")
            output.append(f"Operation: {operation}")

        return "\n".join(output)

