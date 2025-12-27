"""Date range parsing utilities for semantic layer."""

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple


class DateRangeParser:
    """Parser for date range strings in various formats."""

    @staticmethod
    def parse(date_range: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Parse a date range string into start and end datetimes.
        
        Supports formats:
        - "Q4_2024" - Quarter
        - "last_30_days" - Relative days
        - "2024-01-01:2024-12-31" - Date range
        - "2024-12-01" - Single date
        - "this_month", "last_month", "this_quarter", "last_quarter", "this_year", "last_year"
        
        Args:
            date_range: Date range string
            
        Returns:
            Tuple of (start_datetime, end_datetime). Either can be None.
            
        Raises:
            ValueError: If date range cannot be parsed
        """
        date_range = date_range.strip().lower()

        # Quarter format: Q4_2024
        quarter_match = re.match(r"q([1-4])_(\d{4})", date_range)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year = int(quarter_match.group(2))
            start_month = (quarter - 1) * 3 + 1
            end_month = quarter * 3
            
            start = datetime(year, start_month, 1)
            if end_month == 12:
                end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
            else:
                end = datetime(year, end_month + 1, 1) - timedelta(seconds=1)
            
            return start, end

        # Relative days: last_30_days, last_7_days, etc.
        relative_days_match = re.match(r"last_(\d+)_days", date_range)
        if relative_days_match:
            days = int(relative_days_match.group(1))
            end = datetime.now()
            start = end - timedelta(days=days)
            return start, end

        # Date range: 2024-01-01:2024-12-31
        if ":" in date_range:
            parts = date_range.split(":")
            if len(parts) == 2:
                try:
                    start = datetime.fromisoformat(parts[0].strip())
                    end = datetime.fromisoformat(parts[1].strip())
                    # Set end to end of day
                    end = end.replace(hour=23, minute=59, second=59)
                    return start, end
                except ValueError as e:
                    raise ValueError(f"Invalid date range format: {date_range}") from e

        # Single date: 2024-12-01
        try:
            single_date = datetime.fromisoformat(date_range)
            # Return as start of day to end of day
            start = single_date.replace(hour=0, minute=0, second=0)
            end = single_date.replace(hour=23, minute=59, second=59)
            return start, end
        except ValueError:
            pass

        # Relative periods
        now = datetime.now()
        
        if date_range == "this_month":
            start = now.replace(day=1, hour=0, minute=0, second=0)
            end = now
            return start, end
        
        elif date_range == "last_month":
            if now.month == 1:
                start = datetime(now.year - 1, 12, 1)
                end = datetime(now.year, 1, 1) - timedelta(seconds=1)
            else:
                start = datetime(now.year, now.month - 1, 1)
                end = datetime(now.year, now.month, 1) - timedelta(seconds=1)
            return start, end
        
        elif date_range == "this_quarter":
            current_quarter = (now.month - 1) // 3 + 1
            start_month = (current_quarter - 1) * 3 + 1
            start = datetime(now.year, start_month, 1)
            end = now
            return start, end
        
        elif date_range == "last_quarter":
            current_quarter = (now.month - 1) // 3 + 1
            if current_quarter == 1:
                quarter = 4
                year = now.year - 1
            else:
                quarter = current_quarter - 1
                year = now.year
            start_month = (quarter - 1) * 3 + 1
            end_month = quarter * 3
            start = datetime(year, start_month, 1)
            if end_month == 12:
                end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
            else:
                end = datetime(year, end_month + 1, 1) - timedelta(seconds=1)
            return start, end
        
        elif date_range == "this_year":
            start = datetime(now.year, 1, 1)
            end = now
            return start, end
        
        elif date_range == "last_year":
            start = datetime(now.year - 1, 1, 1)
            end = datetime(now.year, 1, 1) - timedelta(seconds=1)
            return start, end

        # If we get here, couldn't parse
        raise ValueError(
            f"Could not parse date range: {date_range}. "
            f"Supported formats: Q4_2024, last_30_days, 2024-01-01:2024-12-31, "
            f"this_month, last_month, this_quarter, last_quarter, this_year, last_year"
        )

    @staticmethod
    def format_for_filter(start: datetime, end: datetime, column_name: str) -> str:
        """
        Format date range as a filter expression.
        
        Args:
            start: Start datetime
            end: End datetime
            column_name: Name of date column
            
        Returns:
            Filter expression string
        """
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
        return f"{column_name} >= '{start_str}' AND {column_name} <= '{end_str}'"

