"""Utility functions for Goldfish."""

from datetime import datetime
from typing import Optional


def parse_datetime(value: str) -> datetime:
    """Parse an ISO format datetime string.

    Args:
        value: ISO format datetime string

    Returns:
        Parsed datetime object
    """
    return datetime.fromisoformat(value)


def parse_optional_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an optional ISO format datetime string.

    Safely handles None values, avoiding the common pattern:
        datetime.fromisoformat(x) if x else None

    Args:
        value: ISO format datetime string or None

    Returns:
        Parsed datetime object or None if value was None
    """
    if value is None:
        return None
    return datetime.fromisoformat(value)
