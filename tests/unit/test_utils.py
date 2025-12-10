"""Comprehensive tests for utils.py module - P2.

Tests cover:
- DateTime parsing from ISO strings
- Optional DateTime handling
- Error handling for invalid formats
- Round-trip conversion consistency
"""

from datetime import UTC, datetime

import pytest


class TestParseDatetime:
    """Tests for parse_datetime() function."""

    def test_parse_datetime_valid_iso_string(self):
        """Test parsing a valid ISO format datetime string."""
        from goldfish.utils import parse_datetime

        # Test with timezone-aware datetime
        dt_str = "2025-12-05T14:30:00+00:00"
        result = parse_datetime(dt_str)

        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 5
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0

    def test_parse_datetime_with_microseconds(self):
        """Test parsing ISO string with microseconds."""
        from goldfish.utils import parse_datetime

        dt_str = "2025-12-05T14:30:00.123456+00:00"
        result = parse_datetime(dt_str)

        assert result.microsecond == 123456

    def test_parse_datetime_utc_format(self):
        """Test parsing UTC datetime with Z suffix."""
        from goldfish.utils import parse_datetime

        # ISO 8601 allows Z for UTC
        dt_str = "2025-12-05T14:30:00Z"
        result = parse_datetime(dt_str)

        assert isinstance(result, datetime)
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_datetime_without_timezone(self):
        """Test parsing datetime without timezone info."""
        from goldfish.utils import parse_datetime

        dt_str = "2025-12-05T14:30:00"
        result = parse_datetime(dt_str)

        assert isinstance(result, datetime)
        assert result.tzinfo is None

    def test_parse_datetime_different_timezones(self):
        """Test parsing datetime with various timezone offsets."""
        from goldfish.utils import parse_datetime

        # Positive offset
        dt_str1 = "2025-12-05T14:30:00+05:30"
        result1 = parse_datetime(dt_str1)
        assert result1.tzinfo is not None

        # Negative offset
        dt_str2 = "2025-12-05T14:30:00-08:00"
        result2 = parse_datetime(dt_str2)
        assert result2.tzinfo is not None

    def test_parse_datetime_invalid_format_raises_error(self):
        """Test that invalid datetime format raises ValueError."""
        from goldfish.utils import parse_datetime

        invalid_strings = [
            "not-a-date",
            "2025-13-01T00:00:00",  # Invalid month
            "2025-12-32T00:00:00",  # Invalid day
            "",  # Empty string
            "12/05/2025",  # Wrong format (US date)
        ]

        for invalid_str in invalid_strings:
            with pytest.raises(ValueError):
                parse_datetime(invalid_str)

    def test_parse_datetime_empty_string_raises_error(self):
        """Test that empty string raises error."""
        from goldfish.utils import parse_datetime

        with pytest.raises(ValueError):
            parse_datetime("")


class TestParseOptionalDatetime:
    """Tests for parse_optional_datetime() function."""

    def test_parse_optional_datetime_none_returns_none(self):
        """Test that None input returns None."""
        from goldfish.utils import parse_optional_datetime

        result = parse_optional_datetime(None)

        assert result is None

    def test_parse_optional_datetime_valid_string(self):
        """Test parsing valid datetime string."""
        from goldfish.utils import parse_optional_datetime

        dt_str = "2025-12-05T14:30:00+00:00"
        result = parse_optional_datetime(dt_str)

        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 5

    def test_parse_optional_datetime_with_microseconds(self):
        """Test that microseconds are preserved."""
        from goldfish.utils import parse_optional_datetime

        dt_str = "2025-12-05T14:30:00.999999+00:00"
        result = parse_optional_datetime(dt_str)

        assert result.microsecond == 999999

    def test_parse_optional_datetime_invalid_raises_error(self):
        """Test that invalid string still raises error."""
        from goldfish.utils import parse_optional_datetime

        with pytest.raises(ValueError):
            parse_optional_datetime("not-a-valid-datetime")

    def test_parse_optional_datetime_empty_string_raises_error(self):
        """Test that empty string raises error (not treated as None)."""
        from goldfish.utils import parse_optional_datetime

        with pytest.raises(ValueError):
            parse_optional_datetime("")


class TestDatetimeRoundTrip:
    """Tests for datetime conversion consistency."""

    def test_datetime_round_trip_with_timezone(self):
        """Test that datetime -> string -> datetime preserves value."""
        from goldfish.utils import parse_datetime

        # Create a datetime
        original = datetime(2025, 12, 5, 14, 30, 0, tzinfo=UTC)

        # Convert to ISO string
        iso_string = original.isoformat()

        # Parse back
        result = parse_datetime(iso_string)

        # Should be equal
        assert result == original

    def test_datetime_round_trip_with_microseconds(self):
        """Test round-trip with microsecond precision."""
        from goldfish.utils import parse_datetime

        original = datetime(2025, 12, 5, 14, 30, 0, 123456, tzinfo=UTC)
        iso_string = original.isoformat()
        result = parse_datetime(iso_string)

        assert result == original
        assert result.microsecond == original.microsecond

    def test_datetime_round_trip_without_timezone(self):
        """Test round-trip for naive datetime."""
        from goldfish.utils import parse_datetime

        original = datetime(2025, 12, 5, 14, 30, 0)
        iso_string = original.isoformat()
        result = parse_datetime(iso_string)

        assert result == original
        assert result.tzinfo is None

    def test_optional_datetime_round_trip_none(self):
        """Test that None round-trips correctly."""
        from goldfish.utils import parse_optional_datetime

        result = parse_optional_datetime(None)
        assert result is None

    def test_optional_datetime_round_trip_with_value(self):
        """Test that optional datetime round-trips with valid value."""
        from goldfish.utils import parse_optional_datetime

        original = datetime(2025, 12, 5, 14, 30, 0, tzinfo=UTC)
        iso_string = original.isoformat()
        result = parse_optional_datetime(iso_string)

        assert result == original


class TestDatetimeEdgeCases:
    """Tests for edge cases in datetime parsing."""

    def test_parse_datetime_leap_year(self):
        """Test parsing February 29 in a leap year."""
        from goldfish.utils import parse_datetime

        # 2024 is a leap year
        dt_str = "2024-02-29T12:00:00+00:00"
        result = parse_datetime(dt_str)

        assert result.year == 2024
        assert result.month == 2
        assert result.day == 29

    def test_parse_datetime_year_boundaries(self):
        """Test parsing dates at year boundaries."""
        from goldfish.utils import parse_datetime

        # First day of year
        dt_str1 = "2025-01-01T00:00:00+00:00"
        result1 = parse_datetime(dt_str1)
        assert result1.month == 1
        assert result1.day == 1

        # Last day of year
        dt_str2 = "2025-12-31T23:59:59+00:00"
        result2 = parse_datetime(dt_str2)
        assert result2.month == 12
        assert result2.day == 31

    def test_parse_datetime_midnight(self):
        """Test parsing midnight time."""
        from goldfish.utils import parse_datetime

        dt_str = "2025-12-05T00:00:00+00:00"
        result = parse_datetime(dt_str)

        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

    def test_parse_datetime_end_of_day(self):
        """Test parsing end of day (23:59:59)."""
        from goldfish.utils import parse_datetime

        dt_str = "2025-12-05T23:59:59.999999+00:00"
        result = parse_datetime(dt_str)

        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59
        assert result.microsecond == 999999
