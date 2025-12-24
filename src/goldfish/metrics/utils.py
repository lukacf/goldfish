"""Utility helpers for metrics normalization and timestamp handling."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from goldfish.validation import (
    InvalidMetricStepError,
    InvalidMetricTimestampError,
    InvalidMetricValueError,
    validate_metric_timestamp,
)


def normalize_metric_value(value: Any) -> float:
    """Normalize metric values to a JSON-serializable float.

    Accepts Python numeric types and NumPy scalars (via .item()).
    """
    if isinstance(value, bool):
        raise InvalidMetricValueError(str(value), "value must be numeric (bool is not allowed)")

    if isinstance(value, int | float):
        return float(value)

    # NumPy scalars or similar types
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return float(item())
        except Exception as exc:  # pragma: no cover - defensive
            raise InvalidMetricValueError(str(value), f"value must be numeric ({type(exc).__name__})") from exc

    raise InvalidMetricValueError(str(value), "value must be numeric")


def normalize_metric_step(step: Any | None) -> int | None:
    """Normalize metric step to a non-negative int."""
    if step is None:
        return None

    if isinstance(step, bool):
        raise InvalidMetricStepError(str(step), "step must be an integer")

    if isinstance(step, int):
        if step < 0:
            raise InvalidMetricStepError(str(step), "step must be >= 0")
        return step

    item = getattr(step, "item", None)
    if callable(item):
        try:
            step_val = int(item())
        except Exception as exc:  # pragma: no cover - defensive
            raise InvalidMetricStepError(str(step), "step must be an integer") from exc
        if step_val < 0:
            raise InvalidMetricStepError(str(step), "step must be >= 0")
        return step_val

    raise InvalidMetricStepError(str(step), "step must be an integer")


_LAST_TIMESTAMP_SECOND: int | None = None
_LAST_TIMESTAMP_ISO: str | None = None


def _cached_now_iso() -> str:
    """Return a cached ISO timestamp for the current second."""
    global _LAST_TIMESTAMP_SECOND, _LAST_TIMESTAMP_ISO

    now = datetime.now(UTC)
    now_sec = int(now.timestamp())
    if _LAST_TIMESTAMP_SECOND == now_sec and _LAST_TIMESTAMP_ISO is not None:
        return _LAST_TIMESTAMP_ISO

    iso = now.isoformat()
    _LAST_TIMESTAMP_SECOND = now_sec
    _LAST_TIMESTAMP_ISO = iso
    return iso


def normalize_metric_timestamp(timestamp: str | float | int | None) -> str:
    """Normalize timestamps to ISO 8601 string in UTC."""
    if timestamp is None:
        return _cached_now_iso()

    if isinstance(timestamp, int | float):
        try:
            dt = datetime.fromtimestamp(float(timestamp), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise InvalidMetricTimestampError(str(timestamp), "timestamp must be a valid Unix timestamp") from exc
        return validate_metric_timestamp(dt.isoformat())

    return validate_metric_timestamp(timestamp)


def timestamp_to_float(timestamp: str | float | int | None) -> float | None:
    """Convert timestamp to Unix float (UTC)."""
    if timestamp is None:
        return None

    if isinstance(timestamp, int | float):
        try:
            dt = datetime.fromtimestamp(float(timestamp), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise InvalidMetricTimestampError(str(timestamp), "timestamp must be a valid Unix timestamp") from exc
        normalized = validate_metric_timestamp(dt.isoformat())
        return datetime.fromisoformat(normalized).timestamp()

    normalized = validate_metric_timestamp(timestamp)
    dt = datetime.fromisoformat(normalized)
    return dt.timestamp()
