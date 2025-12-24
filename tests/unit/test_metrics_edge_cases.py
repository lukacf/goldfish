"""Edge case tests for metrics functionality.

Tests for NaN, Infinity, large values, and other edge cases.
"""

import math
import threading

import pytest

from goldfish.metrics.writer import LocalWriter
from goldfish.validation import (
    InvalidMetricNameError,
    InvalidMetricValueError,
    validate_metric_name,
    validate_metric_value,
)


class TestMetricValueEdgeCases:
    """Test edge cases for metric values."""

    def test_nan_rejected_at_validation(self) -> None:
        """NaN should be rejected during validation."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(float("nan"))

    def test_nan_from_math_module(self) -> None:
        """math.nan should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(math.nan)

    def test_infinity_positive_rejected(self) -> None:
        """Positive infinity should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(float("inf"))

    def test_infinity_negative_rejected(self) -> None:
        """Negative infinity should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(float("-inf"))

    def test_infinity_from_math_module(self) -> None:
        """math.inf should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(math.inf)

    def test_very_large_positive_value_accepted(self) -> None:
        """Very large but finite positive values should be accepted."""
        validate_metric_value(1e308)

    def test_very_large_negative_value_accepted(self) -> None:
        """Very large but finite negative values should be accepted."""
        validate_metric_value(-1e308)

    def test_very_small_positive_value_accepted(self) -> None:
        """Very small positive values should be accepted."""
        validate_metric_value(1e-308)

    def test_very_small_negative_value_accepted(self) -> None:
        """Very small negative values should be accepted."""
        validate_metric_value(-1e-308)

    def test_zero_accepted(self) -> None:
        """Zero should be accepted."""
        validate_metric_value(0.0)
        validate_metric_value(-0.0)

    def test_integer_zero_accepted(self) -> None:
        """Integer zero should be accepted."""
        validate_metric_value(0)


class TestMetricNameEdgeCases:
    """Test edge cases for metric names."""

    def test_single_letter_accepted(self) -> None:
        """Single letter names should be accepted."""
        validate_metric_name("a")
        validate_metric_name("Z")

    def test_max_length_accepted(self) -> None:
        """Names at max length (256) should be accepted."""
        name = "a" * 256
        validate_metric_name(name)

    def test_over_max_length_rejected(self) -> None:
        """Names over max length should be rejected."""
        name = "a" * 257
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name(name)

    def test_empty_name_rejected(self) -> None:
        """Empty names should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("")

    def test_whitespace_only_rejected(self) -> None:
        """Whitespace-only names should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("   ")

    def test_starting_with_number_rejected(self) -> None:
        """Names starting with numbers should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("1loss")

    def test_starting_with_underscore_rejected(self) -> None:
        """Names starting with underscore should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("_loss")


class TestWriterNaNRejection:
    """Test that LocalWriter rejects NaN at log_metric time."""

    def test_nan_rejected_at_log_metric(self, tmp_path) -> None:
        """NaN should be rejected when logging (without raising)."""
        writer = LocalWriter(outputs_dir=tmp_path)
        assert writer.log_metric("loss", float("nan")) is False
        assert writer.get_validation_errors()

    def test_infinity_rejected_at_log_metric(self, tmp_path) -> None:
        """Infinity should be rejected when logging (without raising)."""
        writer = LocalWriter(outputs_dir=tmp_path)
        assert writer.log_metric("loss", float("inf")) is False
        assert writer.get_validation_errors()

    def test_invalid_name_rejected_at_log_metric(self, tmp_path) -> None:
        """Invalid names should be rejected when logging."""
        writer = LocalWriter(outputs_dir=tmp_path)
        with pytest.raises(InvalidMetricNameError):
            writer.log_metric("", 0.5)

    def test_valid_metric_accepted(self, tmp_path) -> None:
        """Valid metrics should be accepted."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5)
        writer.flush()

        # Verify written
        assert (tmp_path / ".goldfish" / "metrics.jsonl").exists()


class TestWriterConfigurableThreshold:
    """Test configurable auto-flush threshold."""

    def test_default_threshold_is_100(self, tmp_path) -> None:
        """Default threshold should be 100."""
        writer = LocalWriter(outputs_dir=tmp_path)
        assert writer._auto_flush_threshold == 100

    def test_custom_threshold_respected(self, tmp_path) -> None:
        """Custom threshold should be respected."""
        writer = LocalWriter(outputs_dir=tmp_path, auto_flush_threshold=50)
        assert writer._auto_flush_threshold == 50

    def test_threshold_clamped_to_minimum(self, tmp_path) -> None:
        """Threshold should be clamped to minimum (10)."""
        writer = LocalWriter(outputs_dir=tmp_path, auto_flush_threshold=1)
        assert writer._auto_flush_threshold == 10

    def test_threshold_clamped_to_maximum(self, tmp_path) -> None:
        """Threshold should be clamped to maximum (10000)."""
        writer = LocalWriter(outputs_dir=tmp_path, auto_flush_threshold=100000)
        assert writer._auto_flush_threshold == 10000


class TestWriterThreadSafety:
    """Test thread safety of LocalWriter."""

    def test_concurrent_log_metric_no_data_loss(self, tmp_path) -> None:
        """Multiple threads logging should not lose data."""
        writer = LocalWriter(outputs_dir=tmp_path, auto_flush_threshold=1000)

        def log_many(thread_id: int) -> None:
            for i in range(50):
                writer.log_metric(f"thread{thread_id}/metric", float(i), step=i)

        # Create and start threads
        threads = [threading.Thread(target=log_many, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        writer.flush()

        # Verify all metrics written (5 threads * 50 metrics = 250)
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
        assert len(lines) == 250

    def test_concurrent_flush_safe(self, tmp_path) -> None:
        """Multiple threads flushing concurrently should be safe."""
        writer = LocalWriter(outputs_dir=tmp_path)

        # Add some metrics
        for i in range(10):
            writer.log_metric("loss", float(i))

        # Flush from multiple threads
        threads = [threading.Thread(target=writer.flush) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should not raise, and file should exist
        assert (tmp_path / ".goldfish" / "metrics.jsonl").exists()
