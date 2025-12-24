"""Unit tests for metrics validation functions.

TDD: These tests define expected behavior for metric name, value, and path validation.
"""

import math
from datetime import UTC, datetime, timedelta

import pytest

from goldfish.validation import (
    InvalidArtifactPathError,
    InvalidMetricNameError,
    InvalidMetricTimestampError,
    InvalidMetricValueError,
    ValidationError,
    validate_artifact_path,
    validate_batch_size,
    validate_metric_name,
    validate_metric_timestamp,
    validate_metric_value,
)


class TestMetricNameValidation:
    """Tests for validate_metric_name()."""

    # === Valid names ===

    def test_valid_simple_name(self) -> None:
        """Simple alphanumeric names should be accepted."""
        validate_metric_name("loss")
        validate_metric_name("accuracy")
        validate_metric_name("f1score")

    def test_valid_name_with_underscore(self) -> None:
        """Names with underscores should be accepted."""
        validate_metric_name("train_loss")
        validate_metric_name("val_accuracy")

    def test_valid_name_with_hyphen(self) -> None:
        """Names with hyphens should be accepted."""
        validate_metric_name("train-loss")
        validate_metric_name("val-accuracy")

    def test_valid_name_with_dot(self) -> None:
        """Names with dots should be accepted."""
        validate_metric_name("model.loss")
        validate_metric_name("layer.0.weight")

    def test_valid_name_with_slash(self) -> None:
        """Hierarchical names with slashes should be accepted."""
        validate_metric_name("train/loss")
        validate_metric_name("epoch/1/accuracy")
        validate_metric_name("model/layer/0/weight")

    def test_valid_name_with_colon(self) -> None:
        """Names with colons should be accepted."""
        validate_metric_name("step:100")
        validate_metric_name("epoch:1/loss")

    def test_valid_max_length_name(self) -> None:
        """Names at max length (256) should be accepted."""
        name = "a" * 256
        validate_metric_name(name)

    def test_valid_single_char_name(self) -> None:
        """Single character names should be accepted."""
        validate_metric_name("x")
        validate_metric_name("L")

    # === Invalid names ===

    def test_invalid_empty_name(self) -> None:
        """Empty names should be rejected."""
        with pytest.raises(InvalidMetricNameError) as exc_info:
            validate_metric_name("")
        assert "cannot be empty" in str(exc_info.value)

    def test_invalid_too_long_name(self) -> None:
        """Names over 256 chars should be rejected."""
        name = "a" * 257
        with pytest.raises(InvalidMetricNameError) as exc_info:
            validate_metric_name(name)
        assert "256" in str(exc_info.value) or "too long" in str(exc_info.value).lower()

    def test_invalid_starts_with_number(self) -> None:
        """Names starting with numbers should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("123loss")
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("0accuracy")

    def test_invalid_starts_with_underscore(self) -> None:
        """Names starting with underscore should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("_loss")

    def test_invalid_starts_with_hyphen(self) -> None:
        """Names starting with hyphen should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("-loss")

    def test_invalid_shell_metacharacters(self) -> None:
        """Shell metacharacters should be rejected."""
        dangerous_names = [
            "loss;rm -rf",
            "loss|cat",
            "loss&echo",
            "loss$VAR",
            "loss`id`",
            'loss"test',
            "loss'test",
            "loss\\test",
            "loss<test",
            "loss>test",
            "loss*test",
            "loss?test",
            "loss[test]",
            "loss{test}",
            "loss~test",
            "loss!test",
        ]
        for name in dangerous_names:
            with pytest.raises(InvalidMetricNameError):
                validate_metric_name(name)

    def test_invalid_path_traversal(self) -> None:
        """Path traversal patterns should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("../loss")
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("train/../loss")
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("..\\loss")

    def test_invalid_whitespace(self) -> None:
        """Whitespace in names should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("train loss")
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("train\tloss")
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("train\nloss")

    def test_invalid_null_byte(self) -> None:
        """Null bytes should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss\x00test")


class TestMetricValueValidation:
    """Tests for validate_metric_value()."""

    # === Valid values ===

    def test_valid_zero(self) -> None:
        """Zero should be accepted."""
        validate_metric_value(0.0)
        validate_metric_value(0)


class TestMetricTimestampValidation:
    """Tests for validate_metric_timestamp()."""

    def test_valid_iso_timestamp(self) -> None:
        """Valid ISO 8601 timestamps should be accepted."""
        ts = validate_metric_timestamp("2024-01-01T00:00:00+00:00")
        assert ts.endswith("+00:00")

    def test_valid_iso_timestamp_z(self) -> None:
        """Z suffix should be accepted and normalized."""
        ts = validate_metric_timestamp("2024-01-01T00:00:00Z")
        assert ts.endswith("+00:00")

    def test_invalid_timestamp_rejected(self) -> None:
        """Invalid timestamp strings should be rejected."""
        with pytest.raises(InvalidMetricTimestampError):
            validate_metric_timestamp("not-a-timestamp")

    def test_future_timestamp_rejected_by_default(self, monkeypatch):
        """Future timestamps beyond the default drift should be rejected."""
        monkeypatch.delenv("GOLDFISH_METRICS_MAX_FUTURE_DRIFT_SECONDS", raising=False)
        future = datetime.now(UTC) + timedelta(days=2)
        with pytest.raises(InvalidMetricTimestampError):
            validate_metric_timestamp(future.isoformat())

    def test_future_timestamp_allowed_with_env_override(self, monkeypatch):
        """Env override should allow larger future drift."""
        monkeypatch.setenv("GOLDFISH_METRICS_MAX_FUTURE_DRIFT_SECONDS", str(7 * 24 * 3600))
        future = datetime.now(UTC) + timedelta(days=2)
        ts = validate_metric_timestamp(future.isoformat())
        assert ts.endswith("+00:00")

    def test_valid_positive(self) -> None:
        """Positive values should be accepted."""
        validate_metric_value(0.5)
        validate_metric_value(1.0)
        validate_metric_value(100)

    def test_valid_negative(self) -> None:
        """Negative values should be accepted."""
        validate_metric_value(-0.5)
        validate_metric_value(-100.0)

    def test_valid_very_small(self) -> None:
        """Very small values should be accepted."""
        validate_metric_value(1e-308)
        validate_metric_value(-1e-308)

    def test_valid_very_large(self) -> None:
        """Very large (but finite) values should be accepted."""
        validate_metric_value(1e308)
        validate_metric_value(-1e308)

    # === Invalid values ===

    def test_invalid_nan(self) -> None:
        """NaN should be rejected."""
        with pytest.raises(InvalidMetricValueError) as exc_info:
            validate_metric_value(float("nan"))
        assert "nan" in str(exc_info.value).lower() or "finite" in str(exc_info.value).lower()

    def test_invalid_positive_infinity(self) -> None:
        """Positive infinity should be rejected."""
        with pytest.raises(InvalidMetricValueError) as exc_info:
            validate_metric_value(float("inf"))
        assert "inf" in str(exc_info.value).lower() or "finite" in str(exc_info.value).lower()

    def test_invalid_negative_infinity(self) -> None:
        """Negative infinity should be rejected."""
        with pytest.raises(InvalidMetricValueError) as exc_info:
            validate_metric_value(float("-inf"))
        assert "inf" in str(exc_info.value).lower() or "finite" in str(exc_info.value).lower()

    def test_invalid_math_nan(self) -> None:
        """math.nan should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(math.nan)

    def test_invalid_math_inf(self) -> None:
        """math.inf should be rejected."""
        with pytest.raises(InvalidMetricValueError):
            validate_metric_value(math.inf)


class TestArtifactPathValidation:
    """Tests for validate_artifact_path()."""

    # === Valid paths ===

    def test_valid_simple_filename(self) -> None:
        """Simple filenames should be accepted."""
        validate_artifact_path("model.pt")
        validate_artifact_path("checkpoint.pth")

    def test_valid_relative_path(self) -> None:
        """Relative paths should be accepted."""
        validate_artifact_path("models/best.pt")
        validate_artifact_path("outputs/results.csv")

    def test_valid_nested_path(self) -> None:
        """Nested paths should be accepted."""
        validate_artifact_path("checkpoints/epoch_10/model.pt")

    # === Invalid paths ===

    def test_invalid_empty_path(self) -> None:
        """Empty paths should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("")

    def test_invalid_path_traversal_prefix(self) -> None:
        """Leading path traversal should be rejected."""
        with pytest.raises(InvalidArtifactPathError) as exc_info:
            validate_artifact_path("../model.pt")
        assert "traversal" in str(exc_info.value).lower()

    def test_invalid_path_traversal_middle(self) -> None:
        """Path traversal in middle should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("models/../../../etc/passwd")

    def test_invalid_path_traversal_multiple(self) -> None:
        """Multiple path traversal should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("../../..")

    def test_invalid_absolute_path_unix(self) -> None:
        """Absolute Unix paths should be rejected."""
        with pytest.raises(InvalidArtifactPathError) as exc_info:
            validate_artifact_path("/etc/passwd")
        assert "relative" in str(exc_info.value).lower()

    def test_invalid_absolute_path_windows(self) -> None:
        """Absolute Windows paths should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("C:\\Windows\\System32")

    def test_invalid_shell_metacharacters(self) -> None:
        """Shell metacharacters in paths should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("model;rm -rf.pt")
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("model`id`.pt")


class TestBatchSizeValidation:
    """Tests for validate_batch_size()."""

    def test_valid_small_batch(self) -> None:
        """Small batches should be accepted."""
        validate_batch_size(1)
        validate_batch_size(100)

    def test_valid_max_batch(self) -> None:
        """Batches at max size should be accepted."""
        validate_batch_size(10000)

    def test_invalid_zero_batch(self) -> None:
        """Zero-size batches should be rejected."""
        with pytest.raises(ValidationError):
            validate_batch_size(0)

    def test_invalid_negative_batch(self) -> None:
        """Negative batch sizes should be rejected."""
        with pytest.raises(ValidationError):
            validate_batch_size(-1)

    def test_invalid_exceeds_max(self) -> None:
        """Batches over max should be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            validate_batch_size(10001)
        assert "10000" in str(exc_info.value) or "limit" in str(exc_info.value).lower()

    def test_custom_max_size(self) -> None:
        """Custom max size should be respected."""
        validate_batch_size(500, max_size=1000)
        with pytest.raises(ValidationError):
            validate_batch_size(1001, max_size=1000)
