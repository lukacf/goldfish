"""Unit tests for results_spec JSON schema validation.

TDD: These tests define the expected behavior for results_spec validation.
Tests should fail initially (RED), then pass after implementation (GREEN).
"""

import pytest

from goldfish.experiment_model.schemas import (
    InvalidResultsSpecError,
    validate_results_spec,
)


class TestResultsSpecValidation:
    """Tests for results_spec schema validation."""

    def test_valid_minimal_results_spec(self) -> None:
        """Minimal valid results_spec with only required fields."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing minimal results spec validation with required fields only.",
        }
        # Should not raise
        validate_results_spec(spec)

    def test_valid_full_results_spec(self) -> None:
        """Full results_spec with all optional fields."""
        spec = {
            "primary_metric": "dir_acc_binary",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.63,
            "dataset_split": "val",
            "tolerance": 0.003,
            "secondary_metrics": ["val_loss", "test_loss"],
            "baseline_run": "stage-b33b632e",
            "failure_threshold": 0.55,
            "known_caveats": ["Small dataset, high variance", "First 5 epochs unstable"],
            "context": "Testing LSTM baseline with 25M parameters for directional accuracy.",
        }
        # Should not raise
        validate_results_spec(spec)

    def test_valid_direction_minimize(self) -> None:
        """Direction can be 'minimize' for loss metrics."""
        spec = {
            "primary_metric": "val_loss",
            "direction": "minimize",
            "min_value": 2.5,
            "goal_value": 2.0,
            "dataset_split": "val",
            "tolerance": 0.05,
            "context": "Testing loss minimization validation scenario.",
        }
        validate_results_spec(spec)

    def test_valid_baseline_run_with_tag_reference(self) -> None:
        """baseline_run can be a tag reference with @ prefix."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "baseline_run": "@best-25m-63pct",
            "context": "Testing baseline run with tag reference format.",
        }
        validate_results_spec(spec)

    def test_valid_baseline_run_null(self) -> None:
        """baseline_run can be null."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "baseline_run": None,
            "context": "Testing baseline run as null value scenario.",
        }
        validate_results_spec(spec)

    def test_valid_dataset_split_test(self) -> None:
        """dataset_split can be 'test'."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "test",
            "tolerance": 0.01,
            "context": "Testing with test dataset split validation.",
        }
        validate_results_spec(spec)

    def test_valid_dataset_split_train(self) -> None:
        """dataset_split can be 'train'."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "train",
            "tolerance": 0.01,
            "context": "Testing with train dataset split validation.",
        }
        validate_results_spec(spec)

    def test_valid_dataset_split_other(self) -> None:
        """dataset_split can be 'other'."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "other",
            "tolerance": 0.01,
            "context": "Testing with other dataset split validation.",
        }
        validate_results_spec(spec)


class TestResultsSpecMissingRequired:
    """Tests for missing required fields in results_spec."""

    def test_missing_primary_metric_raises(self) -> None:
        """Missing primary_metric should raise."""
        spec = {
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing missing primary metric field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "primary_metric" in str(exc_info.value)

    def test_missing_direction_raises(self) -> None:
        """Missing direction should raise."""
        spec = {
            "primary_metric": "accuracy",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing missing direction field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "direction" in str(exc_info.value)

    def test_missing_min_value_raises(self) -> None:
        """Missing min_value should raise."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing missing min_value field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "min_value" in str(exc_info.value)

    def test_missing_goal_value_raises(self) -> None:
        """Missing goal_value should raise."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing missing goal_value field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "goal_value" in str(exc_info.value)

    def test_missing_dataset_split_raises(self) -> None:
        """Missing dataset_split should raise."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "tolerance": 0.01,
            "context": "Testing missing dataset_split field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "dataset_split" in str(exc_info.value)

    def test_missing_tolerance_raises(self) -> None:
        """Missing tolerance should raise."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "context": "Testing missing tolerance field.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "tolerance" in str(exc_info.value)

    def test_missing_context_raises(self) -> None:
        """Missing context should raise."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "context" in str(exc_info.value)


class TestResultsSpecInvalidTypes:
    """Tests for invalid types in results_spec fields."""

    def test_primary_metric_not_string_raises(self) -> None:
        """primary_metric must be a string."""
        spec = {
            "primary_metric": 123,
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing primary_metric type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "primary_metric" in str(exc_info.value)

    def test_primary_metric_empty_string_raises(self) -> None:
        """primary_metric cannot be empty."""
        spec = {
            "primary_metric": "",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing empty primary_metric validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "primary_metric" in str(exc_info.value)

    def test_direction_invalid_value_raises(self) -> None:
        """direction must be 'maximize' or 'minimize'."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "increase",  # Invalid
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing invalid direction value.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "direction" in str(exc_info.value)

    def test_min_value_not_number_raises(self) -> None:
        """min_value must be a number."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": "0.5",  # String instead of number
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing min_value type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "min_value" in str(exc_info.value)

    def test_goal_value_not_number_raises(self) -> None:
        """goal_value must be a number."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": "0.8",  # String instead of number
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing goal_value type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "goal_value" in str(exc_info.value)

    def test_dataset_split_invalid_value_raises(self) -> None:
        """dataset_split must be one of the allowed values."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "validation",  # Invalid - should be "val"
            "tolerance": 0.01,
            "context": "Testing invalid dataset_split value.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "dataset_split" in str(exc_info.value)

    def test_tolerance_negative_raises(self) -> None:
        """tolerance must be non-negative."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": -0.01,  # Negative
            "context": "Testing negative tolerance validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "tolerance" in str(exc_info.value)

    def test_context_too_short_raises(self) -> None:
        """context must be at least 15 characters."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Too short",  # Less than 15 chars
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "context" in str(exc_info.value)


class TestResultsSpecOptionalFields:
    """Tests for optional field validation in results_spec."""

    def test_secondary_metrics_not_array_raises(self) -> None:
        """secondary_metrics must be an array if provided."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "secondary_metrics": "val_loss",  # Should be array
            "context": "Testing secondary_metrics type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "secondary_metrics" in str(exc_info.value)

    def test_secondary_metrics_non_string_items_raises(self) -> None:
        """secondary_metrics array items must be strings."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "secondary_metrics": ["val_loss", 123],  # Non-string item
            "context": "Testing secondary_metrics item type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "secondary_metrics" in str(exc_info.value)

    def test_secondary_metrics_empty_string_raises(self) -> None:
        """secondary_metrics items cannot be empty strings."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "secondary_metrics": ["val_loss", ""],  # Empty string
            "context": "Testing secondary_metrics empty item validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "secondary_metrics" in str(exc_info.value)

    def test_failure_threshold_not_number_raises(self) -> None:
        """failure_threshold must be a number if provided."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "failure_threshold": "0.55",  # String instead of number
            "context": "Testing failure_threshold type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "failure_threshold" in str(exc_info.value)

    def test_known_caveats_not_array_raises(self) -> None:
        """known_caveats must be an array if provided."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "known_caveats": "Small dataset",  # Should be array
            "context": "Testing known_caveats type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "known_caveats" in str(exc_info.value)

    def test_known_caveats_empty_string_raises(self) -> None:
        """known_caveats items cannot be empty strings."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "known_caveats": ["Valid caveat", ""],  # Empty string
            "context": "Testing known_caveats empty item validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "known_caveats" in str(exc_info.value)

    def test_baseline_run_not_string_raises(self) -> None:
        """baseline_run must be a string or null, not other types."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "baseline_run": 12345,  # Integer instead of string
            "context": "Testing baseline_run type validation.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "baseline_run" in str(exc_info.value)

    def test_min_value_boolean_raises(self) -> None:
        """min_value cannot be a boolean (booleans are not valid numbers)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": True,  # Boolean instead of number
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing min_value boolean rejection.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "min_value" in str(exc_info.value)

    def test_goal_value_boolean_raises(self) -> None:
        """goal_value cannot be a boolean (booleans are not valid numbers)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": False,  # Boolean instead of number
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing goal_value boolean rejection.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "goal_value" in str(exc_info.value)

    def test_tolerance_boolean_raises(self) -> None:
        """tolerance cannot be a boolean (booleans are not valid numbers)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": True,  # Boolean instead of number
            "context": "Testing tolerance boolean rejection.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "tolerance" in str(exc_info.value)

    def test_failure_threshold_boolean_raises(self) -> None:
        """failure_threshold cannot be a boolean (booleans are not valid numbers)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "failure_threshold": True,  # Boolean instead of number
            "context": "Testing failure_threshold boolean rejection.",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "failure_threshold" in str(exc_info.value)

    def test_tolerance_zero_valid(self) -> None:
        """tolerance of zero is valid (boundary condition)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0,  # Zero is valid boundary
            "context": "Testing tolerance zero boundary condition.",
        }
        # Should not raise
        validate_results_spec(spec)

    def test_context_exactly_15_chars_valid(self) -> None:
        """context with exactly 15 characters is valid (boundary)."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Exactly 15 char",  # Exactly 15 characters
        }
        # Should not raise
        validate_results_spec(spec)


class TestResultsSpecExtraFields:
    """Tests for extra/unknown fields in results_spec."""

    def test_extra_field_raises(self) -> None:
        """Unknown fields should raise an error."""
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.8,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing extra field rejection.",
            "unknown_field": "should not be here",
        }
        with pytest.raises(InvalidResultsSpecError) as exc_info:
            validate_results_spec(spec)
        assert "unknown_field" in str(exc_info.value) or "additional" in str(exc_info.value).lower()
