"""Unit tests for finalize_run results JSON schema validation.

TDD: These tests define the expected behavior for finalize results validation.
Tests should fail initially (RED), then pass after implementation (GREEN).
"""

import pytest

from goldfish.experiment_model.schemas import (
    InvalidFinalizeResultsError,
    validate_finalize_results,
)


class TestFinalizeResultsValidation:
    """Tests for finalize_run results schema validation."""

    def test_valid_minimal_finalize_results(self) -> None:
        """Minimal valid finalize results with only required fields."""
        results = {
            "primary_metric": "dir_acc_binary",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Achieved target accuracy after 19 epochs of training.",
        }
        # Should not raise
        validate_finalize_results(results)

    def test_valid_full_finalize_results(self) -> None:
        """Full finalize results with all optional fields."""
        results = {
            "primary_metric": "dir_acc_binary",
            "direction": "maximize",
            "value": 0.631,
            "unit": "fraction",
            "dataset_split": "val",
            "step": 19,
            "epoch": 19,
            "secondary": {"val_loss": 2.279, "test_loss": 2.310},
            "termination": {"infra_outcome": "preempted"},
            "ml_outcome": "success",
            "notes": "Achieved target accuracy after 19 epochs. Preempted but results are valid.",
        }
        # Should not raise
        validate_finalize_results(results)

    def test_valid_direction_minimize(self) -> None:
        """Direction can be 'minimize' for loss metrics."""
        results = {
            "primary_metric": "val_loss",
            "direction": "minimize",
            "value": 2.279,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Loss decreased to target value after training completion.",
        }
        validate_finalize_results(results)

    def test_valid_ml_outcome_partial(self) -> None:
        """ml_outcome can be 'partial'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.58,
            "dataset_split": "val",
            "ml_outcome": "partial",
            "notes": "Achieved minimum threshold but not goal. Needs more training.",
        }
        validate_finalize_results(results)

    def test_valid_ml_outcome_miss(self) -> None:
        """ml_outcome can be 'miss'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.45,
            "dataset_split": "val",
            "ml_outcome": "miss",
            "notes": "Failed to achieve minimum threshold. Hypothesis was incorrect.",
        }
        validate_finalize_results(results)

    def test_valid_ml_outcome_unknown(self) -> None:
        """ml_outcome can be 'unknown'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.55,
            "dataset_split": "val",
            "ml_outcome": "unknown",
            "notes": "Results inconclusive due to infrastructure issues during evaluation.",
        }
        validate_finalize_results(results)

    def test_valid_unit_percent(self) -> None:
        """unit can be 'percent'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 63.1,
            "unit": "percent",
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Achieved 63.1% accuracy on validation set.",
        }
        validate_finalize_results(results)

    def test_valid_unit_loss(self) -> None:
        """unit can be 'loss'."""
        results = {
            "primary_metric": "val_loss",
            "direction": "minimize",
            "value": 2.279,
            "unit": "loss",
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Loss value reached target threshold.",
        }
        validate_finalize_results(results)

    def test_valid_unit_null(self) -> None:
        """unit can be null."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "unit": None,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Metric value without explicit unit specification.",
        }
        validate_finalize_results(results)

    def test_valid_infra_outcome_completed(self) -> None:
        """termination.infra_outcome can be 'completed'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "termination": {"infra_outcome": "completed"},
            "ml_outcome": "success",
            "notes": "Run completed normally without infrastructure issues.",
        }
        validate_finalize_results(results)

    def test_valid_infra_outcome_crashed(self) -> None:
        """termination.infra_outcome can be 'crashed'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.55,
            "dataset_split": "val",
            "termination": {"infra_outcome": "crashed"},
            "ml_outcome": "partial",
            "notes": "Run crashed but checkpoint results are usable.",
        }
        validate_finalize_results(results)

    def test_valid_infra_outcome_canceled(self) -> None:
        """termination.infra_outcome can be 'canceled'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.50,
            "dataset_split": "val",
            "termination": {"infra_outcome": "canceled"},
            "ml_outcome": "unknown",
            "notes": "Run was manually canceled before completion.",
        }
        validate_finalize_results(results)

    def test_valid_infra_outcome_unknown(self) -> None:
        """termination.infra_outcome can be 'unknown'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.50,
            "dataset_split": "val",
            "termination": {"infra_outcome": "unknown"},
            "ml_outcome": "unknown",
            "notes": "Infrastructure status could not be determined.",
        }
        validate_finalize_results(results)

    def test_valid_dataset_split_train(self) -> None:
        """dataset_split can be 'train'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "train",
            "ml_outcome": "success",
            "notes": "Evaluation on training dataset split.",
        }
        validate_finalize_results(results)

    def test_valid_dataset_split_test(self) -> None:
        """dataset_split can be 'test'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "test",
            "ml_outcome": "success",
            "notes": "Evaluation on test dataset split for final results.",
        }
        validate_finalize_results(results)

    def test_valid_dataset_split_other(self) -> None:
        """dataset_split can be 'other'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "other",
            "ml_outcome": "success",
            "notes": "Evaluation on custom dataset split for this experiment.",
        }
        validate_finalize_results(results)

    def test_valid_step_zero(self) -> None:
        """step can be zero (valid boundary)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.5,
            "dataset_split": "val",
            "step": 0,
            "ml_outcome": "unknown",
            "notes": "Initial step zero is valid for early evaluation.",
        }
        validate_finalize_results(results)

    def test_valid_epoch_zero(self) -> None:
        """epoch can be zero (valid boundary)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.5,
            "dataset_split": "val",
            "epoch": 0,
            "ml_outcome": "unknown",
            "notes": "Initial epoch zero is valid for pretrained models.",
        }
        validate_finalize_results(results)

    def test_valid_unit_fraction(self) -> None:
        """unit can be 'fraction'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "unit": "fraction",
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Accuracy represented as fraction between 0 and 1.",
        }
        validate_finalize_results(results)

    def test_valid_secondary_with_values(self) -> None:
        """secondary object can contain metric values."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "secondary": {"val_loss": 2.279, "test_loss": 2.310, "f1_score": 0.65},
            "ml_outcome": "success",
            "notes": "Secondary metrics captured for analysis.",
        }
        validate_finalize_results(results)


class TestFinalizeResultsMissingRequired:
    """Tests for missing required fields in finalize results."""

    def test_missing_primary_metric_raises(self) -> None:
        """Missing primary_metric should raise."""
        results = {
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing missing primary_metric field.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "primary_metric" in str(exc_info.value)

    def test_missing_direction_raises(self) -> None:
        """Missing direction should raise."""
        results = {
            "primary_metric": "accuracy",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing missing direction field.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "direction" in str(exc_info.value)

    def test_missing_value_raises(self) -> None:
        """Missing value should raise."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing missing value field.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "value" in str(exc_info.value)

    def test_missing_dataset_split_raises(self) -> None:
        """Missing dataset_split should raise."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "ml_outcome": "success",
            "notes": "Testing missing dataset_split field.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "dataset_split" in str(exc_info.value)

    def test_missing_ml_outcome_raises(self) -> None:
        """Missing ml_outcome should raise."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "notes": "Testing missing ml_outcome field.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "ml_outcome" in str(exc_info.value)

    def test_missing_notes_raises(self) -> None:
        """Missing notes should raise."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "notes" in str(exc_info.value)


class TestFinalizeResultsInvalidTypes:
    """Tests for invalid types in finalize results fields."""

    def test_primary_metric_not_string_raises(self) -> None:
        """primary_metric must be a string."""
        results = {
            "primary_metric": 123,
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing primary_metric type validation.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "primary_metric" in str(exc_info.value)

    def test_primary_metric_empty_string_raises(self) -> None:
        """primary_metric cannot be an empty string."""
        results = {
            "primary_metric": "",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing primary_metric empty string validation.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "primary_metric" in str(exc_info.value)

    def test_direction_invalid_value_raises(self) -> None:
        """direction must be 'maximize' or 'minimize'."""
        results = {
            "primary_metric": "accuracy",
            "direction": "increase",  # Invalid
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing invalid direction value.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "direction" in str(exc_info.value)

    def test_value_not_number_raises(self) -> None:
        """value must be a number."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": "0.631",  # String instead of number
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing value type validation.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "value" in str(exc_info.value)

    def test_dataset_split_invalid_value_raises(self) -> None:
        """dataset_split must be one of the allowed values."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "validation",  # Invalid
            "ml_outcome": "success",
            "notes": "Testing invalid dataset_split value.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "dataset_split" in str(exc_info.value)

    def test_ml_outcome_invalid_value_raises(self) -> None:
        """ml_outcome must be one of the allowed values."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "good",  # Invalid
            "notes": "Testing invalid ml_outcome value.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "ml_outcome" in str(exc_info.value)

    def test_notes_too_short_raises(self) -> None:
        """notes must be at least 15 characters."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Too short",  # Less than 15 chars
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "notes" in str(exc_info.value)

    def test_value_boolean_raises(self) -> None:
        """value cannot be a boolean (booleans are not valid numbers)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": True,  # Boolean instead of number
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing value boolean rejection.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "value" in str(exc_info.value)

    def test_step_boolean_raises(self) -> None:
        """step cannot be a boolean (booleans are not valid integers)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "step": True,  # Boolean instead of integer
            "ml_outcome": "success",
            "notes": "Testing step boolean rejection.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "step" in str(exc_info.value)

    def test_epoch_boolean_raises(self) -> None:
        """epoch cannot be a boolean (booleans are not valid integers)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "epoch": False,  # Boolean instead of integer
            "ml_outcome": "success",
            "notes": "Testing epoch boolean rejection.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "epoch" in str(exc_info.value)

    def test_notes_exactly_15_chars_valid(self) -> None:
        """notes with exactly 15 characters is valid (boundary)."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Exactly 15 char",  # Exactly 15 characters
        }
        # Should not raise
        validate_finalize_results(results)


class TestFinalizeResultsOptionalFields:
    """Tests for optional field validation in finalize results."""

    def test_step_negative_raises(self) -> None:
        """step must be non-negative if provided."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "step": -1,  # Invalid
            "ml_outcome": "success",
            "notes": "Testing negative step value validation.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "step" in str(exc_info.value)

    def test_epoch_negative_raises(self) -> None:
        """epoch must be non-negative if provided."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "epoch": -1,  # Invalid
            "ml_outcome": "success",
            "notes": "Testing negative epoch value validation.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "epoch" in str(exc_info.value)

    def test_secondary_not_object_raises(self) -> None:
        """secondary must be an object if provided."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "secondary": ["val_loss", 2.279],  # Should be object
            "ml_outcome": "success",
            "notes": "Testing secondary type validation as object.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "secondary" in str(exc_info.value)

    def test_termination_missing_infra_outcome_raises(self) -> None:
        """termination.infra_outcome is required if termination is provided."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "termination": {},  # Missing infra_outcome
            "ml_outcome": "success",
            "notes": "Testing termination with missing infra_outcome.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "infra_outcome" in str(exc_info.value)

    def test_termination_invalid_infra_outcome_raises(self) -> None:
        """termination.infra_outcome must be a valid value."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "termination": {"infra_outcome": "stopped"},  # Invalid
            "ml_outcome": "success",
            "notes": "Testing termination with invalid infra_outcome.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "infra_outcome" in str(exc_info.value)

    def test_termination_extra_field_raises(self) -> None:
        """termination should not have extra fields."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "termination": {"infra_outcome": "completed", "extra": "field"},
            "ml_outcome": "success",
            "notes": "Testing termination with extra fields rejected.",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "termination" in str(exc_info.value) or "additional" in str(exc_info.value).lower()


class TestFinalizeResultsExtraFields:
    """Tests for extra/unknown fields in finalize results."""

    def test_extra_field_raises(self) -> None:
        """Unknown fields should raise an error."""
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing extra field rejection in results.",
            "unknown_field": "should not be here",
        }
        with pytest.raises(InvalidFinalizeResultsError) as exc_info:
            validate_finalize_results(results)
        assert "unknown_field" in str(exc_info.value) or "additional" in str(exc_info.value).lower()
