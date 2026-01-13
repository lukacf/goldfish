"""JSON schema validation for experiment model payloads.

This module provides validation for:
- results_spec: Required at run time to specify expected results
- finalize_results: Authoritative ML result finalization payload
"""

from __future__ import annotations

from typing import Any

from goldfish.errors import GoldfishError


class InvalidResultsSpecError(GoldfishError):
    """Results spec validation failed.

    Args:
        message: Human-readable error message
        field: The field that caused the error (if applicable)
        details: Additional error context (list of all errors, count)
    """

    def __init__(self, message: str, field: str | None = None, details: dict[str, Any] | None = None):
        self.field = field
        error_details: dict[str, Any] = details.copy() if details else {}
        if field:
            error_details["field"] = field
        super().__init__(message, error_details)


class InvalidFinalizeResultsError(GoldfishError):
    """Finalize results validation failed.

    Args:
        message: Human-readable error message
        field: The field that caused the error (if applicable)
        details: Additional error context (list of all errors, count)
    """

    def __init__(self, message: str, field: str | None = None, details: dict[str, Any] | None = None):
        self.field = field
        error_details: dict[str, Any] = details.copy() if details else {}
        if field:
            error_details["field"] = field
        super().__init__(message, error_details)


# Valid values for enum fields
_VALID_DIRECTIONS = {"maximize", "minimize"}
_VALID_DATASET_SPLITS = {"train", "val", "test", "other"}
_VALID_ML_OUTCOMES = {"success", "partial", "miss", "unknown"}
_VALID_INFRA_OUTCOMES = {"completed", "preempted", "crashed", "canceled", "unknown"}

# Required fields for each schema
_RESULTS_SPEC_REQUIRED = {
    "primary_metric",
    "direction",
    "min_value",
    "goal_value",
    "dataset_split",
    "tolerance",
    "context",
}

_RESULTS_SPEC_ALLOWED = _RESULTS_SPEC_REQUIRED | {
    "secondary_metrics",
    "baseline_run",
    "failure_threshold",
    "known_caveats",
}

_FINALIZE_RESULTS_REQUIRED = {
    "primary_metric",
    "direction",
    "value",
    "dataset_split",
    "ml_outcome",
    "notes",
}

_FINALIZE_RESULTS_ALLOWED = _FINALIZE_RESULTS_REQUIRED | {
    "unit",
    "step",
    "epoch",
    "secondary",
    "termination",
}

_TERMINATION_REQUIRED = {"infra_outcome"}
_TERMINATION_ALLOWED = _TERMINATION_REQUIRED

# Minimum context/notes length
_MIN_CONTEXT_LENGTH = 15


def validate_results_spec(spec: dict[str, Any]) -> None:
    """Validate a results_spec payload.

    Args:
        spec: The results_spec dict to validate

    Raises:
        InvalidResultsSpecError: If validation fails
    """
    if not isinstance(spec, dict):
        raise InvalidResultsSpecError("results_spec must be a JSON object")

    errors: list[str] = []

    # Check for missing required fields
    missing = _RESULTS_SPEC_REQUIRED - spec.keys()
    if missing:
        for field_name in sorted(missing):
            errors.append(f"Missing required field: {field_name}")

    # Check for extra fields
    extra = spec.keys() - _RESULTS_SPEC_ALLOWED
    if extra:
        for field_name in sorted(extra):
            errors.append(f"Unknown field: {field_name}")

    # Validate primary_metric
    if "primary_metric" in spec:
        pm = spec["primary_metric"]
        if not isinstance(pm, str):
            errors.append("primary_metric must be a string")
        elif len(pm) == 0:
            errors.append("primary_metric cannot be empty")

    # Validate direction
    if "direction" in spec:
        direction = spec["direction"]
        if not isinstance(direction, str) or direction not in _VALID_DIRECTIONS:
            errors.append(f"direction must be one of: {', '.join(sorted(_VALID_DIRECTIONS))}")

    # Validate min_value
    if "min_value" in spec:
        min_val = spec["min_value"]
        if not isinstance(min_val, int | float) or isinstance(min_val, bool):
            errors.append("min_value must be a number")

    # Validate goal_value
    if "goal_value" in spec:
        goal_val = spec["goal_value"]
        if not isinstance(goal_val, int | float) or isinstance(goal_val, bool):
            errors.append("goal_value must be a number")

    # Validate dataset_split
    if "dataset_split" in spec:
        ds = spec["dataset_split"]
        if not isinstance(ds, str) or ds not in _VALID_DATASET_SPLITS:
            errors.append(f"dataset_split must be one of: {', '.join(sorted(_VALID_DATASET_SPLITS))}")

    # Validate tolerance
    if "tolerance" in spec:
        tol = spec["tolerance"]
        if not isinstance(tol, int | float) or isinstance(tol, bool):
            errors.append("tolerance must be a number")
        elif tol < 0:
            errors.append("tolerance must be non-negative")

    # Validate context
    if "context" in spec:
        ctx = spec["context"]
        if not isinstance(ctx, str):
            errors.append("context must be a string")
        elif len(ctx) < _MIN_CONTEXT_LENGTH:
            errors.append(f"context must be at least {_MIN_CONTEXT_LENGTH} characters")

    # Validate optional fields

    # secondary_metrics
    if "secondary_metrics" in spec:
        sm = spec["secondary_metrics"]
        if not isinstance(sm, list):
            errors.append("secondary_metrics must be an array")
        else:
            for i, item in enumerate(sm):
                if not isinstance(item, str):
                    errors.append(f"secondary_metrics[{i}] must be a string")
                elif len(item) == 0:
                    errors.append(f"secondary_metrics[{i}] cannot be empty")

    # baseline_run can be string or null
    if "baseline_run" in spec and spec["baseline_run"] is not None:
        br = spec["baseline_run"]
        if not isinstance(br, str):
            errors.append("baseline_run must be a string or null")

    # failure_threshold
    if "failure_threshold" in spec:
        ft = spec["failure_threshold"]
        if not isinstance(ft, int | float) or isinstance(ft, bool):
            errors.append("failure_threshold must be a number")

    # known_caveats
    if "known_caveats" in spec:
        kc = spec["known_caveats"]
        if not isinstance(kc, list):
            errors.append("known_caveats must be an array")
        else:
            for i, item in enumerate(kc):
                if not isinstance(item, str):
                    errors.append(f"known_caveats[{i}] must be a string")
                elif len(item) == 0:
                    errors.append(f"known_caveats[{i}] cannot be empty")

    if errors:
        # Extract field name from first error for context
        first_error = errors[0]
        error_field: str | None = None
        for f in _RESULTS_SPEC_ALLOWED | {"unknown"}:
            if f in first_error.lower():
                error_field = f
                break
        raise InvalidResultsSpecError(
            f"Invalid results_spec: {first_error}",
            field=error_field,
            details={"errors": errors, "count": len(errors)},
        )


def validate_finalize_results(results: dict[str, Any]) -> None:
    """Validate a finalize_run results payload.

    Args:
        results: The results dict to validate

    Raises:
        InvalidFinalizeResultsError: If validation fails
    """
    if not isinstance(results, dict):
        raise InvalidFinalizeResultsError("results must be a JSON object")

    errors: list[str] = []

    # Check for missing required fields
    missing = _FINALIZE_RESULTS_REQUIRED - results.keys()
    if missing:
        for field_name in sorted(missing):
            errors.append(f"Missing required field: {field_name}")

    # Check for extra fields
    extra = results.keys() - _FINALIZE_RESULTS_ALLOWED
    if extra:
        for field_name in sorted(extra):
            errors.append(f"Unknown field: {field_name}")

    # Validate primary_metric
    if "primary_metric" in results:
        pm = results["primary_metric"]
        if not isinstance(pm, str):
            errors.append("primary_metric must be a string")
        elif len(pm) == 0:
            errors.append("primary_metric cannot be empty")

    # Validate direction
    if "direction" in results:
        direction = results["direction"]
        if not isinstance(direction, str) or direction not in _VALID_DIRECTIONS:
            errors.append(f"direction must be one of: {', '.join(sorted(_VALID_DIRECTIONS))}")

    # Validate value
    if "value" in results:
        val = results["value"]
        if not isinstance(val, int | float) or isinstance(val, bool):
            errors.append("value must be a number")

    # Validate dataset_split
    if "dataset_split" in results:
        ds = results["dataset_split"]
        if not isinstance(ds, str) or ds not in _VALID_DATASET_SPLITS:
            errors.append(f"dataset_split must be one of: {', '.join(sorted(_VALID_DATASET_SPLITS))}")

    # Validate ml_outcome
    if "ml_outcome" in results:
        mo = results["ml_outcome"]
        if not isinstance(mo, str) or mo not in _VALID_ML_OUTCOMES:
            errors.append(f"ml_outcome must be one of: {', '.join(sorted(_VALID_ML_OUTCOMES))}")

    # Validate notes
    if "notes" in results:
        notes = results["notes"]
        if not isinstance(notes, str):
            errors.append("notes must be a string")
        elif len(notes) < _MIN_CONTEXT_LENGTH:
            errors.append(f"notes must be at least {_MIN_CONTEXT_LENGTH} characters")

    # Validate optional fields

    # unit can be string or null
    if "unit" in results and results["unit"] is not None:
        unit = results["unit"]
        if not isinstance(unit, str):
            errors.append("unit must be a string or null")

    # step
    if "step" in results:
        step = results["step"]
        if not isinstance(step, int) or isinstance(step, bool):
            errors.append("step must be an integer")
        elif step < 0:
            errors.append("step must be non-negative")

    # epoch
    if "epoch" in results:
        epoch = results["epoch"]
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            errors.append("epoch must be an integer")
        elif epoch < 0:
            errors.append("epoch must be non-negative")

    # secondary
    if "secondary" in results:
        sec = results["secondary"]
        if not isinstance(sec, dict):
            errors.append("secondary must be an object")

    # termination
    if "termination" in results:
        term = results["termination"]
        if not isinstance(term, dict):
            errors.append("termination must be an object")
        else:
            # Check required termination fields
            term_missing = _TERMINATION_REQUIRED - term.keys()
            if term_missing:
                for field_name in sorted(term_missing):
                    errors.append(f"termination.{field_name} is required")

            # Check for extra termination fields
            term_extra = term.keys() - _TERMINATION_ALLOWED
            if term_extra:
                for field_name in sorted(term_extra):
                    errors.append(f"termination has unknown field: {field_name}")

            # Validate infra_outcome
            if "infra_outcome" in term:
                io = term["infra_outcome"]
                if not isinstance(io, str) or io not in _VALID_INFRA_OUTCOMES:
                    errors.append(
                        f"termination.infra_outcome must be one of: {', '.join(sorted(_VALID_INFRA_OUTCOMES))}"
                    )

    if errors:
        # Extract field name from first error for context
        first_error = errors[0]
        error_field: str | None = None
        for f in _FINALIZE_RESULTS_ALLOWED | {"unknown", "infra_outcome"}:
            if f in first_error.lower():
                error_field = f
                break
        raise InvalidFinalizeResultsError(
            f"Invalid finalize results: {first_error}",
            field=error_field,
            details={"errors": errors, "count": len(errors)},
        )
