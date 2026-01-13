"""Experiment model for Goldfish.

This module provides the new experiment model that makes experiment memory
first-class (results, comparisons, summaries) rather than relying on manual thoughts.
"""

from __future__ import annotations

from goldfish.experiment_model.records import (
    ExperimentRecordManager,
    RecordType,
    generate_record_id,
)
from goldfish.experiment_model.schemas import (
    InvalidFinalizeResultsError,
    InvalidResultsSpecError,
    validate_finalize_results,
    validate_results_spec,
)

__all__ = [
    "ExperimentRecordManager",
    "InvalidFinalizeResultsError",
    "InvalidResultsSpecError",
    "RecordType",
    "generate_record_id",
    "validate_finalize_results",
    "validate_results_spec",
]
