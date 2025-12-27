"""CheckResult dataclass - shared across all SVS checks.

This module is separate to avoid numpy dependency for training_checks
which only needs the result dataclass.
"""

from dataclasses import dataclass


@dataclass
class CheckResult:
    """Result from a data quality check.

    Attributes:
        check: Name of the check that was run
        status: "passed" | "failed" | "warning"
        message: Human-readable description of the result
        details: Optional dictionary with metric values and context
    """

    check: str
    status: str  # "passed" | "failed" | "warning"
    message: str
    details: dict | None = None
