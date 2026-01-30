"""Choke-point integration tests for Logging Context → Execution boundary."""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import goldfish


def test_logging_context_module_when_refactored_exists() -> None:
    """REQ-011/INV-006: logging context should be explicit and importable (Gate 2: RED OK)."""
    assert find_spec("goldfish.config.logging") is not None, "Expected `goldfish.config.logging` module to exist"


def test_stage_executor_when_refactored_sets_contextvars_for_correlation_ids() -> None:
    """All stage execution logs should include stage_run_id via context propagation."""
    package_root = Path(goldfish.__file__).resolve().parent
    source = (package_root / "jobs" / "stage_executor.py").read_text(encoding="utf-8")
    assert "current_stage_run_id.set(" in source, "Expected StageExecutor to set current_stage_run_id during execution"
    assert (
        "current_stage_run_id.reset(" in source
    ), "Expected StageExecutor to reset current_stage_run_id token after execution"
    assert "current_request_id.set(" in source, "Expected StageExecutor to set current_request_id during execution"
    assert (
        "current_request_id.reset(" in source
    ), "Expected StageExecutor to reset current_request_id token after execution"
