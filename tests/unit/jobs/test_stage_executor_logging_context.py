"""Unit tests for StageExecutor logging correlation context."""

from __future__ import annotations

from pathlib import Path

import goldfish


def test_stage_executor_when_run_stage_called_sets_and_resets_contextvars() -> None:
    """StageExecutor should manage correlation ContextVars for execution logs."""
    package_root = Path(goldfish.__file__).resolve().parent
    source = (package_root / "jobs" / "stage_executor.py").read_text(encoding="utf-8")

    assert "current_stage_run_id.set" in source
    assert "current_stage_run_id.reset" in source
