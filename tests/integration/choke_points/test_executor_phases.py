"""Choke-point integration tests for Executor → Phases boundary."""

from __future__ import annotations

from pathlib import Path

import goldfish


def _stage_executor_path() -> Path:
    package_root = Path(goldfish.__file__).resolve().parent
    return package_root / "jobs" / "stage_executor.py"


def test_stage_executor_when_refactored_is_under_line_budget() -> None:
    """REQ-004: stage_executor.py must shrink after phase split (Gate 2: RED OK)."""
    line_count = len(_stage_executor_path().read_text(encoding="utf-8").splitlines())
    assert line_count < 400, f"Expected stage_executor.py < 400 lines after refactor, got {line_count}"


def test_stage_executor_when_refactored_uses_phase_context_and_modules() -> None:
    """REQ-005: executor should orchestrate phase functions via StageRunContext."""
    source = _stage_executor_path().read_text(encoding="utf-8")
    assert "StageRunContext" in source, "Expected StageExecutor to use jobs.phases.context.StageRunContext"
    assert "goldfish.jobs.phases" in source, "Expected StageExecutor to delegate logic to jobs/phases modules"


def test_phases_package_when_refactored_contains_phase_modules() -> None:
    """Phase logic should live in jobs/phases/, not in stage_executor.py."""
    package_root = Path(goldfish.__file__).resolve().parent
    phases_dir = package_root / "jobs" / "phases"
    phase_modules = {p.name for p in phases_dir.glob("*.py")}
    implemented = phase_modules - {"__init__.py", "context.py"}
    assert implemented, "Expected at least one phase module (besides context.py) under goldfish/jobs/phases/"
