"""CI wiring contract tests for import boundary enforcement."""

from __future__ import annotations

from pathlib import Path

import yaml


def test_ci_workflow_when_loaded_has_import_linter_step() -> None:
    """Phase 6 requires CI to run import-linter contracts."""

    workflow_path = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text("utf-8"))

    jobs = workflow.get("jobs", {})
    found = False
    for job in jobs.values():
        for step in job.get("steps", []):
            run = step.get("run")
            if isinstance(run, str) and "make lint-imports" in run:
                found = True
                break
        if found:
            break

    assert found
