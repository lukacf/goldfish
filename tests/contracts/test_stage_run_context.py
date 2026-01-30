"""StageRunContext representation contract tests."""

from __future__ import annotations

import dataclasses


def test_stage_run_context_when_defined_has_required_fields() -> None:
    """StageRunContext carries explicit state between phases."""

    from goldfish.jobs.phases.context import StageRunContext

    assert dataclasses.is_dataclass(StageRunContext)
    assert [f.name for f in dataclasses.fields(StageRunContext)] == [
        "stage_run_id",
        "workspace_name",
        "stage_name",
        "version",
        "pipeline",
        "stage_config",
        "run_backend",
        "storage",
        "settings",
    ]
