"""Unit tests for finalize_outputs phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.finalize import finalize_outputs
from goldfish.models import PipelineDef


def _ctx(*, stage_run_id: str) -> StageRunContext:
    settings = GoldfishSettings(
        project_name="test",
        dev_repo_path=Path("/tmp/dev-repo"),
        workspaces_path=Path("/tmp/workspaces"),
        backend="local",
        db_path=Path("/tmp/goldfish.db"),
        db_backend="sqlite",
        log_format="console",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=60,
    )
    return StageRunContext(
        stage_run_id=stage_run_id,
        workspace_name="ws",
        stage_name="train",
        version="v1",
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_finalize_outputs_when_called_delegates_to_finalize_fn() -> None:
    """finalize_outputs should call the provided finalize_fn."""
    finalize_fn = MagicMock()
    finalize_outputs(finalize_fn, _ctx(stage_run_id="stage-1"), backend="local", status="completed")

    finalize_fn.assert_called_once_with("stage-1", "local", "completed")
