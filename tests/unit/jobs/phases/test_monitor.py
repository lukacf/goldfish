"""Unit tests for monitor_status phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.monitor import monitor_status
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


def test_monitor_status_when_called_delegates_to_wait_fn() -> None:
    """monitor_status should call the provided wait_fn."""
    wait_fn = MagicMock(return_value="completed")
    status = monitor_status(wait_fn, _ctx(stage_run_id="stage-1"), poll_interval=7, timeout=9)

    assert status == "completed"
    wait_fn.assert_called_once_with("stage-1", poll_interval=7, timeout=9)
