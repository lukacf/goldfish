"""Unit tests for sync_workspace phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.sync import sync_workspace
from goldfish.models import PipelineDef


def _ctx(*, workspace_name: str = "ws", stage_name: str = "train") -> StageRunContext:
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
        stage_run_id="stage-abc123",
        workspace_name=workspace_name,
        stage_name=stage_name,
        version="v1",
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_sync_workspace_when_called_sets_ctx_version_and_returns_git_sha() -> None:
    """sync_workspace should delegate to deps._auto_version()."""
    deps = MagicMock()
    deps._auto_version.return_value = ("v2", "sha123")

    ctx = _ctx()
    git_sha = sync_workspace(deps, ctx, reason="because")

    assert git_sha == "sha123"
    assert ctx.version == "v2"
    deps._auto_version.assert_called_once_with("ws", "train", "because")
