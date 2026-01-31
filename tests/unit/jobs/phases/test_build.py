"""Unit tests for build_image phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.build import build_image
from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import PipelineDef


def _ctx(*, workspace_name: str = "ws", stage_name: str = "train", version: str = "v1") -> StageRunContext:
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
        version=version,
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_build_image_when_called_delegates_to_executor() -> None:
    """build_image should delegate to deps._build_docker_image()."""
    deps = MagicMock()
    deps._build_docker_image.return_value = ("goldfish-test:latest", "0" * 64)

    ctx = _ctx(version="v2")
    image_tag, build_context_hash = build_image(deps, ctx, profile_name="cpu-small")

    assert image_tag == "goldfish-test:latest"
    assert build_context_hash == "0" * 64
    deps._build_docker_image.assert_called_once_with("ws", "v2", profile_name="cpu-small")
