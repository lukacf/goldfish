"""Unit tests for validate_stage phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.validate import validate_stage
from goldfish.models import PipelineDef, StageDef


def _ctx(*, stage_name: str = "train") -> StageRunContext:
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
        workspace_name="ws",
        stage_name=stage_name,
        version="v1",
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_validate_stage_when_pipeline_and_config_loaded_updates_context() -> None:
    """validate_stage should load pipeline + config and write them into ctx."""
    stage_def = StageDef(name="train", inputs={}, outputs={})
    pipeline = PipelineDef(name="pipe", stages=[stage_def])

    executor = MagicMock()
    executor.pipeline_manager.get_pipeline.return_value = pipeline
    executor._find_stage.return_value = stage_def
    executor._load_stage_config.return_value = {"compute": {"profile": "cpu-small"}}

    ctx = _ctx(stage_name="train")
    stage = validate_stage(executor, ctx, pipeline_name=None, config_override={"hints": {"k": "v"}})

    assert stage == stage_def
    assert ctx.pipeline == pipeline
    assert ctx.stage_config == {"compute": {"profile": "cpu-small"}, "hints": {"k": "v"}}

    executor.pipeline_manager.get_pipeline.assert_called_once_with("ws", None)
    executor._find_stage.assert_called_once_with(pipeline, "train")
    executor._load_stage_config.assert_called_once_with("ws", "train")


def test_validate_stage_when_stage_config_missing_uses_empty_dict() -> None:
    """validate_stage should tolerate missing stage config."""
    stage_def = StageDef(name="train", inputs={}, outputs={})
    pipeline = PipelineDef(name="pipe", stages=[stage_def])

    executor = MagicMock()
    executor.pipeline_manager.get_pipeline.return_value = pipeline
    executor._find_stage.return_value = stage_def
    executor._load_stage_config.return_value = None

    ctx = _ctx(stage_name="train")
    _ = validate_stage(executor, ctx, pipeline_name=None, config_override=None)

    assert ctx.stage_config == {}
