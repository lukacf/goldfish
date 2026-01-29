"""Unit tests for resolve_inputs phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.resolve import resolve_inputs
from goldfish.models import PipelineDef, StageDef


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


def test_resolve_inputs_when_called_delegates_to_executor() -> None:
    """resolve_inputs should delegate to deps._resolve_inputs()."""
    stage = StageDef(name="train", inputs={}, outputs={})
    inputs = {"raw": "gs://bucket/raw"}
    sources = {"raw": {"source_type": "dataset"}}
    input_context = [{"input": "raw"}]

    deps = MagicMock()
    deps._resolve_inputs.return_value = (inputs, sources, input_context)

    ctx = _ctx()
    got_inputs, got_sources, got_context = resolve_inputs(
        deps,
        ctx,
        stage,
        inputs_override={"raw": "override"},
        pipeline_run_id="pipe-1",
    )

    assert got_inputs == inputs
    assert got_sources == sources
    assert got_context == input_context
    deps._resolve_inputs.assert_called_once_with("ws", stage, {"raw": "override"}, pipeline_run_id="pipe-1")
