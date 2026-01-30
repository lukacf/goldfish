"""Unit tests for launch_container phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.launch import launch_container
from goldfish.models import PipelineDef, SignalDef, StageDef


def _ctx() -> StageRunContext:
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
        stage_name="train",
        version="v1",
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={"compute": {"profile": "cpu-small"}},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_launch_container_when_called_builds_signal_configs_and_delegates() -> None:
    """launch_container should build input/output configs for goldfish.io."""
    stage = StageDef(
        name="train",
        inputs={
            "raw": SignalDef(name="raw", type="dataset"),
            "labels": SignalDef(name="labels", type="csv", format="csv"),
        },
        outputs={
            "model": SignalDef(name="model", type="directory"),
            "metrics": SignalDef(name="metrics", type="file", format="json"),
        },
    )
    inputs = {"raw": "gs://bucket/raw", "labels": "gs://bucket/labels.csv"}

    deps = MagicMock()
    deps._launch_container = MagicMock()

    ctx = _ctx()
    launch_container(
        deps,
        ctx,
        stage,
        image_tag="img:tag",
        inputs=inputs,
        git_sha="sha123",
        run_reason={"description": "test"},
        config_override={"x": 1},
        inputs_override={"raw": "override"},
        pipeline_name="pipeline.yaml",
        results_spec={"primary_metric": "acc"},
    )

    _, kwargs = deps._launch_container.call_args
    assert kwargs["stage_run_id"] == "stage-abc123"
    assert kwargs["workspace"] == "ws"
    assert kwargs["stage_name"] == "train"
    assert kwargs["image_tag"] == "img:tag"
    assert kwargs["inputs"] == inputs
    assert kwargs["user_config"] == ctx.stage_config
    assert kwargs["git_sha"] == "sha123"

    input_configs = kwargs["input_configs"]
    assert input_configs["raw"]["location"] == "gs://bucket/raw"
    assert input_configs["raw"]["format"] == "dataset"
    assert input_configs["raw"]["type"] == "dataset"
    assert input_configs["raw"]["schema"] is None
    assert input_configs["labels"]["format"] == "csv"

    output_configs = kwargs["output_configs"]
    assert output_configs["metrics"]["format"] == "json"
    assert output_configs["metrics"]["type"] == "file"
    assert output_configs["metrics"]["schema"] is None
