"""Unit tests for StageExecutor Overdrive env defaults."""

from __future__ import annotations

from unittest.mock import MagicMock

from goldfish.jobs.stage_executor import StageExecutor


def test_stage_executor_sets_overdrive_env_defaults(test_db, test_config, tmp_path) -> None:
    """StageExecutor should set PYTHONUNBUFFERED and metrics flush interval defaults."""
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
    )

    executor.local_executor.launch_container = MagicMock()

    executor._launch_container(
        stage_run_id="stage-abc123",
        workspace="ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    _, kwargs = executor.local_executor.launch_container.call_args
    env = kwargs["goldfish_env"]

    assert env["PYTHONUNBUFFERED"] == "1"
    assert env["GOLDFISH_METRICS_FLUSH_INTERVAL"] == "5"
