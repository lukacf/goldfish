"""Unit tests for StageExecutor Overdrive env defaults."""

from __future__ import annotations

from unittest.mock import MagicMock

from goldfish.cloud.contracts import RunHandle
from goldfish.jobs.stage_executor import StageExecutor


def test_stage_executor_sets_overdrive_env_defaults(test_db, test_config, tmp_path) -> None:
    """StageExecutor should set PYTHONUNBUFFERED and metrics flush interval defaults."""
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    # Create workspace directory for stage config loading
    workspace_path = project_root / "workspaces" / "ws"
    workspace_path.mkdir(parents=True)

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

    # Create mock run_backend
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-abc123",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=mock_workspace_manager,
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
        run_backend=mock_backend,
    )

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

    # Check that run_backend.launch was called with RunSpec containing the overdrive env vars
    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]
    env = run_spec.env

    assert env["PYTHONUNBUFFERED"] == "1"
    assert env["GOLDFISH_METRICS_FLUSH_INTERVAL"] == "5"
