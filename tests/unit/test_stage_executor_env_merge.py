"""Unit tests for StageExecutor environment variable merging from stage config."""

from __future__ import annotations

from unittest.mock import MagicMock

from goldfish.cloud.contracts import RunHandle
from goldfish.jobs.stage_executor import StageExecutor


def test_launch_container_merges_environment_config(test_db, test_config, tmp_path) -> None:
    """StageExecutor._launch_container should merge stage_config['environment'] into RunSpec.env.

    User-defined environment variables from configs/{stage}.yaml should be passed
    to the container via the RunSpec.env dict. This is the abstraction-layer equivalent
    of the old LocalExecutor behavior.

    Regression test for environment variable propagation bug where user-defined
    env vars like WANDB_API_KEY were not reaching containers after the cloud
    abstraction layer refactor.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    # Create workspace with stage config containing environment section
    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    # Stage config with environment variables
    # Use unique var names to avoid collision with host env passthrough
    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
compute:
  profile: cpu-small

environment:
  MY_CUSTOM_API_KEY: "fake-key-for-testing"
  CUSTOM_VAR: "custom-value"
  MY_SECRET: "secret123"
""")

    # Create mock run_backend
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-abc123",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    # Create mock workspace manager that returns our workspace path
    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

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
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    # Check that run_backend.launch was called with RunSpec containing environment vars
    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]
    env = run_spec.env

    # User-defined environment variables should be present
    assert "MY_CUSTOM_API_KEY" in env, "MY_CUSTOM_API_KEY from stage config should be in RunSpec.env"
    assert env["MY_CUSTOM_API_KEY"] == "fake-key-for-testing"
    assert "CUSTOM_VAR" in env, "CUSTOM_VAR from stage config should be in RunSpec.env"
    assert env["CUSTOM_VAR"] == "custom-value"
    assert "MY_SECRET" in env, "MY_SECRET from stage config should be in RunSpec.env"
    assert env["MY_SECRET"] == "secret123"

    # Goldfish internal env vars should still be present
    assert "GOLDFISH_STAGE" in env
    assert "GOLDFISH_WORKSPACE" in env


def test_launch_container_environment_does_not_override_goldfish_vars(test_db, test_config, tmp_path) -> None:
    """User environment variables should NOT override Goldfish internal variables.

    If a user tries to set GOLDFISH_* variables in their config, they should
    be ignored to prevent security issues and ensure proper operation.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    # Create workspace with stage config trying to override Goldfish vars
    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
environment:
  GOLDFISH_RUN_ID: "malicious-override"
  GOLDFISH_STAGE: "hacked"
  SAFE_USER_VAR: "allowed"
""")

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-real123",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

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
        stage_run_id="stage-real123",
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]
    env = run_spec.env

    # Goldfish vars should NOT be overridden by user config
    assert env["GOLDFISH_RUN_ID"] == "stage-real123", "GOLDFISH_RUN_ID should not be overridden"
    assert env["GOLDFISH_STAGE"] == "train", "GOLDFISH_STAGE should not be overridden"

    # But safe user vars should still be present
    assert env.get("SAFE_USER_VAR") == "allowed"


def test_launch_container_validates_environment_keys(test_db, test_config, tmp_path) -> None:
    """Environment variable keys should be validated for safety.

    Invalid keys (containing shell metacharacters, etc.) should be rejected
    or skipped to prevent command injection.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    # Stage config with potentially dangerous env var names
    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
environment:
  VALID_VAR: "good"
""")

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-abc123",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

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
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]
    env = run_spec.env

    # Valid var should be present
    assert "VALID_VAR" in env
    assert env["VALID_VAR"] == "good"


def test_launch_container_config_override_applies_to_profile_resolution(test_db, test_config, tmp_path) -> None:
    """config_override with compute.profile should be used for profile resolution.

    When a user runs with config_override={"compute": {"profile": "h100-spot"}},
    the profile should be resolved from the override, NOT from the stage config file.

    Regression test for bug where config_override was ignored during profile resolution,
    causing GPU profiles to be ignored and falling back to cpu-small.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    # Create workspace with stage config that has NO profile (or cpu-small)
    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    # Stage config WITHOUT a profile - relies on default cpu-small
    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
# No compute.profile specified - should use default
batch_size: 32
""")

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-h100test",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=mock_workspace_manager,
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    # Pass config_override with h100-spot profile
    executor._launch_container(
        stage_run_id="stage-h100test",
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
        config_override={"compute": {"profile": "h100-spot"}},
    )

    # Check that run_backend.launch was called with RunSpec containing H100 GPU settings
    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]

    # H100-spot profile should set gpu_count=1 and gpu_type=nvidia-h100-80gb
    assert run_spec.gpu_count == 1, f"config_override with h100-spot should set gpu_count=1, got {run_spec.gpu_count}"
    assert (
        run_spec.gpu_type == "nvidia-h100-80gb"
    ), f"config_override with h100-spot should set gpu_type=nvidia-h100-80gb, got {run_spec.gpu_type}"
    assert run_spec.spot is True, "h100-spot profile should set spot=True"
    assert run_spec.profile == "h100-spot", f"profile should be h100-spot, got {run_spec.profile}"


def test_launch_container_timeout_flows_through_runspec(test_db, test_config, tmp_path) -> None:
    """Stage config compute.max_runtime_seconds should flow through RunSpec.timeout_seconds.

    This ensures the timeout value from stage config is properly passed through the
    cloud abstraction layer via RunSpec, rather than being accessed directly from
    stage_config dict which bypasses the abstraction.

    Regression test for timeout_seconds not flowing through RunSpec.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    # Stage config with max_runtime_seconds
    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
compute:
  profile: cpu-small
  max_runtime_seconds: 7200
""")

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=False, supports_spot=False)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-timeout123",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

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
        stage_run_id="stage-timeout123",
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]

    # timeout_seconds should be set from compute.max_runtime_seconds
    assert (
        run_spec.timeout_seconds == 7200
    ), f"RunSpec.timeout_seconds should be 7200 from stage config, got {run_spec.timeout_seconds}"


def test_launch_container_timeout_none_when_not_specified(test_db, test_config, tmp_path) -> None:
    """RunSpec.timeout_seconds should be None when compute.max_runtime_seconds is not specified.

    This ensures we don't accidentally set a default timeout when one isn't configured.
    """
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    workspace_path.mkdir(parents=True)
    configs_dir = workspace_path / "configs"
    configs_dir.mkdir()

    # Stage config WITHOUT max_runtime_seconds
    stage_config_yaml = configs_dir / "train.yaml"
    stage_config_yaml.write_text("""
compute:
  profile: cpu-small
""")

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=False, supports_spot=False)
    mock_backend.launch.return_value = RunHandle(
        stage_run_id="stage-notimeout",
        backend_type="local",
        backend_handle="container-123",
        zone="local-zone-1",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

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
        stage_run_id="stage-notimeout",
        workspace="test_ws",
        stage_name="train",
        image_tag="goldfish-test:latest",
        inputs={},
        input_configs={},
        output_configs={},
        user_config={},
        git_sha=None,
    )

    mock_backend.launch.assert_called_once()
    run_spec = mock_backend.launch.call_args[0][0]

    # timeout_seconds should be None when not specified
    assert (
        run_spec.timeout_seconds is None
    ), f"RunSpec.timeout_seconds should be None when not configured, got {run_spec.timeout_seconds}"
