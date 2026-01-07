"""Tests for local Docker container execution - TDD Phase 6.2."""

from unittest.mock import MagicMock, patch

from goldfish.infra.local_executor import LocalExecutor


class TestContainerLaunch:
    """Test launching Docker containers locally."""

    def test_launch_container_basic(self, temp_dir):
        """Should launch container with basic configuration."""
        # Setup
        executor = LocalExecutor()
        stage_config = {
            "stage": "tokenize",
            "inputs": {"features": "/mnt/inputs/features"},
            "outputs": {"tokens": "/mnt/outputs/tokens"},
        }

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            container_id = executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash\necho 'Running stage'",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            # Verify
            assert container_id == "stage-abc123"
            mock_popen.assert_called_once()

    def test_launch_container_mounts_volumes(self, temp_dir):
        """Should mount input/output directories as volumes."""
        # Setup
        executor = LocalExecutor()
        inputs_dir = temp_dir / "inputs"
        outputs_dir = temp_dir / "outputs"
        inputs_dir.mkdir()
        outputs_dir.mkdir()

        stage_config = {"inputs": {}, "outputs": {}}

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
                inputs_dir=inputs_dir,
                outputs_dir=outputs_dir,
            )

            # Verify - should have volume mounts
            args = mock_popen.call_args[0][0]
            # Check for -v or --volume flags
            has_volume = any("-v" in str(arg) or "--volume" in str(arg) for arg in args)
            assert has_volume

    def test_launch_container_sets_environment(self, temp_dir):
        """Should pass stage config as environment variable."""
        # Setup
        executor = LocalExecutor()
        stage_config = {"stage": "tokenize", "config": {"VOCAB_SIZE": "10000"}}

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            # Verify - should set GOLDFISH_STAGE_CONFIG env var
            args = mock_popen.call_args[0][0]
            assert any("GOLDFISH_STAGE_CONFIG" in str(arg) for arg in args)

    def test_launch_container_preserves_user_config_values(self, temp_dir):
        """Should preserve user config values (freeze_backbone, epochs) in GOLDFISH_STAGE_CONFIG.

        Regression test for bug where user config values from configs/{stage}.yaml
        were not being passed to the container - only stage/inputs/outputs were set.
        """
        import json

        # Setup - user config values that should be preserved
        executor = LocalExecutor()
        stage_config = {
            "stage": "train_film",
            "inputs": {"tokens": {"location": "/mnt/inputs/tokens"}},
            "outputs": {"model": {"type": "directory"}},
            # User config values that MUST be preserved
            "freeze_backbone": False,
            "epochs": 20,
            "lr": 3e-4,
            "wandb_project": "my-project",
        }

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            # Extract GOLDFISH_STAGE_CONFIG from docker args
            args = mock_popen.call_args[0][0]
            config_json = None
            for i, arg in enumerate(args):
                if arg == "-e" and i + 1 < len(args) and "GOLDFISH_STAGE_CONFIG=" in args[i + 1]:
                    config_json = args[i + 1].split("=", 1)[1]
                    break

            assert config_json is not None, "GOLDFISH_STAGE_CONFIG not found in docker args"
            parsed_config = json.loads(config_json)

            # Verify user config values are preserved
            assert parsed_config["freeze_backbone"] is False, "freeze_backbone should be False"
            assert parsed_config["epochs"] == 20, "epochs should be 20"
            assert parsed_config["lr"] == 3e-4, "lr should be preserved"
            assert parsed_config["wandb_project"] == "my-project", "wandb_project should be preserved"
            # Also verify standard fields are present
            assert parsed_config["stage"] == "train_film"
            assert "inputs" in parsed_config
            assert "outputs" in parsed_config

    def test_launch_container_sets_environment_vars_from_config(self, temp_dir):
        """Should set user-defined environment variables from config.environment section.

        Regression test for WANDB_API_KEY and similar env vars that need to be
        set directly as Docker env vars (not just in the JSON config).
        """
        # Setup - stage config with environment section
        executor = LocalExecutor()
        stage_config = {
            "stage": "train_film",
            "inputs": {},
            "outputs": {},
            "environment": {
                "WANDB_API_KEY": "fake-key-for-testing",
                "CUSTOM_VAR": "custom-value",
            },
        }

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            # Extract all -e flags from docker args
            args = mock_popen.call_args[0][0]
            env_vars = {}
            for i, arg in enumerate(args):
                if arg == "-e" and i + 1 < len(args):
                    env_val = args[i + 1]
                    if "=" in env_val:
                        name, value = env_val.split("=", 1)
                        env_vars[name] = value

            # Verify environment vars are set directly as Docker env vars
            assert "WANDB_API_KEY" in env_vars, "WANDB_API_KEY should be set as Docker env var"
            assert env_vars["WANDB_API_KEY"] == "fake-key-for-testing"
            assert "CUSTOM_VAR" in env_vars, "CUSTOM_VAR should be set as Docker env var"
            assert env_vars["CUSTOM_VAR"] == "custom-value"
            # Also verify GOLDFISH_STAGE_CONFIG is still set
            assert "GOLDFISH_STAGE_CONFIG" in env_vars

    def test_launch_container_sets_goldfish_env_vars(self, temp_dir):
        """Should set Goldfish environment variables for metrics and provenance."""
        # Setup
        executor = LocalExecutor()
        stage_config = {"stage": "train", "inputs": {}, "outputs": {}}

        # Goldfish environment variables (metrics, provenance, etc.)
        goldfish_env = {
            "GOLDFISH_PROJECT_NAME": "my-ml-project",
            "GOLDFISH_WORKSPACE": "baseline_lstm",
            "GOLDFISH_STAGE": "train",
            "GOLDFISH_RUN_ID": "stage-abc123",
            "GOLDFISH_GIT_SHA": "abc123def456",
            "GOLDFISH_OUTPUTS_DIR": "/mnt/outputs",
            "GOLDFISH_METRICS_BACKEND": "wandb",
            "GOLDFISH_WANDB_PROJECT": "my-wandb-project",
            "GOLDFISH_WANDB_GROUP": "baseline_lstm",
            "GOLDFISH_WANDB_ENTITY": "my-team",
            "WANDB_API_KEY": "fake-wandb-key",
        }

        # Mock subprocess
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            # Execute
            executor.launch_container(
                image_tag="goldfish-test_ws-v1",
                stage_run_id="stage-abc123",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
                goldfish_env=goldfish_env,
            )

            # Extract all -e flags from docker args
            args = mock_popen.call_args[0][0]
            env_vars = {}
            for i, arg in enumerate(args):
                if arg == "-e" and i + 1 < len(args):
                    env_val = args[i + 1]
                    if "=" in env_val:
                        name, value = env_val.split("=", 1)
                        env_vars[name] = value

            # Verify all Goldfish env vars are set
            assert env_vars["GOLDFISH_PROJECT_NAME"] == "my-ml-project"
            assert env_vars["GOLDFISH_WORKSPACE"] == "baseline_lstm"
            assert env_vars["GOLDFISH_STAGE"] == "train"
            assert env_vars["GOLDFISH_RUN_ID"] == "stage-abc123"
            assert env_vars["GOLDFISH_GIT_SHA"] == "abc123def456"
            assert env_vars["GOLDFISH_OUTPUTS_DIR"] == "/mnt/outputs"
            assert env_vars["GOLDFISH_METRICS_BACKEND"] == "wandb"
            assert env_vars["GOLDFISH_WANDB_PROJECT"] == "my-wandb-project"
            assert env_vars["GOLDFISH_WANDB_GROUP"] == "baseline_lstm"
            assert env_vars["GOLDFISH_WANDB_ENTITY"] == "my-team"
            assert env_vars["WANDB_API_KEY"] == "fake-wandb-key"


class TestContainerMonitoring:
    """Test monitoring running containers."""

    def test_get_container_status_running(self):
        """Should return status of running container."""
        # Setup
        executor = LocalExecutor()

        # Mock docker inspect
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='[{"State": {"Status": "running"}}]')

            # Execute
            status = executor.get_container_status("stage-abc123")

            # Verify
            assert status == "running"

    def test_get_container_status_completed(self):
        """Should return completed status with exit code."""
        # Setup
        executor = LocalExecutor()

        # Mock docker inspect
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='[{"State": {"Status": "exited", "ExitCode": 0}}]')

            # Execute
            status = executor.get_container_status("stage-abc123")

            # Verify
            assert status == "completed"

    def test_get_container_logs(self):
        """Should retrieve container logs."""
        # Setup
        executor = LocalExecutor()

        # Mock docker logs
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="Stage output\nProcessing...\nDone!")

            # Execute
            logs = executor.get_container_logs("stage-abc123")

            # Verify
            assert "Stage output" in logs
            assert "Processing..." in logs


class TestContainerCleanup:
    """Test container cleanup."""

    def test_stop_container(self):
        """Should stop running container."""
        # Setup
        executor = LocalExecutor()

        # Mock docker stop
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Execute
            executor.stop_container("stage-abc123")

            # Verify
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert "docker" in args
            assert "stop" in args
            assert "stage-abc123" in args

    def test_remove_container(self):
        """Should remove stopped container."""
        # Setup
        executor = LocalExecutor()

        # Mock docker rm
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Execute
            executor.remove_container("stage-abc123")

            # Verify
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert "docker" in args
            assert "rm" in args


class TestContainerResourceLimits:
    """Test configurable container resource limits."""

    def test_default_resource_limits(self, temp_dir):
        """Should use default resource limits when not specified."""
        executor = LocalExecutor()
        stage_config = {"stage": "test"}

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            executor.launch_container(
                image_tag="test:latest",
                stage_run_id="stage-test",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            args = mock_popen.call_args[0][0]
            args_str = " ".join(args)
            # Default limits
            assert "--memory" in args_str
            assert "--cpus" in args_str

    def test_custom_resource_limits(self, temp_dir):
        """Should use custom resource limits when specified."""
        executor = LocalExecutor(
            memory_limit="8g",
            cpu_limit="4.0",
            pids_limit=200,
        )
        stage_config = {"stage": "test"}

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            executor.launch_container(
                image_tag="test:latest",
                stage_run_id="stage-test",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            args = mock_popen.call_args[0][0]
            args_str = " ".join(args)
            assert "8g" in args_str
            assert "4.0" in args_str
            assert "200" in args_str

    def test_disable_resource_limits(self, temp_dir):
        """Should allow disabling resource limits entirely."""
        executor = LocalExecutor(
            memory_limit=None,
            cpu_limit=None,
            pids_limit=None,
        )
        stage_config = {"stage": "test"}

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            executor.launch_container(
                image_tag="test:latest",
                stage_run_id="stage-test",
                entrypoint_script="#!/bin/bash",
                stage_config=stage_config,
                work_dir=temp_dir,
            )

            args = mock_popen.call_args[0][0]
            args_str = " ".join(args)
            # When limits are None, these flags should not be present
            assert "--memory" not in args_str
            assert "--pids-limit" not in args_str
