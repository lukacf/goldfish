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
