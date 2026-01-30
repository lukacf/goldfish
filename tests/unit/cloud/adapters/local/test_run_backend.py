"""Unit tests for LocalRunBackend adapter.

Tests the local Docker implementation of the RunBackend protocol.
All Docker CLI calls are mocked.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.contracts import RunHandle, RunSpec, RunStatus

# --- Fixtures ---


@pytest.fixture
def backend():
    """Create a LocalRunBackend for testing."""
    return LocalRunBackend()


@pytest.fixture
def sample_handle():
    """Create a sample RunHandle for testing."""
    return RunHandle(
        stage_run_id="stage-abc123",
        backend_type="local",
        backend_handle="container123abc",
        zone="local-zone-1",
    )


# --- Get Logs Tests (with since parameter) ---


class TestLocalRunBackendGetLogs:
    """Tests for get_logs method with since parameter."""

    def test_get_logs_without_since_works(self, backend, sample_handle):
        """Get logs without since parameter works as before."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="Line 1\nLine 2\n",
                stderr="",
                returncode=0,
            )

            logs = backend.get_logs(sample_handle, tail=100)

            assert logs == "Line 1\nLine 2\n"
            cmd = mock_run.call_args[0][0]
            assert "--since" not in cmd

    def test_get_logs_with_since_passes_since_to_docker(self, backend, sample_handle):
        """Get logs with since parameter passes it to docker logs command."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="Recent line\n",
                stderr="",
                returncode=0,
            )

            logs = backend.get_logs(sample_handle, tail=100, since="2024-01-01T00:00:00Z")

            assert logs == "Recent line\n"
            cmd = mock_run.call_args[0][0]
            assert "--since" in cmd
            assert "2024-01-01T00:00:00Z" in cmd

    def test_get_logs_with_since_none_does_not_include_since_flag(self, backend, sample_handle):
        """Get logs with since=None does not include --since flag."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="All logs\n",
                stderr="",
                returncode=0,
            )

            backend.get_logs(sample_handle, tail=50, since=None)

            cmd = mock_run.call_args[0][0]
            assert "--since" not in cmd

    def test_get_logs_default_since_is_none(self, backend, sample_handle):
        """Get logs defaults since to None."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="logs",
                stderr="",
                returncode=0,
            )

            # Call without since parameter
            backend.get_logs(sample_handle)

            cmd = mock_run.call_args[0][0]
            assert "--since" not in cmd

    def test_get_logs_tail_zero_returns_all(self, backend, sample_handle):
        """Get logs with tail=0 returns all logs per protocol.

        Protocol documents tail=0 as 'return all logs'. Docker's --tail 0
        returns NO logs, so we must omit the --tail flag entirely when tail=0.
        """
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="All log lines here\n",
                stderr="",
                returncode=0,
            )

            logs = backend.get_logs(sample_handle, tail=0)

            assert logs == "All log lines here\n"
            cmd = mock_run.call_args[0][0]
            # When tail=0, --tail flag should NOT be present
            # (or should use a very large number)
            assert "--tail" not in cmd or "0" not in cmd[cmd.index("--tail") + 1]


# --- Get Zone Tests ---


class TestLocalRunBackendGetZone:
    """Tests for get_zone method."""

    def test_get_zone_returns_zone_from_handle(self, backend, sample_handle):
        """Get zone returns zone stored in RunHandle."""
        zone = backend.get_zone(sample_handle)
        assert zone == "local-zone-1"

    def test_get_zone_returns_none_when_handle_has_no_zone(self, backend):
        """Get zone returns None when handle has no zone."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="local",
            backend_handle="container456def",
            zone=None,
        )

        zone = backend.get_zone(handle)
        assert zone is None

    def test_get_zone_with_empty_zone_returns_none_equivalent(self, backend):
        """Get zone with empty string zone is falsy (implementation may vary)."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="local",
            backend_handle="container456def",
            zone="",
        )

        zone = backend.get_zone(handle)
        # Empty string is returned, caller can treat as falsy
        assert zone == ""


# --- Launch with Entrypoint Script Tests ---


class TestLocalRunBackendLaunchWithEntrypoint:
    """Tests for launch method with entrypoint scripts."""

    def test_launch_with_bash_wrapped_script(self, backend):
        """Launch with bash-wrapped script executes properly.

        When spec.command uses ["bash", "-c", script] format (as generated by
        StageExecutor._build_entrypoint_script), the backend passes it through
        to Docker correctly.

        Note: Callers are responsible for wrapping scripts with bash -c.
        LocalRunBackend passes commands through as-is.
        """
        multiline_script = """set -euo pipefail
echo "Running stage: train"
cd /app
python -m modules.train
"""
        spec = RunSpec(
            stage_run_id="stage-abc123def456",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["bash", "-c", multiline_script],  # Caller wraps with bash
            env={"GOLDFISH_STAGE": "train"},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_abc123",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            # Verify the docker command was called
            cmd = mock_run.call_args[0][0]

            # The command should include bash -c as specified
            assert "bash" in cmd, "bash should be passed through"
            assert "-c" in cmd, "-c flag should be passed through"

            # The actual script content should be in the command args
            assert any("echo" in str(arg) for arg in cmd), "Script content should be in command"
            assert any("python -m modules.train" in str(arg) for arg in cmd), "Script content should be preserved"

    def test_launch_with_simple_command_still_works(self, backend):
        """Launch with simple command (no script) still works as before.

        Simple commands like ["python", "-m", "train"] should be passed through
        without wrapping in bash.
        """
        spec = RunSpec(
            stage_run_id="stage-def789abc012",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "-m", "train"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_simple_id",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]
            cmd_str = " ".join(cmd)

            # Simple command should be passed through directly
            assert "python" in cmd_str
            assert "-m" in cmd_str
            assert "train" in cmd_str


# --- Launch Docker Command Tests ---


class TestLocalRunBackendLaunchDockerCommand:
    """Tests for launch method docker command construction."""

    def test_launch_builds_correct_docker_command_structure(self, backend):
        """Launch builds a properly structured docker run command."""
        spec = RunSpec(
            stage_run_id="stage-abc123def456",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={"KEY": "value"},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_123",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]

            # Must start with docker run
            assert cmd[0] == "docker"
            assert cmd[1] == "run"

            # Must have -d (detached)
            assert "-d" in cmd

            # Must have --name with correct format
            assert "--name" in cmd
            name_idx = cmd.index("--name")
            assert cmd[name_idx + 1] == "goldfish-stage-abc123def456"

    def test_launch_applies_resource_limits(self, backend):
        """Launch applies memory and CPU limits from spec."""
        spec = RunSpec(
            stage_run_id="stage-abc123def789",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
            memory_gb=16.0,
            cpu_count=8.0,
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_456",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]

            # Check memory limit
            assert "--memory" in cmd
            mem_idx = cmd.index("--memory")
            assert cmd[mem_idx + 1] == "16.0g"

            # Check CPU limit
            assert "--cpus" in cmd
            cpu_idx = cmd.index("--cpus")
            assert cmd[cpu_idx + 1] == "8.0"

    def test_launch_applies_default_resource_limits(self, backend):
        """Launch applies default memory (4g) and CPU (2.0) when not specified."""
        spec = RunSpec(
            stage_run_id="stage-abc123defabc",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
            # memory_gb and cpu_count not specified
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_789",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]

            # Check default memory limit
            assert "--memory" in cmd
            mem_idx = cmd.index("--memory")
            assert cmd[mem_idx + 1] == "4.0g"

            # Check default CPU limit
            assert "--cpus" in cmd
            cpu_idx = cmd.index("--cpus")
            assert cmd[cpu_idx + 1] == "2.0"

    def test_launch_passes_all_env_vars(self, backend):
        """Launch passes all environment variables to docker command."""
        spec = RunSpec(
            stage_run_id="stage-abc123defeee",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={
                "EPOCHS": "100",
                "BATCH_SIZE": "32",
                "LEARNING_RATE": "0.001",
                "MODEL_NAME": "transformer",
            },
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_env",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]
            cmd_str = " ".join(cmd)

            # All env vars should be present with -e flag
            assert "-e EPOCHS=100" in cmd_str
            assert "-e BATCH_SIZE=32" in cmd_str
            assert "-e LEARNING_RATE=0.001" in cmd_str
            assert "-e MODEL_NAME=transformer" in cmd_str

    def test_launch_passes_timeout_as_env_var(self, backend):
        """Launch passes timeout_seconds as GOLDFISH_TIMEOUT env var."""
        spec = RunSpec(
            stage_run_id="stage-abc123deffff",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
            timeout_seconds=3600,
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_timeout",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]
            cmd_str = " ".join(cmd)

            assert "-e GOLDFISH_TIMEOUT=3600" in cmd_str

    def test_launch_applies_security_hardening(self, backend):
        """Launch applies pids-limit and non-root user for security."""
        spec = RunSpec(
            stage_run_id="stage-abc123defddd",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_security",
                stderr="",
                returncode=0,
            )

            backend.launch(spec)

            cmd = mock_run.call_args[0][0]

            # Check pids-limit
            assert "--pids-limit" in cmd
            pids_idx = cmd.index("--pids-limit")
            assert cmd[pids_idx + 1] == "100"

            # Check non-root user
            assert "--user" in cmd
            user_idx = cmd.index("--user")
            assert cmd[user_idx + 1] == "1000:1000"

    def test_launch_returns_run_handle_with_zone(self, backend):
        """Launch returns RunHandle with selected zone."""
        spec = RunSpec(
            stage_run_id="stage-abc123defccc",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_abc123",
                stderr="",
                returncode=0,
            )

            handle = backend.launch(spec)

            assert handle.stage_run_id == "stage-abc123defccc"
            assert handle.backend_type == "local"
            assert handle.backend_handle == "container_abc123"
            assert handle.zone == "local-zone-1"  # Default zone


# --- Launch Zone Availability Tests ---


class TestLocalRunBackendLaunchZoneAvailability:
    """Tests for launch method zone availability simulation."""

    def test_launch_raises_capacity_error_when_no_zones_available(self):
        """Launch raises CapacityError when all zones are unavailable."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        # Create backend with all zones unavailable
        mock_config = MagicMock()
        mock_config.docker_socket = "/var/run/docker.sock"
        mock_config.simulate_preemption_after_seconds = None
        mock_config.preemption_grace_period_seconds = 30
        mock_config.zone_availability = {
            "zone-1": False,
            "zone-2": False,
            "zone-3": False,
        }

        backend = LocalRunBackend(config=mock_config)

        spec = RunSpec(
            stage_run_id="stage-abc123defbbb",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        from goldfish.errors import CapacityError

        with pytest.raises(CapacityError) as exc_info:
            backend.launch(spec)

        assert "No zones available" in str(exc_info.value)

    def test_launch_selects_first_available_zone(self):
        """Launch selects the first available zone in order."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        mock_config = MagicMock()
        mock_config.docker_socket = "/var/run/docker.sock"
        mock_config.simulate_preemption_after_seconds = None
        mock_config.preemption_grace_period_seconds = 30
        mock_config.zone_availability = {
            "zone-1": False,  # unavailable
            "zone-2": True,  # first available
            "zone-3": True,  # also available
        }

        backend = LocalRunBackend(config=mock_config)

        spec = RunSpec(
            stage_run_id="stage-abc123defaaa",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_zone_id",
                stderr="",
                returncode=0,
            )

            handle = backend.launch(spec)

            # Should select zone-2 (first available)
            assert handle.zone == "zone-2"


# --- Launch Error Handling Tests ---


class TestLocalRunBackendLaunchErrors:
    """Tests for launch method error handling."""

    def test_launch_raises_launch_error_on_docker_failure(self, backend):
        """Launch raises LaunchError when docker command fails."""
        spec = RunSpec(
            stage_run_id="stage-abc123def999",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=125,
                cmd=["docker", "run"],
                stderr="Error: image not found",
            )

            from goldfish.errors import LaunchError

            with pytest.raises(LaunchError) as exc_info:
                backend.launch(spec)

            assert "Failed to launch container" in str(exc_info.value)

    def test_launch_raises_launch_error_when_docker_not_installed(self, backend):
        """Launch raises LaunchError when docker command is not found.

        FileNotFoundError is raised by subprocess when the executable doesn't exist.
        This should be wrapped in LaunchError with a helpful message.
        """
        spec = RunSpec(
            stage_run_id="stage-abc123def001",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("[Errno 2] No such file or directory: 'docker'")

            from goldfish.errors import LaunchError

            with pytest.raises(LaunchError) as exc_info:
                backend.launch(spec)

            assert "docker" in str(exc_info.value).lower()
            assert exc_info.value.details.get("stage_run_id") == "stage-abc123def001"
            assert exc_info.value.details.get("cause") == "docker_not_found"

    def test_launch_raises_launch_error_on_permission_denied(self, backend):
        """Launch raises LaunchError when docker command has permission issues.

        OSError/PermissionError is raised when user lacks permission to run docker
        (e.g., not in docker group). This should be wrapped in LaunchError.
        """
        spec = RunSpec(
            stage_run_id="stage-abc123def002",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
        )

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = PermissionError("[Errno 13] Permission denied: '/var/run/docker.sock'")

            from goldfish.errors import LaunchError

            with pytest.raises(LaunchError) as exc_info:
                backend.launch(spec)

            assert "permission" in str(exc_info.value).lower()
            assert exc_info.value.details.get("stage_run_id") == "stage-abc123def002"
            assert exc_info.value.details.get("cause") == "docker_permission_denied"


# --- Get Status Tests ---


class TestLocalRunBackendGetStatus:
    """Tests for get_status method with all Docker states."""

    def test_get_status_running_returns_running(self, backend, sample_handle):
        """Running container returns RUNNING status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="running:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.RUNNING

    def test_get_status_exited_success_returns_completed(self, backend, sample_handle):
        """Exited container with exit code 0 returns COMPLETED."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="exited:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.COMPLETED
            assert status.exit_code == 0

    def test_get_status_exited_failure_returns_failed(self, backend, sample_handle):
        """Exited container with non-zero exit code returns FAILED."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="exited:1",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.FAILED
            assert status.exit_code == 1

    def test_get_status_exited_oom_returns_terminated(self, backend, sample_handle):
        """Exited container with OOM exit code 137 returns TERMINATED."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="exited:137",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.TERMINATED
            assert status.exit_code == 137

    def test_get_status_created_returns_preparing(self, backend, sample_handle):
        """Created container returns PREPARING status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="created:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.PREPARING

    def test_get_status_dead_returns_terminated(self, backend, sample_handle):
        """Dead container returns TERMINATED status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="dead:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.TERMINATED

    def test_get_status_removing_returns_terminated(self, backend, sample_handle):
        """Removing container returns TERMINATED status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="removing:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.TERMINATED

    def test_get_status_paused_returns_running(self, backend, sample_handle):
        """Paused container returns RUNNING status (can resume)."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="paused:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.RUNNING

    def test_get_status_restarting_returns_running(self, backend, sample_handle):
        """Restarting container returns RUNNING status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="restarting:0",
                stderr="",
                returncode=0,
            )

            status = backend.get_status(sample_handle)

            assert status.status == RunStatus.RUNNING

    def test_get_status_not_found_raises_error(self, backend, sample_handle):
        """Non-existent container raises NotFoundError."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["docker", "inspect"],
            )

            from goldfish.errors import NotFoundError

            with pytest.raises(NotFoundError):
                backend.get_status(sample_handle)


# --- Terminate Tests ---


class TestLocalRunBackendTerminate:
    """Tests for terminate method."""

    def test_terminate_calls_docker_stop(self, backend, sample_handle):
        """Terminate calls docker stop with 10s timeout."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            backend.terminate(sample_handle)

            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "docker"
            assert cmd[1] == "stop"
            assert "-t" in cmd
            assert "10" in cmd
            assert sample_handle.backend_handle in cmd

    def test_terminate_ignores_already_stopped(self, backend, sample_handle):
        """Terminate doesn't raise if container already stopped."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["docker", "stop"],
            )

            # Should not raise
            backend.terminate(sample_handle)


# --- Cleanup Tests ---


class TestLocalRunBackendCleanup:
    """Tests for cleanup method."""

    def test_cleanup_calls_docker_rm(self, backend, sample_handle):
        """Cleanup calls docker rm -f."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            backend.cleanup(sample_handle)

            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "docker"
            assert cmd[1] == "rm"
            assert "-f" in cmd
            assert sample_handle.backend_handle in cmd

    def test_cleanup_ignores_already_removed(self, backend, sample_handle):
        """Cleanup doesn't raise if container already removed."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["docker", "rm"],
            )

            # Should not raise
            backend.cleanup(sample_handle)


# --- Capabilities Tests ---


class TestLocalRunBackendCapabilities:
    """Tests for capabilities property."""

    def test_capabilities_returns_backend_capabilities(self, backend):
        """Capabilities returns BackendCapabilities dataclass."""
        from goldfish.cloud.contracts import BackendCapabilities

        caps = backend.capabilities
        assert isinstance(caps, BackendCapabilities)

    def test_capabilities_supports_preemption(self, backend):
        """Local backend always supports preemption (SIGTERM)."""
        assert backend.capabilities.supports_preemption is True

    def test_capabilities_supports_live_logs(self, backend):
        """Local backend supports live logs via docker logs."""
        assert backend.capabilities.supports_live_logs is True

    def test_capabilities_does_not_support_spot(self, backend):
        """Local backend doesn't support real spot pricing."""
        assert backend.capabilities.supports_spot is False

    def test_capabilities_does_not_support_metrics(self, backend):
        """Local backend doesn't support metrics collection."""
        assert backend.capabilities.supports_metrics is False

    def test_capabilities_no_max_run_duration(self, backend):
        """Local backend has no max run duration limit."""
        assert backend.capabilities.max_run_duration_hours is None

    def test_capabilities_preemption_detection_when_configured(self):
        """Preemption detection is True when simulation is configured."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        mock_config = MagicMock()
        mock_config.docker_socket = "/var/run/docker.sock"
        mock_config.simulate_preemption_after_seconds = 60  # Configured
        mock_config.preemption_grace_period_seconds = 30
        mock_config.zone_availability = {"local-zone-1": True}

        backend = LocalRunBackend(config=mock_config)

        assert backend.capabilities.supports_preemption_detection is True

    def test_capabilities_no_preemption_detection_when_not_configured(self, backend):
        """Preemption detection is False when simulation not configured."""
        assert backend.capabilities.supports_preemption_detection is False

    def test_capabilities_gpu_detection(self, backend):
        """GPU support depends on nvidia runtime availability."""
        with patch.object(backend, "_check_nvidia_runtime", return_value=True):
            assert backend.capabilities.supports_gpu is True

        with patch.object(backend, "_check_nvidia_runtime", return_value=False):
            assert backend.capabilities.supports_gpu is False


# --- Get Output Dir Tests ---


class TestLocalRunBackendGetOutputDir:
    """Tests for get_output_dir method."""

    def test_get_output_dir_returns_none_when_no_output(self, backend, sample_handle):
        """Get output dir returns None when no output was configured."""
        result = backend.get_output_dir(sample_handle)
        assert result is None

    def test_get_output_dir_returns_path_after_launch(self, backend):
        """Get output dir returns path after launch with output_uri."""
        from goldfish.cloud.contracts import StorageURI

        spec = RunSpec(
            stage_run_id="stage-abc123def888",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
            output_uri=StorageURI("file", "", "/tmp/outputs"),
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_output_id",
                stderr="",
                returncode=0,
            )

            handle = backend.launch(spec)

            # Output dir should now be available
            output_dir = backend.get_output_dir(handle)
            assert output_dir is not None
            assert output_dir.exists()

            # Cleanup
            import shutil

            shutil.rmtree(output_dir, ignore_errors=True)


# --- Input/Output Mounting Tests ---


class TestLocalRunBackendMounting:
    """Tests for input/output volume mounting."""

    def test_launch_mounts_output_directory(self, backend):
        """Launch mounts output directory as read-write at /mnt/outputs."""
        from goldfish.cloud.contracts import StorageURI

        spec = RunSpec(
            stage_run_id="stage-abc123def777",
            workspace_name="test_workspace",
            stage_name="train",
            image="goldfish-test:latest",
            command=["python", "train.py"],
            env={},
            output_uri=StorageURI("file", "", "/tmp/outputs"),
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_mount_id",
                stderr="",
                returncode=0,
            )

            handle = backend.launch(spec)

            cmd = mock_run.call_args[0][0]
            cmd_str = " ".join(cmd)

            # Should have volume mount for outputs
            assert "-v" in cmd
            assert "/mnt/outputs:rw" in cmd_str

            # Should set GOLDFISH_OUTPUT_DIR env var
            assert "GOLDFISH_OUTPUT_DIR=/mnt/outputs" in cmd_str

            # Cleanup output dir
            output_dir = backend.get_output_dir(handle)
            if output_dir and output_dir.exists():
                import shutil

                shutil.rmtree(output_dir, ignore_errors=True)
