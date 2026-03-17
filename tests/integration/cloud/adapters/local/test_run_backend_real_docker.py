"""Integration tests for LocalRunBackend with real Docker.

These tests verify that LocalRunBackend works with an actual Docker daemon.
They are skipped when Docker is not available.

Run with: pytest -m requires_docker
Or run all integration tests: make test-integration
"""

from __future__ import annotations

import shutil
import subprocess
from uuid import uuid4

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.contracts import RunSpec, RunStatus


def _make_stage_run_id() -> str:
    """Generate a valid stage run ID for testing."""
    return f"stage-{uuid4().hex[:12]}"


def _docker_available() -> bool:
    """Check if Docker daemon is available and responsive."""
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


# Skip entire module if Docker is not available
pytestmark = [
    pytest.mark.requires_docker,
    pytest.mark.skipif(not _docker_available(), reason="Docker daemon not available"),
]


class TestLocalRunBackendRealDocker:
    """Integration tests using real Docker containers."""

    def test_launch_and_complete_simple_container(self):
        """LocalRunBackend can launch a real container that runs to completion.

        This verifies the full lifecycle:
        1. launch() creates a real container
        2. get_status() reports RUNNING then COMPLETED
        3. get_logs() returns actual output
        4. cleanup() removes the container
        """
        backend = LocalRunBackend()
        stage_run_id = _make_stage_run_id()

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            stage_name="echo_test",
            image="alpine:latest",
            command=["echo", "hello from goldfish"],
            env={"TEST_VAR": "test_value"},
        )

        # Launch container
        handle = backend.launch(spec)

        try:
            assert handle.stage_run_id == stage_run_id
            assert handle.backend_type == "local"
            assert handle.backend_handle  # Should have container ID

            # Wait for completion (alpine echo is fast)
            final_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED},
                timeout=30.0,
            )

            assert final_status.status == RunStatus.COMPLETED
            assert final_status.exit_code == 0

            # Verify logs
            logs = backend.get_logs(handle, tail=100)
            assert "hello from goldfish" in logs

        finally:
            # Always cleanup
            backend.cleanup(handle)

        # Verify container was removed
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name=goldfish-{spec.stage_run_id}", "--format", "{{.ID}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.stdout.strip() == "", "Container should be removed after cleanup"

    def test_launch_container_with_nonzero_exit(self):
        """LocalRunBackend correctly reports FAILED status for non-zero exit code."""
        backend = LocalRunBackend()
        stage_run_id = _make_stage_run_id()

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            stage_name="fail_test",
            image="alpine:latest",
            command=["sh", "-c", "exit 42"],
            env={},
        )

        handle = backend.launch(spec)

        try:
            final_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED},
                timeout=30.0,
            )

            assert final_status.status == RunStatus.FAILED
            assert final_status.exit_code == 42

        finally:
            backend.cleanup(handle)

    def test_launch_container_with_multiline_script(self):
        """LocalRunBackend can execute multiline bash scripts.

        This tests the same scenario as StageExecutor's entrypoint scripts.
        Uses ubuntu:22.04 which has bash (unlike Alpine).
        """
        backend = LocalRunBackend()
        stage_run_id = _make_stage_run_id()

        script = """set -e
echo "Step 1: Starting"
echo "Step 2: Working"
echo "Step 3: Done"
exit 0
"""

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            stage_name="script_test",
            image="ubuntu:22.04",  # Use ubuntu which has bash
            command=["bash", "-c", script],  # Use bash -c to execute script
            env={},
        )

        handle = backend.launch(spec)

        try:
            final_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED},
                timeout=30.0,
            )

            assert final_status.status == RunStatus.COMPLETED
            assert final_status.exit_code == 0

            logs = backend.get_logs(handle, tail=100)
            assert "Step 1: Starting" in logs
            assert "Step 2: Working" in logs
            assert "Step 3: Done" in logs

        finally:
            backend.cleanup(handle)

    def test_terminate_running_container(self):
        """LocalRunBackend can terminate a running container."""
        backend = LocalRunBackend()
        stage_run_id = _make_stage_run_id()

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            stage_name="sleep_test",
            image="alpine:latest",
            command=["sleep", "300"],  # Sleep for 5 minutes (we'll kill it)
            env={},
        )

        handle = backend.launch(spec)

        try:
            # Wait for container to start running
            running_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.RUNNING},
                timeout=10.0,
            )
            assert running_status.status == RunStatus.RUNNING

            # Terminate it
            backend.terminate(handle)

            # Should now be terminated
            final_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED},
                timeout=15.0,
            )
            assert final_status.status in (RunStatus.TERMINATED, RunStatus.FAILED)

        finally:
            backend.cleanup(handle)

    def test_get_output_dir_returns_valid_path(self):
        """LocalRunBackend creates and returns output directory when output_uri specified."""
        from goldfish.cloud.contracts import StorageURI

        backend = LocalRunBackend()
        stage_run_id = _make_stage_run_id()

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            stage_name="output_test",
            image="alpine:latest",
            command=["sh", "-c", "echo 'test output' > /mnt/outputs/result.txt"],
            env={},
            output_uri=StorageURI("file", "", "/tmp/goldfish-test-outputs"),
        )

        handle = backend.launch(spec)

        try:
            # Wait for completion
            final_status = backend.wait_for_status(
                handle,
                target_statuses={RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED},
                timeout=30.0,
            )
            assert final_status.status == RunStatus.COMPLETED

            # Get output directory
            output_dir = backend.get_output_dir(handle)
            assert output_dir is not None
            assert output_dir.exists()

            # Verify output file was written
            result_file = output_dir / "result.txt"
            assert result_file.exists()
            assert "test output" in result_file.read_text()

        finally:
            backend.cleanup(handle)

    def test_capabilities_reflect_system_state(self):
        """LocalRunBackend capabilities reflect actual system capabilities."""
        backend = LocalRunBackend()
        caps = backend.capabilities

        # These should always be true for local backend
        assert caps.supports_preemption is True
        assert caps.supports_live_logs is True
        assert caps.supports_spot is False  # Local doesn't have real spot

        # GPU support depends on nvidia-docker runtime
        # We just verify it's a boolean (not that it's any specific value)
        assert isinstance(caps.supports_gpu, bool)
