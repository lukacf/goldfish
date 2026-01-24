"""Unit tests for GCERunBackend adapter.

Tests the GCE implementation of the RunBackend protocol.
All GCE/GCELauncher calls are mocked.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
from goldfish.cloud.contracts import (
    BackendCapabilities,
    BackendStatus,
    RunHandle,
    RunSpec,
    RunStatus,
    StorageURI,
)
from goldfish.errors import CapacityError, LaunchError, NotFoundError
from goldfish.state_machine.types import StageState
from goldfish.validation import ValidationError

# --- Fixtures ---


@pytest.fixture
def mock_launcher():
    """Create a mock GCELauncher."""
    with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        mock_instance.default_zone = "us-central1-a"
        yield mock_instance


@pytest.fixture
def backend(mock_launcher):
    """Create a GCERunBackend with mocked GCELauncher."""
    return GCERunBackend(
        project_id="test-project",
        zones=["us-central1-a", "us-central1-b"],
        bucket="test-bucket",
        gpu_preference=["nvidia-tesla-t4"],
        service_account="test@test.iam.gserviceaccount.com",
    )


@pytest.fixture
def sample_run_spec():
    """Create a sample RunSpec for testing."""
    return RunSpec(
        stage_run_id="stage-abc123",
        workspace_name="test-workspace",
        stage_name="train",
        image="gcr.io/test-project/test-image:latest",
        command=["python", "train.py"],
        env={"EPOCHS": "10", "BATCH_SIZE": "32"},
        profile="gpu-small",
        gpu_count=1,
        memory_gb=8.0,
        cpu_count=4.0,
        inputs={"dataset": StorageURI("gs", "test-bucket", "data/input.npy")},
        output_uri=StorageURI("gs", "test-bucket", "outputs/stage-abc123"),
        spot=True,
        timeout_seconds=3600,
    )


@pytest.fixture
def sample_handle():
    """Create a sample RunHandle for testing."""
    return RunHandle(
        stage_run_id="stage-abc123",
        backend_type="gce",
        backend_handle="goldfish-stage-abc123",
        zone="us-central1-a",
    )


# --- Initialization Tests ---


class TestGCERunBackendInit:
    """Tests for GCERunBackend initialization."""

    def test_init_with_all_params_passes_to_launcher(self, mock_launcher):
        """Verify all init params are passed to GCELauncher."""
        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher") as mock_class:
            GCERunBackend(
                project_id="my-project",
                zones=["us-east1-a", "us-east1-b"],
                bucket="my-bucket",
                gpu_preference=["nvidia-a100-80gb"],
                service_account="sa@proj.iam.gserviceaccount.com",
            )

            mock_class.assert_called_once_with(
                project_id="my-project",
                zone="us-east1-a",  # First zone becomes default
                bucket="my-bucket",
                zones=["us-east1-a", "us-east1-b"],
                gpu_preference=["nvidia-a100-80gb"],
                service_account="sa@proj.iam.gserviceaccount.com",
            )

    def test_init_with_no_zones_uses_default(self, mock_launcher):
        """When no zones specified, default zone is used."""
        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher") as mock_class:
            GCERunBackend(project_id="test")

            call_args = mock_class.call_args
            assert call_args.kwargs["zone"] == "us-central1-a"

    def test_init_with_none_params_uses_defaults(self, mock_launcher):
        """Backend can be created with None values for optional params."""
        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            backend = GCERunBackend()
            assert backend._project_id is None


# --- Capabilities Tests ---


class TestGCERunBackendCapabilities:
    """Tests for capabilities property."""

    def test_capabilities_returns_backend_capabilities(self, backend):
        """Capabilities returns BackendCapabilities dataclass."""
        caps = backend.capabilities
        assert isinstance(caps, BackendCapabilities)

    def test_capabilities_supports_gpu_with_preference(self, backend):
        """GPU support is True when gpu_preference is configured."""
        caps = backend.capabilities
        assert caps.supports_gpu is True

    def test_capabilities_supports_gpu_with_zones(self, mock_launcher):
        """GPU support is True when zones are configured (even without gpu_preference)."""
        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            backend = GCERunBackend(zones=["us-central1-a"])
            caps = backend.capabilities
            assert caps.supports_gpu is True

    def test_capabilities_supports_gpu_always(self, mock_launcher):
        """GPU support is always True for GCE backend.

        GCE supports GPUs as a platform regardless of configuration.
        The supports_gpu capability reflects what the backend CAN do,
        not what the current run configuration requests.
        """
        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            backend = GCERunBackend()
            backend._zones = []  # No zones configured
            backend._gpu_preference = None  # No GPU preference
            caps = backend.capabilities
            # GCE always supports GPUs as a capability
            assert caps.supports_gpu is True

    def test_capabilities_supports_spot(self, backend):
        """GCE supports spot/preemptible instances."""
        assert backend.capabilities.supports_spot is True

    def test_capabilities_supports_preemption(self, backend):
        """GCE supports graceful preemption handling."""
        assert backend.capabilities.supports_preemption is True

    def test_capabilities_supports_preemption_detection(self, backend):
        """GCE supports preemption detection via metadata."""
        assert backend.capabilities.supports_preemption_detection is True

    def test_capabilities_supports_live_logs(self, backend):
        """GCE supports live log streaming via GCS."""
        assert backend.capabilities.supports_live_logs is True

    def test_capabilities_supports_metrics(self, backend):
        """GCE supports metrics collection."""
        assert backend.capabilities.supports_metrics is True

    def test_capabilities_max_run_duration_is_24h(self, backend):
        """GCE max run duration is 24 hours."""
        assert backend.capabilities.max_run_duration_hours == 24


# --- Launch Tests ---


class TestGCERunBackendLaunch:
    """Tests for launch method."""

    @dataclass
    class MockLaunchResult:
        """Mock result from GCELauncher.launch_instance."""

        instance_name: str
        zone: str

    def test_launch_returns_run_handle(self, backend, mock_launcher, sample_run_spec):
        """Successful launch returns a RunHandle."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        handle = backend.launch(sample_run_spec)

        assert isinstance(handle, RunHandle)
        assert handle.stage_run_id == "stage-abc123"
        assert handle.backend_type == "gce"
        assert handle.backend_handle == "goldfish-stage-abc123"
        assert handle.zone == "us-central1-a"

    def test_launch_passes_image_to_launcher(self, backend, mock_launcher, sample_run_spec):
        """Launch passes correct image tag to GCELauncher."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["image_tag"] == "gcr.io/test-project/test-image:latest"

    def test_launch_passes_stage_run_id(self, backend, mock_launcher, sample_run_spec):
        """Launch passes stage_run_id to GCELauncher."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["stage_run_id"] == "stage-abc123"

    def test_launch_builds_entrypoint_from_command(self, backend, mock_launcher, sample_run_spec):
        """Launch builds entrypoint script from command list."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["entrypoint_script"] == "python train.py"

    def test_launch_preserves_command_quoting(self, backend, mock_launcher):
        """Launch properly quotes command args with spaces using shlex.join.

        A command like ["python", "script.py", "arg with spaces"] must be
        properly quoted so it doesn't become "python script.py arg with spaces"
        which breaks argument boundaries.
        """
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            command=["python", "script.py", "arg with spaces", "--flag=value with space"],
        )

        backend.launch(spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        entrypoint = call_kwargs["entrypoint_script"]

        # shlex.join properly quotes args with spaces
        # Result should be: python script.py 'arg with spaces' '--flag=value with space'
        assert "arg with spaces" in entrypoint
        assert "--flag=value with space" in entrypoint
        # The key test: spaces within args must be quoted, not treated as separators
        # shlex.join produces: python script.py 'arg with spaces' '--flag=value with space'
        assert entrypoint == "python script.py 'arg with spaces' '--flag=value with space'"

    def test_launch_with_no_command_uses_echo(self, backend, mock_launcher, sample_run_spec):
        """Launch with no command uses echo as fallback."""
        sample_run_spec.command = None
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["entrypoint_script"] == "echo 'No command'"

    def test_launch_passes_environment_variables(self, backend, mock_launcher, sample_run_spec):
        """Launch passes env vars to GCELauncher."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["goldfish_env"]["EPOCHS"] == "10"
        assert call_kwargs["goldfish_env"]["BATCH_SIZE"] == "32"

    def test_launch_passes_gpu_count(self, backend, mock_launcher, sample_run_spec):
        """Launch passes GPU count to GCELauncher."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["gpu_count"] == 1
        # Default gpu_type when not specified in RunSpec
        assert call_kwargs["gpu_type"] == "nvidia-tesla-t4"

    def test_launch_respects_gpu_type_from_profile(self, backend, mock_launcher):
        """Launch passes gpu_type from RunSpec to GCELauncher.

        This tests the fix for the GCE profile/resource selection regression
        where GPU type was hardcoded to nvidia-tesla-t4 instead of using
        the profile-specified accelerator (H100, A100, etc.).
        """
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-h100test",
            zone="us-central1-a",
        )

        # Test H100 GPU type
        spec_h100 = RunSpec(
            stage_run_id="stage-h100test",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            profile="h100-spot",
            gpu_count=1,
            gpu_type="nvidia-h100-80gb",  # Profile-specified GPU type
        )

        backend.launch(spec_h100)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["gpu_count"] == 1
        assert call_kwargs["gpu_type"] == "nvidia-h100-80gb"

        # Test A100 GPU type
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-a100test",
            zone="us-central1-a",
        )

        spec_a100 = RunSpec(
            stage_run_id="stage-a100test",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            profile="a100-spot",
            gpu_count=1,
            gpu_type="nvidia-tesla-a100",  # Profile-specified GPU type
        )

        backend.launch(spec_a100)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["gpu_count"] == 1
        assert call_kwargs["gpu_type"] == "nvidia-tesla-a100"

        # Test T4 GPU type (existing behavior)
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-t4test",
            zone="us-central1-a",
        )

        spec_t4 = RunSpec(
            stage_run_id="stage-t4test",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            profile="gpu-small",
            gpu_count=1,
            gpu_type="nvidia-tesla-t4",  # Profile-specified GPU type
        )

        backend.launch(spec_t4)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["gpu_count"] == 1
        assert call_kwargs["gpu_type"] == "nvidia-tesla-t4"

    def test_launch_with_no_gpu_sets_zero_count(self, backend, mock_launcher, sample_run_spec):
        """Launch with no GPU sets gpu_count=0."""
        sample_run_spec.gpu_count = 0
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["gpu_count"] == 0
        assert call_kwargs["gpu_type"] is None

    def test_launch_maps_cpu_count_to_machine_type(self, backend, mock_launcher, sample_run_spec):
        """Launch maps CPU count to appropriate machine type (CPU-only, no GPU)."""
        test_cases = [
            (2, "n1-standard-2"),
            (4, "n1-standard-4"),
            (8, "n1-standard-8"),
            (16, "n1-standard-16"),
        ]

        for cpu_count, expected_machine_type in test_cases:
            # Set gpu_count=0 to test CPU-only machine type mapping
            # (default fixture has gpu_count=1 which triggers GPU mapping)
            sample_run_spec.gpu_count = 0
            sample_run_spec.cpu_count = cpu_count
            mock_launcher.launch_instance.return_value = self.MockLaunchResult(
                instance_name="goldfish-stage-abc123",
                zone="us-central1-a",
            )

            backend.launch(sample_run_spec)

            call_kwargs = mock_launcher.launch_instance.call_args.kwargs
            assert call_kwargs["machine_type"] == expected_machine_type

    def test_launch_passes_spot_preference(self, backend, mock_launcher, sample_run_spec):
        """Launch passes spot/preemptible preference."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["preemptible"] is True

    def test_launch_passes_zones_list(self, backend, mock_launcher, sample_run_spec):
        """Launch passes zones list for multi-zone capacity search."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert call_kwargs["zones"] == ["us-central1-a", "us-central1-b"]

    def test_launch_raises_capacity_error_on_quota(self, backend, mock_launcher, sample_run_spec):
        """Launch raises CapacityError when quota is exhausted."""
        mock_launcher.launch_instance.side_effect = Exception("Quota 'GPUS_ALL_REGIONS' exceeded")

        with pytest.raises(CapacityError) as exc_info:
            backend.launch(sample_run_spec)

        assert "No capacity available" in str(exc_info.value)
        assert exc_info.value.details.get("zones_tried") == ["us-central1-a", "us-central1-b"]

    def test_launch_raises_capacity_error_on_resources_message(self, backend, mock_launcher, sample_run_spec):
        """Launch raises CapacityError when resources message contains 'capacity'."""
        mock_launcher.launch_instance.side_effect = Exception(
            "Zone has insufficient capacity for the requested resources"
        )

        with pytest.raises(CapacityError):
            backend.launch(sample_run_spec)

    def test_launch_raises_capacity_error_on_exhausted_message(self, backend, mock_launcher, sample_run_spec):
        """Launch raises CapacityError when resources are exhausted."""
        mock_launcher.launch_instance.side_effect = Exception("Resources exhausted in zone")

        with pytest.raises(CapacityError):
            backend.launch(sample_run_spec)

    def test_launch_raises_launch_error_on_other_failure(self, backend, mock_launcher, sample_run_spec):
        """Launch raises LaunchError for non-capacity failures."""
        mock_launcher.launch_instance.side_effect = Exception("Permission denied")

        with pytest.raises(LaunchError) as exc_info:
            backend.launch(sample_run_spec)

        assert "Failed to launch GCE instance" in str(exc_info.value)
        assert exc_info.value.details.get("stage_run_id") == "stage-abc123"
        assert exc_info.value.details.get("cause") == "gce_error"


# --- Get Status Tests ---


class TestGCERunBackendGetStatus:
    """Tests for get_status method."""

    def test_get_status_running_returns_running(self, backend, mock_launcher, sample_handle):
        """Running instance returns RUNNING status."""
        mock_launcher.get_instance_status.return_value = StageState.RUNNING

        status = backend.get_status(sample_handle)

        assert isinstance(status, BackendStatus)
        assert status.status == RunStatus.RUNNING

    def test_get_status_completed_returns_completed_with_exit_0(self, backend, mock_launcher, sample_handle):
        """Completed instance returns COMPLETED with exit_code=0."""
        mock_launcher.get_instance_status.return_value = StageState.COMPLETED

        status = backend.get_status(sample_handle)

        assert status.status == RunStatus.COMPLETED
        assert status.exit_code == 0

    def test_get_status_failed_checks_exit_code(self, backend, mock_launcher, sample_handle):
        """Failed instance checks exit code for details."""

        @dataclass
        class ExitResult:
            exists: bool
            code: int | None

        mock_launcher.get_instance_status.return_value = StageState.FAILED
        mock_launcher._get_exit_code.return_value = ExitResult(exists=True, code=137)

        status = backend.get_status(sample_handle)

        assert status.status == RunStatus.TERMINATED
        assert status.exit_code == 137

    def test_get_status_failed_without_exit_code_returns_exit_1(self, backend, mock_launcher, sample_handle):
        """Failed instance without exit code defaults to exit_code=1."""

        @dataclass
        class ExitResult:
            exists: bool
            code: int | None

        mock_launcher.get_instance_status.return_value = StageState.FAILED
        mock_launcher._get_exit_code.return_value = ExitResult(exists=False, code=None)

        status = backend.get_status(sample_handle)

        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1

    def test_get_status_not_found_raises_not_found_error(self, backend, mock_launcher, sample_handle):
        """Not found instance raises NotFoundError."""
        mock_launcher.get_instance_status.return_value = "not_found"
        # Mock _get_exit_code to return no exit code found (instance never ran)
        exit_result = MagicMock()
        exit_result.exists = False
        exit_result.code = None
        mock_launcher._get_exit_code.return_value = exit_result

        with pytest.raises(NotFoundError) as exc_info:
            backend.get_status(sample_handle)

        assert "instance:goldfish-stage-abc123" in str(exc_info.value)

    def test_get_status_unknown_state_returns_running(self, backend, mock_launcher, sample_handle):
        """Unknown state is treated as RUNNING."""
        mock_launcher.get_instance_status.return_value = "some_unknown_state"

        status = backend.get_status(sample_handle)

        assert status.status == RunStatus.RUNNING

    def test_get_status_exception_with_not_found_raises_not_found_error(self, backend, mock_launcher, sample_handle):
        """Exception containing 'not found' raises NotFoundError."""
        mock_launcher.get_instance_status.side_effect = Exception("Instance not found in zone")

        with pytest.raises(NotFoundError):
            backend.get_status(sample_handle)

    def test_get_status_returns_unknown_on_api_error(self, backend, mock_launcher, sample_handle):
        """API errors return UNKNOWN status with descriptive message (not RUNNING).

        Returning RUNNING on API errors can cause stuck runs because the state machine
        will keep polling forever. UNKNOWN signals uncertainty and allows proper handling.
        """
        mock_launcher.get_instance_status.side_effect = Exception("Network timeout")

        status = backend.get_status(sample_handle)

        assert status.status == RunStatus.UNKNOWN
        assert status.message is not None
        assert "Network timeout" in status.message


# --- Get Logs Tests ---


class TestGCERunBackendGetLogs:
    """Tests for get_logs method."""

    def test_get_logs_returns_log_content(self, backend, mock_launcher, sample_handle):
        """Get logs returns log content from launcher."""
        mock_launcher.get_instance_logs.return_value = "Line 1\nLine 2\nLine 3"

        logs = backend.get_logs(sample_handle)

        assert logs == "Line 1\nLine 2\nLine 3"

    def test_get_logs_passes_tail_count(self, backend, mock_launcher, sample_handle):
        """Get logs passes tail count to launcher."""
        mock_launcher.get_instance_logs.return_value = "last line"

        backend.get_logs(sample_handle, tail=50)

        mock_launcher.get_instance_logs.assert_called_once_with(
            instance_name="goldfish-stage-abc123",
            tail_lines=50,
            since=None,
        )

    def test_get_logs_passes_since_parameter(self, backend, mock_launcher, sample_handle):
        """Get logs passes since parameter to launcher."""
        mock_launcher.get_instance_logs.return_value = "recent logs"

        backend.get_logs(sample_handle, tail=100, since="2024-01-01T00:00:00Z")

        mock_launcher.get_instance_logs.assert_called_once_with(
            instance_name="goldfish-stage-abc123",
            tail_lines=100,
            since="2024-01-01T00:00:00Z",
        )

    def test_get_logs_with_since_none_does_not_filter(self, backend, mock_launcher, sample_handle):
        """Get logs with since=None passes None to launcher (no filtering)."""
        mock_launcher.get_instance_logs.return_value = "all logs"

        backend.get_logs(sample_handle, tail=200, since=None)

        call_kwargs = mock_launcher.get_instance_logs.call_args.kwargs
        assert call_kwargs["since"] is None

    def test_get_logs_with_zero_tail_passes_none(self, backend, mock_launcher, sample_handle):
        """Get logs with tail=0 passes None (no tail limit)."""
        mock_launcher.get_instance_logs.return_value = "all logs"

        backend.get_logs(sample_handle, tail=0)

        mock_launcher.get_instance_logs.assert_called_once_with(
            instance_name="goldfish-stage-abc123",
            tail_lines=None,
            since=None,
        )

    def test_get_logs_includes_error_on_failure(self, backend, mock_launcher, sample_handle):
        """Get logs returns descriptive error message on failure (not empty string).

        Returning empty string hides issues from the caller. Including an error
        message in the logs allows debugging and proper error visibility.
        """
        mock_launcher.get_instance_logs.side_effect = Exception("Failed to fetch logs from GCS")

        logs = backend.get_logs(sample_handle)

        assert logs != ""  # Not empty
        assert "error" in logs.lower() or "Error" in logs  # Contains error indicator
        assert "Failed to fetch logs from GCS" in logs  # Contains actual error message


# --- Terminate Tests ---


class TestGCERunBackendTerminate:
    """Tests for terminate method.

    Terminate delegates to GCELauncher.delete_instance which handles:
    - Zone lookup via _find_instance_zone() if needed
    - Idempotency (no error if already deleted)
    - Project ID configuration
    - Proper logging
    """

    def test_terminate_delegates_to_launcher(self, backend, mock_launcher, sample_handle):
        """Terminate delegates to GCELauncher.delete_instance for consolidated logic.

        GCELauncher.delete_instance handles zone lookup, idempotency, and logging
        in one place. This test ensures terminate uses the launcher method instead
        of duplicating the gcloud command logic.
        """
        backend.terminate(sample_handle)

        # Should call launcher's delete_instance method with instance name
        mock_launcher.delete_instance.assert_called_once_with("goldfish-stage-abc123")

    def test_terminate_passes_correct_instance_name(self, backend, mock_launcher, sample_handle):
        """Terminate passes the backend_handle as instance name."""
        sample_handle.backend_handle = "custom-instance-name"

        backend.terminate(sample_handle)

        mock_launcher.delete_instance.assert_called_once_with("custom-instance-name")

    def test_terminate_is_idempotent_when_launcher_succeeds(self, backend, mock_launcher, sample_handle):
        """Terminate is idempotent - no error raised when launcher succeeds."""
        # GCELauncher.delete_instance is idempotent - doesn't raise for missing instances
        mock_launcher.delete_instance.return_value = None

        # Should not raise
        backend.terminate(sample_handle)

    def test_terminate_does_not_catch_launcher_exceptions(self, backend, mock_launcher, sample_handle):
        """Terminate propagates exceptions from launcher for proper error handling.

        The launcher handles idempotency internally - if it raises, it's a real error
        that should be propagated (e.g., permission denied, API error).
        """
        mock_launcher.delete_instance.side_effect = Exception("Permission denied")

        with pytest.raises(Exception) as exc_info:
            backend.terminate(sample_handle)

        assert "Permission denied" in str(exc_info.value)


# --- Cleanup Tests ---


class TestGCERunBackendCleanup:
    """Tests for cleanup method."""

    def test_cleanup_is_noop(self, backend, sample_handle):
        """Cleanup is a no-op for GCE (delete removes all resources)."""
        # Should not raise and do nothing
        backend.cleanup(sample_handle)

    def test_cleanup_accepts_any_handle(self, backend):
        """Cleanup accepts any handle without error."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="gce",
            backend_handle="some-instance",
        )
        backend.cleanup(handle)


# --- Get Zone Tests ---


class TestGCERunBackendGetZone:
    """Tests for get_zone method."""

    def test_get_zone_returns_zone_from_handle(self, backend, mock_launcher, sample_handle):
        """Get zone returns zone stored in RunHandle when available."""
        zone = backend.get_zone(sample_handle)
        assert zone == "us-central1-a"

    def test_get_zone_returns_none_when_handle_has_no_zone(self, backend, mock_launcher):
        """Get zone returns None when handle has no zone."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="gce",
            backend_handle="goldfish-stage-xyz789",
            zone=None,
        )
        # When handle has no zone, get_zone should try to find it
        mock_launcher._find_instance_zone.return_value = None

        zone = backend.get_zone(handle)
        assert zone is None

    def test_get_zone_uses_launcher_find_when_handle_has_no_zone(self, backend, mock_launcher):
        """Get zone uses launcher._find_instance_zone when handle has no zone."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="gce",
            backend_handle="goldfish-stage-xyz789",
            zone=None,
        )
        mock_launcher._find_instance_zone.return_value = "us-west1-b"

        zone = backend.get_zone(handle)

        assert zone == "us-west1-b"
        mock_launcher._find_instance_zone.assert_called_once_with("goldfish-stage-xyz789")

    def test_get_zone_prefers_handle_zone_over_lookup(self, backend, mock_launcher, sample_handle):
        """Get zone uses handle zone if available, doesn't call launcher."""
        zone = backend.get_zone(sample_handle)

        assert zone == "us-central1-a"
        mock_launcher._find_instance_zone.assert_not_called()

    def test_get_zone_handles_launcher_exception(self, backend, mock_launcher):
        """Get zone returns None when launcher raises exception."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="gce",
            backend_handle="goldfish-stage-xyz789",
            zone=None,
        )
        mock_launcher._find_instance_zone.side_effect = Exception("API error")

        zone = backend.get_zone(handle)
        assert zone is None


# --- StorageURI Serialization Tests ---


class TestGCERunBackendStorageURISerialization:
    """Tests for StorageURI serialization in launch method.

    GCELauncher expects inputs to be strings (GCS URIs) or dicts,
    not StorageURI objects. The backend must serialize StorageURIs
    before passing them to stage_config.
    """

    @dataclass
    class MockLaunchResult:
        """Mock result from GCELauncher.launch_instance."""

        instance_name: str
        zone: str

    def test_launch_serializes_storage_uris(self, backend, mock_launcher, sample_run_spec):
        """Launch serializes StorageURI objects to URI strings in stage_config.

        GCELauncher.launch_instance receives stage_config["inputs"] which must
        contain strings (gs://...) not StorageURI objects. The GCELauncher's
        input staging code only handles str or dict types, skipping others.
        """
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        # sample_run_spec has inputs with StorageURI objects
        assert isinstance(sample_run_spec.inputs["dataset"], StorageURI)

        backend.launch(sample_run_spec)

        # Verify stage_config["inputs"] contains strings, not StorageURI objects
        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        stage_config = call_kwargs["stage_config"]
        inputs = stage_config["inputs"]

        # Should be string, not StorageURI
        assert isinstance(inputs["dataset"], str)
        assert inputs["dataset"] == "gs://test-bucket/data/input.npy"

    def test_launch_serializes_multiple_storage_uris(self, backend, mock_launcher):
        """Launch serializes all StorageURI objects in inputs."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "features": StorageURI("gs", "bucket-a", "data/features.npy"),
                "labels": StorageURI("gs", "bucket-b", "data/labels.csv"),
                "model": StorageURI("gs", "bucket-a", "models/pretrained"),
            },
        )

        backend.launch(spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        stage_config = call_kwargs["stage_config"]
        inputs = stage_config["inputs"]

        # All inputs should be serialized to strings
        assert inputs["features"] == "gs://bucket-a/data/features.npy"
        assert inputs["labels"] == "gs://bucket-b/data/labels.csv"
        assert inputs["model"] == "gs://bucket-a/models/pretrained"

    def test_launch_rejects_file_uri(self, backend, mock_launcher):
        """Launch rejects file:// URIs - GCE only supports GCS inputs.

        GCELauncher only stages gs:// inputs. Other schemes are silently skipped,
        causing the input to be missing at runtime. Validation catches this early.
        """
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "local_data": StorageURI("file", "", "/tmp/data.npy"),
            },
        )

        with pytest.raises(ValidationError) as exc_info:
            backend.launch(spec)

        assert "file://" in str(exc_info.value) or "file" in str(exc_info.value).lower()
        assert "local_data" in str(exc_info.value)

    def test_launch_handles_empty_inputs(self, backend, mock_launcher):
        """Launch handles empty inputs dict."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={},
        )

        backend.launch(spec)

        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        stage_config = call_kwargs["stage_config"]

        assert stage_config["inputs"] == {}


# --- Temp Directory Cleanup Tests ---


class TestGCERunBackendTempDirCleanup:
    """Tests verifying no temp directory leak in launch method.

    The GCERunBackend previously created a temp work_dir that was passed to
    GCELauncher.launch_instance but never cleaned up. Since GCELauncher doesn't
    use this work_dir (it builds everything via GCS), the temp dir should not
    be created at all.
    """

    @dataclass
    class MockLaunchResult:
        """Mock result from GCELauncher.launch_instance."""

        instance_name: str
        zone: str

    def test_launch_does_not_import_tempfile(self, backend, mock_launcher, sample_run_spec):
        """Launch should not use tempfile module since GCELauncher doesn't need work_dir.

        The work_dir parameter exists in GCELauncher.launch_instance signature but
        is never used - inputs/outputs are handled via GCS. The backend should not
        import tempfile at all.
        """
        import goldfish.cloud.adapters.gcp.run_backend as run_backend_module

        # Verify tempfile is not imported in the module
        assert not hasattr(
            run_backend_module, "tempfile"
        ), "tempfile should not be imported - work_dir is unused by GCELauncher"

    def test_launch_passes_dummy_work_dir_to_launcher(self, backend, mock_launcher, sample_run_spec):
        """Launch passes a dummy work_dir to GCELauncher (required param but unused).

        GCELauncher.launch_instance requires work_dir in its signature but doesn't
        use it - all inputs/outputs are handled via GCS. The backend passes a
        dummy Path("/tmp") to satisfy the signature.
        """
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        backend.launch(sample_run_spec)

        # work_dir is passed as required param (but unused by GCELauncher)
        call_kwargs = mock_launcher.launch_instance.call_args.kwargs
        assert "work_dir" in call_kwargs
        assert call_kwargs["work_dir"] == Path("/tmp")


# --- Get Output Dir Tests ---


class TestGCERunBackendGetOutputDir:
    """Tests for get_output_dir method."""

    def test_get_output_dir_returns_none(self, backend, sample_handle):
        """Get output dir always returns None for GCE (outputs are in GCS)."""
        result = backend.get_output_dir(sample_handle)
        assert result is None

    def test_get_output_dir_accepts_any_handle(self, backend):
        """Get output dir accepts any handle and returns None."""
        handle = RunHandle(
            stage_run_id="stage-xyz789",
            backend_type="gce",
            backend_handle="some-instance",
            zone="us-west1-a",
        )

        result = backend.get_output_dir(handle)
        assert result is None


# --- Input Scheme Validation Tests ---


class TestGCERunBackendInputSchemeValidation:
    """Tests for input URI scheme validation in launch method.

    GCE backend only supports GCS (gs://) inputs. Non-GCS inputs like file://
    will be silently skipped during staging, causing confusing runtime failures.
    The backend should validate input schemes upfront and reject non-GCS inputs.
    """

    @dataclass
    class MockLaunchResult:
        """Mock result from GCELauncher.launch_instance."""

        instance_name: str
        zone: str

    def test_launch_rejects_non_gcs_input_scheme(self, backend, mock_launcher):
        """Launch rejects inputs with non-gs:// scheme (file://, etc.).

        GCELauncher only stages gs:// inputs. Other schemes are silently skipped,
        causing the input to be missing at runtime. This validation catches the
        issue early with a clear error message.
        """
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "local_data": StorageURI("file", "", "/tmp/data.npy"),
            },
        )

        with pytest.raises(ValidationError) as exc_info:
            backend.launch(spec)

        assert "file://" in str(exc_info.value) or "file" in str(exc_info.value).lower()
        assert "local_data" in str(exc_info.value) or exc_info.value.details.get("field") == "local_data"

    def test_launch_rejects_multiple_non_gcs_inputs(self, backend, mock_launcher):
        """Launch rejects when any input has non-GCS scheme."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "valid_input": StorageURI("gs", "bucket", "data/valid.npy"),
                "invalid_input": StorageURI("file", "", "/tmp/invalid.npy"),
            },
        )

        with pytest.raises(ValidationError) as exc_info:
            backend.launch(spec)

        assert "invalid_input" in str(exc_info.value) or exc_info.value.details.get("field") == "invalid_input"

    def test_launch_accepts_all_gcs_inputs(self, backend, mock_launcher):
        """Launch accepts inputs when all have gs:// scheme."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "input1": StorageURI("gs", "bucket-a", "data/input1.npy"),
                "input2": StorageURI("gs", "bucket-b", "data/input2.csv"),
            },
        )

        # Should not raise
        handle = backend.launch(spec)
        assert handle.stage_run_id == "stage-abc123"

    def test_launch_accepts_empty_inputs(self, backend, mock_launcher):
        """Launch accepts empty inputs dict (no validation needed)."""
        mock_launcher.launch_instance.return_value = self.MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={},
        )

        # Should not raise
        handle = backend.launch(spec)
        assert handle.stage_run_id == "stage-abc123"

    def test_launch_rejects_http_scheme(self, backend, mock_launcher):
        """Launch rejects http:// inputs."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "web_data": StorageURI("http", "example.com", "data.csv"),
            },
        )

        with pytest.raises(ValidationError):
            backend.launch(spec)

    def test_launch_rejects_s3_scheme(self, backend, mock_launcher):
        """Launch rejects s3:// inputs (AWS, not GCP)."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "s3_data": StorageURI("s3", "my-bucket", "data.npy"),
            },
        )

        with pytest.raises(ValidationError):
            backend.launch(spec)
