"""E2E Tests for Cloud Abstraction Layer.

These tests define the expected behavior of the abstracted cloud backend.
The tests ARE the specification - they exercise real code paths using
the contract types defined in goldfish.cloud.

E2E-GCP-*: Full GCP path tests (xfail - GCP adapters not yet implemented)
E2E-LOCAL-*: Full local path tests (passing - local adapters complete)
E2E-PARITY-*: GCP ≈ Local equivalence tests (passing for local adapter behavior)
"""

from __future__ import annotations

import pytest

from goldfish.cloud.contracts import (
    BackendCapabilities,
    BackendStatus,
    RunHandle,
    RunSpec,
    RunStatus,
    StorageURI,
)


class TestStorageURIContract:
    """Tests for StorageURI contract type.

    These tests validate the URI abstraction works correctly.
    Based on RCT-GCS-1, RCT-LOCAL-STORAGE-1 observations.
    """

    def test_parse_gs_uri(self):
        """Parse gs:// URI correctly."""
        uri = StorageURI.parse("gs://my-bucket/path/to/file.txt")

        assert uri.scheme == "gs"
        assert uri.bucket == "my-bucket"
        assert uri.path == "path/to/file.txt"

    def test_parse_s3_uri(self):
        """Parse s3:// URI correctly."""
        uri = StorageURI.parse("s3://my-bucket/path/to/file.txt")

        assert uri.scheme == "s3"
        assert uri.bucket == "my-bucket"
        assert uri.path == "path/to/file.txt"

    def test_parse_file_uri(self):
        """Parse file:// URI correctly.

        file:// URIs preserve the absolute path (leading slash).
        file:///local/path -> path="/local/path"
        """
        uri = StorageURI.parse("file:///local/path/file.txt")

        assert uri.scheme == "file"
        assert uri.bucket == ""
        assert uri.path == "/local/path/file.txt"  # Absolute path preserved

    def test_str_round_trip(self):
        """URI string round-trips correctly."""
        original = "gs://bucket/path/to/file.txt"
        uri = StorageURI.parse(original)

        assert str(uri) == original

    def test_join_path(self):
        """Join path components to URI."""
        base = StorageURI.parse("gs://bucket/runs")
        joined = base.join("stage-123", "outputs", "model.pt")

        assert str(joined) == "gs://bucket/runs/stage-123/outputs/model.pt"

    def test_equality(self):
        """URIs with same components are equal."""
        uri1 = StorageURI.parse("gs://bucket/path")
        uri2 = StorageURI("gs", "bucket", "path")

        assert uri1 == uri2
        assert hash(uri1) == hash(uri2)

    def test_invalid_uri_raises(self):
        """Invalid URI format raises ValueError."""
        with pytest.raises(ValueError, match="missing scheme"):
            StorageURI.parse("bucket/path/no/scheme")


class TestRunStatusContract:
    """Tests for RunStatus contract type.

    Based on RCT-GCE-2, RCT-EXIT-1 observations.
    """

    def test_terminal_states(self):
        """Terminal states are correctly identified."""
        terminal = [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED, RunStatus.CANCELED]
        non_terminal = [RunStatus.PENDING, RunStatus.PREPARING, RunStatus.RUNNING]

        for status in terminal:
            assert status.is_terminal(), f"{status} should be terminal"

        for status in non_terminal:
            assert not status.is_terminal(), f"{status} should not be terminal"

    def test_success_state(self):
        """Only COMPLETED is success."""
        assert RunStatus.COMPLETED.is_success()
        assert not RunStatus.FAILED.is_success()
        assert not RunStatus.TERMINATED.is_success()


class TestBackendStatusContract:
    """Tests for BackendStatus contract type.

    Based on RCT-EXIT-1 exit code semantics.
    """

    def test_exit_code_0_is_completed(self):
        """Exit code 0 maps to COMPLETED."""
        status = BackendStatus.from_exit_code(0)

        assert status.status == RunStatus.COMPLETED
        assert status.exit_code == 0

    def test_exit_code_1_is_failed(self):
        """Exit code 1 maps to FAILED."""
        status = BackendStatus.from_exit_code(1)

        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1

    def test_exit_code_137_is_oom(self):
        """Exit code 137 (SIGKILL) maps to TERMINATED with OOM."""
        status = BackendStatus.from_exit_code(137)

        assert status.status == RunStatus.TERMINATED
        assert status.exit_code == 137
        assert status.termination_cause == "oom"

    def test_exit_code_143_is_preemption(self):
        """Exit code 143 (SIGTERM) maps to TERMINATED with preemption."""
        status = BackendStatus.from_exit_code(143)

        assert status.status == RunStatus.TERMINATED
        assert status.exit_code == 143
        assert status.termination_cause == "preemption"

    def test_termination_cause_override(self):
        """Explicit termination cause overrides inference."""
        status = BackendStatus.from_exit_code(143, termination_cause="timeout")

        assert status.termination_cause == "timeout"


class TestRunSpecContract:
    """Tests for RunSpec contract type."""

    def test_runspec_defaults(self):
        """RunSpec has sensible defaults."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="baseline",
            stage_name="train",
            image="gcr.io/project/train:v1",
        )

        assert spec.profile == "cpu-small"
        assert spec.gpu_count == 0
        assert spec.spot is False
        assert spec.env == {}
        assert spec.inputs == {}

    def test_runspec_with_inputs(self):
        """RunSpec with input URIs."""
        inputs = {
            "features": StorageURI.parse("gs://bucket/features.npy"),
            "labels": StorageURI.parse("gs://bucket/labels.npy"),
        }
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="baseline",
            stage_name="train",
            image="train:latest",
            inputs=inputs,
        )

        assert len(spec.inputs) == 2
        assert spec.inputs["features"].path == "features.npy"


class TestRunHandleContract:
    """Tests for RunHandle contract type."""

    def test_handle_serialization(self):
        """RunHandle round-trips through dict."""
        handle = RunHandle(
            stage_run_id="stage-abc123",
            backend_type="gce",
            backend_handle="goldfish-stage-abc123",  # Valid GCE instance name
            zone="us-central1-a",
        )

        data = handle.to_dict()
        restored = RunHandle.from_dict(data)

        assert restored.stage_run_id == handle.stage_run_id
        assert restored.backend_type == handle.backend_type
        assert restored.backend_handle == handle.backend_handle
        assert restored.zone == handle.zone


class TestBackendCapabilitiesContract:
    """Tests for BackendCapabilities contract type."""

    def test_local_typical_capabilities(self):
        """Local backend typical capabilities."""
        caps = BackendCapabilities(
            supports_gpu=False,  # Unless nvidia-docker
            supports_spot=False,  # Not applicable
            supports_preemption=True,  # Always supports SIGTERM
            supports_preemption_detection=True,  # Via simulation
            supports_live_logs=True,
        )

        assert not caps.supports_gpu
        assert caps.supports_preemption  # Always supports graceful shutdown
        assert caps.supports_preemption_detection

    def test_gcp_typical_capabilities(self):
        """GCP backend typical capabilities."""
        caps = BackendCapabilities(
            supports_gpu=True,
            supports_spot=True,
            supports_preemption=True,  # GCE handles SIGTERM gracefully
            supports_preemption_detection=True,
            supports_live_logs=True,
            supports_metrics=True,
            max_run_duration_hours=24,
        )

        assert caps.supports_gpu
        assert caps.supports_spot
        assert caps.max_run_duration_hours == 24


# =============================================================================
# E2E Tests - These require adapter implementations (xfail until Phase 3)
# =============================================================================


class TestE2EObjectStorage:
    """E2E tests for ObjectStorage protocol.

    LocalObjectStorage is implemented - these tests verify the adapter works.
    """

    def test_e2e_local_storage_round_trip(self, tmp_path):
        """E2E-LOCAL-STORAGE: Local storage round-trip.

        Given: LocalObjectStorage adapter
        When: put() then get()
        Then: Data is identical
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")
        uri = StorageURI.parse("gs://test-bucket/test-file.bin")
        data = b"test data with \x00 null bytes"

        storage.put(uri, data)
        retrieved = storage.get(uri)

        assert retrieved == data

    def test_e2e_local_storage_list_prefix(self, tmp_path):
        """E2E-LOCAL-STORAGE: List prefix works correctly."""
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")
        base = StorageURI.parse("gs://bucket/prefix")

        # Upload several files
        storage.put(base.join("file1.txt"), b"1")
        storage.put(base.join("file2.txt"), b"2")
        storage.put(base.join("subdir", "file3.txt"), b"3")

        # List should return all
        results = storage.list_prefix(base)

        assert len(results) == 3

    def test_e2e_local_storage_get_local_path(self, tmp_path):
        """E2E-LOCAL-STORAGE: Local path available for existing files."""
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")
        uri = StorageURI.parse("gs://bucket/path/file.txt")

        # Must write the file first - get_local_path only works for existing files
        storage.put(uri, b"test data")
        local_path = storage.get_local_path(uri)

        assert local_path is not None
        assert "bucket" in str(local_path)
        assert "path" in str(local_path)


class TestE2ERunBackend:
    """E2E tests for RunBackend protocol.

    LocalRunBackend is implemented - these tests verify the adapter works.
    NOTE: Requires Docker to be running.
    """

    def test_e2e_local_backend_capabilities(self):
        """E2E-LOCAL-1: Local backend reports capabilities."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()
        caps = backend.capabilities

        assert isinstance(caps, BackendCapabilities)
        assert caps.supports_live_logs is True
        # GPU depends on nvidia-docker availability

    def test_e2e_local_backend_launch_and_status(self):
        """E2E-LOCAL-1: Launch container and check status."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()
        spec = RunSpec(
            stage_run_id="stage-7e57a001",
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["echo", "hello"],
        )

        handle = backend.launch(spec)
        assert handle.backend_type == "local"

        # Wait for completion
        import time

        status = backend.get_status(handle)
        for _ in range(30):
            status = backend.get_status(handle)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        assert status.status == RunStatus.COMPLETED
        assert status.exit_code == 0

        backend.cleanup(handle)

    def test_e2e_local_backend_failure_status(self):
        """E2E-PARITY-3: Failed container reports FAILED status."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()
        spec = RunSpec(
            stage_run_id="stage-fa11a002",
            workspace_name="test",
            stage_name="fail",
            image="alpine:latest",
            command=["sh", "-c", "exit 1"],
        )

        handle = backend.launch(spec)

        # Wait for completion
        import time

        status = backend.get_status(handle)
        for _ in range(30):
            status = backend.get_status(handle)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1

        backend.cleanup(handle)


class TestExitCodeSemantics:
    """Tests for exit code → status mapping.

    This tests the contract mapping, not adapters.
    Based on RCT-EXIT-1 observations.
    """

    def test_exit_code_mapping_consistency(self):
        """E2E-PARITY-3: Exit codes map consistently across all backends.

        Based on RCT-EXIT-1: exit 0 -> COMPLETED, exit 1 -> FAILED
        This mapping is enforced by BackendStatus.from_exit_code().
        """
        assert BackendStatus.from_exit_code(0).status == RunStatus.COMPLETED
        assert BackendStatus.from_exit_code(1).status == RunStatus.FAILED
        assert BackendStatus.from_exit_code(42).status == RunStatus.FAILED
        assert BackendStatus.from_exit_code(137).status == RunStatus.TERMINATED
        assert BackendStatus.from_exit_code(143).status == RunStatus.TERMINATED


@pytest.mark.xfail(reason="GCP adapters not yet implemented", strict=True)
class TestE2EGCP:
    """E2E tests for full GCP path.

    These tests validate GCP backend behavior.
    """

    def test_e2e_gcp_1_training_stage_on_gce(self):
        """E2E-GCP-1: Run training stage on GCE with GCS storage.

        Given: workspace mounted, pipeline defined, GCP backend configured
        When: run("w1", stages=["train"])
        Then:
            - Stage runs on GCE instance
            - Outputs uploaded to GCS
            - Logs available in expected path
            - Exit code reflects actual outcome
        """
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.cloud.adapters.gcp.storage import GCSObjectStorage

        storage = GCSObjectStorage(bucket="test-bucket")
        backend = GCERunBackend(project="test-project")

        spec = RunSpec(
            stage_run_id="stage-9c9a003",
            workspace_name="test",
            stage_name="train",
            image="gcr.io/test-project/train:latest",
            inputs={"data": StorageURI.parse("gs://test-bucket/inputs/data.npy")},
            output_uri=StorageURI.parse("gs://test-bucket/outputs"),
        )

        handle = backend.launch(spec)
        assert handle.backend_type == "gcp"

        # Would wait for completion and verify outputs
        backend.cleanup(handle)

    def test_e2e_gcp_2_preemption_handling(self):
        """E2E-GCP-2: Preemption handling.

        Given: stage running on preemptible instance
        When: instance is preempted
        Then:
            - Preemption detected
            - Status shows TERMINATED with preemption cause
        """
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        backend = GCERunBackend(project="test-project")

        spec = RunSpec(
            stage_run_id="stage-90e0a004",
            workspace_name="test",
            stage_name="long_train",
            image="gcr.io/test-project/train:latest",
            spot=True,  # Request preemptible
        )

        handle = backend.launch(spec)

        # Would need to wait for or trigger preemption
        # Then verify status.termination_cause == "preemption"
        backend.cleanup(handle)


class TestE2ELocalCapabilities:
    """E2E tests for local backend capability handling."""

    def test_e2e_local_2_capability_limitations_respected(self):
        """E2E-LOCAL-2: Capability limitations respected.

        Given: local backend
        When: check capabilities
        Then: capabilities accurately reflect available features
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()
        caps = backend.capabilities

        # Local backend should NOT support spot (it's simulated, not real)
        assert caps.supports_spot is False

        # Should support live logs
        assert caps.supports_live_logs is True

        # Preemption detection depends on simulation config
        # (could be True with simulation enabled)


class TestE2EParity:
    """E2E parity tests - GCP ≈ Local equivalence.

    These tests verify local backend parity with expected GCP behavior.
    GCP adapters are not yet implemented, but local adapters are complete.
    NOTE: Requires Docker to be running.
    """

    def test_e2e_parity_1_same_inputs_same_outputs(self, tmp_path):
        """E2E-PARITY-1: Same inputs → same outputs.

        Given: deterministic stage
        When: run on local backend
        Then: outputs are deterministic (byte-identical to input)
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")
        # Pass storage to backend so inputs get mounted
        backend = LocalRunBackend(storage=storage)

        # Upload deterministic input
        input_data = b"deterministic input data"
        input_uri = StorageURI.parse("gs://bucket/input.txt")
        storage.put(input_uri, input_data)

        spec = RunSpec(
            stage_run_id="stage-de7a0005",
            workspace_name="test",
            stage_name="copy",
            image="alpine:latest",
            # Input file mounts directly at /mnt/inputs/{signal_name} (not as subdirectory)
            command=["cp", "/mnt/inputs/input", "/mnt/outputs/output.txt"],
            inputs={"input": input_uri},
            output_uri=StorageURI.parse("gs://bucket/outputs"),
        )

        handle = backend.launch(spec)

        import time

        status = backend.get_status(handle)  # Initialize before loop
        for _ in range(30):
            status = backend.get_status(handle)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        # Verify run completed successfully
        assert status.status == RunStatus.COMPLETED, f"Expected COMPLETED but got {status.status}"

        # Verify output matches input (byte-identical)
        output_dir = backend.get_output_dir(handle)
        assert output_dir is not None, "Output directory should exist"
        output_file = output_dir / "output.txt"
        assert output_file.exists(), f"Output file should exist at {output_file}"
        output_data = output_file.read_bytes()
        assert output_data == input_data, f"Output should match input: {output_data!r} != {input_data!r}"

        backend.cleanup(handle)

    def test_e2e_parity_2_same_status_transitions(self):
        """E2E-PARITY-2: Same status transitions on both backends.

        Given: Simple stage that runs and exits 0
        When: Run on local backend
        Then: Status sequence includes RUNNING -> COMPLETED
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()
        spec = RunSpec(
            stage_run_id="stage-0a017a006",
            workspace_name="test",
            stage_name="sleep",
            image="alpine:latest",
            command=["sleep", "1"],
        )

        handle = backend.launch(spec)
        statuses_seen: list[RunStatus] = []

        import time

        for _ in range(50):
            status = backend.get_status(handle)
            if status.status not in statuses_seen:
                statuses_seen.append(status.status)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        # Must have seen RUNNING at some point
        assert RunStatus.RUNNING in statuses_seen
        # Must end with COMPLETED
        assert statuses_seen[-1] == RunStatus.COMPLETED

        backend.cleanup(handle)

    def test_e2e_parity_4_preemption_simulation(self):
        """E2E-PARITY-4: Preemption simulation matches GCP behavior.

        Given: local configured with preemption simulation
        When: run stage
        Then: preemption detected, status TERMINATED with preemption cause
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.config import LocalComputeConfig

        # Configure preemption simulation via config object
        config = LocalComputeConfig(
            simulate_preemption_after_seconds=2,
            preemption_grace_period_seconds=1,
        )
        backend = LocalRunBackend(config=config)

        spec = RunSpec(
            stage_run_id="stage-5190e0a007",
            workspace_name="test",
            stage_name="long",
            image="alpine:latest",
            command=["sleep", "10"],  # Takes longer than preemption timeout
        )

        handle = backend.launch(spec)

        import time

        status = backend.get_status(handle)
        for _ in range(50):
            status = backend.get_status(handle)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        # Should be terminated due to preemption simulation
        assert status.status == RunStatus.TERMINATED
        assert status.termination_cause == "preemption"

        backend.cleanup(handle)
