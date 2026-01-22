"""RCT Tests for Local Backend Parity.

These tests validate that the local backend correctly emulates GCP behavior.
They run WITHOUT --rct flag since they don't need real GCP.

RCT-LOCAL-STORAGE-*: Storage emulation tests
RCT-LOCAL-META-*: Metadata emulation tests
RCT-LOCAL-COMPUTE-*: Compute emulation tests (requires Docker)
RCT-LOCAL-EXIT-*: Exit code handling tests
RCT-LOCAL-SIM-*: Simulation controls tests
"""

import concurrent.futures
import time
from pathlib import Path

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.adapters.local.storage import LocalObjectStorage
from goldfish.cloud.contracts import (
    BackendStatus,
    RunSpec,
    RunStatus,
    StorageURI,
)
from goldfish.config import LocalComputeConfig, LocalSignalingConfig, LocalStorageConfig
from goldfish.errors import CapacityError, MetadataSizeLimitError, NotFoundError, StorageError
from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus

# NOTE: These tests run WITHOUT --rct flag since they don't need real GCP.
# The conftest.py only skips tests with explicit @pytest.mark.rct marker.


class TestLocalStorageEmulation:
    """RCT-LOCAL-STORAGE: Validate LocalObjectStorage matches GCS semantics."""

    def test_rct_local_storage_1_uri_mapping(self, tmp_path: Path):
        """RCT-LOCAL-STORAGE-1: gs:// URI maps to local path correctly."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://my-bucket/path/to/file.txt")
        data = b"test content"

        storage.put(uri, data)

        # Verify file exists at expected local path
        expected_path = tmp_path / ".local_gcs" / "my-bucket" / "path" / "to" / "file.txt"
        assert expected_path.exists()
        assert expected_path.read_bytes() == data

    def test_rct_local_storage_2_round_trip(self, tmp_path: Path):
        """RCT-LOCAL-STORAGE-2: Upload/download round-trip identical to GCS."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/test/data.bin")
        original_data = b"\x00\x01\x02\x03" * 1000  # Binary data

        storage.put(uri, original_data)
        retrieved_data = storage.get(uri)

        assert retrieved_data == original_data

    def test_rct_local_storage_3_list_prefix(self, tmp_path: Path):
        """RCT-LOCAL-STORAGE-3: list_prefix returns same results as GCS.

        Verifies:
        - All files under prefix are returned
        - Results are in lexicographic order (per RCT-GCS-2)
        """
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # Create multiple files under a prefix
        base = StorageURI.parse("gs://bucket/runs/stage-123/outputs")
        storage.put(base.join("model.pt"), b"model")
        storage.put(base.join("metrics.json"), b"metrics")
        storage.put(base.join("nested/config.yaml"), b"config")

        # List all under prefix
        results = storage.list_prefix(base)

        assert len(results) == 3
        paths = [str(r) for r in results]
        assert any("model.pt" in p for p in paths)
        assert any("metrics.json" in p for p in paths)
        assert any("config.yaml" in p for p in paths)

        # Verify lexicographic ordering (matches GCS behavior per RCT-GCS-2)
        assert paths == sorted(paths), "list_prefix must return results in lexicographic order"

    def test_rct_local_storage_5_missing_file_error(self, tmp_path: Path):
        """RCT-LOCAL-STORAGE-5: Missing file raises NotFoundError (like GCS)."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/does/not/exist.txt")

        with pytest.raises(NotFoundError):
            storage.get(uri)

    def test_rct_local_storage_exists(self, tmp_path: Path):
        """Validate exists() matches GCS behavior."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/test/file.txt")

        assert storage.exists(uri) is False

        storage.put(uri, b"data")

        assert storage.exists(uri) is True

    def test_rct_local_storage_delete_idempotent(self, tmp_path: Path):
        """Validate delete() is idempotent (like GCS)."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/test/delete-me.txt")
        storage.put(uri, b"data")

        # Delete twice - should not raise
        storage.delete(uri)
        storage.delete(uri)  # Second delete is no-op

        assert storage.exists(uri) is False

    def test_rct_local_storage_get_local_path(self, tmp_path: Path):
        """Validate get_local_path returns actual filesystem path."""
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/test/file.txt")
        storage.put(uri, b"data")

        local_path = storage.get_local_path(uri)

        assert local_path is not None
        assert local_path.exists()
        assert local_path.read_bytes() == b"data"

    def test_rct_local_storage_path_traversal_blocked(self):
        """Validate path traversal is blocked at URI parse time."""
        # Path traversal should be rejected at URI parse level
        with pytest.raises(ValueError, match="Path traversal"):
            StorageURI.parse("gs://bucket/../../../etc/passwd")

    def test_rct_local_storage_put_error_wrapped(self, tmp_path: Path):
        """Validate put() errors are wrapped in StorageError."""
        # Create read-only root
        readonly_root = tmp_path / ".readonly_gcs"
        readonly_root.mkdir()
        readonly_root.chmod(0o555)

        storage = LocalObjectStorage(root=readonly_root)

        try:
            uri = StorageURI.parse("gs://bucket/test/file.txt")
            with pytest.raises(StorageError):
                storage.put(uri, b"data")
        finally:
            readonly_root.chmod(0o755)


class TestLocalMetadataEmulation:
    """RCT-LOCAL-META: Validate LocalMetadataBus matches GCP metadata semantics."""

    def test_rct_local_meta_1_size_limit(self, tmp_path: Path):
        """RCT-LOCAL-META-1: 256KB size limit enforced (GCP metadata limit)."""
        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Create signal exceeding 256KB
        large_payload = {"data": "x" * 300000}
        signal = MetadataSignal(command="sync", request_id="req-large", payload=large_payload)

        with pytest.raises(MetadataSizeLimitError) as exc_info:
            bus.set_signal("goldfish", signal)

        assert exc_info.value.details["actual_size"] > 262144

    def test_rct_local_meta_signal_round_trip(self, tmp_path: Path):
        """RCT-LOCAL-META-2: Signal round-trip matches GCP."""
        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        signal = MetadataSignal(command="sync", request_id="req-123", payload={"key": "value"})
        bus.set_signal("goldfish", signal)

        retrieved = bus.get_signal("goldfish")

        assert retrieved is not None
        assert retrieved.command == "sync"
        assert retrieved.request_id == "req-123"
        assert retrieved.payload == {"key": "value"}

    def test_rct_local_meta_2_ack_round_trip(self, tmp_path: Path):
        """RCT-LOCAL-META-2: Ack round-trip matches GCP."""
        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        bus.set_ack("goldfish", "req-456")
        ack = bus.get_ack("goldfish")

        assert ack == "req-456"

    def test_rct_local_meta_4_concurrent_access(self, tmp_path: Path):
        """RCT-LOCAL-META-4: Concurrent access with file locking works."""
        metadata_file = tmp_path / "metadata.json"

        def write_signal(i: int) -> str:
            bus = LocalMetadataBus(metadata_file)
            signal = MetadataSignal(command="sync", request_id=f"req-{i}")
            bus.set_signal("goldfish", signal)
            return f"req-{i}"

        # Run 10 concurrent writes
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(write_signal, i) for i in range(10)]
            concurrent.futures.wait(futures)

        # Final state should be valid JSON with one of the request IDs
        bus = LocalMetadataBus(metadata_file)
        signal = bus.get_signal("goldfish")

        assert signal is not None
        assert signal.request_id.startswith("req-")

    def test_rct_local_meta_clear_signal(self, tmp_path: Path):
        """Validate clear_signal removes the signal."""
        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        signal = MetadataSignal(command="sync", request_id="req-789")
        bus.set_signal("goldfish", signal)
        bus.clear_signal("goldfish")

        assert bus.get_signal("goldfish") is None

    def test_rct_local_meta_missing_returns_none(self, tmp_path: Path):
        """Validate missing signal returns None (not error)."""
        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        result = bus.get_signal("nonexistent")

        assert result is None


class TestLocalComputeEmulation:
    """RCT-LOCAL-COMPUTE: Validate LocalRunBackend matches GCE behavior.

    These tests require Docker and are marked as slow.
    """

    @pytest.mark.slow
    def test_rct_local_compute_1_lifecycle_states(self):
        """RCT-LOCAL-COMPUTE-1: Container lifecycle maps to GCE states."""
        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-11fe0001",
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["sleep", "5"],
        )

        handle = backend.launch(spec)

        try:
            import time

            time.sleep(0.5)  # Give container time to start

            # Container should be RUNNING
            status = backend.get_status(handle)
            assert status.status == RunStatus.RUNNING

            # Terminate it
            backend.terminate(handle)

            # Wait for termination
            for _ in range(30):
                status = backend.get_status(handle)
                if status.status.is_terminal():
                    break
                time.sleep(0.1)

            # Should be terminal (TERMINATED or CANCELED)
            assert status.status.is_terminal()
        finally:
            backend.cleanup(handle)

    @pytest.mark.slow
    def test_rct_local_compute_4_logs_retrieved(self):
        """RCT-LOCAL-COMPUTE-4: Logs retrieved correctly."""
        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-10850002",
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["sh", "-c", "echo 'hello from container' && sleep 1"],
        )

        handle = backend.launch(spec)

        try:
            import time

            time.sleep(1.5)  # Give container time to produce logs

            logs = backend.get_logs(handle, tail=10)
            assert "hello from container" in logs
        finally:
            backend.terminate(handle)
            backend.cleanup(handle)

    @pytest.mark.slow
    def test_rct_local_compute_capabilities(self):
        """Validate capabilities are accurately reported per LOCAL_PARITY_SPEC."""
        backend = LocalRunBackend()

        caps = backend.capabilities

        assert caps.supports_live_logs is True
        assert caps.supports_spot is False  # Local doesn't have spot pricing
        assert caps.supports_preemption is True  # Always supports SIGTERM
        assert caps.supports_preemption_detection is False  # Only when simulation configured
        # supports_gpu depends on nvidia-docker availability (not asserted)

    @pytest.mark.slow
    def test_rct_local_compute_env_vars(self):
        """Validate environment variables are passed to container."""
        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-e0b00003",
            workspace_name="test",
            stage_name="env",
            image="alpine:latest",
            command=["sh", "-c", "echo MY_VAR=$MY_VAR"],
            env={"MY_VAR": "test_value"},
        )

        handle = backend.launch(spec)

        try:
            import time

            time.sleep(1)

            logs = backend.get_logs(handle, tail=10)
            assert "MY_VAR=test_value" in logs
        finally:
            backend.terminate(handle)
            backend.cleanup(handle)

    @pytest.mark.slow
    def test_rct_local_compute_not_found_error(self):
        """Validate NotFoundError for missing container."""
        from goldfish.cloud.contracts import RunHandle

        backend = LocalRunBackend()

        fake_handle = RunHandle(
            stage_run_id="stage-face0004",
            backend_type="local",
            backend_handle="nonexistent-container-id",
        )

        with pytest.raises(NotFoundError):
            backend.get_status(fake_handle)

    @pytest.mark.slow
    def test_rct_local_compute_preemption_simulation(self):
        """RCT-LOCAL-COMPUTE-2: Preemption simulation triggers SIGTERM.

        Tests that simulate_preemption_after_seconds triggers container termination.
        Uses a short preemption delay (2s) and grace period (1s) for test speed.
        """
        config = LocalComputeConfig(
            simulate_preemption_after_seconds=2,
            preemption_grace_period_seconds=1,
        )
        backend = LocalRunBackend(config=config)

        spec = RunSpec(
            stage_run_id="stage-90e00005",
            workspace_name="test",
            stage_name="sleep",
            image="alpine:latest",
            command=["sleep", "30"],  # Would run 30s if not preempted
        )

        handle = backend.launch(spec)

        try:
            # Give container time to start
            time.sleep(0.5)
            status = backend.get_status(handle)
            assert status.status == RunStatus.RUNNING

            # Wait for preemption (2s delay + 1s grace + buffer)
            time.sleep(4)

            # Container should be terminated after preemption
            status = backend.get_status(handle)
            assert status.status.is_terminal(), f"Expected terminal status, got {status.status}"
            # Verify preemption is correctly identified (per spec)
            assert (
                status.termination_cause == "preemption"
            ), f"Expected termination_cause='preemption', got {status.termination_cause}"
        finally:
            backend.terminate(handle)
            backend.cleanup(handle)


class TestLocalExitCodeEmulation:
    """RCT-LOCAL-EXIT: Validate exit code handling matches GCE."""

    def test_rct_local_exit_status_from_exit_code(self):
        """RCT-LOCAL-EXIT-1 through EXIT-4: Exit code to status mapping."""
        # Test exit code 0 -> COMPLETED
        status_0 = BackendStatus.from_exit_code(0)
        assert status_0.status == RunStatus.COMPLETED
        assert status_0.exit_code == 0

        # Test non-zero -> FAILED
        status_1 = BackendStatus.from_exit_code(1)
        assert status_1.status == RunStatus.FAILED
        assert status_1.exit_code == 1

        # Test SIGKILL (137) -> TERMINATED (OOM)
        status_137 = BackendStatus.from_exit_code(137)
        assert status_137.status == RunStatus.TERMINATED
        assert status_137.termination_cause == "oom"

        # Test SIGTERM (143) -> TERMINATED (preemption)
        status_143 = BackendStatus.from_exit_code(143)
        assert status_143.status == RunStatus.TERMINATED
        assert status_143.termination_cause == "preemption"

    @pytest.mark.slow
    def test_rct_local_exit_container_exit_code(self):
        """Validate container exit codes map correctly."""
        backend = LocalRunBackend()

        # Test successful exit
        spec = RunSpec(
            stage_run_id="stage-e1170006",
            workspace_name="test",
            stage_name="exit",
            image="alpine:latest",
            command=["sh", "-c", "exit 0"],
        )

        handle = backend.launch(spec)

        try:
            import time

            time.sleep(1)

            status = backend.get_status(handle)
            assert status.status == RunStatus.COMPLETED
            assert status.exit_code == 0
        finally:
            backend.cleanup(handle)

    @pytest.mark.slow
    def test_rct_local_exit_container_failure(self):
        """Validate container failure exit codes."""
        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-e1170007",
            workspace_name="test",
            stage_name="fail",
            image="alpine:latest",
            command=["sh", "-c", "exit 1"],
        )

        handle = backend.launch(spec)

        try:
            import time

            time.sleep(1)

            status = backend.get_status(handle)
            assert status.status == RunStatus.FAILED
            assert status.exit_code == 1
        finally:
            backend.cleanup(handle)


class TestStorageURIContract:
    """Test StorageURI contract types."""

    def test_uri_parse_gs(self):
        """Validate gs:// URI parsing."""
        uri = StorageURI.parse("gs://my-bucket/path/to/file.txt")

        assert uri.scheme == "gs"
        assert uri.bucket == "my-bucket"
        assert uri.path == "path/to/file.txt"
        assert str(uri) == "gs://my-bucket/path/to/file.txt"

    def test_uri_parse_file(self):
        """Validate file:// URI parsing preserves absolute path."""
        uri = StorageURI.parse("file:///tmp/test/file.txt")

        assert uri.scheme == "file"
        assert uri.bucket == ""
        assert uri.path == "/tmp/test/file.txt"
        assert str(uri) == "file:///tmp/test/file.txt"

    def test_uri_join(self):
        """Validate URI path joining."""
        base = StorageURI.parse("gs://bucket/prefix")
        joined = base.join("subdir", "file.txt")

        assert str(joined) == "gs://bucket/prefix/subdir/file.txt"

    def test_uri_equality(self):
        """Validate URI equality comparison."""
        uri1 = StorageURI.parse("gs://bucket/path/file.txt")
        uri2 = StorageURI.parse("gs://bucket/path/file.txt")
        uri3 = StorageURI.parse("gs://bucket/other/file.txt")

        assert uri1 == uri2
        assert uri1 != uri3

    def test_uri_path_traversal_rejected(self):
        """Validate path traversal is rejected."""
        with pytest.raises(ValueError, match="Path traversal"):
            StorageURI.parse("gs://bucket/../../../etc/passwd")

        with pytest.raises(ValueError, match="Path traversal"):
            StorageURI.parse("gs://..bucket/path")


class TestSimulationControls:
    """RCT-LOCAL-SIM: Validate simulation controls work per LOCAL_PARITY_SPEC."""

    def test_rct_local_sim_storage_consistency_delay(self, tmp_path: Path):
        """RCT-LOCAL-SIM-1: Storage consistency delay simulates GCS eventual consistency."""
        config = LocalStorageConfig(consistency_delay_ms=100)
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs", config=config)

        uri = StorageURI.parse("gs://bucket/test/file.txt")
        storage.put(uri, b"test data")

        # Measure time to read (should include delay)
        start = time.time()
        storage.get(uri)
        elapsed_ms = (time.time() - start) * 1000

        # Should take at least 100ms due to configured delay
        assert elapsed_ms >= 90  # Small tolerance for timing

    def test_rct_local_sim_storage_size_limit(self, tmp_path: Path):
        """RCT-LOCAL-SIM-2: Storage size limit enforced."""
        config = LocalStorageConfig(size_limit_mb=1)  # 1MB limit
        storage = LocalObjectStorage(root=tmp_path / ".local_gcs", config=config)

        uri = StorageURI.parse("gs://bucket/test/large.bin")
        large_data = b"x" * (2 * 1024 * 1024)  # 2MB

        with pytest.raises(StorageError, match="exceeds limit"):
            storage.put(uri, large_data)

    def test_rct_local_sim_metadata_latency(self, tmp_path: Path):
        """RCT-LOCAL-SIM-3: Metadata latency simulates GCP metadata server."""
        config = LocalSignalingConfig(latency_ms=100)
        bus = LocalMetadataBus(tmp_path / "metadata.json", config=config)

        signal = MetadataSignal(command="sync", request_id="req-latency")

        # Measure time to set signal (should include latency)
        start = time.time()
        bus.set_signal("goldfish", signal)
        elapsed_ms = (time.time() - start) * 1000

        # Should take at least 100ms due to configured latency
        assert elapsed_ms >= 90  # Small tolerance for timing

    def test_rct_local_sim_metadata_custom_size_limit(self, tmp_path: Path):
        """RCT-LOCAL-SIM-4: Metadata size limit is configurable."""
        # Use a smaller limit (100 bytes) for testing
        config = LocalSignalingConfig(size_limit_bytes=100)
        bus = LocalMetadataBus(tmp_path / "metadata.json", config=config)

        # Create signal exceeding 100 bytes
        signal = MetadataSignal(command="sync", request_id="req-size", payload={"data": "x" * 200})

        with pytest.raises(MetadataSizeLimitError):
            bus.set_signal("goldfish", signal)

    def test_rct_local_sim_compute_zone_unavailable(self):
        """RCT-LOCAL-SIM-5: Zone availability affects launch (simulates capacity)."""
        config = LocalComputeConfig(zone_availability={"local-zone-1": False})
        backend = LocalRunBackend(config=config)

        spec = RunSpec(
            stage_run_id="stage-20de0008",
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["echo", "hi"],
        )

        with pytest.raises(CapacityError, match="No zones available"):
            backend.launch(spec)

    def test_rct_local_sim_compute_zone_available(self):
        """RCT-LOCAL-SIM-6: Launch succeeds when zone is available."""
        config = LocalComputeConfig(zone_availability={"local-zone-1": True})
        backend = LocalRunBackend(config=config)

        spec = RunSpec(
            stage_run_id="stage-20de0009",
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["echo", "hi"],
        )

        # This should not raise - zone is available
        # We don't actually run Docker in this test, just verify no CapacityError
        try:
            handle = backend.launch(spec)
            # Clean up if it succeeded
            backend.cleanup(handle)
        except CapacityError:
            pytest.fail("CapacityError should not be raised when zone is available")
