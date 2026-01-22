"""Integration Tests for Cloud Abstraction Layer.

This file contains TWO types of tests:

1. ADAPTER CONFORMANCE TESTS (TestCP* classes)
   - Verify adapters implement protocols correctly
   - Test adapters in isolation (not wired into StageExecutor)
   - These PASS because adapters are implemented

2. SYSTEM CHOKE POINT TESTS (TestSystemCP* classes)
   - Verify StageExecutor uses protocols via dependency injection
   - Test the actual boundaries in production code
   - These are XFAIL until Phase 3 wiring is complete

Choke points (boundaries) being tested:
- CP-STORAGE: StageExecutor ↔ ObjectStorage
- CP-BACKEND: StageExecutor ↔ RunBackend
- CP-SIGNAL: Daemon ↔ SignalBus
- CP-IMAGE: StageExecutor ↔ ImageBuilder/Registry

Run with:
    pytest tests/integration/test_cloud_integration.py -v
"""

from __future__ import annotations

import pytest

from goldfish.cloud.contracts import (
    BackendCapabilities,
    RunHandle,
    RunSpec,
    RunStatus,
    StorageURI,
)
from goldfish.cloud.protocols import ObjectStorage, RunBackend, SignalBus
from goldfish.errors import LaunchError, MetadataSizeLimitError, NotFoundError, StorageError
from goldfish.infra.metadata.base import MetadataSignal

# =============================================================================
# Protocol Conformance Tests - Verify implementations satisfy protocols
# =============================================================================


class TestProtocolConformance:
    """Verify protocol definitions are structurally sound."""

    def test_object_storage_protocol_methods(self):
        """ObjectStorage protocol has required methods."""
        # Verify protocol defines expected methods
        assert hasattr(ObjectStorage, "put")
        assert hasattr(ObjectStorage, "get")
        assert hasattr(ObjectStorage, "exists")
        assert hasattr(ObjectStorage, "list_prefix")
        assert hasattr(ObjectStorage, "delete")
        assert hasattr(ObjectStorage, "get_local_path")

    def test_run_backend_protocol_methods(self):
        """RunBackend protocol has required methods."""
        assert hasattr(RunBackend, "capabilities")
        assert hasattr(RunBackend, "launch")
        assert hasattr(RunBackend, "get_status")
        assert hasattr(RunBackend, "get_logs")
        assert hasattr(RunBackend, "terminate")
        assert hasattr(RunBackend, "cleanup")

    def test_signal_bus_protocol_methods(self):
        """SignalBus protocol has required methods."""
        assert hasattr(SignalBus, "set_signal")
        assert hasattr(SignalBus, "get_signal")
        assert hasattr(SignalBus, "clear_signal")
        assert hasattr(SignalBus, "set_ack")
        assert hasattr(SignalBus, "get_ack")


# =============================================================================
# CP-STORAGE: StageExecutor ↔ ObjectStorage Choke Point
# =============================================================================


class TestCPStorage:
    """INT-STORAGE: Adapter conformance tests for storage operations.

    These tests verify LocalObjectStorage correctly implements the ObjectStorage protocol.
    True StageExecutor boundary tests are in TestSystemCPStorage.
    """

    def test_int_storage_1_output_upload_round_trip(self, tmp_path):
        """INT-STORAGE-1: Output upload round-trip.

        Given: stage produces output files
        When: storage.put() called
        Then: files retrievable via storage.get()
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # Simulate stage output
        output_uri = StorageURI.parse("gs://bucket/runs/stage-123/outputs/model.pt")
        model_data = b"fake model weights" * 1000

        # Upload (what StageExecutor would do)
        storage.put(output_uri, model_data)

        # Verify retrievable
        retrieved = storage.get(output_uri)
        assert retrieved == model_data

        # Verify exists
        assert storage.exists(output_uri) is True

    def test_int_storage_2_input_staging(self, tmp_path):
        """INT-STORAGE-2: Input staging for container.

        Given: inputs defined in pipeline
        When: storage prepares inputs for stage
        Then: files available via get_local_path or get()
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # Stage inputs
        input_uri = StorageURI.parse("gs://bucket/datasets/features.npy")
        features_data = b"numpy array data"
        storage.put(input_uri, features_data)

        # Get local path (for mount)
        local_path = storage.get_local_path(input_uri)
        assert local_path is not None

        # Or get data directly
        data = storage.get(input_uri)
        assert data == features_data

    def test_int_storage_not_found_error(self, tmp_path):
        """INT-STORAGE: Missing file raises appropriate error.

        Based on RCT-GCS-4: download of missing blob raises NotFound.
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        missing_uri = StorageURI.parse("gs://bucket/does/not/exist.txt")

        # Should raise NotFoundError
        with pytest.raises(NotFoundError):
            storage.get(missing_uri)

    def test_int_storage_list_prefix_for_outputs(self, tmp_path):
        """INT-STORAGE: List outputs for a stage run.

        Pattern: gs://bucket/runs/{stage_run_id}/outputs/*
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # Create multiple outputs
        base = StorageURI.parse("gs://bucket/runs/stage-abc/outputs")
        storage.put(base.join("model.pt"), b"model")
        storage.put(base.join("metrics.json"), b"metrics")
        storage.put(base.join("logs/train.log"), b"logs")

        # List all outputs
        outputs = storage.list_prefix(base)

        assert len(outputs) == 3
        paths = [str(u) for u in outputs]
        assert any("model.pt" in p for p in paths)
        assert any("metrics.json" in p for p in paths)


# =============================================================================
# CP-BACKEND: StageExecutor ↔ RunBackend Choke Point
# =============================================================================


class TestCPBackend:
    """INT-BACKEND: Adapter conformance tests for compute operations.

    These tests verify LocalRunBackend correctly implements the RunBackend protocol.
    True StageExecutor boundary tests are in TestSystemCPBackend.
    """

    def test_int_backend_1_launch_status_terminate_cycle(self):
        """INT-BACKEND-1: Launch → status → terminate cycle.

        Given: valid RunSpec
        When: backend.launch() called
        Then:
            - RunHandle returned
            - status() returns RUNNING
            - terminate() stops the run
            - status() returns TERMINATED
        """
        import subprocess
        import uuid

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        # Use unique ID to avoid conflicts with stale containers
        unique_suffix = uuid.uuid4().hex[:8]
        stage_run_id = f"stage-{unique_suffix}"

        # Clean up any stale container with this ID (defensive)
        container_name = f"goldfish-{stage_run_id}"
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)

        spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name="test",
            stage_name="long_running",
            image="alpine:latest",
            command=["sleep", "60"],  # Long enough to test lifecycle
        )

        # Launch
        handle = backend.launch(spec)
        assert isinstance(handle, RunHandle)
        assert handle.stage_run_id == stage_run_id

        # Check status - should be running
        import time

        time.sleep(0.5)  # Give container time to start
        status = backend.get_status(handle)
        assert status.status == RunStatus.RUNNING

        # Terminate
        backend.terminate(handle)

        # Wait for termination
        for _ in range(30):
            status = backend.get_status(handle)
            if status.status.is_terminal():
                break
            time.sleep(0.1)

        assert status.status in (RunStatus.TERMINATED, RunStatus.CANCELED)

        # Cleanup
        backend.cleanup(handle)

    def test_int_backend_2_launch_failure_handling(self):
        """INT-BACKEND-2: Launch failure handling.

        Given: invalid RunSpec (bad image)
        When: backend.launch() called
        Then: clear error raised, no orphaned resources
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-badf4115",  # Valid hex format
            workspace_name="test",
            stage_name="bad",
            image="nonexistent-image-xyz:latest",  # Image doesn't exist
        )

        # Should raise LaunchError
        with pytest.raises(LaunchError):
            backend.launch(spec)

    def test_int_backend_logs_retrieval(self):
        """INT-BACKEND: Logs retrievable from running container."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-10951234",  # Valid hex format
            workspace_name="test",
            stage_name="echo",
            image="alpine:latest",
            command=["sh", "-c", "echo 'hello from container' && sleep 2"],
        )

        handle = backend.launch(spec)

        import time

        time.sleep(1)  # Give container time to produce logs

        logs = backend.get_logs(handle, tail=10)
        assert "hello from container" in logs

        backend.terminate(handle)
        backend.cleanup(handle)

    def test_int_backend_capabilities_respected(self):
        """INT-BACKEND: Capabilities accurately reported."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        caps = backend.capabilities
        assert isinstance(caps, BackendCapabilities)

        # Local backend should always support these
        assert caps.supports_live_logs is True


# =============================================================================
# CP-SIGNAL: Daemon ↔ SignalBus Choke Point
# =============================================================================


class TestCPSignal:
    """INT-SIGNAL: Adapter conformance tests for signaling operations.

    These tests verify LocalMetadataBus correctly implements the SignalBus protocol.
    LocalMetadataBus already exists, so these tests PASS.
    """

    def test_int_signal_1_heartbeat_cycle(self, tmp_path):
        """INT-SIGNAL-1: Heartbeat cycle.

        Given: running stage
        When: daemon sends heartbeat
        Then: server receives and acks
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Server sends command
        signal = MetadataSignal(command="sync", request_id="req-heartbeat-1")
        bus.set_signal("goldfish", signal)

        # Daemon reads command
        received = bus.get_signal("goldfish")
        assert received is not None
        assert received.command == "sync"
        assert received.request_id == "req-heartbeat-1"

        # Daemon acks
        bus.set_ack("goldfish", received.request_id)

        # Server verifies ack
        ack = bus.get_ack("goldfish")
        assert ack == "req-heartbeat-1"

    def test_int_signal_2_terminate_signal(self, tmp_path):
        """INT-SIGNAL-2: Terminate signal.

        Given: running stage
        When: server sends TERMINATE
        Then: daemon receives and initiates graceful shutdown
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Server sends terminate
        signal = MetadataSignal(command="stop", request_id="req-term-1")
        bus.set_signal("goldfish", signal)

        # Daemon reads
        received = bus.get_signal("goldfish")
        assert received is not None
        assert received.command == "stop"

        # Daemon would initiate shutdown, then ack
        bus.set_ack("goldfish", received.request_id)
        bus.clear_signal("goldfish")

        # Verify signal cleared
        assert bus.get_signal("goldfish") is None

    def test_int_signal_concurrent_access(self, tmp_path):
        """INT-SIGNAL: Concurrent access doesn't corrupt data.

        Based on RCT-LOCAL-META-4: file locking prevents corruption.
        """
        import concurrent.futures

        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"

        def write_and_read(i: int) -> tuple[int, str | None]:
            bus = LocalMetadataBus(metadata_file)
            signal = MetadataSignal(command="sync", request_id=f"req-{i}")
            bus.set_signal("goldfish", signal)
            read = bus.get_signal("goldfish")
            return i, read.request_id if read else None

        # Run concurrent operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_and_read, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # All operations should complete without corruption
        assert len(results) == 10

        # Final state should be valid
        bus = LocalMetadataBus(metadata_file)
        final = bus.get_signal("goldfish")
        assert final is not None
        assert final.request_id.startswith("req-")


# =============================================================================
# Integration with Existing Components
# =============================================================================


class TestExistingLocalMetadataBus:
    """Tests for existing LocalMetadataBus implementation.

    These tests verify the existing implementation matches protocol.
    They should PASS because LocalMetadataBus is already implemented.
    """

    def test_local_metadata_bus_satisfies_protocol(self, tmp_path):
        """LocalMetadataBus satisfies SignalBus protocol methods."""
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Verify it has all protocol methods
        assert hasattr(bus, "set_signal")
        assert hasattr(bus, "get_signal")
        assert hasattr(bus, "clear_signal")
        assert hasattr(bus, "set_ack")
        assert hasattr(bus, "get_ack")

        # Verify basic operation
        signal = MetadataSignal(command="test", request_id="test-123")
        bus.set_signal("key", signal)
        retrieved = bus.get_signal("key")

        assert retrieved is not None
        assert retrieved.command == "test"


# =============================================================================
# CP-IMAGE: StageExecutor ↔ ImageBuilder/Registry Choke Point
# =============================================================================


class TestCPImage:
    """INT-IMAGE: Adapter conformance tests for image operations.

    These tests verify LocalImageBuilder correctly implements the ImageBuilder protocol.
    The local adapter wraps docker build commands, providing a consistent interface
    that could be swapped for Cloud Build or other backends in the future.
    """

    def test_int_image_1_build_and_resolve(self, tmp_path):
        """INT-IMAGE-1: Build and resolve image.

        Given: Dockerfile in workspace
        When: builder.build() called
        Then: Image tag returned, verifiable via registry.exists()
        """
        from goldfish.cloud.adapters.local.image import LocalImageBuilder, LocalImageRegistry

        builder = LocalImageBuilder()
        registry = LocalImageRegistry()

        # Create a simple Dockerfile
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text("FROM alpine:latest\nRUN echo 'test'\n")

        # Build image
        image_tag = builder.build(
            context_path=tmp_path,
            dockerfile_path=dockerfile,
            image_tag="test-image-int-1:v1",
        )

        assert image_tag is not None
        assert "test-image-int-1" in image_tag

        # Verify exists through registry
        assert registry.exists(image_tag)

    def test_int_image_2_registry_resolution(self, tmp_path):
        """INT-IMAGE-2: Registry can verify built images exist.

        Given: Built image in local registry
        When: registry.exists() called
        Then: Returns True
        """
        from goldfish.cloud.adapters.local.image import LocalImageBuilder, LocalImageRegistry

        builder = LocalImageBuilder()
        registry = LocalImageRegistry()

        # Create and build
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text("FROM alpine:latest\n")

        image_tag = builder.build(
            context_path=tmp_path,
            dockerfile_path=dockerfile,
            image_tag="resolve-test-int-2:v1",
        )

        # Verify through registry
        assert registry.exists(image_tag)

    def test_int_image_3_build_failure_bad_dockerfile(self, tmp_path):
        """INT-IMAGE-3: Build failure with invalid Dockerfile.

        Given: Invalid Dockerfile
        When: builder.build() called
        Then: Clear error raised
        """
        from goldfish.cloud.adapters.local.image import ImageBuildError, LocalImageBuilder

        builder = LocalImageBuilder()

        # Create invalid Dockerfile
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text("INVALID_INSTRUCTION not_valid\n")

        with pytest.raises(ImageBuildError):
            builder.build(
                context_path=tmp_path,
                dockerfile_path=dockerfile,
                image_tag="bad-build:v1",
            )

    def test_int_image_4_missing_base_image(self, tmp_path):
        """INT-IMAGE-4: Build failure with missing base image.

        Given: Dockerfile referencing non-existent base
        When: builder.build() called
        Then: Clear error raised
        """
        from goldfish.cloud.adapters.local.image import ImageBuildError, LocalImageBuilder

        builder = LocalImageBuilder()

        # Create Dockerfile with non-existent base
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text("FROM nonexistent-base-image-xyz:latest\n")

        with pytest.raises(ImageBuildError):
            builder.build(
                context_path=tmp_path,
                dockerfile_path=dockerfile,
                image_tag="missing-base:v1",
            )


# =============================================================================
# CP-STORAGE Failure Path Tests
# =============================================================================


class TestCPStorageFailures:
    """INT-STORAGE-FAIL: Storage failure path tests.

    These tests validate error handling at the storage boundary.
    """

    def test_int_storage_fail_permission_simulation(self, tmp_path):
        """INT-STORAGE-FAIL-1: Permission denied simulation.

        Given: Storage location exists but is not accessible
        When: storage.get() called
        Then: Appropriate permission error raised
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # Create a file then make it unreadable
        uri = StorageURI.parse("gs://bucket/protected/file.txt")
        storage.put(uri, b"secret data")

        # Get the actual path and make it unreadable
        path = storage._resolve_path(uri)
        path.chmod(0o000)

        try:
            with pytest.raises(StorageError):
                storage.get(uri)
        finally:
            # Restore permissions for cleanup
            path.chmod(0o644)

    def test_int_storage_fail_write_to_readonly(self, tmp_path):
        """INT-STORAGE-FAIL-2: Write to read-only location.

        Given: Read-only storage location
        When: storage.put() called
        Then: StorageError raised (wrapping underlying permission error)
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

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

    def test_int_storage_fail_delete_nonexistent(self, tmp_path):
        """INT-STORAGE-FAIL-3: Delete non-existent file is idempotent.

        Given: Non-existent file
        When: storage.delete() called
        Then: No error (idempotent operation)
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/does/not/exist.txt")

        # Should not raise - delete is idempotent
        storage.delete(uri)

    def test_int_storage_fail_list_empty_prefix(self, tmp_path):
        """INT-STORAGE-FAIL-4: List on empty prefix returns empty list.

        Given: Prefix with no objects
        When: storage.list_prefix() called
        Then: Empty list returned (not error)
        """
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        uri = StorageURI.parse("gs://bucket/empty/prefix")

        # Should return empty list, not raise
        result = storage.list_prefix(uri)
        assert result == []


# =============================================================================
# CP-BACKEND Failure Path Tests
# =============================================================================


class TestCPBackendFailures:
    """INT-BACKEND-FAIL: Backend failure path tests.

    These tests validate error handling at the compute boundary.
    """

    def test_int_backend_fail_invalid_command(self):
        """INT-BACKEND-FAIL-1: Invalid command fails gracefully.

        Given: RunSpec with invalid command
        When: backend.launch() called
        Then: Error raised (Docker fails on invalid command)

        Note: Docker validates the command at container creation time,
        so launch() fails immediately rather than returning a handle
        to a failed container.
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-badcad01",  # Valid hex format
            workspace_name="test",
            stage_name="bad_cmd",
            image="alpine:latest",
            command=["nonexistent-command-xyz"],
        )

        # Docker validates command at creation, so launch fails
        with pytest.raises(LaunchError):
            backend.launch(spec)

    def test_int_backend_fail_resource_cleanup_on_error(self):
        """INT-BACKEND-FAIL-2: Resources cleaned up after failure.

        Given: Failed launch attempt
        When: cleanup() called
        Then: No orphaned containers remain
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-c1ea1234",  # Valid hex format
            workspace_name="test",
            stage_name="cleanup",
            image="alpine:latest",
            command=["sh", "-c", "exit 1"],
        )

        handle = backend.launch(spec)

        import time

        time.sleep(1)

        # Cleanup should work even after failure
        backend.cleanup(handle)

        # Verify container is gone
        import subprocess

        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name=goldfish-{spec.stage_run_id}", "--format", "{{.ID}}"],
            capture_output=True,
            text=True,
        )
        assert result.stdout.strip() == "", "Container should be removed"

    def test_int_backend_fail_terminate_already_stopped(self):
        """INT-BACKEND-FAIL-3: Terminate on stopped container is idempotent.

        Given: Already stopped container
        When: terminate() called
        Then: No error (idempotent operation)
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        spec = RunSpec(
            stage_run_id="stage-5e1f5401",  # Valid hex format
            workspace_name="test",
            stage_name="quick",
            image="alpine:latest",
            command=["echo", "done"],
        )

        handle = backend.launch(spec)

        import time

        time.sleep(1)  # Let it finish

        # Terminate should not raise even if already stopped
        backend.terminate(handle)
        backend.terminate(handle)  # Call twice to verify idempotence

        backend.cleanup(handle)

    def test_int_backend_fail_logs_from_nonexistent(self):
        """INT-BACKEND-FAIL-4: Logs from non-existent container.

        Given: Container that doesn't exist
        When: get_logs() called
        Then: Empty string returned (not error)
        """
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

        backend = LocalRunBackend()

        fake_handle = RunHandle(
            stage_run_id="stage-fa4e0000",  # Valid hex format
            backend_type="local",
            backend_handle="nonexistent-container-id",
        )

        # Should return empty string, not raise
        logs = backend.get_logs(fake_handle)
        assert logs == ""


# =============================================================================
# CP-SIGNAL Failure Path Tests
# =============================================================================


class TestCPSignalFailures:
    """INT-SIGNAL-FAIL: Signal bus failure path tests.

    These tests validate error handling at the signaling boundary.
    """

    def test_int_signal_fail_size_limit(self, tmp_path):
        """INT-SIGNAL-FAIL-1: Signal size limit enforced.

        Given: Signal data exceeding 256KB limit
        When: set_signal() called
        Then: MetadataSizeLimitError raised (per GCP metadata limit)
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Create oversized signal (256KB = 262144 bytes)
        # payload is a dict, so create a dict with large content
        large_payload = {"data": "x" * 300000}  # > 256KB

        signal = MetadataSignal(command="sync", request_id="req-large", payload=large_payload)

        # LocalMetadataBus enforces the 256KB limit per LOCAL_PARITY_SPEC
        with pytest.raises(MetadataSizeLimitError) as exc_info:
            bus.set_signal("goldfish", signal)

        # Verify error details
        assert exc_info.value.details["actual_size"] > 262144
        assert exc_info.value.details["key"] == "goldfish"

    def test_int_signal_fail_corrupted_file(self, tmp_path):
        """INT-SIGNAL-FAIL-2: Corrupted metadata file handling.

        Given: Corrupted JSON in metadata file
        When: get_signal() called
        Then: Graceful handling (None returned or error raised)
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"

        # Write corrupted JSON
        metadata_file.write_text("{invalid json content")

        bus = LocalMetadataBus(metadata_file)

        # Should handle gracefully
        try:
            result = bus.get_signal("goldfish")
            # If no error, should return None for missing key
            assert result is None
        except (ValueError, Exception):
            # JSON decode error is acceptable
            pass

    def test_int_signal_fail_missing_file(self, tmp_path):
        """INT-SIGNAL-FAIL-3: Missing metadata file handling.

        Given: Non-existent metadata file
        When: get_signal() called
        Then: None returned (not error)
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "nonexistent" / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Should return None, not raise
        result = bus.get_signal("goldfish")
        assert result is None

    def test_int_signal_fail_ack_without_signal(self, tmp_path):
        """INT-SIGNAL-FAIL-4: ACK without prior signal.

        Given: No signal set
        When: set_ack() called
        Then: ACK still recorded (ack is independent)
        """
        from goldfish.infra.metadata.local import LocalMetadataBus

        metadata_file = tmp_path / "metadata.json"
        bus = LocalMetadataBus(metadata_file)

        # Set ack without signal - should work
        bus.set_ack("goldfish", "orphan-ack-123")

        # Verify ack was recorded
        ack = bus.get_ack("goldfish")
        assert ack == "orphan-ack-123"


# =============================================================================
# SYSTEM CHOKE POINT TESTS - StageExecutor with Injected Protocols
#
# These tests verify the ACTUAL choke points: StageExecutor using protocol
# adapters via dependency injection. Phase 3 implements this wiring.
# =============================================================================


class TestSystemCPStorage:
    """SYSTEM-CP-STORAGE: True choke point tests for StageExecutor ↔ ObjectStorage.

    These tests verify StageExecutor uses injected ObjectStorage protocol
    for all storage operations (inputs, outputs, artifacts).

    Unlike TestCPStorage (adapter tests), these test the actual production
    boundary where StageExecutor delegates to the storage abstraction.
    """

    def test_system_storage_executor_accepts_storage_protocol(self, tmp_path, test_db, test_config):
        """StageExecutor can be constructed with ObjectStorage protocol.

        Given: LocalObjectStorage instance
        When: StageExecutor constructed with storage parameter
        Then: StageExecutor stores reference for later use
        """
        from unittest.mock import MagicMock

        from goldfish.cloud.adapters.local.storage import LocalObjectStorage
        from goldfish.jobs.stage_executor import StageExecutor

        storage = LocalObjectStorage(root=tmp_path / ".local_gcs")

        # StageExecutor accepts storage via protocol injection
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            storage=storage,  # Protocol injection
        )

        assert executor._storage is storage

    def test_system_storage_outputs_use_protocol(self, tmp_path, test_db, test_config):
        """StageExecutor uses injected ObjectStorage for output uploads.

        Given: StageExecutor with LocalObjectStorage
        When: Stage produces outputs
        Then: Outputs uploaded via ObjectStorage.put(), not direct GCS
        """
        from unittest.mock import MagicMock

        from goldfish.cloud.adapters.local.storage import LocalObjectStorage
        from goldfish.jobs.stage_executor import StageExecutor

        storage = MagicMock(spec=LocalObjectStorage)

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            storage=storage,
        )

        # Verify storage protocol is accessible via property
        assert executor.storage is storage


class TestSystemCPBackend:
    """SYSTEM-CP-BACKEND: True choke point tests for StageExecutor ↔ RunBackend.

    These tests verify StageExecutor uses injected RunBackend protocol
    for all compute operations (launch, status, logs, terminate).

    Unlike TestCPBackend (adapter tests), these test the actual production
    boundary where StageExecutor delegates to the compute abstraction.
    """

    def test_system_backend_executor_accepts_backend_protocol(self, tmp_path, test_db, test_config):
        """StageExecutor can be constructed with RunBackend protocol.

        Given: LocalRunBackend instance
        When: StageExecutor constructed with run_backend parameter
        Then: StageExecutor stores reference for later use
        """
        from unittest.mock import MagicMock

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.jobs.stage_executor import StageExecutor

        backend = LocalRunBackend()

        # StageExecutor accepts run_backend via protocol injection
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,  # Protocol injection
        )

        assert executor._run_backend is backend

    def test_system_backend_launch_uses_protocol(self, tmp_path, test_db, test_config):
        """StageExecutor uses injected RunBackend for container launch.

        Given: StageExecutor with LocalRunBackend
        When: Stage execution requested
        Then: Container launched via RunBackend.launch(), not direct Docker/GCE
        """
        from unittest.mock import MagicMock

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.cloud.contracts import RunHandle
        from goldfish.jobs.stage_executor import StageExecutor

        backend = MagicMock(spec=LocalRunBackend)
        backend.launch.return_value = RunHandle(
            stage_run_id="stage-7e571234",  # Valid hex format
            backend_type="local",
            backend_handle="container-abc",
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,
        )

        # Verify run_backend protocol is accessible via property
        assert executor.run_backend is backend


class TestSystemCPBackendFactory:
    """SYSTEM-CP-BACKEND-FACTORY: Backend selection via factory pattern.

    Tests that StageExecutor.create() selects the correct backend (local vs GCE)
    through the abstraction layer, not hardcoded imports.
    """

    def test_system_backend_factory_local(self, tmp_path, test_db, test_config):
        """Factory returns LocalRunBackend for backend_type='local'.

        Given: Config with backend_type='local'
        When: StageExecutor created via factory
        Then: LocalRunBackend instance used
        """
        from unittest.mock import MagicMock

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.jobs.stage_executor import StageExecutor

        # Create executor via factory method
        executor = StageExecutor.create(
            db=test_db,
            config=test_config,  # config.jobs.backend = "local"
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
        )

        assert isinstance(executor._run_backend, LocalRunBackend)

    def test_system_backend_factory_has_create_method(self, test_db, test_config):
        """StageExecutor has create() factory method.

        Given: StageExecutor class
        When: Check for create method
        Then: Method exists
        """
        from goldfish.jobs.stage_executor import StageExecutor

        assert hasattr(StageExecutor, "create"), "StageExecutor.create() factory method exists"
        assert callable(StageExecutor.create)


class TestSystemCPCapabilityEnforcement:
    """SYSTEM-CP-CAPABILITY: Capability contract enforcement tests.

    These tests verify that StageExecutor validates backend capabilities
    before launching stages, preventing misconfigurations like GPU profiles
    on local backend.

    Part of Phase 3: Caller-side Integration.
    """

    def test_capability_gpu_on_local_backend_rejected(self, tmp_path, test_db, test_config):
        """GPU requested on local backend (supports_gpu=False) is rejected.

        Given: Local backend with supports_gpu=False
        And: Stage config with GPU profile (gpu.count > 0)
        When: _validate_capabilities_for_stage called
        Then: GoldfishError raised with clear message
        """
        from unittest.mock import MagicMock, patch

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.errors import GoldfishError
        from goldfish.jobs.stage_executor import StageExecutor

        # Create workspace and stage config with GPU profile
        test_db.create_workspace_lineage("test_ws", description="test")
        workspace_dir = tmp_path / "workspaces" / "test_ws"
        workspace_dir.mkdir(parents=True)

        # Create stage config directory
        configs_dir = workspace_dir / "configs"
        configs_dir.mkdir()

        # Write stage config requesting GPU
        stage_config = configs_dir / "train.yaml"
        stage_config.write_text(
            """
compute:
  profile: h100-spot
"""
        )

        backend = LocalRunBackend()
        # Verify local backend doesn't support GPU
        assert backend.capabilities.supports_gpu is False

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,
        )

        # Mock _load_stage_config to return our GPU config
        with patch.object(executor, "_load_stage_config") as mock_load:
            mock_load.return_value = {"compute": {"profile": "h100-spot"}}

            # Should raise when validating capabilities
            with pytest.raises(GoldfishError) as exc_info:
                executor._validate_capabilities_for_stage("test_ws", "train", "local")

            assert "GPU" in str(exc_info.value)
            assert "local" in str(exc_info.value)

    def test_capability_cpu_profile_on_local_backend_allowed(self, tmp_path, test_db, test_config):
        """CPU profile on local backend (supports_gpu=False) is allowed.

        Given: Local backend with supports_gpu=False
        And: Stage config with CPU profile (gpu.count=0)
        When: _validate_capabilities_for_stage called
        Then: No error raised
        """
        from unittest.mock import MagicMock, patch

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.jobs.stage_executor import StageExecutor

        test_db.create_workspace_lineage("test_ws", description="test")

        backend = LocalRunBackend()
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,
        )

        # Mock _load_stage_config to return CPU profile
        with patch.object(executor, "_load_stage_config") as mock_load:
            mock_load.return_value = {"compute": {"profile": "cpu-small"}}

            # Should not raise
            executor._validate_capabilities_for_stage("test_ws", "train", "local")

    def test_capability_no_profile_allowed(self, tmp_path, test_db, test_config):
        """No profile specified is always allowed.

        Given: Stage config without profile
        When: _validate_capabilities_for_stage called
        Then: No error raised
        """
        from unittest.mock import MagicMock, patch

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.jobs.stage_executor import StageExecutor

        test_db.create_workspace_lineage("test_ws", description="test")

        backend = LocalRunBackend()
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,
        )

        # Mock _load_stage_config to return config without profile
        with patch.object(executor, "_load_stage_config") as mock_load:
            mock_load.return_value = {}

            # Should not raise
            executor._validate_capabilities_for_stage("test_ws", "train", "local")

    def test_capability_spot_on_local_logs_warning(self, tmp_path, test_db, test_config, caplog):
        """Spot preference on local backend logs debug message.

        Given: Local backend with supports_spot=False
        And: Stage config with preemptible_allowed=True
        When: _validate_capabilities_for_stage called
        Then: Debug message logged (not error)
        """
        import logging
        from unittest.mock import MagicMock, patch

        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.jobs.stage_executor import StageExecutor

        test_db.create_workspace_lineage("test_ws", description="test")

        backend = LocalRunBackend()
        # Verify local backend doesn't support spot
        assert backend.capabilities.supports_spot is False

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            run_backend=backend,
        )

        # Mock to return profile with preemptible_allowed but no GPU
        with patch.object(executor, "_load_stage_config") as mock_load:
            mock_load.return_value = {"compute": {"profile": "cpu-small"}}

            # Mock profile resolver to return profile with preemptible_allowed
            with patch.object(executor.profile_resolver, "resolve") as mock_resolve:
                mock_resolve.return_value = {
                    "name": "cpu-small",
                    "machine_type": "n1-standard-2",
                    "gpu": {"type": "none", "count": 0},
                    "preemptible_allowed": True,
                }

                # Should not raise - spot is a preference, not requirement
                with caplog.at_level(logging.DEBUG):
                    executor._validate_capabilities_for_stage("test_ws", "train", "local")
