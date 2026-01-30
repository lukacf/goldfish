"""Tests for cloud contract types."""

from __future__ import annotations

import pytest

from goldfish.cloud.contracts import BackendCapabilities, RunHandle, StorageURI


class TestRunHandleFromDict:
    """Tests for RunHandle.from_dict deserialization."""

    def test_from_dict_valid_local_handle(self) -> None:
        """Valid local backend handle deserializes correctly."""
        data = {
            "stage_run_id": "stage-abc123",
            "backend_type": "local",
            "backend_handle": "abc123def456",
            "created_at": "2025-01-23T12:00:00Z",
            "zone": None,
        }
        handle = RunHandle.from_dict(data)
        assert handle.stage_run_id == "stage-abc123"
        assert handle.backend_type == "local"
        assert handle.backend_handle == "abc123def456"
        assert handle.created_at == "2025-01-23T12:00:00Z"
        assert handle.zone is None

    def test_from_dict_valid_gce_handle(self) -> None:
        """Valid GCE backend handle deserializes correctly."""
        data = {
            "stage_run_id": "stage-def456",
            "backend_type": "gce",
            "backend_handle": "goldfish-stage-def456",
            "created_at": None,
            "zone": "us-central1-a",
        }
        handle = RunHandle.from_dict(data)
        assert handle.stage_run_id == "stage-def456"
        assert handle.backend_type == "gce"
        assert handle.backend_handle == "goldfish-stage-def456"
        assert handle.zone == "us-central1-a"

    def test_from_dict_rejects_none_handle(self) -> None:
        """from_dict raises ValueError when backend_handle is None.

        This is a critical validation - None handle converted to "None" string
        would pass validation but create a bogus handle that can't be used.
        """
        data = {
            "stage_run_id": "stage-abc123",
            "backend_type": "local",
            "backend_handle": None,  # Bug: becomes "None" string via str()
            "created_at": None,
            "zone": None,
        }
        with pytest.raises(ValueError, match="backend_handle.*None"):
            RunHandle.from_dict(data)

    def test_from_dict_rejects_missing_handle(self) -> None:
        """from_dict raises error when backend_handle key is missing."""
        data = {
            "stage_run_id": "stage-abc123",
            "backend_type": "local",
            # backend_handle key missing entirely
            "created_at": None,
            "zone": None,
        }
        with pytest.raises((KeyError, ValueError)):
            RunHandle.from_dict(data)


class TestStorageURIParse:
    """Tests for StorageURI.parse method."""

    def test_storage_uri_rejects_empty_bucket_gs(self) -> None:
        """gs:// scheme must have non-empty bucket.

        gs:///path parses with bucket="" which would cause downstream issues
        when trying to access cloud storage. This should raise ValueError.
        """
        with pytest.raises(ValueError, match="bucket"):
            StorageURI.parse("gs:///path/to/file")

    def test_storage_uri_rejects_empty_bucket_s3(self) -> None:
        """s3:// scheme must have non-empty bucket.

        s3:///path parses with bucket="" which would cause downstream issues
        when trying to access cloud storage. This should raise ValueError.
        """
        with pytest.raises(ValueError, match="bucket"):
            StorageURI.parse("s3:///path/to/file")

    def test_storage_uri_accepts_file_scheme_without_bucket(self) -> None:
        """file:// scheme legitimately has no bucket (empty authority).

        file:///path/to/file is valid and bucket should be empty.
        """
        uri = StorageURI.parse("file:///path/to/file")
        assert uri.scheme == "file"
        assert uri.bucket == ""
        assert uri.path == "/path/to/file"

    def test_storage_uri_parses_valid_gs_uri(self) -> None:
        """Valid gs:// URI with bucket should parse correctly."""
        uri = StorageURI.parse("gs://my-bucket/path/to/file.txt")
        assert uri.scheme == "gs"
        assert uri.bucket == "my-bucket"
        assert uri.path == "path/to/file.txt"

    def test_storage_uri_parses_valid_s3_uri(self) -> None:
        """Valid s3:// URI with bucket should parse correctly."""
        uri = StorageURI.parse("s3://my-bucket/path/to/file.txt")
        assert uri.scheme == "s3"
        assert uri.bucket == "my-bucket"
        assert uri.path == "path/to/file.txt"

    def test_storage_uri_rejects_path_traversal(self) -> None:
        """Path traversal should be rejected."""
        with pytest.raises(ValueError, match="traversal"):
            StorageURI.parse("gs://bucket/../secret")


class TestBackendCapabilitiesRoundTrip:
    """Tests for BackendCapabilities serialization round-trip."""

    def test_backend_capabilities_roundtrip_default(self) -> None:
        """Default BackendCapabilities serializes and deserializes correctly."""
        import dataclasses

        caps = BackendCapabilities()
        # Serialize to dict
        data = dataclasses.asdict(caps)
        # Deserialize back
        caps2 = BackendCapabilities(**data)
        # Verify all fields match
        assert caps == caps2

    def test_backend_capabilities_roundtrip_custom(self) -> None:
        """Custom BackendCapabilities serializes and deserializes correctly."""
        import dataclasses

        caps = BackendCapabilities(
            supports_gpu=True,
            supports_spot=True,
            supports_preemption=True,
            supports_preemption_detection=True,
            supports_live_logs=True,
            supports_metrics=True,
            max_run_duration_hours=24,
            ack_timeout_seconds=3.0,
            ack_timeout_running_seconds=4.0,
            has_launch_delay=True,
            logs_unavailable_message="Custom message",
            timeout_becomes_pending=True,
            status_message_for_preparing="Custom preparing...",
            zone_resolution_method="handle",
        )
        # Serialize to dict
        data = dataclasses.asdict(caps)
        # Deserialize back
        caps2 = BackendCapabilities(**data)
        # Verify all fields match
        assert caps == caps2
        assert caps2.ack_timeout_seconds == 3.0
        assert caps2.logs_unavailable_message == "Custom message"
        assert caps2.status_message_for_preparing == "Custom preparing..."
        assert caps2.zone_resolution_method == "handle"

    def test_backend_capabilities_has_all_required_fields(self) -> None:
        """BackendCapabilities has all fields required for de-googlify."""
        caps = BackendCapabilities()

        # Sync behavior fields (required for Phase 0)
        assert hasattr(caps, "ack_timeout_seconds")
        assert hasattr(caps, "logs_unavailable_message")
        assert hasattr(caps, "has_launch_delay")
        assert hasattr(caps, "timeout_becomes_pending")
        assert hasattr(caps, "status_message_for_preparing")
        assert hasattr(caps, "zone_resolution_method")

        # Verify types
        assert isinstance(caps.ack_timeout_seconds, float)
        assert isinstance(caps.logs_unavailable_message, str)
        assert isinstance(caps.has_launch_delay, bool)
        assert isinstance(caps.timeout_becomes_pending, bool)
        assert isinstance(caps.status_message_for_preparing, str)
        assert isinstance(caps.zone_resolution_method, str)
