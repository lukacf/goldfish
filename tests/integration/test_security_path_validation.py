"""Tests for path validation security - CRITICAL.

TDD: These tests must fail first, then we implement the fixes.
"""

from unittest.mock import MagicMock

import pytest

from goldfish.errors import GoldfishError


def get_tool_fn(tool):
    """Get the underlying function from a FastMCP tool."""
    return tool.fn if hasattr(tool, "fn") else tool


def valid_file_metadata() -> dict:
    """Return valid file metadata for promote_artifact tests."""
    return {
        "schema_version": 1,
        "description": "Model artifact JSON file for validation tests.",
        "source": {
            "format": "file",
            "size_bytes": 123,
            "created_at": "2025-12-24T12:00:00Z",
        },
        "schema": {"kind": "file", "content_type": "application/json"},
    }


class TestLogUriPathTraversal:
    """Tests that log_uri is validated before file read."""

    def test_rejects_absolute_path_outside_project(self, temp_dir):
        """log_uri pointing to /etc/passwd should be rejected."""
        from goldfish.db.database import Database
        from goldfish.jobs.tracker import JobTracker

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        # Create a job with malicious log_uri
        db.create_job(
            job_id="job-malicious",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-malicious",
            status="completed",
            log_uri="/etc/passwd",  # Path traversal attempt
        )

        # Should reject this path
        with pytest.raises(GoldfishError, match="Invalid log path"):
            tracker.get_job_logs("job-malicious")

    def test_rejects_path_traversal_in_log_uri(self, temp_dir):
        """log_uri with ../ should be rejected."""
        from goldfish.db.database import Database
        from goldfish.jobs.tracker import JobTracker

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        db.create_job(
            job_id="job-traversal",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-traversal",
            status="completed",
            log_uri=f"file://{temp_dir}/../../../etc/passwd",
        )

        with pytest.raises(GoldfishError, match="Invalid log path"):
            tracker.get_job_logs("job-traversal")

    def test_accepts_valid_log_path_within_project(self, temp_dir):
        """Valid log_uri within project should work."""
        from goldfish.db.database import Database
        from goldfish.jobs.tracker import JobTracker

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        # Create a valid log file
        log_file = temp_dir / "experiments" / "job-valid" / "output.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("Job completed successfully")

        db.create_job(
            job_id="job-valid",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-valid",
            status="completed",
            log_uri=f"file://{log_file}",
        )

        logs = tracker.get_job_logs("job-valid")
        assert logs == "Job completed successfully"


class TestArtifactUriValidation:
    """Tests that artifact_uri is validated in promote_artifact."""

    def test_rejects_artifact_uri_with_path_traversal(self, temp_dir):
        """artifact_uri with ../ should be rejected."""
        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Create a job with malicious artifact_uri
        # Note: job ID must match format job-{8 hex chars}
        db.create_job(
            job_id="job-a1b2c3d4",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-a1b2c3d4",
            status="completed",
            artifact_uri="gs://attacker-bucket/../../../sensitive-bucket/data",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = get_tool_fn(server.promote_artifact)
            with pytest.raises(GoldfishError, match="Invalid artifact URI"):
                promote_fn(
                    job_id="job-a1b2c3d4",
                    output_name="model",
                    source_name="promoted_model",
                    metadata=valid_file_metadata(),
                    reason="Testing path traversal rejection",
                )
        finally:
            server.reset_server()

    def test_rejects_artifact_uri_without_gs_prefix(self, temp_dir):
        """artifact_uri must start with gs://."""
        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Note: job ID must match format job-{8 hex chars}
        db.create_job(
            job_id="job-b2c3d4e5",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-b2c3d4e5",
            status="completed",
            artifact_uri="/etc/passwd",  # Not GCS
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = get_tool_fn(server.promote_artifact)
            with pytest.raises(GoldfishError, match="Invalid artifact URI"):
                promote_fn(
                    job_id="job-b2c3d4e5",
                    output_name="model",
                    source_name="promoted_model",
                    metadata=valid_file_metadata(),
                    reason="Testing non-GCS URI rejection",
                )
        finally:
            server.reset_server()

    def test_accepts_valid_gcs_artifact_uri(self, temp_dir):
        """Valid gs:// artifact_uri should work."""
        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Note: job ID must match format job-{8 hex chars}
        db.create_job(
            job_id="job-c3d4e5f6",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-c3d4e5f6",
            status="completed",
            artifact_uri="gs://my-bucket/experiments/job-c3d4e5f6/",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = get_tool_fn(server.promote_artifact)
            result = promote_fn(
                job_id="job-c3d4e5f6",
                output_name="model",
                source_name="promoted_model",
                metadata=valid_file_metadata(),
                reason="Testing valid GCS URI acceptance",
            )
            assert result.success
            assert "gs://my-bucket" in result.source.gcs_location
        finally:
            server.reset_server()
