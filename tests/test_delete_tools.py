"""Tests for delete tools - P1.

TDD: Write failing tests first, then implement.
"""

import pytest


class TestDeleteJob:
    """Tests for delete_job tool - P1."""

    def test_delete_completed_job(self, temp_dir):
        """Test deleting a completed job."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create a source and job with inputs
        db.create_source(
            source_id="test-source",
            name="Test Source",
            gcs_location="gs://bucket/test",
            created_by="external",
        )

        db.create_job_with_inputs(
            job_id="job-test123",
            workspace="test-ws",
            snapshot_id="snap-abc-20251205-120000",
            script="train.py",
            inputs={"data": "test-source"},
        )

        db.update_job_status("job-test123", "completed")

        # Delete the job
        deleted, inputs_count = db.delete_job("job-test123")

        assert deleted is True
        assert inputs_count == 1

        # Verify job is gone
        job = db.get_job("job-test123")
        assert job is None

        # Verify inputs are gone
        inputs = db.get_job_inputs("job-test123")
        assert len(inputs) == 0

    def test_delete_job_rejects_running(self, temp_dir):
        """Test that delete_job rejects running jobs."""
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError

        db = Database(temp_dir / "test.db")

        # Create a running job
        db.create_job(
            job_id="job-running",
            workspace="test-ws",
            snapshot_id="snap-abc-20251205-120000",
            script="train.py",
        )
        db.update_job_status("job-running", "running")

        # Attempt to delete - should raise
        with pytest.raises(GoldfishError) as exc_info:
            db.delete_job("job-running")

        assert "running" in str(exc_info.value).lower() or "cannot delete" in str(exc_info.value).lower()

        # Verify job still exists
        job = db.get_job("job-running")
        assert job is not None

    def test_delete_job_rejects_pending(self, temp_dir):
        """Test that delete_job rejects pending jobs."""
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError

        db = Database(temp_dir / "test.db")

        # Create a pending job
        db.create_job(
            job_id="job-pending",
            workspace="test-ws",
            snapshot_id="snap-abc-20251205-120000",
            script="train.py",
        )

        # Attempt to delete - should raise
        with pytest.raises(GoldfishError) as exc_info:
            db.delete_job("job-pending")

        assert "pending" in str(exc_info.value).lower() or "cannot delete" in str(exc_info.value).lower()

    def test_delete_job_allows_failed(self, temp_dir):
        """Test that delete_job allows deleting failed jobs."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        db.create_job(
            job_id="job-failed",
            workspace="test-ws",
            snapshot_id="snap-abc-20251205-120000",
            script="train.py",
        )
        db.update_job_status("job-failed", "failed", error="Something went wrong")

        # Should succeed
        deleted, inputs_count = db.delete_job("job-failed")
        assert deleted is True

    def test_delete_nonexistent_job(self, temp_dir):
        """Test deleting a job that doesn't exist."""
        from goldfish.db.database import Database
        from goldfish.errors import JobNotFoundError

        db = Database(temp_dir / "test.db")

        with pytest.raises(JobNotFoundError):
            db.delete_job("nonexistent-job")
