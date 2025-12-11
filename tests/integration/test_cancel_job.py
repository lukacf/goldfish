"""Tests for cancel functionality.

Tests for the internal JobTracker.cancel_job method.
The MCP cancel() tool tests are in test_execution_tools.py.
"""

from unittest.mock import MagicMock

import pytest


class TestJobTrackerCancel:
    """Tests for job tracker cancel implementation."""

    def test_cancel_updates_status_to_cancelled(self, temp_dir):
        """cancel_job should update job status to cancelled."""
        from goldfish.jobs.tracker import JobTracker

        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "running",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": None,
            "log_uri": None,
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(mock_db, temp_dir)
        result = tracker.cancel_job("job-a1b2c3d4", "Testing cancellation")

        assert result.success is True
        mock_db.update_job_status.assert_called_once()
        call_kwargs = mock_db.update_job_status.call_args[1]
        assert call_kwargs["status"] == "cancelled"

    def test_cancel_fails_for_completed_job(self, temp_dir):
        """Cannot cancel already completed job."""
        from goldfish.errors import GoldfishError
        from goldfish.jobs.tracker import JobTracker

        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": None,
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(mock_db, temp_dir)

        with pytest.raises(GoldfishError) as exc_info:
            tracker.cancel_job("job-a1b2c3d4", "Testing cancellation")

        assert "already" in str(exc_info.value).lower()

    def test_cancel_fails_for_nonexistent_job(self, temp_dir):
        """Cannot cancel nonexistent job."""
        from goldfish.errors import JobNotFoundError
        from goldfish.jobs.tracker import JobTracker

        mock_db = MagicMock()
        mock_db.get_job.return_value = None

        tracker = JobTracker(mock_db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.cancel_job("nonexistent", "Testing cancellation")
