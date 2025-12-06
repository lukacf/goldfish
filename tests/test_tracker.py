"""Tests for job tracker - P0 Core Functionality.

TDD: Write failing tests first, then implement.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.jobs.tracker import JobTracker
from goldfish.errors import JobNotFoundError
from goldfish.models import JobInfo


class TestGetJob:
    """Tests for get_job."""

    def test_returns_job_info(self, temp_dir):
        """Should return JobInfo for existing job."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
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

        tracker = JobTracker(db, temp_dir)
        result = tracker.get_job("job-123")

        assert isinstance(result, JobInfo)
        assert result.job_id == "job-123"
        assert result.status == "running"
        assert result.workspace == "test-ws"

    def test_raises_for_nonexistent_job(self, temp_dir):
        """Should raise JobNotFoundError for missing job."""
        db = MagicMock()
        db.get_job.return_value = None

        tracker = JobTracker(db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.get_job("nonexistent")


class TestPollStatus:
    """Tests for poll_status - P0."""

    def test_returns_current_status_for_completed_jobs(self, temp_dir):
        """Completed jobs should not be polled."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
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

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        assert result.status == "completed"
        # Should not call any update methods
        db.update_job_status.assert_not_called()

    def test_returns_current_status_for_failed_jobs(self, temp_dir):
        """Failed jobs should not be polled."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
            "status": "failed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": None,
            "artifact_uri": None,
            "error": "some error",
        }

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        assert result.status == "failed"
        db.update_job_status.assert_not_called()

    def test_raises_for_nonexistent_job(self, temp_dir):
        """Should raise JobNotFoundError for missing job."""
        db = MagicMock()
        db.get_job.return_value = None

        tracker = JobTracker(db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.poll_status("nonexistent")

    def test_checks_completion_marker_for_running_job(self, temp_dir):
        """Running jobs should check for completion marker."""
        db = MagicMock()
        exp_dir = temp_dir / "experiments" / "test-exp"
        exp_dir.mkdir(parents=True)

        # Mock returns running first, then completed after update
        call_count = [0]

        def get_job_side_effect(job_id):
            call_count[0] += 1
            status = "running" if call_count[0] == 1 else "completed"
            return {
                "id": "job-123",
                "status": status,
                "workspace": "test-ws",
                "snapshot_id": "snap-abc",
                "script": "run.py",
                "started_at": "2024-01-01T00:00:00+00:00",
                "completed_at": "2024-01-01T01:00:00+00:00" if status == "completed" else None,
                "log_uri": None,
                "artifact_uri": None,
                "error": None,
                "experiment_dir": str(exp_dir),
            }

        db.get_job.side_effect = get_job_side_effect

        # Create completion marker
        (exp_dir / "COMPLETED").write_text("0")

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        assert result.status == "completed"
        db.update_job_status.assert_called()

    def test_checks_failed_marker_for_running_job(self, temp_dir):
        """Running jobs should check for failure marker."""
        db = MagicMock()
        exp_dir = temp_dir / "experiments" / "test-exp"
        exp_dir.mkdir(parents=True)

        # Mock returns running first, then failed after update
        call_count = [0]

        def get_job_side_effect(job_id):
            call_count[0] += 1
            status = "running" if call_count[0] == 1 else "failed"
            return {
                "id": "job-123",
                "status": status,
                "workspace": "test-ws",
                "snapshot_id": "snap-abc",
                "script": "run.py",
                "started_at": "2024-01-01T00:00:00+00:00",
                "completed_at": "2024-01-01T01:00:00+00:00" if status == "failed" else None,
                "log_uri": None,
                "artifact_uri": None,
                "error": "Error: OOM killed" if status == "failed" else None,
                "experiment_dir": str(exp_dir),
            }

        db.get_job.side_effect = get_job_side_effect

        # Create failure marker
        (exp_dir / "FAILED").write_text("Error: OOM killed")

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        assert result.status == "failed"
        db.update_job_status.assert_called()
        # Should include error message
        call_kwargs = db.update_job_status.call_args[1]
        assert "error" in call_kwargs

    def test_no_status_change_without_markers(self, temp_dir):
        """Running jobs without markers should remain running."""
        db = MagicMock()
        exp_dir = temp_dir / "experiments" / "test-exp"
        exp_dir.mkdir(parents=True)

        def get_job_side_effect(job_id):
            return {
                "id": "job-123",
                "status": "running",
                "workspace": "test-ws",
                "snapshot_id": "snap-abc",
                "script": "run.py",
                "started_at": "2024-01-01T00:00:00+00:00",
                "completed_at": None,
                "log_uri": None,
                "artifact_uri": None,
                "error": None,
                "experiment_dir": str(exp_dir),
            }

        db.get_job.side_effect = get_job_side_effect

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        assert result.status == "running"
        db.update_job_status.assert_not_called()

    def test_handles_missing_experiment_dir_gracefully(self, temp_dir):
        """Should handle missing experiment_dir without crashing."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
            "status": "running",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": None,
            "log_uri": None,
            "artifact_uri": None,
            "error": None,
            # No experiment_dir
        }

        tracker = JobTracker(db, temp_dir)
        result = tracker.poll_status("job-123")

        # Should return current status without crashing
        assert result.status == "running"


class TestUpdateJobStatus:
    """Tests for update_job_status."""

    def test_updates_status_in_database(self, temp_dir):
        """Should update status in database."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
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

        tracker = JobTracker(db, temp_dir)
        tracker.update_job_status("job-123", "completed")

        db.update_job_status.assert_called_once()

    def test_raises_for_nonexistent_job(self, temp_dir):
        """Should raise for missing job."""
        db = MagicMock()
        db.get_job.return_value = None

        tracker = JobTracker(db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.update_job_status("nonexistent", "completed")

    def test_sets_completed_at_for_terminal_states(self, temp_dir):
        """Should set completed_at for completed/failed."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
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

        tracker = JobTracker(db, temp_dir)
        tracker.update_job_status("job-123", "completed")

        call_kwargs = db.update_job_status.call_args[1]
        assert "completed_at" in call_kwargs
        assert call_kwargs["completed_at"] is not None


class TestGetJobLogs:
    """Tests for get_job_logs."""

    def test_returns_none_for_no_log_uri(self, temp_dir):
        """Should return None if no log_uri."""
        db = MagicMock()
        db.get_job.return_value = {
            "id": "job-123",
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

        tracker = JobTracker(db, temp_dir)
        result = tracker.get_job_logs("job-123")

        assert result is None

    def test_reads_local_log_file(self, temp_dir):
        """Should read log content from local file."""
        db = MagicMock()
        log_file = temp_dir / "logs" / "job.log"
        log_file.parent.mkdir(parents=True)
        log_file.write_text("Log line 1\nLog line 2")

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": str(log_file),
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)
        result = tracker.get_job_logs("job-123")

        assert result == "Log line 1\nLog line 2"

    def test_raises_for_nonexistent_job(self, temp_dir):
        """Should raise for missing job."""
        db = MagicMock()
        db.get_job.return_value = None

        tracker = JobTracker(db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.get_job_logs("nonexistent")
