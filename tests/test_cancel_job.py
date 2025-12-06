"""Tests for cancel_job() tool - P2.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestCancelJobTool:
    """Tests for cancel_job tool."""

    def test_cancel_job_tool_exists(self):
        """Server should have cancel_job tool."""
        from goldfish import server

        assert hasattr(server, "cancel_job")

    def test_cancel_job_cancels_running_job(self, temp_dir):
        """cancel_job should cancel a running job."""
        from goldfish import server
        from goldfish.models import CancelJobResponse

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15

        mock_job_tracker = MagicMock()
        mock_job_tracker.cancel_job.return_value = CancelJobResponse(
            success=True,
            job_id="job-a1b2c3d4",
            previous_status="running",
            state_md="# State",
        )

        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# State"

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_all_slots.return_value = []

        mock_db = MagicMock()
        mock_db.get_active_jobs.return_value = []
        mock_db.list_sources.return_value = []

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=mock_state_manager,
            job_launcher=MagicMock(),
            job_tracker=mock_job_tracker,
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            cancel_fn = server.cancel_job.fn if hasattr(server.cancel_job, 'fn') else server.cancel_job
            result = cancel_fn(
                job_id="job-a1b2c3d4",
                reason="Job taking too long, need to iterate",
            )

            assert result.success is True
            assert result.job_id == "job-a1b2c3d4"
            mock_job_tracker.cancel_job.assert_called_once()
        finally:
            server.reset_server()

    def test_cancel_job_validates_reason(self, temp_dir):
        """cancel_job should validate reason length."""
        from goldfish import server
        from goldfish.errors import GoldfishError

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15

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
            cancel_fn = server.cancel_job.fn if hasattr(server.cancel_job, 'fn') else server.cancel_job
            with pytest.raises(GoldfishError):
                cancel_fn(
                    job_id="job-a1b2c3d4",
                    reason="short",  # Too short
                )
        finally:
            server.reset_server()


class TestJobTrackerCancel:
    """Tests for job tracker cancel implementation."""

    def test_job_tracker_has_cancel_job_method(self):
        """JobTracker should have cancel_job method."""
        from goldfish.jobs.tracker import JobTracker

        assert hasattr(JobTracker, "cancel_job")

    def test_cancel_updates_status_to_cancelled(self, temp_dir):
        """cancel_job should update job status to cancelled."""
        from goldfish.jobs.tracker import JobTracker
        from goldfish.models import CancelJobResponse

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
        from goldfish.jobs.tracker import JobTracker
        from goldfish.errors import GoldfishError

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
        from goldfish.jobs.tracker import JobTracker
        from goldfish.errors import JobNotFoundError

        mock_db = MagicMock()
        mock_db.get_job.return_value = None

        tracker = JobTracker(mock_db, temp_dir)

        with pytest.raises(JobNotFoundError):
            tracker.cancel_job("nonexistent", "Testing cancellation")
