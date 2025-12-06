"""Tests for get_job_logs() tool - P1.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestGetJobLogsTool:
    """Tests for get_job_logs tool."""

    def test_get_job_logs_tool_exists(self):
        """Server should have get_job_logs tool."""
        from goldfish import server

        assert hasattr(server, "get_job_logs")

    def test_returns_logs_for_job(self, temp_dir):
        """get_job_logs should return log content."""
        from goldfish import server
        from goldfish.models import JobLogsResponse

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc1234-20251205-120000",
            "script": "train.py",
            "started_at": "2025-12-05T12:00:00",
            "log_uri": "/tmp/logs/job-a1b2c3d4.log",
        }

        mock_job_tracker = MagicMock()
        mock_job_tracker.get_job_logs.return_value = "Line 1\nLine 2\nLine 3"

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=mock_job_tracker,
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_job_logs_fn = server.get_job_logs.fn if hasattr(server.get_job_logs, 'fn') else server.get_job_logs
            result = get_job_logs_fn(job_id="job-a1b2c3d4")

            assert isinstance(result, JobLogsResponse)
            assert result.job_id == "job-a1b2c3d4"
            assert result.status == "completed"
            assert result.logs == "Line 1\nLine 2\nLine 3"
            assert result.error is None
        finally:
            server.reset_server()

    def test_returns_error_when_logs_unavailable(self, temp_dir):
        """get_job_logs should return error when logs are not available."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "pending",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc1234-20251205-120000",
            "script": "train.py",
            "started_at": "2025-12-05T12:00:00",
        }

        mock_job_tracker = MagicMock()
        mock_job_tracker.get_job_logs.return_value = None

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=mock_job_tracker,
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_job_logs_fn = server.get_job_logs.fn if hasattr(server.get_job_logs, 'fn') else server.get_job_logs
            result = get_job_logs_fn(job_id="job-a1b2c3d4")

            assert result.logs is None
            assert result.error == "Logs not available"
        finally:
            server.reset_server()

    def test_tails_logs_when_requested(self, temp_dir):
        """get_job_logs should tail logs to requested number of lines."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc1234-20251205-120000",
            "script": "train.py",
            "started_at": "2025-12-05T12:00:00",
        }

        # Create 10 lines of logs
        log_lines = "\n".join([f"Line {i}" for i in range(10)])
        mock_job_tracker = MagicMock()
        mock_job_tracker.get_job_logs.return_value = log_lines

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=mock_job_tracker,
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_job_logs_fn = server.get_job_logs.fn if hasattr(server.get_job_logs, 'fn') else server.get_job_logs
            # Request only last 3 lines
            result = get_job_logs_fn(job_id="job-a1b2c3d4", tail_lines=3)

            assert result.logs == "Line 7\nLine 8\nLine 9"
        finally:
            server.reset_server()

    def test_raises_on_missing_job(self, temp_dir):
        """get_job_logs should raise JobNotFoundError for missing job."""
        from goldfish import server
        from goldfish.errors import JobNotFoundError

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_job.return_value = None

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
            get_job_logs_fn = server.get_job_logs.fn if hasattr(server.get_job_logs, 'fn') else server.get_job_logs
            # Use valid format but nonexistent job
            with pytest.raises(JobNotFoundError):
                get_job_logs_fn(job_id="job-ffffffff")
        finally:
            server.reset_server()

    def test_includes_log_uri(self, temp_dir):
        """get_job_logs should include log_uri in response."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_job.return_value = {
            "id": "job-a1b2c3d4",
            "status": "running",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc1234-20251205-120000",
            "script": "train.py",
            "started_at": "2025-12-05T12:00:00",
            "log_uri": "gs://bucket/logs/job-a1b2c3d4.log",
        }

        mock_job_tracker = MagicMock()
        mock_job_tracker.get_job_logs.return_value = "Some logs"

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=mock_job_tracker,
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_job_logs_fn = server.get_job_logs.fn if hasattr(server.get_job_logs, 'fn') else server.get_job_logs
            result = get_job_logs_fn(job_id="job-a1b2c3d4")

            assert result.log_uri == "gs://bucket/logs/job-a1b2c3d4.log"
        finally:
            server.reset_server()
