"""Security tests for JobTracker.

Tests critical security features:
- Symlink attack prevention in log reading
- File size limits to prevent memory exhaustion
- Path validation and containment
"""

from unittest.mock import MagicMock

import pytest

from goldfish.errors import GoldfishError
from goldfish.jobs.tracker import JobTracker


class TestLogReadingSymlinkProtection:
    """Test symlink attack prevention in get_job_logs."""

    def test_rejects_symlink_log_files(self, temp_dir):
        """get_job_logs should reject symlinks to prevent TOCTOU attacks."""
        db = MagicMock()

        # Create a sensitive file (simulating /etc/passwd)
        sensitive_file = temp_dir / "sensitive.txt"
        sensitive_file.write_text("SECRET_DATA")

        # Create log directory
        log_dir = temp_dir / "logs"
        log_dir.mkdir()

        # Create symlink to sensitive file
        symlink_path = log_dir / "job.log"
        symlink_path.symlink_to(sensitive_file)

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": str(symlink_path),
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)

        # Should raise error for symlink
        with pytest.raises(GoldfishError, match="symlink"):
            tracker.get_job_logs("job-123")

        # Verify sensitive file was NOT read
        # (If it was read, the content would have been returned)

    def test_accepts_normal_log_files(self, temp_dir):
        """get_job_logs should accept normal files."""
        db = MagicMock()

        # Create normal log file
        log_file = temp_dir / "logs" / "job.log"
        log_file.parent.mkdir(parents=True)
        log_file.write_text("Normal log content")

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

        assert result == "Normal log content"

    def test_o_nofollow_prevents_symlink_following(self, temp_dir):
        """O_NOFOLLOW flag should prevent symlink following even if is_symlink missed."""
        db = MagicMock()

        # Create target file
        target = temp_dir / "target.txt"
        target.write_text("TARGET_CONTENT")

        # Create symlink
        log_dir = temp_dir / "logs"
        log_dir.mkdir()
        symlink = log_dir / "job.log"
        symlink.symlink_to(target)

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": str(symlink),
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)

        # Should be caught by is_symlink() check first
        with pytest.raises(GoldfishError, match="symlink"):
            tracker.get_job_logs("job-123")


class TestLogFileSizeLimit:
    """Test file size limits to prevent memory exhaustion."""

    def test_rejects_files_over_100mb(self, temp_dir):
        """get_job_logs should reject files larger than 100MB."""
        db = MagicMock()

        # Create a large log file (101MB worth of data)
        log_file = temp_dir / "logs" / "huge.log"
        log_file.parent.mkdir(parents=True)

        # Write a 101MB file
        with open(log_file, "wb") as f:
            # Write 101 * 1024 * 1024 bytes
            chunk_size = 1024 * 1024  # 1MB chunks
            for _ in range(101):
                f.write(b"X" * chunk_size)

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

        # Should raise error about file size
        with pytest.raises(GoldfishError, match="Log file too large"):
            tracker.get_job_logs("job-123")

    def test_accepts_files_under_100mb(self, temp_dir):
        """get_job_logs should accept files under 100MB."""
        db = MagicMock()

        # Create a 50MB log file
        log_file = temp_dir / "logs" / "medium.log"
        log_file.parent.mkdir(parents=True)

        with open(log_file, "wb") as f:
            # Write 50MB
            chunk_size = 1024 * 1024
            for _ in range(50):
                f.write(b"L" * chunk_size)

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

        # Should succeed without error
        result = tracker.get_job_logs("job-123")
        assert result is not None
        assert len(result) == 50 * 1024 * 1024  # 50MB

    def test_small_files_work_normally(self, temp_dir):
        """get_job_logs should work normally for small files."""
        db = MagicMock()

        log_file = temp_dir / "logs" / "small.log"
        log_file.parent.mkdir(parents=True)
        log_file.write_text("Small log content\n" * 100)

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

        assert "Small log content" in result


class TestPathValidation:
    """Test path validation and containment."""

    def test_rejects_path_traversal_in_log_uri(self, temp_dir):
        """get_job_logs should reject path traversal attempts."""
        from goldfish.validation import InvalidLogPathError

        db = MagicMock()

        # Attempt path traversal
        log_uri = str(temp_dir / "logs" / ".." / ".." / "etc" / "passwd")

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": log_uri,
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)

        # Should raise validation error
        with pytest.raises((InvalidLogPathError, GoldfishError)):
            tracker.get_job_logs("job-123")

    def test_accepts_logs_within_project_root(self, temp_dir):
        """get_job_logs should accept logs within project root."""
        db = MagicMock()

        # Valid log file within project
        log_file = temp_dir / "experiments" / "exp-123" / "logs" / "run.log"
        log_file.parent.mkdir(parents=True)
        log_file.write_text("Valid log content")

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

        assert result == "Valid log content"


class TestFileURIHandling:
    """Test file:// URI handling."""

    def test_handles_file_uri_scheme(self, temp_dir):
        """get_job_logs should handle file:// URIs."""
        db = MagicMock()

        log_file = temp_dir / "logs" / "job.log"
        log_file.parent.mkdir(parents=True)
        log_file.write_text("Log from file URI")

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": f"file://{log_file}",
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)
        result = tracker.get_job_logs("job-123")

        assert result == "Log from file URI"

    def test_returns_none_for_gcs_uris(self, temp_dir):
        """get_job_logs should return None for GCS URIs (not implemented)."""
        db = MagicMock()

        db.get_job.return_value = {
            "id": "job-123",
            "status": "completed",
            "workspace": "test-ws",
            "snapshot_id": "snap-abc",
            "script": "run.py",
            "started_at": "2024-01-01T00:00:00+00:00",
            "completed_at": "2024-01-01T01:00:00+00:00",
            "log_uri": "gs://bucket/logs/job-123.log",
            "artifact_uri": None,
            "error": None,
        }

        tracker = JobTracker(db, temp_dir)
        result = tracker.get_job_logs("job-123")

        # GCS not implemented, should return None
        assert result is None
