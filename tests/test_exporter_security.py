"""Tests for exporter security - P0.

TDD: Write failing tests first, then implement.
"""

import os
from pathlib import Path

import pytest

from goldfish.jobs.exporter import SnapshotExporter
from goldfish.errors import GoldfishError


class TestExporterSymlinkSecurity:
    """Tests for symlink attack prevention in exporter."""

    def test_rejects_symlink_in_experiment_path(self, temp_dir):
        """Should reject if experiment path is a symlink."""
        experiments_dir = temp_dir / "experiments"
        experiments_dir.mkdir()

        exporter = SnapshotExporter(experiments_dir)

        # Create workspace
        workspace_path = temp_dir / "workspace"
        workspace_path.mkdir()
        (workspace_path / "code").mkdir()

        # Pre-create a symlink where the experiment directory would be
        # This simulates an attacker creating a symlink during the race window
        malicious_target = temp_dir / "malicious_target"
        malicious_target.mkdir()

        # Predict experiment name and create symlink
        # (In reality, attacker would need to guess timestamp)
        symlink_path = experiments_dir / "goldfish-test-ws-ATTACK"
        symlink_path.symlink_to(malicious_target)

        # Try to export - if the code doesn't check for symlinks,
        # it would write into the malicious_target directory

        # Since we can't predict the exact timestamp, we test the
        # underlying _safe_mkdir function instead
        from goldfish.jobs.exporter import _safe_create_directory

        test_symlink = experiments_dir / "test-symlink"
        test_symlink.symlink_to(malicious_target)

        with pytest.raises(GoldfishError) as exc_info:
            _safe_create_directory(test_symlink)

        assert "symlink" in str(exc_info.value).lower()

    def test_rejects_existing_directory(self, temp_dir):
        """Should reject if directory already exists."""
        experiments_dir = temp_dir / "experiments"
        experiments_dir.mkdir()

        # Pre-create the directory
        existing_dir = experiments_dir / "existing"
        existing_dir.mkdir()

        from goldfish.jobs.exporter import _safe_create_directory

        with pytest.raises(GoldfishError) as exc_info:
            _safe_create_directory(existing_dir)

        assert "exists" in str(exc_info.value).lower()

    def test_creates_directory_safely(self, temp_dir):
        """Should create directory when path is clean."""
        experiments_dir = temp_dir / "experiments"
        experiments_dir.mkdir()

        new_dir = experiments_dir / "new-experiment"

        from goldfish.jobs.exporter import _safe_create_directory

        _safe_create_directory(new_dir)

        assert new_dir.exists()
        assert new_dir.is_dir()
        assert not new_dir.is_symlink()


class TestExporterCopySecurity:
    """Tests for copytree symlink handling."""

    def test_does_not_follow_symlinks_in_workspace(self, temp_dir):
        """copytree should not follow symlinks in workspace directory."""
        experiments_dir = temp_dir / "experiments"
        experiments_dir.mkdir()

        exporter = SnapshotExporter(experiments_dir)

        # Create workspace with a symlink pointing outside
        workspace_path = temp_dir / "workspace"
        workspace_path.mkdir()
        code_dir = workspace_path / "code"
        code_dir.mkdir()

        # Create a file in code/
        (code_dir / "real_file.py").write_text("# real code")

        # Create a symlink inside code/ pointing to /etc/passwd (or similar)
        sensitive_file = temp_dir / "sensitive_data.txt"
        sensitive_file.write_text("SECRET DATA")
        symlink_in_code = code_dir / "sneaky_link"
        symlink_in_code.symlink_to(sensitive_file)

        # Export
        exp_dir = exporter.export(
            workspace_path=workspace_path,
            workspace_name="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="scripts/train.py",
            reason="Testing symlink handling",
        )

        # The symlink should either:
        # 1. Not be copied at all
        # 2. Be copied as a symlink (not dereferenced)
        # 3. Raise an error

        copied_path = exp_dir / "code" / "sneaky_link"
        if copied_path.exists():
            # If copied, should be a symlink, not the actual content
            assert copied_path.is_symlink(), "Symlink should remain a symlink, not be dereferenced"
            # And the target should be relative/broken, not pointing to sensitive data
        else:
            # Not copied is also acceptable
            pass

        # Real file should be copied
        assert (exp_dir / "code" / "real_file.py").exists()


class TestLogReadingSymlinkSecurity:
    """Tests for symlink attack prevention in log reading."""

    def test_log_reading_rejects_symlinks(self, temp_dir):
        """Test that get_job_logs() rejects symlinks to prevent TOCTOU attacks."""
        from goldfish.jobs.tracker import JobTracker
        from goldfish.db.database import Database

        # Setup
        project_root = temp_dir / "project"
        project_root.mkdir()
        logs_dir = project_root / "logs"
        logs_dir.mkdir()

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        tracker = JobTracker(db, project_root)

        # Create a valid log file
        log_file = logs_dir / "job-test123.log"
        log_file.write_text("Job output here")

        # Create a job record with this log
        job_id = "job-test123"
        db.create_job(
            job_id=job_id,
            workspace="test-ws",
            snapshot_id="snap-123",
            script="train.py",
            experiment_dir=str(temp_dir / "exp"),
        )
        db.update_job_status(
            job_id,
            "completed",
            log_uri=f"file://{log_file}",
        )

        # Verify we can read the log initially
        logs = tracker.get_job_logs(job_id)
        assert "Job output here" in logs

        # Now simulate TOCTOU attack: replace log file with symlink to sensitive file
        # WITHIN the project directory (more subtle attack)
        sensitive_file = project_root / ".goldfish" / "goldfish.db"
        sensitive_file.parent.mkdir(exist_ok=True)
        sensitive_file.write_text("SECRET DATABASE CONTENTS")

        # Delete the log file and replace with symlink to the database file
        log_file.unlink()
        log_file.symlink_to(sensitive_file)

        # Attempt to read logs - should raise error (O_NOFOLLOW blocks symlinks)
        # But currently it probably DOESN'T raise an error and reads the sensitive file
        try:
            logs_after_symlink = tracker.get_job_logs(job_id)
            # If we get here, the vulnerability exists - symlink was followed
            pytest.fail(
                f"SECURITY VULNERABILITY: get_job_logs() followed symlink and read: {logs_after_symlink[:100]}"
            )
        except (GoldfishError, OSError) as e:
            # Good - an error was raised
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in ["symlink", "failed to read", "operation not permitted"]), \
                f"Expected error about symlink but got: {e}"

    def test_log_reading_handles_missing_file(self, temp_dir):
        """Test that get_job_logs() handles missing log files gracefully."""
        from goldfish.jobs.tracker import JobTracker
        from goldfish.db.database import Database

        project_root = temp_dir / "project"
        project_root.mkdir()

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        tracker = JobTracker(db, project_root)

        # Create a job with a log URI pointing to non-existent file
        job_id = "job-missing-log"
        db.create_job(
            job_id=job_id,
            workspace="test-ws",
            snapshot_id="snap-123",
            script="train.py",
            experiment_dir=str(temp_dir / "exp"),
        )
        db.update_job_status(
            job_id,
            "completed",
            log_uri=f"file://{project_root}/logs/nonexistent.log",
        )

        # Should return None for missing file (existing behavior)
        logs = tracker.get_job_logs(job_id)
        assert logs is None or logs == ""
