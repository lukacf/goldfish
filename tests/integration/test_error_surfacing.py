"""Tests for error surfacing - ensure errors are not silently swallowed.

TDD: These tests verify that errors are logged/surfaced appropriately.
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestStateMdWriteErrors:
    """Tests that STATE.md write failures are logged, not silently swallowed."""

    def test_write_failure_is_logged(self, temp_dir, caplog):
        """STATE.md write failures should be logged."""

        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.state.state_md import StateManager

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )

        # Create a read-only directory to cause write failure
        state_path = temp_dir / "readonly" / "STATE.md"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        manager = StateManager(state_path, config)

        # Make tempfile.mkstemp fail (simulates write failure)
        with patch("tempfile.mkstemp", side_effect=PermissionError("Access denied")):
            with caplog.at_level(logging.WARNING):
                manager.regenerate(slots=[], jobs=[], source_count=0)

        # Should have logged a warning
        assert any("STATE.md" in record.message or "write" in record.message.lower() for record in caplog.records), (
            "Expected warning about STATE.md write failure"
        )

    def test_mkdir_failure_is_logged(self, temp_dir, caplog):
        """Directory creation failures should be logged."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.state.state_md import StateManager

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )

        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, config)

        # Make mkdir fail
        with patch.object(Path, "mkdir", side_effect=OSError("Cannot create directory")):
            with caplog.at_level(logging.WARNING):
                manager._write_content("test content")

        # Should have logged a warning
        assert any(
            "directory" in record.message.lower() or "mkdir" in record.message.lower() for record in caplog.records
        ), "Expected warning about directory creation failure"

    def test_temp_file_cleanup_failure_is_logged(self, temp_dir, caplog):
        """Temp file cleanup failures should be logged."""
        import os
        import tempfile

        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.state.state_md import StateManager

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )

        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, config)

        # Create temp file first
        fd, tmp_path = tempfile.mkstemp(dir=temp_dir, prefix=".state_md_", suffix=".tmp")
        os.close(fd)  # Close file descriptor

        # Mock mkstemp to return our temp file, then make rename fail
        with patch("tempfile.mkstemp", return_value=(fd, tmp_path)):
            with patch("os.fdopen", return_value=MagicMock()):  # Mock write success
                with patch.object(Path, "rename", side_effect=OSError("Rename failed")):
                    with patch.object(Path, "unlink", side_effect=OSError("Cannot delete")):
                        with caplog.at_level(logging.WARNING):
                            try:
                                manager._write_content("test content")
                            except OSError:
                                pass  # Expected

        # Should log cleanup failure about temp file
        assert any(
            "temp" in record.message.lower() or "clean" in record.message.lower() for record in caplog.records
        ), f"Expected warning about cleanup failure, got: {[r.message for r in caplog.records]}"


class TestGitMetadataErrors:
    """Tests that git metadata retrieval errors are surfaced appropriately."""

    def test_branch_info_failure_includes_warning(self, temp_dir, caplog):
        """get_branch_info failures should include warning in response."""
        from goldfish.errors import GoldfishError
        from goldfish.workspace.git_layer import GitLayer

        # Create a mock git repo
        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        (dev_repo / ".git").mkdir()

        git = GitLayer(dev_repo, temp_dir, "workspaces")

        # Mock _run_git to fail for branch info queries
        original_run_git = git._run_git

        def failing_run_git(*args, **kwargs):
            if "log" in args or "tag" in args:
                raise GoldfishError("Git operation failed")
            return original_run_git(*args, **kwargs)

        with patch.object(git, "_run_git", side_effect=failing_run_git):
            with caplog.at_level(logging.WARNING):
                result = git.get_branch_info("test-workspace")

        # Result should still be returned (with defaults) but warning logged
        assert result["created_at"] is None
        assert result["last_activity"] is None
        # Should have logged about the failure
        assert any(
            "metadata" in record.message.lower() or "git" in record.message.lower() for record in caplog.records
        ), "Expected warning about git metadata retrieval failure"

    def test_snapshot_info_failure_logs_warning(self, temp_dir, caplog):
        """get_snapshot_info failures should be logged."""
        from goldfish.errors import GoldfishError
        from goldfish.workspace.git_layer import GitLayer

        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        (dev_repo / ".git").mkdir()

        git = GitLayer(dev_repo, temp_dir, "workspaces")

        with patch.object(git, "_run_git", side_effect=GoldfishError("Tag not found")):
            with caplog.at_level(logging.WARNING):
                result = git.get_snapshot_info("snap-nonexistent-20251205-120000")

        assert result["commit_date"] is None
        assert result["message"] == ""
        # Should log warning about failure
        assert any("snapshot" in record.message.lower() for record in caplog.records), (
            "Expected warning about snapshot info retrieval failure"
        )

    def test_list_snapshots_failure_logs_warning(self, temp_dir, caplog):
        """list_snapshots failures should be logged."""
        from goldfish.errors import GoldfishError
        from goldfish.workspace.git_layer import GitLayer

        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        (dev_repo / ".git").mkdir()

        git = GitLayer(dev_repo, temp_dir, "workspaces")

        with patch.object(git, "_run_git", side_effect=GoldfishError("Branch not found")):
            with caplog.at_level(logging.WARNING):
                result = git.list_snapshots("nonexistent-workspace")

        assert result == []
        # Should log warning
        assert any(
            "snapshot" in record.message.lower() or "list" in record.message.lower() for record in caplog.records
        ), "Expected warning about snapshot listing failure"


class TestConfigLoadErrors:
    """Tests that config loading errors include sufficient context."""

    def test_yaml_parse_error_includes_line_info(self, temp_dir):
        """YAML parse errors should include line number."""
        from goldfish.config import GoldfishConfig
        from goldfish.errors import GoldfishError

        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("""
project_name: test
dev_repo_path: ../test
slots:
  - w1
  - w2: invalid  # This line has bad syntax
  - w3
""")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        error_msg = str(exc_info.value)
        # Error should mention line number, location, or config position
        assert "line" in error_msg.lower() or "yaml" in error_msg.lower() or "slots" in error_msg.lower(), (
            f"Expected position info in error: {error_msg}"
        )

    def test_missing_field_error_includes_field_name(self, temp_dir):
        """Missing field errors should include the field name."""
        from goldfish.config import GoldfishConfig
        from goldfish.errors import GoldfishError

        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("""
# Missing project_name
dev_repo_path: ../test
""")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        error_msg = str(exc_info.value)
        assert "project_name" in error_msg, f"Expected field name in error: {error_msg}"


class TestDatabaseErrors:
    """Tests that database errors include sufficient context."""

    def test_db_init_error_includes_path(self, temp_dir):
        """Database init errors should include the path."""
        from goldfish.db.database import Database, DatabaseError

        # Create a file where directory should be
        blocking_file = temp_dir / "blocking"
        blocking_file.write_text("I'm a file, not a directory")

        with pytest.raises(DatabaseError) as exc_info:
            Database(blocking_file / "test.db")

        error_msg = str(exc_info.value)
        # Should mention the path
        assert "blocking" in error_msg or str(blocking_file) in error_msg, f"Expected path in error: {error_msg}"

    def test_job_not_found_includes_job_id(self, temp_dir):
        """JobNotFoundError should include the job ID."""
        from goldfish.db.database import Database
        from goldfish.errors import JobNotFoundError
        from goldfish.jobs.tracker import JobTracker

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        with pytest.raises(JobNotFoundError) as exc_info:
            tracker.get_job("job-that-doesnt-exist-12345")

        error_msg = str(exc_info.value)
        assert "job-that-doesnt-exist-12345" in error_msg, f"Expected job ID in error: {error_msg}"
