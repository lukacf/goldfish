"""Unit tests for execution_tools - logs follow mode.

Tests for the real-time log streaming feature that returns only NEW logs
since the last call, enabling efficient polling without re-fetching entire logs.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest


# Mock the server module to avoid circular imports
# This must happen before importing execution_tools
@pytest.fixture(autouse=True)
def mock_server_imports():
    """Mock server imports to avoid circular import issues."""
    # Create mock mcp and server functions
    mock_mcp = MagicMock()
    mock_mcp.tool = MagicMock(return_value=lambda f: f)

    # Patch the server module imports before importing execution_tools
    with patch.dict(
        sys.modules,
        {
            "goldfish.server": MagicMock(
                _get_config=MagicMock(),
                _get_db=MagicMock(),
                _get_pipeline_executor=MagicMock(),
                _get_stage_executor=MagicMock(),
                _get_workspace_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        # Clear any cached imports
        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("goldfish.server_tools"):
                sys.modules.pop(mod_name, None)
        yield


class TestLogsFollowMode:
    """Tests for logs() follow mode - incremental log retrieval."""

    @pytest.fixture
    def mock_db(self):
        """Create a mock database."""
        return MagicMock()

    @pytest.fixture
    def mock_stage_run(self):
        """Create a mock stage run row."""
        return {
            "id": "stage-abc123",
            "workspace_name": "test-workspace",
            "stage_name": "train",
            "status": "running",
            "backend_type": "local",
            "backend_handle": "stage-abc123",
            "log_uri": None,
        }

    def test_logs_follow_mode_first_call_returns_tail_lines(self, mock_db, mock_stage_run):
        """First follow call should return last N lines and set cursor."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        # Clear any existing cursors
        _log_cursors.clear()

        full_logs = "line1\nline2\nline3\nline4\nline5\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = full_logs

            result = logs("stage-abc123", tail=3, follow=True)

            # Should return last 3 lines
            assert "line3" in result["logs"]
            assert "line4" in result["logs"]
            assert "line5" in result["logs"]

            # Should have cursor set
            assert result.get("cursor_position") is not None
            assert result["cursor_position"] > 0

            # Should indicate there was content
            assert result.get("has_new_content") is True

    def test_logs_follow_mode_subsequent_calls_return_only_new_content(self, mock_db, mock_stage_run):
        """Subsequent follow calls should return only content since cursor."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        # Clear cursors
        _log_cursors.clear()

        initial_logs = "line1\nline2\nline3\n"
        updated_logs = "line1\nline2\nline3\nline4\nline5\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = initial_logs

            # First call - sets cursor
            result1 = logs("stage-abc123", tail=10, follow=True)
            assert "line1" in result1["logs"]

            # Simulate more logs appearing
            mock_executor.return_value.local_executor.get_container_logs.return_value = updated_logs

            # Second call - should only return new content
            result2 = logs("stage-abc123", tail=10, follow=True)

            # Should only contain the NEW lines (line4, line5)
            assert "line4" in result2["logs"]
            assert "line5" in result2["logs"]
            # Should NOT contain old lines
            assert "line1" not in result2["logs"]
            assert "line2" not in result2["logs"]

    def test_logs_follow_mode_resets_cursor_on_completion(self, mock_db, mock_stage_run):
        """Cursor should be cleaned up when run reaches terminal state."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = "logs\n"

            # First call sets cursor
            logs("stage-abc123", follow=True)
            assert "stage-abc123" in _log_cursors

            # Now run completes
            mock_stage_run["status"] = "completed"

            # Call should clean up cursor
            logs("stage-abc123", follow=True)
            assert "stage-abc123" not in _log_cursors

    def test_logs_follow_mode_handles_empty_new_content(self, mock_db, mock_stage_run):
        """Should return has_new_content=False when no new logs."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        same_logs = "line1\nline2\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = same_logs

            # First call
            logs("stage-abc123", follow=True)

            # Second call with no new logs
            result = logs("stage-abc123", follow=True)

            assert result["logs"] == ""
            assert result["has_new_content"] is False

    def test_logs_without_follow_unchanged(self, mock_db, mock_stage_run):
        """Existing logs() behavior without follow=True should be unchanged."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        full_logs = "line1\nline2\nline3\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = full_logs

            # Call without follow
            result = logs("stage-abc123", tail=10)

            # Should return full logs
            assert "line1" in result["logs"]
            assert "line2" in result["logs"]
            assert "line3" in result["logs"]

            # Should NOT have follow-specific fields
            assert "cursor_position" not in result
            assert "has_new_content" not in result

            # Should NOT create cursor
            assert "stage-abc123" not in _log_cursors

    def test_logs_follow_mode_returns_correct_response_structure(self, mock_db, mock_stage_run):
        """Follow mode should return correct response structure."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = "logs\n"

            result = logs("stage-abc123", follow=True)

            # Standard fields
            assert "run_id" in result
            assert "status" in result
            assert "logs" in result
            assert "log_uri" in result

            # Follow-specific fields
            assert "cursor_position" in result
            assert "has_new_content" in result
            assert isinstance(result["cursor_position"], int)
            assert isinstance(result["has_new_content"], bool)

    def test_logs_follow_mode_cursor_persists_across_calls(self, mock_db, mock_stage_run):
        """Cursor should persist between calls for same run_id."""
        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        logs_v1 = "line1\n"
        logs_v2 = "line1\nline2\n"
        logs_v3 = "line1\nline2\nline3\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = logs_v1

            result1 = logs("stage-abc123", follow=True)
            cursor1 = result1["cursor_position"]

            mock_executor.return_value.local_executor.get_container_logs.return_value = logs_v2
            result2 = logs("stage-abc123", follow=True)
            cursor2 = result2["cursor_position"]

            mock_executor.return_value.local_executor.get_container_logs.return_value = logs_v3
            result3 = logs("stage-abc123", follow=True)
            cursor3 = result3["cursor_position"]

            # Cursors should increase
            assert cursor2 > cursor1
            assert cursor3 > cursor2

            # Each result should only have new content
            assert "line1" in result1["logs"]
            assert "line2" in result2["logs"]
            assert "line1" not in result2["logs"]
            assert "line3" in result3["logs"]
            assert "line1" not in result3["logs"]
            assert "line2" not in result3["logs"]


class TestCursorCleanup:
    """Tests for cursor TTL cleanup to prevent memory leaks."""

    def test_cleanup_removes_stale_cursors(self):
        """Cursors older than TTL should be removed."""
        import time

        from goldfish.server_tools.execution_tools import (
            _CURSOR_TTL_SECONDS,
            _cleanup_stale_cursors,
            _log_cursors,
        )

        _log_cursors.clear()

        # Add a fresh cursor
        now = time.time()
        _log_cursors["fresh-run"] = (100, now)

        # Add a stale cursor (older than TTL)
        stale_time = now - _CURSOR_TTL_SECONDS - 1
        _log_cursors["stale-run"] = (50, stale_time)

        _cleanup_stale_cursors()

        # Fresh cursor should remain
        assert "fresh-run" in _log_cursors
        # Stale cursor should be removed
        assert "stale-run" not in _log_cursors

    def test_cleanup_enforces_max_cursors(self):
        """Cleanup should remove oldest entries when over MAX_CURSORS."""
        import time

        from goldfish.server_tools.execution_tools import (
            _MAX_CURSORS,
            _cleanup_stale_cursors,
            _log_cursors,
        )

        _log_cursors.clear()

        now = time.time()

        # Add more cursors than MAX_CURSORS
        for i in range(_MAX_CURSORS + 10):
            # Stagger timestamps so we know which are oldest
            _log_cursors[f"run-{i}"] = (i * 10, now - i)

        _cleanup_stale_cursors()

        # Should be at most MAX_CURSORS
        assert len(_log_cursors) <= _MAX_CURSORS

        # Newest cursors should remain (run-0 through run-{MAX_CURSORS-1})
        # Oldest cursors should be removed (run-{MAX_CURSORS} through run-{MAX_CURSORS+9})
        assert "run-0" in _log_cursors  # Newest
        assert f"run-{_MAX_CURSORS + 9}" not in _log_cursors  # Oldest

    def test_cursor_stores_timestamp(self):
        """Cursor entries should store (position, timestamp) tuple."""
        from unittest.mock import MagicMock, patch

        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        mock_db = MagicMock()
        mock_stage_run = {
            "id": "stage-test",
            "workspace_name": "test",
            "stage_name": "train",
            "status": "running",
            "backend_type": "local",
            "backend_handle": "stage-test",
            "log_uri": None,
        }

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = "log\n"

            logs("stage-test", follow=True)

            # Check cursor is a tuple with (position, timestamp)
            assert "stage-test" in _log_cursors
            cursor_entry = _log_cursors["stage-test"]
            assert isinstance(cursor_entry, tuple)
            assert len(cursor_entry) == 2
            assert isinstance(cursor_entry[0], int)  # position
            assert isinstance(cursor_entry[1], float)  # timestamp

    def test_logs_follow_mode_concurrent_calls_no_race(self):
        """Concurrent follow calls should not crash with cursor races."""
        import threading
        from unittest.mock import MagicMock, patch

        from goldfish.server_tools.execution_tools import _log_cursors, logs

        _log_cursors.clear()

        mock_db = MagicMock()
        mock_stage_run = {
            "id": "stage-abc123",
            "workspace_name": "test-workspace",
            "stage_name": "train",
            "status": "running",
            "backend_type": "local",
            "backend_handle": "stage-abc123",
            "log_uri": None,
        }

        full_logs = "line1\nline2\nline3\n"

        with (
            patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.execution_tools._get_stage_executor") as mock_executor,
        ):
            mock_db.get_stage_run.return_value = mock_stage_run
            mock_executor.return_value.local_executor.get_container_logs.return_value = full_logs

            errors: list[Exception] = []

            def worker() -> None:
                try:
                    for _ in range(50):
                        logs("stage-abc123", tail=3, follow=True)
                except Exception as exc:  # pragma: no cover - should not happen
                    errors.append(exc)

            threads = [threading.Thread(target=worker) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert errors == []
