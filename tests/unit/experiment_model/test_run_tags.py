"""Unit tests for run tags functionality.

TDD: Tests for tagging experiment records.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from goldfish.experiment_model.records import ExperimentRecordManager


class TestTagRecord:
    """Tests for tagging records."""

    def test_tag_run_record(self) -> None:
        """Can tag a run record - creates BOTH run_tag AND version_tag."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
        }
        mock_conn = MagicMock()
        # First call returns record for get_record
        # Then None for run_tags check, None for version_tags check
        mock_conn.execute.return_value.fetchone.side_effect = [mock_record, None, None]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        result = manager.tag_record("rec123", "best-run")

        # Should return dict with confirmation
        assert result["record_id"] == "rec123"
        assert result["tag"] == "best-run"
        assert result["record_type"] == "run"

        # For run records: SELECT record, SELECT run_tags, SELECT version_tags, INSERT run_tags, INSERT version_tags
        assert mock_conn.execute.call_count >= 4

    def test_tag_checkpoint_record(self) -> None:
        """Can tag a checkpoint record (version tag ONLY, no run_tag)."""
        mock_record = {
            "record_id": "rec456",
            "workspace_name": "test_ws",
            "type": "checkpoint",
            "stage_run_id": None,  # Checkpoints have no stage_run_id
            "version": "v1",
        }
        mock_conn = MagicMock()
        # First call returns record for get_record
        # Then None for run_tags check, None for version_tags check
        mock_conn.execute.return_value.fetchone.side_effect = [mock_record, None, None]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        result = manager.tag_record("rec456", "stable-checkpoint")

        # Should return dict with confirmation
        assert result["record_id"] == "rec456"
        assert result["tag"] == "stable-checkpoint"
        assert result["record_type"] == "checkpoint"

        # For checkpoint records: SELECT record, SELECT run_tags, SELECT version_tags, INSERT version_tags (no run_tag)
        assert mock_conn.execute.call_count >= 3

    def test_tag_record_validates_tag_name(self) -> None:
        """Tag names are validated."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(ValueError, match="invalid"):
            manager.tag_record("rec123", "")  # Empty tag

    def test_tag_record_not_found(self) -> None:
        """Raises error when record not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(ValueError, match="not found"):
            manager.tag_record("nonexistent", "best-run")


class TestGetRunTags:
    """Tests for retrieving run tags (merged from run_tags and version_tags)."""

    def test_get_run_tags(self) -> None:
        """Can retrieve tags for a record (merged from both tables)."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "version": "v1",
            "type": "run",
            "stage_run_id": "stage-abc123",
        }
        mock_run_tags = [
            {"tag_name": "best-run"},
        ]
        mock_version_tags = [
            {"tag_name": "v1-final"},
        ]
        mock_conn = MagicMock()
        # First fetchone returns record, then fetchall calls for run_tags and version_tags
        mock_conn.execute.return_value.fetchone.return_value = mock_record
        mock_conn.execute.return_value.fetchall.side_effect = [mock_run_tags, mock_version_tags]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        tags = manager.get_record_tags("rec123")

        assert len(tags) == 2
        assert "best-run" in tags
        assert "v1-final" in tags

    def test_get_run_tags_empty(self) -> None:
        """Returns empty list when no tags."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None  # record not found

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        tags = manager.get_record_tags("rec123")

        assert tags == []


class TestGetRecordByTag:
    """Tests for looking up records by tag."""

    def test_get_record_by_tag(self) -> None:
        """Can look up a record by tag."""
        mock_tag_row = {"tag_name": "best-run", "record_id": "rec123", "workspace_name": "test_ws"}
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [mock_tag_row, mock_record]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record_by_tag("test_ws", "best-run")

        assert record is not None
        assert record["record_id"] == "rec123"

    def test_get_record_by_tag_not_found(self) -> None:
        """Returns None when tag not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record_by_tag("test_ws", "nonexistent")

        assert record is None

    def test_get_record_by_tag_validates_workspace_name(self) -> None:
        """Raises error for invalid workspace name."""
        from goldfish.validation import InvalidWorkspaceNameError

        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(InvalidWorkspaceNameError):
            manager.get_record_by_tag("invalid@workspace!", "best-run")

    def test_get_record_by_tag_validates_tag_name(self) -> None:
        """Raises error for empty tag name."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(ValueError, match="invalid"):
            manager.get_record_by_tag("test_ws", "")

        with pytest.raises(ValueError, match="invalid"):
            manager.get_record_by_tag("test_ws", "   ")


class TestRemoveTag:
    """Tests for removing tags."""

    def test_remove_tag(self) -> None:
        """Can remove a tag from BOTH run_tags and version_tags."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.remove_tag("test_ws", "best-run")

        # Should delete from both tables
        assert mock_conn.execute.call_count == 2
        calls = mock_conn.execute.call_args_list
        assert "DELETE FROM run_tags" in calls[0][0][0]
        assert "DELETE FROM workspace_version_tags" in calls[1][0][0]

    def test_remove_tag_validates_workspace_name(self) -> None:
        """Raises error for invalid workspace name."""
        from goldfish.validation import InvalidWorkspaceNameError

        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(InvalidWorkspaceNameError):
            manager.remove_tag("invalid@workspace!", "best-run")

    def test_remove_tag_validates_tag_name(self) -> None:
        """Raises error for empty tag name."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(ValueError, match="invalid"):
            manager.remove_tag("test_ws", "")

        with pytest.raises(ValueError, match="invalid"):
            manager.remove_tag("test_ws", "   ")
