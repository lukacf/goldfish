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
        """Can tag a run record."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
        }
        mock_conn = MagicMock()
        # First call returns record, second returns None (no existing tag)
        mock_conn.execute.return_value.fetchone.side_effect = [mock_record, None]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        manager.tag_record("rec123", "best-run")

        # Should insert into run_tags
        assert mock_conn.execute.call_count >= 2  # SELECT record, SELECT existing, INSERT

    def test_tag_checkpoint_record(self) -> None:
        """Can tag a checkpoint record (version tag only)."""
        mock_record = {
            "record_id": "rec456",
            "workspace_name": "test_ws",
            "type": "checkpoint",
            "stage_run_id": None,  # Checkpoints have no stage_run_id
            "version": "v1",
        }
        mock_conn = MagicMock()
        # First call returns record, second returns None (no existing tag)
        mock_conn.execute.return_value.fetchone.side_effect = [mock_record, None]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        manager.tag_record("rec456", "stable-checkpoint")

        assert mock_conn.execute.call_count >= 2  # SELECT record, SELECT existing, INSERT

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
    """Tests for retrieving run tags."""

    def test_get_run_tags(self) -> None:
        """Can retrieve tags for a record."""
        mock_tags = [
            {"tag_name": "best-run", "record_id": "rec123"},
            {"tag_name": "v1-final", "record_id": "rec123"},
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_tags

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
        mock_conn.execute.return_value.fetchall.return_value = []

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


class TestRemoveTag:
    """Tests for removing tags."""

    def test_remove_tag(self) -> None:
        """Can remove a tag from a record."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.remove_tag("test_ws", "best-run")

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "DELETE FROM run_tags" in call_args[0][0]
