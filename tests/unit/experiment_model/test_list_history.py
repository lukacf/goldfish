"""Unit tests for list_history functionality.

TDD: Tests for listing experiment history.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from goldfish.experiment_model.records import ExperimentRecordManager


class TestListHistory:
    """Tests for list_history method."""

    def test_list_history_basic(self) -> None:
        """Can list experiment history."""
        mock_rows = [
            {
                "record_id": "rec123",
                "workspace_name": "test_ws",
                "type": "run",
                "stage_run_id": "stage-abc123",
                "version": "v1",
                "created_at": "2025-01-13T14:00:00Z",
            },
        ]

        # Set up mock to return different results for different queries
        # First call = count query, second call = main query,
        # subsequent calls = enrichment queries (return empty)
        mock_conn = MagicMock()
        call_count = [0]

        def mock_execute(*args: object, **kwargs: object) -> MagicMock:
            result = MagicMock()
            if call_count[0] == 0:
                # Count query returns total
                result.fetchone.return_value = {"cnt": 1}
            elif call_count[0] == 1:
                # Main query returns records
                result.fetchall.return_value = mock_rows
            else:
                # Enrichment queries return empty lists
                result.fetchall.return_value = []
                result.fetchone.return_value = {"stage_name": "train"}
            call_count[0] += 1
            return result

        mock_conn.execute.side_effect = mock_execute

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.list_history("test_ws")

        assert result is not None
        assert "records" in result
        assert len(result["records"]) == 1
        # Enrichment adds these fields
        assert "tags" in result["records"][0]

    def test_list_history_filter_by_type(self) -> None:
        """Can filter history by type."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", record_type="run")

        # Verify type filter was used
        call_args = mock_conn.execute.call_args
        assert "type = ?" in call_args[0][0]

    def test_list_history_filter_by_stage(self) -> None:
        """Can filter history by stage."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", stage="train")

        # Verify stage filter was used
        call_args = mock_conn.execute.call_args
        assert "stage_name = ?" in call_args[0][0]

    def test_list_history_filter_by_tagged(self) -> None:
        """Can filter to only tagged records."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", tagged=True)

        # Verify tagged filter was used
        call_args = mock_conn.execute.call_args
        assert "run_tags" in call_args[0][0]

    def test_list_history_filter_by_specific_tag(self) -> None:
        """Can filter to records with specific tag."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", tagged="best-run")

        # Verify specific tag filter was used
        call_args = mock_conn.execute.call_args
        assert "tag_name = ?" in call_args[0][0]

    def test_list_history_with_limit_offset(self) -> None:
        """Supports limit and offset."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", limit=10, offset=20)

        call_args = mock_conn.execute.call_args
        assert "LIMIT" in call_args[0][0]
        assert "OFFSET" in call_args[0][0]

    def test_list_history_sort_by_created(self) -> None:
        """Can sort by created_at."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_history("test_ws", sort_by="created", desc=True)

        call_args = mock_conn.execute.call_args
        assert "record_id DESC" in call_args[0][0]  # ULID sorting


class TestInspectRecord:
    """Tests for inspect_record method."""

    def test_inspect_record_by_record_id(self) -> None:
        """Can inspect record by record_id."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_record
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.inspect_record("rec123")

        assert result is not None
        assert result["record_id"] == "rec123"

    def test_inspect_record_include_results(self) -> None:
        """Can include results in inspection."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_results = {
            "stage_run_id": "stage-abc123",
            "record_id": "rec123",
            "results_status": "finalized",
            "results_auto": json.dumps({"value": 0.70}),
            "results_final": json.dumps({"value": 0.75}),
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [mock_record, mock_results]
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.inspect_record("rec123", include=["results"])

        assert "results" in result

    def test_inspect_record_include_tags(self) -> None:
        """Can include tags in inspection."""
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_tags = [{"tag_name": "best-run"}]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_record
        mock_conn.execute.return_value.fetchall.return_value = mock_tags

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.inspect_record("rec123", include=["tags"])

        assert "tags" in result

    def test_inspect_record_not_found(self) -> None:
        """Returns None when record not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.inspect_record("nonexistent")

        assert result is None

    def test_inspect_record_by_tag(self) -> None:
        """Can inspect record by tag reference."""
        mock_tag_row = {"record_id": "rec123", "workspace_name": "test_ws", "tag_name": "best-run"}
        mock_record = {
            "record_id": "rec123",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        # First: tag lookup (SELECT record_id FROM run_tags)
        # Second: record lookup (SELECT * FROM experiment_records)
        mock_conn.execute.return_value.fetchone.side_effect = [mock_tag_row, mock_record]
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.inspect_record("@best-run", workspace_name="test_ws")

        assert result is not None
