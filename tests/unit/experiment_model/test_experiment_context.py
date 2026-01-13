"""Unit tests for experiment context retrieval.

TDD: Tests for getting experiment context for mount/dashboard.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from goldfish.experiment_model.records import ExperimentRecordManager


class TestGetExperimentContext:
    """Tests for get_experiment_context method."""

    def test_get_experiment_context_basic(self) -> None:
        """Can get experiment context for a workspace."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        context = manager.get_experiment_context("test_ws")

        assert context is not None
        assert "current_best" in context
        assert "awaiting_finalization" in context
        assert "recent_trend" in context

    def test_get_experiment_context_includes_current_best(self) -> None:
        """Context includes current best tagged record."""
        # Setup mock for best tagged record lookup
        # get_current_best uses fetchone twice: tag_row, then results_row
        mock_tag_row = {"tag_name": "best-run", "record_id": "rec-best"}
        mock_results = {
            "results_final": json.dumps({"value": 0.80, "primary_metric": "accuracy"}),
        }

        mock_conn = MagicMock()
        # get_current_best: 2x fetchone (tag_row, results_row)
        mock_conn.execute.return_value.fetchone.side_effect = [mock_tag_row, mock_results]
        # list_unfinalized_runs: 1x fetchall, get_recent_trend: 1x fetchall
        mock_conn.execute.return_value.fetchall.side_effect = [
            [],  # Unfinalized runs
            [],  # Recent finalized
        ]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        context = manager.get_experiment_context("test_ws")

        # Should have current_best if a tagged record exists
        assert "current_best" in context
        assert context["current_best"] is not None
        assert context["current_best"]["record_id"] == "rec-best"
        assert context["current_best"]["value"] == 0.80

    def test_get_experiment_context_includes_awaiting_finalization(self) -> None:
        """Context includes records awaiting finalization."""
        mock_unfinalized = [
            {"stage_run_id": "stage-1", "record_id": "rec1", "results_status": "auto", "infra_outcome": "completed"},
            {"stage_run_id": "stage-2", "record_id": "rec2", "results_status": "missing", "infra_outcome": "crashed"},
        ]

        mock_conn = MagicMock()
        # get_current_best: 1x fetchone (tag_row) - returns None so no second call
        mock_conn.execute.return_value.fetchone.return_value = None
        # list_unfinalized_runs: 1x fetchall, get_recent_trend: 1x fetchall
        mock_conn.execute.return_value.fetchall.side_effect = [
            mock_unfinalized,  # Unfinalized runs
            [],  # Recent finalized
        ]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        context = manager.get_experiment_context("test_ws")

        assert len(context["awaiting_finalization"]) == 2


class TestGetRecentTrend:
    """Tests for recent trend retrieval."""

    def test_get_recent_trend_returns_values(self) -> None:
        """Can get recent finalized values for trend."""
        mock_rows = [
            {"record_id": "rec1", "results_final": json.dumps({"value": 0.75})},
            {"record_id": "rec2", "results_final": json.dumps({"value": 0.73})},
            {"record_id": "rec3", "results_final": json.dumps({"value": 0.70})},
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        trend = manager.get_recent_trend("test_ws", limit=5)

        assert len(trend) == 3
        assert trend[0]["value"] == 0.75

    def test_get_recent_trend_empty(self) -> None:
        """Returns empty list when no finalized runs."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        trend = manager.get_recent_trend("test_ws")

        assert trend == []


class TestGetCurrentBest:
    """Tests for getting current best tagged record."""

    def test_get_current_best_by_tag_prefix(self) -> None:
        """Can find best record by tag prefix."""
        mock_tag_row = {"tag_name": "best-accuracy", "record_id": "rec-best"}
        mock_results = {
            "results_final": json.dumps({"value": 0.80, "primary_metric": "accuracy"}),
        }

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [mock_tag_row, mock_results]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        best = manager.get_current_best("test_ws", tag_prefix="best-")

        assert best is not None

    def test_get_current_best_none_when_no_tags(self) -> None:
        """Returns None when no best tag found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        best = manager.get_current_best("test_ws")

        assert best is None
