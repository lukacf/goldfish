"""Unit tests for strict finalization gate.

TDD: Tests for blocking runs when unfinalized terminal runs exist.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from goldfish.experiment_model.records import ExperimentRecordManager


class TestListUnfinalizedRuns:
    """Tests for list_unfinalized_runs method."""

    def test_list_unfinalized_runs_finds_terminal(self) -> None:
        """Finds terminal infra runs that are not finalized."""
        mock_rows = [
            {
                "stage_run_id": "stage-abc123",
                "record_id": "rec123",
                "results_status": "auto",
                "infra_outcome": "completed",
            },
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.list_unfinalized_runs("test_ws")

        assert len(result) == 1
        assert result[0]["stage_run_id"] == "stage-abc123"

    def test_list_unfinalized_runs_empty_when_all_finalized(self) -> None:
        """Returns empty list when all terminal runs are finalized."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.list_unfinalized_runs("test_ws")

        assert result == []

    def test_list_unfinalized_runs_filters_terminal_outcomes(self) -> None:
        """Only considers terminal infra outcomes."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_unfinalized_runs("test_ws")

        # Verify query filters by terminal outcomes
        call_args = mock_conn.execute.call_args
        query = call_args[0][0]
        assert "completed" in query or "infra_outcome IN" in query


class TestCheckFinalizationGate:
    """Tests for checking if runs are blocked."""

    def test_check_finalization_gate_blocks_when_unfinalized(self) -> None:
        """Returns blocking info when unfinalized runs exist."""
        mock_rows = [
            {
                "stage_run_id": "stage-abc123",
                "record_id": "rec123",
                "results_status": "auto",
                "infra_outcome": "completed",
            },
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 1

    def test_check_finalization_gate_allows_when_all_finalized(self) -> None:
        """Returns not blocked when all terminal runs finalized."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is False
        assert result["unfinalized"] == []

    def test_check_finalization_gate_returns_record_ids(self) -> None:
        """Blocked response includes record IDs for unfinalized runs."""
        mock_rows = [
            {"stage_run_id": "stage-1", "record_id": "rec1", "results_status": "auto", "infra_outcome": "completed"},
            {"stage_run_id": "stage-2", "record_id": "rec2", "results_status": "missing", "infra_outcome": "crashed"},
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True
        record_ids = [r["record_id"] for r in result["unfinalized"]]
        assert "rec1" in record_ids
        assert "rec2" in record_ids


class TestTerminalInfraOutcomes:
    """Tests that correct infra outcomes require finalization.

    Note: 'is_terminal_infra_outcome' means "requires finalization before new runs",
    not just "the run has ended". Canceled runs do NOT require finalization because
    the cancel() call already captures the reason.
    """

    def test_completed_requires_finalization(self) -> None:
        """'completed' requires finalization."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        assert manager.is_terminal_infra_outcome("completed") is True

    def test_preempted_requires_finalization(self) -> None:
        """'preempted' requires finalization."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        assert manager.is_terminal_infra_outcome("preempted") is True

    def test_crashed_requires_finalization(self) -> None:
        """'crashed' requires finalization."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        assert manager.is_terminal_infra_outcome("crashed") is True

    def test_canceled_does_not_require_finalization(self) -> None:
        """'canceled' does NOT require finalization - cancel() already captured reason."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        assert manager.is_terminal_infra_outcome("canceled") is False

    def test_unknown_is_not_terminal(self) -> None:
        """'unknown' is NOT a terminal infra outcome."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        assert manager.is_terminal_infra_outcome("unknown") is False

    def test_running_status_is_not_terminal(self) -> None:
        """Running status does not block."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)
        # Unknown infra outcome (running/pending runs)
        assert manager.is_terminal_infra_outcome("running") is False
