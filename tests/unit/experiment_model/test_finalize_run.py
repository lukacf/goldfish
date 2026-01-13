"""Unit tests for finalize_run functionality.

TDD: Tests for finalizing ML results.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import InvalidFinalizeResultsError


class TestFinalizeRun:
    """Tests for finalizing run results."""

    def test_finalize_run_basic(self) -> None:
        """Can finalize a run with valid results."""
        # Setup mock to return a row from get_run_results
        mock_run_results = {"stage_run_id": "stage-abc123", "record_id": "rec123"}
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_run_results
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Good results with stable training.",
        }

        manager.finalize_run("stage-abc123", results)

        # Verify UPDATE was called (among other calls for resolution)
        assert mock_conn.execute.call_count >= 2  # SELECT + UPDATE
        # Find the UPDATE call
        update_call = None
        for call in mock_conn.execute.call_args_list:
            if "UPDATE run_results" in call[0][0]:
                update_call = call
                break
        assert update_call is not None
        assert "results_final" in update_call[0][0]
        assert "results_status" in update_call[0][0]
        assert "ml_outcome" in update_call[0][0]

    def test_finalize_run_validates_results(self) -> None:
        """Finalize rejects invalid results."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        invalid_results = {
            "primary_metric": "accuracy",
            # Missing required fields
        }

        with pytest.raises(InvalidFinalizeResultsError):
            manager.finalize_run("stage-abc123", invalid_results)

    def test_finalize_run_sets_status_to_finalized(self) -> None:
        """Finalize sets results_status to 'finalized'."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Good results with stable training.",
        }

        manager.finalize_run("stage-abc123", results)

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        assert "finalized" in params

    def test_finalize_run_serializes_results(self) -> None:
        """Results are serialized to JSON."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Good results with stable training.",
        }

        manager.finalize_run("stage-abc123", results)

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        # First param should be JSON string
        results_json = params[0]
        parsed = json.loads(results_json)
        assert parsed["primary_metric"] == "accuracy"

    def test_finalize_run_with_optional_fields(self) -> None:
        """Finalize works with all optional fields."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "unit": "fraction",
            "dataset_split": "val",
            "step": 1000,
            "epoch": 10,
            "secondary": {"loss": 0.25, "f1": 0.70},
            "termination": {"infra_outcome": "completed"},
            "ml_outcome": "success",
            "notes": "Excellent results with all optional fields.",
        }

        manager.finalize_run("stage-abc123", results)

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        results_json = params[0]
        parsed = json.loads(results_json)
        assert parsed["unit"] == "fraction"
        assert parsed["step"] == 1000
        assert parsed["secondary"]["loss"] == 0.25


class TestFinalizeRunByRecordId:
    """Tests for finalizing by record_id instead of stage_run_id."""

    def test_finalize_run_by_record_id(self) -> None:
        """Can finalize using record_id."""
        # Mock that returns a record row with stage_run_id
        mock_record_row = {
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_record_row

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Good results via record ID.",
        }

        # Should resolve record_id to stage_run_id
        manager.finalize_run("01HXYZ1234567890ABCDEFGH", results)

        # Verify calls were made
        assert mock_conn.execute.called

    def test_finalize_run_record_not_found(self) -> None:
        """Raises error when record not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Results for nonexistent record.",
        }

        with pytest.raises(ValueError, match="not found"):
            manager.finalize_run("nonexistent-record", results)


class TestGetFinalizedResults:
    """Tests for retrieving finalized results."""

    def test_get_finalized_results(self) -> None:
        """Can retrieve finalized results."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "results_status": "finalized",
            "infra_outcome": "completed",
            "ml_outcome": "success",
            "results_auto": '{"value": 0.70}',
            "results_final": '{"value": 0.75, "primary_metric": "accuracy"}',
            "comparison": None,
            "finalized_by": "ml_claude",
            "finalized_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        parsed = manager.get_finalized_results("stage-abc123")

        assert parsed is not None
        assert parsed["value"] == 0.75
        assert parsed["primary_metric"] == "accuracy"

    def test_get_finalized_results_not_finalized(self) -> None:
        """Returns None when results not finalized."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "results_status": "auto",
            "results_final": None,
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        parsed = manager.get_finalized_results("stage-abc123")

        assert parsed is None
