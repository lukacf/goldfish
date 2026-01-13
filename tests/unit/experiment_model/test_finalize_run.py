"""Unit tests for finalize_run functionality.

TDD: Tests for finalizing ML results.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import InvalidFinalizeResultsError


def _setup_finalize_mocks(mock_conn: MagicMock) -> None:
    """Setup standard mocks for finalize_run tests.

    Returns data for: run_results, record, stage_runs, spec, previous runs, current_best.
    """
    mock_run_results = {"stage_run_id": "stage-abc123", "record_id": "rec123"}
    mock_record = {
        "record_id": "rec123",
        "workspace_name": "test_ws",
        "version": "v1",
        "type": "run",
        "stage_run_id": "stage-abc123",
    }
    mock_stage_row = {"stage_name": "train"}

    mock_conn.execute.return_value.fetchone.side_effect = [
        mock_run_results,  # get_run_results
        mock_record,  # get_record_by_stage_run
        mock_stage_row,  # get stage_name
        None,  # get_results_spec_parsed (no spec)
        None,  # get_current_best (no best)
    ]
    mock_conn.execute.return_value.fetchall.return_value = []  # no previous runs


class TestFinalizeRun:
    """Tests for finalizing run results."""

    def test_finalize_run_basic(self) -> None:
        """Can finalize a run with valid results."""
        mock_conn = MagicMock()
        _setup_finalize_mocks(mock_conn)
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

        result = manager.finalize_run("stage-abc123", results)

        # Should return result dict
        assert result["record_id"] == "rec123"
        assert result["results_status"] == "finalized"
        assert "comparison" in result

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
        _setup_finalize_mocks(mock_conn)
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

        result = manager.finalize_run("stage-abc123", results)

        # Should return finalized status
        assert result["results_status"] == "finalized"

    def test_finalize_run_serializes_results(self) -> None:
        """Results are serialized to JSON."""
        mock_conn = MagicMock()
        _setup_finalize_mocks(mock_conn)
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

        # Find the UPDATE call
        update_call = None
        for call in mock_conn.execute.call_args_list:
            if "UPDATE run_results" in call[0][0]:
                update_call = call
                break
        assert update_call is not None
        params = update_call[0][1]
        # First param should be JSON string
        results_json = params[0]
        parsed = json.loads(results_json)
        assert parsed["primary_metric"] == "accuracy"

    def test_finalize_run_with_optional_fields(self) -> None:
        """Finalize works with all optional fields."""
        mock_conn = MagicMock()
        _setup_finalize_mocks(mock_conn)
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

        # Find the UPDATE call
        update_call = None
        for call in mock_conn.execute.call_args_list:
            if "UPDATE run_results" in call[0][0]:
                update_call = call
                break
        assert update_call is not None
        params = update_call[0][1]
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
        mock_run_results = {"stage_run_id": "stage-abc123", "record_id": "01HXYZ1234567890ABCDEFGH"}
        mock_stage_row = {"stage_name": "train"}

        mock_conn = MagicMock()
        # Sequence through finalize_run when called with record_id:
        # 1. _resolve_stage_run_id: get_run_results(record_id) -> None
        # 2. _resolve_stage_run_id: get_record(record_id) -> record_row
        # 3. finalize_run: get_record_by_stage_run(stage_run_id) -> record_row
        # 4. finalize_run: get stage_name from stage_runs -> stage_row
        # 5. compute_comparison -> _compute_vs_best: get_record_by_stage_run -> record_row
        # 6. _compute_vs_best: get_results_spec_parsed -> None
        # 7. _compute_vs_best: get_current_best -> None
        mock_conn.execute.return_value.fetchone.side_effect = [
            None,  # _resolve_stage_run_id: get_run_results(record_id) - not found
            mock_record_row,  # _resolve_stage_run_id: get_record(record_id) - found
            mock_record_row,  # finalize_run: get_record_by_stage_run
            mock_stage_row,  # finalize_run: stage_name lookup
            mock_record_row,  # _compute_vs_best: get_record_by_stage_run
            None,  # _compute_vs_best: get_results_spec_parsed - None
            None,  # _compute_vs_best: get_current_best - run_tags query
            None,  # _compute_vs_best: get_current_best - version_tags fallback
        ]
        mock_conn.execute.return_value.fetchall.return_value = []  # no previous runs

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
        result = manager.finalize_run("01HXYZ1234567890ABCDEFGH", results)

        # Verify finalization succeeded
        assert result["results_status"] == "finalized"
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
