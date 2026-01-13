"""Unit tests for comparison block computation.

TDD: Tests for computing comparison blocks at finalization time.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from goldfish.experiment_model.records import ExperimentRecordManager


class TestComputeComparison:
    """Tests for computing comparison blocks."""

    def test_compute_comparison_basic(self) -> None:
        """Can compute a basic comparison block."""
        # Setup mock for previous run
        mock_prev_results = {
            "stage_run_id": "stage-prev",
            "record_id": "rec-prev",
            "results_status": "finalized",
            "results_final": json.dumps({"value": 0.70, "primary_metric": "accuracy"}),
            "ml_outcome": "success",
        }

        mock_conn = MagicMock()
        # fetchall returns previous runs
        mock_conn.execute.return_value.fetchall.return_value = [mock_prev_results]
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        # Current run finalized results
        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }

        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert comparison is not None
        assert "vs_previous" in comparison

    def test_compute_comparison_vs_previous_delta(self) -> None:
        """Delta is computed correctly for previous run."""
        mock_prev_results = {
            "stage_run_id": "stage-prev",
            "record_id": "rec-prev",
            "results_status": "finalized",
            "results_final": json.dumps({"value": 0.70, "primary_metric": "accuracy"}),
            "ml_outcome": "success",
        }

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [mock_prev_results]
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }

        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert comparison["vs_previous"]["delta"] == 0.05  # 0.75 - 0.70

    def test_compute_comparison_no_previous(self) -> None:
        """Returns None for vs_previous when no previous run exists."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []  # No previous runs
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }

        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert comparison["vs_previous"] is None

    def test_compute_comparison_minimize_direction(self) -> None:
        """Delta is computed correctly for minimize direction."""
        mock_prev_results = {
            "stage_run_id": "stage-prev",
            "record_id": "rec-prev",
            "results_status": "finalized",
            "results_final": json.dumps({"value": 0.30, "primary_metric": "loss"}),
            "ml_outcome": "success",
        }

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [mock_prev_results]
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        current_results = {
            "value": 0.25,  # Lower is better for minimize
            "primary_metric": "loss",
            "direction": "minimize",
        }

        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        # Delta is still just current - previous
        assert comparison["vs_previous"]["delta"] == -0.05  # 0.25 - 0.30


class TestComputeComparisonVsBest:
    """Tests for vs_best comparison with baseline_run."""

    def test_compute_comparison_vs_best_with_baseline(self) -> None:
        """vs_best uses baseline_run when specified in spec."""
        mock_spec = {
            "stage_run_id": "stage-current",
            "record_id": "rec-current",
            "spec_json": json.dumps({"baseline_run": "stage-baseline", "primary_metric": "accuracy"}),
            "created_at": "2025-01-13T14:00:00Z",
        }

        mock_current_record = {
            "record_id": "rec-current",
            "workspace_name": "test_ws",
            "version": "v1",
            "type": "run",
            "stage_run_id": "stage-current",
        }

        mock_baseline_record = {
            "record_id": "rec-baseline",
            "workspace_name": "test_ws",
            "version": "v1",
            "type": "run",
            "stage_run_id": "stage-baseline",
        }

        mock_baseline_results = {
            "stage_run_id": "stage-baseline",
            "record_id": "rec-baseline",
            "results_status": "finalized",
            "results_final": json.dumps({"value": 0.80, "primary_metric": "accuracy"}),
            "ml_outcome": "success",
        }

        mock_conn = MagicMock()
        # Setup different returns for different queries
        mock_conn.execute.return_value.fetchall.return_value = []  # No previous runs
        mock_conn.execute.return_value.fetchone.side_effect = [
            # vs_previous: get_record_by_stage_run first, but no previous runs
            # vs_best: get_record_by_stage_run (for workspace), then spec, then baseline resolution
            mock_current_record,  # _compute_vs_best: get_record_by_stage_run for workspace
            mock_spec,  # _compute_vs_best: get_results_spec_parsed
            None,  # _compute_vs_best: get_record (baseline as record_id) - not found
            mock_baseline_record,  # _compute_vs_best: get_record_by_stage_run for baseline
            mock_baseline_results,  # _compute_vs_best: get_finalized_results
        ]

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }

        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert "vs_best" in comparison
        assert comparison["vs_best"] is not None
        assert comparison["vs_best"]["record"] == "rec-baseline"
        assert comparison["vs_best"]["delta"] == -0.05  # 0.75 - 0.80


class TestStoreComparison:
    """Tests for storing comparison in run_results."""

    def test_store_comparison(self) -> None:
        """Can store comparison in run_results."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        comparison = {
            "vs_previous": {"record": "rec-prev", "delta": 0.05},
            "vs_best": None,
        }

        manager.store_comparison("stage-abc123", comparison)

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "UPDATE run_results" in call_args[0][0]
        assert "comparison" in call_args[0][0]

    def test_store_comparison_serializes_json(self) -> None:
        """Comparison is serialized to JSON."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        comparison = {
            "vs_previous": {"record": "rec-prev", "delta": 0.05},
        }

        manager.store_comparison("stage-abc123", comparison)

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        # First param should be JSON
        parsed = json.loads(params[0])
        assert parsed["vs_previous"]["delta"] == 0.05


class TestGetComparison:
    """Tests for retrieving comparison."""

    def test_get_comparison(self) -> None:
        """Can retrieve comparison from run_results."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "rec123",
            "comparison": json.dumps({"vs_previous": {"record": "rec-prev", "delta": 0.05}}),
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        comparison = manager.get_comparison("stage-abc123")

        assert comparison is not None
        assert comparison["vs_previous"]["delta"] == 0.05

    def test_get_comparison_none(self) -> None:
        """Returns None when no comparison stored."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "rec123",
            "comparison": None,
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        comparison = manager.get_comparison("stage-abc123")

        assert comparison is None
