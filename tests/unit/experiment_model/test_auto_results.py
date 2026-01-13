"""Unit tests for auto results extraction.

TDD: Tests for extracting results_auto from run metrics.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from goldfish.experiment_model.records import ExperimentRecordManager


class TestExtractAutoResults:
    """Tests for auto results extraction from metrics."""

    def test_extract_auto_results_basic(self) -> None:
        """Can extract auto results from metrics summary."""
        # Setup mock data
        mock_spec_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": json.dumps(
                {
                    "primary_metric": "accuracy",
                    "direction": "maximize",
                    "min_value": 0.60,
                    "goal_value": 0.80,
                    "dataset_split": "val",
                    "tolerance": 0.01,
                    "context": "Testing accuracy.",
                }
            ),
            "created_at": "2025-01-13T14:00:00Z",
        }

        mock_metrics = [
            {"name": "accuracy", "last_value": 0.75, "min_value": 0.50, "max_value": 0.75},
        ]

        mock_conn = MagicMock()
        # First call returns spec, second returns metrics
        mock_conn.execute.return_value.fetchone.side_effect = [mock_spec_row, None]
        mock_conn.execute.return_value.fetchall.return_value = mock_metrics

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.extract_auto_results("stage-abc123")

        assert result is not None
        assert result["primary_metric"] == "accuracy"
        assert result["value"] == 0.75
        assert result["direction"] == "maximize"

    def test_extract_auto_results_with_secondary_metrics(self) -> None:
        """Auto results include secondary metrics when specified."""
        mock_spec_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": json.dumps(
                {
                    "primary_metric": "accuracy",
                    "direction": "maximize",
                    "min_value": 0.60,
                    "goal_value": 0.80,
                    "dataset_split": "val",
                    "tolerance": 0.01,
                    "secondary_metrics": ["loss", "f1"],
                    "context": "Testing accuracy with secondary metrics.",
                }
            ),
            "created_at": "2025-01-13T14:00:00Z",
        }

        mock_metrics = [
            {"name": "accuracy", "last_value": 0.75, "min_value": 0.50, "max_value": 0.75},
            {"name": "loss", "last_value": 0.25, "min_value": 0.20, "max_value": 0.50},
            {"name": "f1", "last_value": 0.70, "min_value": 0.40, "max_value": 0.70},
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [mock_spec_row, None]
        mock_conn.execute.return_value.fetchall.return_value = mock_metrics

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.extract_auto_results("stage-abc123")

        assert result is not None
        assert "secondary" in result
        assert result["secondary"]["loss"] == 0.25
        assert result["secondary"]["f1"] == 0.70

    def test_extract_auto_results_no_spec(self) -> None:
        """Returns None when no results spec exists."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.extract_auto_results("stage-no-spec")

        assert result is None

    def test_extract_auto_results_no_metrics(self) -> None:
        """Returns partial results when metrics not found."""
        mock_spec_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": json.dumps(
                {
                    "primary_metric": "accuracy",
                    "direction": "maximize",
                    "min_value": 0.60,
                    "goal_value": 0.80,
                    "dataset_split": "val",
                    "tolerance": 0.01,
                    "context": "Testing accuracy.",
                }
            ),
            "created_at": "2025-01-13T14:00:00Z",
        }

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [mock_spec_row, None]
        mock_conn.execute.return_value.fetchall.return_value = []  # No metrics

        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.extract_auto_results("stage-abc123")

        # Should still return result but with None value
        assert result is not None
        assert result["primary_metric"] == "accuracy"
        assert result["value"] is None


class TestUpdateAutoResults:
    """Tests for updating run_results with auto-extracted data."""

    def test_update_auto_results(self) -> None:
        """Can update run_results with auto-extracted data."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        auto_results = {
            "primary_metric": "accuracy",
            "value": 0.75,
            "direction": "maximize",
            "dataset_split": "val",
        }

        manager.update_auto_results("stage-abc123", auto_results, "completed")

        # Verify UPDATE was called (there's also a SELECT for error text)
        assert mock_conn.execute.call_count >= 1
        # Find the UPDATE call
        update_called = False
        for call in mock_conn.execute.call_args_list:
            if call[0] and "UPDATE run_results" in call[0][0]:
                update_called = True
                sql = call[0][0]
                assert "results_auto" in sql
                assert "results_status" in sql
                assert "infra_outcome" in sql
                break
        assert update_called, "UPDATE run_results was not called"

    def test_update_auto_results_sets_status_to_auto(self) -> None:
        """Updating auto results sets results_status to 'auto'."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        auto_results = {"primary_metric": "accuracy", "value": 0.75}
        manager.update_auto_results("stage-abc123", auto_results, "completed")

        # Check params include 'auto' for status
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        # Find the 'auto' status in params
        assert "auto" in params


class TestDeriveInfraOutcome:
    """Tests for deriving infra_outcome from run status."""

    def test_derive_infra_outcome_completed(self) -> None:
        """Status 'completed' maps to infra_outcome 'completed'."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        assert manager.derive_infra_outcome("completed") == "completed"

    def test_derive_infra_outcome_failed(self) -> None:
        """Status 'failed' maps to infra_outcome 'crashed'."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        assert manager.derive_infra_outcome("failed") == "crashed"

    def test_derive_infra_outcome_preempted(self) -> None:
        """Status 'preempted' maps to infra_outcome 'preempted'."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        assert manager.derive_infra_outcome("preempted") == "preempted"

    def test_derive_infra_outcome_canceled(self) -> None:
        """Status 'canceled' maps to infra_outcome 'canceled'."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        assert manager.derive_infra_outcome("canceled") == "canceled"

    def test_derive_infra_outcome_unknown(self) -> None:
        """Unknown status maps to infra_outcome 'unknown'."""
        mock_db = MagicMock()
        manager = ExperimentRecordManager(mock_db)

        assert manager.derive_infra_outcome("running") == "unknown"
        assert manager.derive_infra_outcome("pending") == "unknown"
        assert manager.derive_infra_outcome("foo") == "unknown"
