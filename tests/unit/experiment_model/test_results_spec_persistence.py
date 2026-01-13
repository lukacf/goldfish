"""Unit tests for run_results_spec persistence.

TDD: These tests define the expected behavior for storing and retrieving results specs.
Tests should fail initially (RED), then pass after implementation (GREEN).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import InvalidResultsSpecError


class TestStoreResultsSpec:
    """Tests for storing run_results_spec."""

    def test_store_results_spec(self) -> None:
        """Can store a valid results_spec for a stage run."""
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing model accuracy on validation set.",
        }

        manager.store_results_spec(
            stage_run_id="stage-abc123",
            record_id="01HXYZ1234567890ABCDEFGH",
            spec=spec,
        )

        # Verify database insert was called
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO run_results_spec" in call_args[0][0]

    def test_store_results_spec_validates_spec(self) -> None:
        """Storing an invalid spec raises InvalidResultsSpecError."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        invalid_spec = {
            "primary_metric": "accuracy",
            # Missing required fields
        }

        with pytest.raises(InvalidResultsSpecError):
            manager.store_results_spec(
                stage_run_id="stage-abc123",
                record_id="01HXYZ1234567890ABCDEFGH",
                spec=invalid_spec,
            )

    def test_store_results_spec_serializes_to_json(self) -> None:
        """The spec is serialized to JSON before storage."""
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing model accuracy on validation set.",
        }

        manager.store_results_spec(
            stage_run_id="stage-abc123",
            record_id="01HXYZ1234567890ABCDEFGH",
            spec=spec,
        )

        # Verify JSON serialization
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        # spec_json should be a JSON string
        spec_json = params[2]  # Third parameter is spec_json
        assert isinstance(spec_json, str)
        parsed = json.loads(spec_json)
        assert parsed["primary_metric"] == "accuracy"


class TestGetResultsSpec:
    """Tests for retrieving run_results_spec."""

    def test_get_results_spec_by_stage_run(self) -> None:
        """Can retrieve results spec by stage_run_id."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": '{"primary_metric": "accuracy", "direction": "maximize"}',
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec("stage-abc123")

        assert result is not None
        assert result["stage_run_id"] == "stage-abc123"
        assert result["spec_json"] == '{"primary_metric": "accuracy", "direction": "maximize"}'

    def test_get_results_spec_not_found(self) -> None:
        """Returns None when results spec not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec("nonexistent")

        assert result is None

    def test_get_results_spec_by_record(self) -> None:
        """Can retrieve results spec by record_id."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": '{"primary_metric": "loss", "direction": "minimize"}',
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec_by_record("01HXYZ1234567890ABCDEFGH")

        assert result is not None
        assert result["record_id"] == "01HXYZ1234567890ABCDEFGH"

    def test_get_results_spec_by_record_not_found(self) -> None:
        """Returns None when results spec not found by record_id."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec_by_record("nonexistent-record")

        assert result is None


class TestGetResultsSpecParsed:
    """Tests for retrieving parsed results specs."""

    def test_get_results_spec_parsed(self) -> None:
        """Can retrieve results spec as parsed dict."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEFGH",
            "spec_json": '{"primary_metric": "accuracy", "direction": "maximize", "min_value": 0.6}',
            "created_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec_parsed("stage-abc123")

        assert result is not None
        assert result["primary_metric"] == "accuracy"
        assert result["direction"] == "maximize"
        assert result["min_value"] == 0.6

    def test_get_results_spec_parsed_not_found(self) -> None:
        """Returns None when results spec not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        result = manager.get_results_spec_parsed("nonexistent")

        assert result is None
