"""Unit tests for the compare_runs tool."""

import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_server_imports():
    """Mock server imports to avoid circular import issues."""
    mock_mcp = MagicMock()
    mock_mcp.tool = MagicMock(return_value=lambda f: f)

    with patch.dict(
        sys.modules,
        {
            "goldfish.server_core": MagicMock(
                _get_config=MagicMock(),
                _get_db=MagicMock(),
                _get_workspace_manager=MagicMock(),
                _get_metadata_bus=MagicMock(),
                _get_pipeline_executor=MagicMock(),
                _get_stage_executor=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        # Clear tool module from sys.modules to ensure fresh import with mocks
        sys.modules.pop("goldfish.server_tools.execution_tools", None)
        yield


class TestCompareRuns:
    """Tests for the compare_runs tool."""

    def test_compare_runs_returns_config_diff(self):
        """Test that compare_runs shows config differences between runs."""
        from goldfish.server_tools.execution_tools import compare_runs

        mock_db = MagicMock()
        mock_db.get_stage_run.side_effect = [
            {
                "id": "stage-run1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "config_json": '{"learning_rate": 0.01, "batch_size": 32}',
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T10:00:00Z",
                "completed_at": "2025-01-04T11:00:00Z",
                "error": None,
                "outcome": "success",
            },
            {
                "id": "stage-run2",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "config_json": '{"learning_rate": 0.001, "batch_size": 64}',
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T12:00:00Z",
                "completed_at": "2025-01-04T13:00:00Z",
                "error": None,
                "outcome": "bad_results",
            },
        ]
        mock_db.get_metrics_summary.side_effect = [[], []]

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = compare_runs("stage-run1", "stage-run2")

        assert "config_diff" in result
        assert result["config_diff"]["learning_rate"] == {"run_a": 0.01, "run_b": 0.001}
        assert result["config_diff"]["batch_size"] == {"run_a": 32, "run_b": 64}

    def test_compare_runs_returns_outcome_diff(self):
        """Test that compare_runs shows outcome differences."""
        from goldfish.server_tools.execution_tools import compare_runs

        mock_db = MagicMock()
        mock_db.get_stage_run.side_effect = [
            {
                "id": "stage-run1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "config_json": "{}",
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T10:00:00Z",
                "completed_at": "2025-01-04T11:00:00Z",
                "error": None,
                "outcome": "success",
            },
            {
                "id": "stage-run2",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "failed",
                "config_json": "{}",
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T12:00:00Z",
                "completed_at": "2025-01-04T13:00:00Z",
                "error": "CUDA OOM",
                "outcome": None,
            },
        ]
        mock_db.get_metrics_summary.side_effect = [[], []]

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = compare_runs("stage-run1", "stage-run2")

        assert result["run_a"]["status"] == "completed"
        assert result["run_b"]["status"] == "failed"
        assert result["run_a"]["outcome"] == "success"
        assert result["run_b"]["error"] == "CUDA OOM"

    def test_compare_runs_returns_metrics_diff(self):
        """Test that compare_runs shows metric differences."""
        from goldfish.server_tools.execution_tools import compare_runs

        mock_db = MagicMock()
        mock_db.get_stage_run.side_effect = [
            {
                "id": "stage-run1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "config_json": "{}",
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T10:00:00Z",
                "completed_at": "2025-01-04T11:00:00Z",
                "error": None,
                "outcome": "success",
            },
            {
                "id": "stage-run2",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "config_json": "{}",
                "inputs_json": "{}",
                "outputs_json": "[]",
                "started_at": "2025-01-04T12:00:00Z",
                "completed_at": "2025-01-04T13:00:00Z",
                "error": None,
                "outcome": "success",
            },
        ]
        mock_db.get_metrics_summary.side_effect = [
            [
                {"name": "accuracy", "last_value": 0.85, "min_value": 0.5, "max_value": 0.85, "count": 10},
                {"name": "loss", "last_value": 0.15, "min_value": 0.15, "max_value": 0.5, "count": 10},
            ],
            [
                {"name": "accuracy", "last_value": 0.92, "min_value": 0.6, "max_value": 0.92, "count": 10},
                {"name": "loss", "last_value": 0.08, "min_value": 0.08, "max_value": 0.4, "count": 10},
            ],
        ]

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = compare_runs("stage-run1", "stage-run2")

        assert "metrics_diff" in result
        assert result["metrics_diff"]["accuracy"]["run_a"] == 0.85
        assert result["metrics_diff"]["accuracy"]["run_b"] == 0.92
        assert result["metrics_diff"]["accuracy"]["delta"] == pytest.approx(0.07)

    def test_compare_runs_not_found(self):
        """Test that compare_runs returns error when run not found."""
        from goldfish.server_tools.execution_tools import compare_runs

        mock_db = MagicMock()
        mock_db.get_stage_run.return_value = None

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = compare_runs("stage-nonexistent", "stage-run2")

        assert result.get("error") is not None
        assert "not found" in result["error"].lower()
