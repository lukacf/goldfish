"""Unit tests for the list_all_runs tool."""

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


class TestListAllRuns:
    """Tests for the list_all_runs tool."""

    def test_list_all_runs_returns_runs_across_workspaces(self):
        """Test that list_all_runs shows runs from all workspaces."""
        from goldfish.server_tools.execution_tools import list_all_runs

        mock_db = MagicMock()
        mock_db.list_all_stage_runs.return_value = [
            {
                "id": "stage-run1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "completed",
                "outcome": "success",
                "started_at": "2025-01-04T10:00:00Z",
                "completed_at": "2025-01-04T11:00:00Z",
                "error": None,
                "total_count": 3,
            },
            {
                "id": "stage-run2",
                "workspace_name": "experiment1",
                "stage_name": "preprocess",
                "status": "running",
                "outcome": None,
                "started_at": "2025-01-04T12:00:00Z",
                "completed_at": None,
                "error": None,
                "total_count": 3,
            },
            {
                "id": "stage-run3",
                "workspace_name": "baseline",
                "stage_name": "evaluate",
                "status": "failed",
                "outcome": None,
                "started_at": "2025-01-04T09:00:00Z",
                "completed_at": "2025-01-04T09:30:00Z",
                "error": "OOM",
                "total_count": 3,
            },
        ]

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = list_all_runs()

        assert len(result["runs"]) == 3
        assert result["total"] == 3
        # Check different workspaces are included
        workspaces = {r["workspace"] for r in result["runs"]}
        assert "baseline" in workspaces
        assert "experiment1" in workspaces

    def test_list_all_runs_respects_limit_and_offset(self):
        """Test that list_all_runs supports pagination."""
        from goldfish.server_tools.execution_tools import list_all_runs

        mock_db = MagicMock()
        mock_db.list_all_stage_runs.return_value = []

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            list_all_runs(limit=20, offset=40)

        mock_db.list_all_stage_runs.assert_called_once_with(status=None, limit=20, offset=40)

    def test_list_all_runs_filters_by_status(self):
        """Test that list_all_runs can filter by status."""
        from goldfish.server_tools.execution_tools import list_all_runs

        mock_db = MagicMock()
        mock_db.list_all_stage_runs.return_value = []

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            list_all_runs(status="failed")

        mock_db.list_all_stage_runs.assert_called_once_with(status="failed", limit=50, offset=0)

    def test_list_all_runs_returns_empty_when_no_runs(self):
        """Test that list_all_runs returns empty list when no runs exist."""
        from goldfish.server_tools.execution_tools import list_all_runs

        mock_db = MagicMock()
        mock_db.list_all_stage_runs.return_value = []

        with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
            result = list_all_runs()

        assert result["runs"] == []
        assert result["total"] == 0
