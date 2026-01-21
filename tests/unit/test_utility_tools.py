"""Unit tests for utility tools including get_workspace_thoughts and dashboard."""

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
                _get_state_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        # Clear tool module from sys.modules to ensure fresh import with mocks
        sys.modules.pop("goldfish.server_tools.utility_tools", None)
        yield


class TestGetWorkspaceThoughts:
    """Tests for the get_workspace_thoughts tool."""

    def test_get_workspace_thoughts_returns_thoughts_for_workspace(self):
        """Test that get_workspace_thoughts returns thoughts associated with a workspace."""
        from goldfish.server_tools.utility_tools import get_workspace_thoughts

        mock_db = MagicMock()
        mock_db.get_workspace_thoughts.return_value = [
            {
                "id": 1,
                "timestamp": "2025-01-04T10:00:00Z",
                "operation": "thought",
                "workspace": "baseline",
                "reason": "Testing hypothesis about learning rate",
                "details": None,
            },
            {
                "id": 2,
                "timestamp": "2025-01-04T11:00:00Z",
                "operation": "thought",
                "workspace": "baseline",
                "reason": "Learning rate 0.001 seems to work better",
                "details": '{"run_id": "stage-abc123"}',
            },
        ]
        mock_db.count_workspace_thoughts.return_value = 2

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = None

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = get_workspace_thoughts("baseline")

        assert result["workspace"] == "baseline"
        assert len(result["thoughts"]) == 2
        assert result["thoughts"][0]["thought"] == "Testing hypothesis about learning rate"
        assert result["thoughts"][1]["run_id"] == "stage-abc123"
        mock_db.get_workspace_thoughts.assert_called_once_with("baseline", limit=50, offset=0)

    def test_get_workspace_thoughts_with_pagination(self):
        """Test that get_workspace_thoughts supports pagination."""
        from goldfish.server_tools.utility_tools import get_workspace_thoughts

        mock_db = MagicMock()
        mock_db.get_workspace_thoughts.return_value = []
        mock_db.count_workspace_thoughts.return_value = 0

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = None

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            get_workspace_thoughts("baseline", limit=10, offset=20)

        mock_db.get_workspace_thoughts.assert_called_once_with("baseline", limit=10, offset=20)

    def test_get_workspace_thoughts_resolves_slot(self):
        """Test that get_workspace_thoughts resolves slot names to workspace names."""
        from goldfish.server_tools.utility_tools import get_workspace_thoughts

        mock_db = MagicMock()
        mock_db.get_workspace_thoughts.return_value = []
        mock_db.count_workspace_thoughts.return_value = 0

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "actual_workspace"

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = get_workspace_thoughts("w1")

        assert result["workspace"] == "actual_workspace"
        mock_db.get_workspace_thoughts.assert_called_once_with("actual_workspace", limit=50, offset=0)

    def test_get_workspace_thoughts_empty_returns_empty_list(self):
        """Test that get_workspace_thoughts returns empty list when no thoughts exist."""
        from goldfish.server_tools.utility_tools import get_workspace_thoughts

        mock_db = MagicMock()
        mock_db.get_workspace_thoughts.return_value = []
        mock_db.count_workspace_thoughts.return_value = 0

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = None

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = get_workspace_thoughts("baseline")

        assert result["workspace"] == "baseline"
        assert result["thoughts"] == []
        assert result["total"] == 0


class TestDashboard:
    """Tests for the dashboard tool."""

    def test_dashboard_returns_failed_runs(self):
        """Test that dashboard shows recent failed runs in alerts section."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_db.get_recent_failed_runs.return_value = [
            {
                "id": "stage-fail1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "status": "failed",
                "error": "CUDA out of memory",
                "completed_at": "2025-01-04T10:00:00Z",
            },
        ]
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_ws_manager = MagicMock()
        mock_ws_manager.list_workspaces.return_value = []

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = dashboard()

        # New structure: alerts > failed_recent
        assert len(result["alerts"]["failed_recent"]) == 1
        assert result["alerts"]["failed_recent"][0]["run_id"] == "stage-fail1"
        assert result["alerts"]["failed_recent"][0]["error"] == "CUDA out of memory"

    def test_dashboard_returns_active_runs(self):
        """Test that dashboard shows currently active runs in active section."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = [
            {
                "id": "stage-running1",
                "workspace_name": "baseline",
                "stage_name": "train",
                "state": "running",  # State machine state (source of truth)
                "started_at": "2025-01-04T10:00:00Z",
            },
        ]
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_ws_manager = MagicMock()
        mock_ws_manager.list_workspaces.return_value = []

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = dashboard()

        # New structure: active > running
        assert len(result["active"]["running"]) == 1
        assert result["active"]["running"][0]["run_id"] == "stage-running1"
        assert result["active"]["running"][0]["workspace"] == "baseline"

    def test_dashboard_returns_workspace_summary(self):
        """Test that dashboard shows workspace summary with dirty status."""
        from datetime import UTC, datetime

        from goldfish.server_tools.utility_tools import dashboard
        from goldfish.workspace.manager import DirtyState, WorkspaceInfo

        mock_db = MagicMock()
        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_ws_manager = MagicMock()
        mock_ws_manager.list_workspaces.return_value = [
            WorkspaceInfo(
                name="baseline",
                created_at=datetime.now(UTC),
                goal="Test goal",
                snapshot_count=5,
                last_activity=datetime.now(UTC),
                is_mounted=True,
                mounted_slot="w1",
            ),
        ]
        mock_ws_manager.get_slot_info.return_value = MagicMock(dirty=DirtyState.DIRTY)

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = dashboard()

        # New structure: workspaces > mounted
        assert len(result["workspaces"]["mounted"]) == 1
        assert result["workspaces"]["mounted"][0]["name"] == "baseline"
        assert result["workspaces"]["mounted"][0]["dirty"] is True

    def test_dashboard_returns_source_count(self):
        """Test that dashboard shows data source count."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 42
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_ws_manager = MagicMock()
        mock_ws_manager.list_workspaces.return_value = []

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = dashboard()

        assert result["source_count"] == 42

    def test_dashboard_returns_recent_outcomes(self):
        """Test that dashboard shows recent run outcomes for quick trend view."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = [
            {
                "workspace_name": "baseline",
                "stage_name": "train",
                "outcome": "success",
                "completed_at": "2025-01-04T10:00:00Z",
            },
            {
                "workspace_name": "baseline",
                "stage_name": "train",
                "outcome": "bad_results",
                "completed_at": "2025-01-04T09:00:00Z",
            },
        ]
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_ws_manager = MagicMock()
        mock_ws_manager.list_workspaces.return_value = []

        with (
            patch("goldfish.server_tools.utility_tools._get_db", return_value=mock_db),
            patch("goldfish.server_tools.utility_tools._get_workspace_manager", return_value=mock_ws_manager),
        ):
            result = dashboard()

        assert len(result["recent_outcomes"]) == 2
        assert result["recent_outcomes"][0]["outcome"] == "success"
