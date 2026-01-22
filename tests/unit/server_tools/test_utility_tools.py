"""Unit tests for utility tools MCP endpoints."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch


class TestDashboardImprovements:
    """Tests for dashboard restructuring."""

    @patch("goldfish.experiment_model.records.ExperimentRecordManager")
    @patch("goldfish.server_tools.utility_tools._get_db")
    @patch("goldfish.server_tools.utility_tools._get_workspace_manager")
    def test_dashboard_groups_pending_by_workspace(self, mock_get_ws_manager, mock_get_db, mock_exp_cls):
        """Test that pending finalization is grouped by workspace with counts."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        # Mock workspace manager
        mock_ws_manager = MagicMock()
        mock_get_ws_manager.return_value = mock_ws_manager

        ws_info = MagicMock()
        ws_info.name = "test-ws"
        ws_info.is_mounted = True
        ws_info.mounted_slot = "w1"
        ws_info.goal = "Test goal"
        mock_ws_manager.list_workspaces.return_value = [ws_info]

        slot_info = MagicMock()
        slot_info.dirty = MagicMock()
        slot_info.dirty.value = "clean"
        mock_ws_manager.get_slot_info.return_value = slot_info

        # Mock database methods
        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        # Mock pending finalizations via the ExperimentRecordManager
        mock_exp_manager = MagicMock()
        mock_exp_cls.return_value = mock_exp_manager
        mock_exp_manager.list_unfinalized_runs.return_value = [
            {
                "record_id": "01ABC",
                "stage_run_id": "stage-1",
                "stage_name": "train",
                "infra_outcome": "completed",
                "reason": "Testing new features",
                "primary_metric": {"name": "dir_acc", "value": 0.65},
            },
            {
                "record_id": "01DEF",
                "stage_run_id": "stage-2",
                "stage_name": "train",
                "infra_outcome": "completed",
                "reason": "LR test",
                "primary_metric": {"name": "dir_acc", "value": 0.60},
            },
        ]

        result = dashboard.fn()

        # Check that pending_finalization is in blocks section
        assert "blocks" in result
        assert "pending_finalization" in result["blocks"]
        assert "by_workspace" in result["blocks"]["pending_finalization"]
        # Should be grouped by workspace
        assert "test-ws" in result["blocks"]["pending_finalization"]["by_workspace"]
        ws_pending = result["blocks"]["pending_finalization"]["by_workspace"]["test-ws"]
        assert ws_pending["count"] == 2
        # Should have an example showing reason -> metric
        assert "example" in ws_pending

    @patch("goldfish.experiment_model.records.ExperimentRecordManager")
    @patch("goldfish.server_tools.utility_tools._get_db")
    @patch("goldfish.server_tools.utility_tools._get_workspace_manager")
    def test_dashboard_shows_only_mounted_workspaces(self, mock_get_ws_manager, mock_get_db, mock_exp_cls):
        """Test that workspaces section only shows mounted workspaces."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        mock_ws_manager = MagicMock()
        mock_get_ws_manager.return_value = mock_ws_manager

        # Create mounted and unmounted workspaces
        mounted_ws = MagicMock()
        mounted_ws.name = "mounted-ws"
        mounted_ws.is_mounted = True
        mounted_ws.mounted_slot = "w1"
        mounted_ws.goal = "Mounted workspace goal"

        unmounted_ws = MagicMock()
        unmounted_ws.name = "unmounted-ws"
        unmounted_ws.is_mounted = False
        unmounted_ws.mounted_slot = None
        unmounted_ws.goal = "Unmounted workspace goal"

        mock_ws_manager.list_workspaces.return_value = [mounted_ws, unmounted_ws]

        slot_info = MagicMock()
        slot_info.dirty = MagicMock()
        slot_info.dirty.value = "clean"
        mock_ws_manager.get_slot_info.return_value = slot_info

        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_exp_cls.return_value.list_unfinalized_runs.return_value = []

        result = dashboard.fn()

        # Check workspaces structure
        assert "workspaces" in result
        assert "mounted" in result["workspaces"]
        assert "unmounted_count" in result["workspaces"]

        # Only mounted workspace should be in the mounted list
        assert len(result["workspaces"]["mounted"]) == 1
        assert result["workspaces"]["mounted"][0]["name"] == "mounted-ws"
        assert result["workspaces"]["unmounted_count"] == 1

    @patch("goldfish.experiment_model.records.ExperimentRecordManager")
    @patch("goldfish.server_tools.utility_tools._get_db")
    @patch("goldfish.server_tools.utility_tools._get_workspace_manager")
    def test_dashboard_truncates_goals(self, mock_get_ws_manager, mock_get_db, mock_exp_cls):
        """Test that workspace goals are truncated to 80 chars."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        mock_ws_manager = MagicMock()
        mock_get_ws_manager.return_value = mock_ws_manager

        # Create workspace with very long goal
        long_goal = "A" * 150  # 150 chars
        ws_info = MagicMock()
        ws_info.name = "test-ws"
        ws_info.is_mounted = True
        ws_info.mounted_slot = "w1"
        ws_info.goal = long_goal

        mock_ws_manager.list_workspaces.return_value = [ws_info]

        slot_info = MagicMock()
        slot_info.dirty = MagicMock()
        slot_info.dirty.value = "clean"
        mock_ws_manager.get_slot_info.return_value = slot_info

        mock_db.get_recent_failed_runs.return_value = []
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_exp_cls.return_value.list_unfinalized_runs.return_value = []

        result = dashboard.fn()

        # Goal should be truncated
        goal = result["workspaces"]["mounted"][0]["goal"]
        assert len(goal) <= 83  # 80 + "..." = 83
        assert goal.endswith("...")

    @patch("goldfish.experiment_model.records.ExperimentRecordManager")
    @patch("goldfish.server_tools.utility_tools._get_db")
    @patch("goldfish.server_tools.utility_tools._get_workspace_manager")
    def test_dashboard_uses_age_instead_of_timestamps(self, mock_get_ws_manager, mock_get_db, mock_exp_cls):
        """Test that dashboard uses relative age instead of timestamps."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        mock_ws_manager = MagicMock()
        mock_get_ws_manager.return_value = mock_ws_manager
        mock_ws_manager.list_workspaces.return_value = []

        # Mock failed run with timestamp
        now = datetime.now(UTC)
        failed_time = (now - timedelta(hours=2)).isoformat()
        mock_db.get_recent_failed_runs.return_value = [
            {
                "id": "stage-abc123",
                "workspace_name": "test-ws",
                "stage_name": "train",
                "error": "OOM error",
                "completed_at": failed_time,
                "reason_json": '{"description": "Testing failure"}',
            }
        ]
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_exp_cls.return_value.list_unfinalized_runs.return_value = []

        result = dashboard.fn()

        # Check that failed_recent uses "age" instead of "completed_at"
        assert "alerts" in result
        assert "failed_recent" in result["alerts"]
        if result["alerts"]["failed_recent"]:
            failed_run = result["alerts"]["failed_recent"][0]
            assert "age" in failed_run
            assert "ago" in failed_run["age"]
            assert "completed_at" not in failed_run

    @patch("goldfish.experiment_model.records.ExperimentRecordManager")
    @patch("goldfish.server_tools.utility_tools._get_db")
    @patch("goldfish.server_tools.utility_tools._get_workspace_manager")
    def test_dashboard_includes_reason_in_failed_runs(self, mock_get_ws_manager, mock_get_db, mock_exp_cls):
        """Test that failed runs include the reason."""
        from goldfish.server_tools.utility_tools import dashboard

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        mock_ws_manager = MagicMock()
        mock_get_ws_manager.return_value = mock_ws_manager
        mock_ws_manager.list_workspaces.return_value = []

        mock_db.get_recent_failed_runs.return_value = [
            {
                "id": "stage-abc123",
                "workspace_name": "test-ws",
                "stage_name": "train",
                "error": "OOM error",
                "completed_at": datetime.now(UTC).isoformat(),
                "reason_json": '{"description": "Testing higher learning rate"}',
            }
        ]
        mock_db.get_active_runs.return_value = []
        mock_db.count_sources.return_value = 5
        mock_db.get_recent_outcomes.return_value = []
        mock_db.get_unnotified_svs_reviews.return_value = []

        mock_exp_cls.return_value.list_unfinalized_runs.return_value = []

        result = dashboard.fn()

        # Check that reason is included
        failed_run = result["alerts"]["failed_recent"][0]
        assert "reason" in failed_run
        assert failed_run["reason"] == "Testing higher learning rate"
