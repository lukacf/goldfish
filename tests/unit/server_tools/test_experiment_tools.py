"""Unit tests for experiment tools MCP endpoints."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from goldfish.server_tools.experiment_tools import (
    _format_age,
    _get_lineage_impl,
)


class TestFormatAge:
    """Tests for the _format_age helper function."""

    def test_format_age_minutes(self):
        """Test age formatting for recent timestamps (minutes)."""
        now = datetime.now(UTC)
        timestamp = (now - timedelta(minutes=5)).isoformat()
        assert _format_age(timestamp) == "5m ago"

    def test_format_age_hours(self):
        """Test age formatting for hours."""
        now = datetime.now(UTC)
        timestamp = (now - timedelta(hours=3)).isoformat()
        assert _format_age(timestamp) == "3h ago"

    def test_format_age_days(self):
        """Test age formatting for days."""
        now = datetime.now(UTC)
        timestamp = (now - timedelta(days=2)).isoformat()
        assert _format_age(timestamp) == "2d ago"

    def test_format_age_zero_minutes(self):
        """Test age formatting for very recent timestamps."""
        now = datetime.now(UTC)
        timestamp = now.isoformat()
        assert _format_age(timestamp) == "0m ago"

    def test_format_age_handles_z_suffix(self):
        """Test age formatting handles Z suffix for UTC."""
        now = datetime.now(UTC)
        timestamp = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        assert _format_age(timestamp) == "1h ago"


class TestGetLineage:
    """Tests for the _get_lineage_impl MCP tool."""

    @patch("goldfish.server_tools.experiment_tools._get_db")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test__get_lineage_impl_downstream_returns_consumers(self, mock_get_ws_manager, mock_get_db):
        """Test that _get_lineage_impl returns downstream consumers."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        now = datetime.now(UTC)

        # Mock the stage run
        mock_db.get_stage_run.return_value = {
            "id": "stage-abc123",
            "workspace_name": "test-ws",
            "stage_name": "preprocess",
        }

        # Mock downstream signals (runs that consumed this run's outputs)
        mock_db.list_signals.return_value = [
            {
                "stage_run_id": "stage-consumer1",
                "signal_name": "features",
                "signal_type": "input",
                "source_stage_run_id": "stage-abc123",
            },
            {
                "stage_run_id": "stage-consumer2",
                "signal_name": "features",
                "signal_type": "input",
                "source_stage_run_id": "stage-abc123",
            },
        ]

        # Mock consumer run details
        mock_db.get_stage_run.side_effect = [
            # First call for validating run_id
            {"id": "stage-abc123", "workspace_name": "test-ws", "stage_name": "preprocess"},
            # Calls for consumer runs
            {
                "id": "stage-consumer1",
                "workspace_name": "test-ws",
                "stage_name": "train",
                "reason_json": '{"description": "Testing new features"}',
                "state": "completed",
                "started_at": (now - timedelta(hours=2)).isoformat(),
            },
            {
                "id": "stage-consumer2",
                "workspace_name": "test-ws",
                "stage_name": "evaluate",
                "reason_json": '{"description": "Evaluating model"}',
                "state": "completed",
                "started_at": (now - timedelta(hours=1)).isoformat(),
            },
        ]

        result = _get_lineage_impl("stage-abc123", direction="downstream")

        assert result["run_id"] == "stage-abc123"
        assert result["direction"] == "downstream"
        assert len(result["consumers"]) == 2
        assert result["consumers"][0]["run_id"] == "stage-consumer1"
        assert result["consumers"][0]["stage"] == "train"
        assert result["consumers"][0]["reason"] == "Testing new features"
        assert "ago" in result["consumers"][0]["age"]

    @patch("goldfish.server_tools.experiment_tools._get_db")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test__get_lineage_impl_upstream_returns_producers(self, mock_get_ws_manager, mock_get_db):
        """Test that _get_lineage_impl returns upstream producers."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        now = datetime.now(UTC)

        # Mock the stage run
        mock_db.get_stage_run.return_value = {
            "id": "stage-consumer",
            "workspace_name": "test-ws",
            "stage_name": "train",
        }

        # Mock input signals (signals this run consumed)
        mock_db.list_signals.return_value = [
            {
                "stage_run_id": "stage-consumer",
                "signal_name": "features",
                "signal_type": "input",
                "source_stage_run_id": "stage-producer1",
            },
        ]

        # Mock producer run details
        def get_stage_run_side_effect(run_id):
            if run_id == "stage-consumer":
                return {
                    "id": "stage-consumer",
                    "workspace_name": "test-ws",
                    "stage_name": "train",
                }
            elif run_id == "stage-producer1":
                return {
                    "id": "stage-producer1",
                    "workspace_name": "test-ws",
                    "stage_name": "preprocess",
                    "reason_json": '{"description": "Initial preprocessing"}',
                    "state": "completed",
                    "started_at": (now - timedelta(hours=3)).isoformat(),
                }
            return None

        mock_db.get_stage_run.side_effect = get_stage_run_side_effect

        result = _get_lineage_impl("stage-consumer", direction="upstream")

        assert result["run_id"] == "stage-consumer"
        assert result["direction"] == "upstream"
        assert len(result["producers"]) == 1
        assert result["producers"][0]["run_id"] == "stage-producer1"
        assert result["producers"][0]["stage"] == "preprocess"
        assert result["producers"][0]["reason"] == "Initial preprocessing"

    @patch("goldfish.server_tools.experiment_tools._get_db")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test__get_lineage_impl_with_no_connections_returns_empty(self, mock_get_ws_manager, mock_get_db):
        """Test that _get_lineage_impl returns empty list when no connections exist."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        mock_db.get_stage_run.return_value = {
            "id": "stage-isolated",
            "workspace_name": "test-ws",
            "stage_name": "standalone",
        }
        mock_db.list_signals.return_value = []

        result = _get_lineage_impl("stage-isolated", direction="downstream")

        assert result["run_id"] == "stage-isolated"
        assert result["direction"] == "downstream"
        assert result["consumers"] == []

    @patch("goldfish.server_tools.experiment_tools._get_db")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test__get_lineage_impl_raises_on_unknown_run(self, mock_get_ws_manager, mock_get_db):
        """Test that _get_lineage_impl raises error for unknown run ID."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        mock_db.get_stage_run.return_value = None

        with pytest.raises(Exception) as exc_info:
            _get_lineage_impl("stage-nonexistent", direction="downstream")

        assert "not found" in str(exc_info.value).lower()

    @patch("goldfish.server_tools.experiment_tools._get_db")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test__get_lineage_impl_downstream_includes_signal_consumed(self, mock_get_ws_manager, mock_get_db):
        """Test that downstream includes which signal was consumed."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        now = datetime.now(UTC)

        mock_db.get_stage_run.return_value = {
            "id": "stage-abc123",
            "workspace_name": "test-ws",
            "stage_name": "preprocess",
        }

        mock_db.list_signals.return_value = [
            {
                "stage_run_id": "stage-consumer",
                "signal_name": "tokens",  # Input name in consumer
                "signal_type": "input",
                "source_stage_run_id": "stage-abc123",
            },
        ]

        mock_db.get_stage_run.side_effect = [
            {"id": "stage-abc123", "workspace_name": "test-ws", "stage_name": "preprocess"},
            {
                "id": "stage-consumer",
                "workspace_name": "test-ws",
                "stage_name": "train",
                "reason_json": None,
                "state": "completed",
                "started_at": now.isoformat(),
            },
        ]

        result = _get_lineage_impl("stage-abc123", direction="downstream")

        assert len(result["consumers"]) == 1
        assert result["consumers"][0]["signal_consumed"] == "tokens"


class TestListHistoryImprovements:
    """Tests for list_history improvements."""

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_default_limit_is_20(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that list_history default limit is now 20."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {"records": [], "total": 0, "has_more": False}
        mock_get_experiment_manager.return_value = mock_exp_manager

        # Call with no explicit limit - should use default of 20
        list_history.fn("test-ws")

        # Verify the manager was called with limit=20
        mock_exp_manager.list_history.assert_called_once()
        call_kwargs = mock_exp_manager.list_history.call_args[1]
        assert call_kwargs["limit"] == 20

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_excludes_workspace_from_records(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that workspace is removed from individual records (caller knows it)."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {
                    "record_id": "01ABC",
                    "workspace_name": "test-ws",  # Should be removed
                    "version": "v1",
                    "type": "run",
                    "tags": [],
                }
            ],
            "total": 1,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # workspace_name should be removed from individual records
        assert "workspace_name" not in result["records"][0]
        # workspace should be at top level instead
        assert result["workspace"] == "test-ws"

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_excludes_empty_tags(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that empty tags arrays are excluded from records."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {"record_id": "01ABC", "version": "v1", "type": "run", "tags": []},
                {"record_id": "01DEF", "version": "v2", "type": "run", "tags": ["best"]},
            ],
            "total": 2,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # First record should not have tags key (was empty)
        assert "tags" not in result["records"][0]
        # Second record should have tags (non-empty)
        assert result["records"][1]["tags"] == ["best"]

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_excludes_null_experiment_group(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that null experiment_group is excluded from records."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {"record_id": "01ABC", "version": "v1", "type": "run", "experiment_group": None},
                {"record_id": "01DEF", "version": "v2", "type": "run", "experiment_group": "group-a"},
            ],
            "total": 2,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # First record should not have experiment_group key (was None)
        assert "experiment_group" not in result["records"][0]
        # Second record should have experiment_group
        assert result["records"][1]["experiment_group"] == "group-a"

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_finalized_only_parameter(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that finalized_only parameter is passed to manager."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {"records": [], "total": 0, "has_more": False}
        mock_get_experiment_manager.return_value = mock_exp_manager

        # Call with finalized_only=True
        list_history.fn("test-ws", finalized_only=True)

        # Verify the manager was called with finalized_only=True
        mock_exp_manager.list_history.assert_called_once()
        call_kwargs = mock_exp_manager.list_history.call_args[1]
        assert call_kwargs["finalized_only"] is True

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_includes_reason(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that list_history includes reason field from stage_runs."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {
                    "record_id": "01ABC",
                    "version": "v1",
                    "type": "run",
                    "reason": "Testing higher learning rate",
                }
            ],
            "total": 1,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # Should include reason field
        assert "reason" in result["records"][0]
        assert result["records"][0]["reason"] == "Testing higher learning rate"

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_includes_primary_metric(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that list_history includes primary_metric field."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {
                    "record_id": "01ABC",
                    "version": "v1",
                    "type": "run",
                    "primary_metric": {"name": "dir_acc", "value": 0.65},
                }
            ],
            "total": 1,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # Should include primary_metric field
        assert "primary_metric" in result["records"][0]
        assert result["records"][0]["primary_metric"]["name"] == "dir_acc"
        assert result["records"][0]["primary_metric"]["value"] == 0.65

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_includes_age(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that list_history includes age field instead of created_at."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {
                    "record_id": "01ABC",
                    "version": "v1",
                    "type": "run",
                    "age": "2h ago",
                }
            ],
            "total": 1,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # Should include age field (relative time)
        assert "age" in result["records"][0]
        assert "ago" in result["records"][0]["age"]

    @patch("goldfish.server_tools.experiment_tools._get_experiment_manager")
    @patch("goldfish.server_tools.experiment_tools._get_workspace_manager")
    def test_list_history_excludes_type_from_records(self, mock_get_ws_manager, mock_get_experiment_manager):
        """Test that list_history excludes redundant type field from run records."""
        from goldfish.server_tools.experiment_tools import list_history

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_workspace_for_slot.return_value = "test-ws"
        mock_get_ws_manager.return_value = mock_ws_manager

        mock_exp_manager = MagicMock()
        mock_exp_manager.list_history.return_value = {
            "records": [
                {
                    "record_id": "01ABC",
                    "version": "v1",
                    "type": "run",
                }
            ],
            "total": 1,
            "has_more": False,
        }
        mock_get_experiment_manager.return_value = mock_exp_manager

        result = list_history.fn("test-ws")

        # Type should be excluded (redundant - almost all records are "run")
        assert "type" not in result["records"][0]
