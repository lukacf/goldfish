"""Unit tests for the consolidated inspect_run tool."""

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
                _get_metadata_bus=MagicMock(),
                _get_pipeline_executor=MagicMock(),
                _get_stage_executor=MagicMock(),
                _get_workspace_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        # Clear tool module from sys.modules to ensure fresh import with mocks
        sys.modules.pop("goldfish.server_tools.execution_tools", None)
        yield


def test_inspect_run_basic():
    """Test that inspect_run returns basic run information."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "100%",
        "reason_json": None,
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id)

    assert result["run_id"] == run_id
    assert result["status"] == "completed"
    assert result["dashboard"]["progress"] == "100%"


def test_inspect_run_triggers_sync_when_running():
    """Test that inspect_run triggers a sync signal for running runs."""
    from goldfish.infra.metadata.base import MetadataSignal
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abc1234"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "running",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "50%",
        "reason_json": None,
        "backend_type": "gce",
        "backend_handle": "instance-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    mock_bus = MagicMock()

    mock_stage_exec = MagicMock()
    mock_stage_exec.gce_launcher._find_instance_zone.return_value = "us-west1-b"

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_stage_exec),
    ):
        inspect_run(run_id)

    # Verify sync signal was set with correct target URI
    mock_bus.set_signal.assert_called_once()
    args, kwargs = mock_bus.set_signal.call_args
    assert args[0] == "goldfish"
    assert isinstance(args[1], MetadataSignal)
    assert args[1].command == "sync"
    assert args[1].payload["run_id"] == run_id
    assert kwargs["target"] == "zones/us-west1-b/instances/instance-1"


def test_inspect_run_skips_sync_when_launching():
    """GCE runs in launch/build should not report timeout sync."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abcd1234"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "running",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "launch",
        "reason_json": None,
        "backend_type": "gce",
        "backend_handle": "instance-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    mock_bus = MagicMock()

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
    ):
        result = inspect_run(run_id)

    assert result["dashboard"]["sync_status"] == "starting"
    mock_bus.set_signal.assert_not_called()
