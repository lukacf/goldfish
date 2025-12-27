"""Integration tests for consolidated execution tools."""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.infra.metadata.local import LocalMetadataBus
from goldfish.models import StageRunStatus
from goldfish.server_tools.execution_tools import inspect_run


@pytest.fixture
def mock_db():
    db = MagicMock()
    # Mock a completed run
    db.get_stage_run.return_value = {
        "id": "stage-123",
        "workspace_name": "w1",
        "stage_name": "train",
        "status": StageRunStatus.COMPLETED,
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": '{"lr": 0.01}',
        "inputs_json": '{"data": "gs://bucket/data"}',
        "outputs_json": '[{"name": "model", "path": "model.pt"}]',
        "progress": "Epoch 10/10",
        "reason_json": '{"description": "Test run"}',
        "backend_type": "local",
        "backend_handle": "cont-123",
    }
    db.get_metrics_summary.return_value = [
        {"name": "loss", "last_value": 0.5, "min_value": 0.4, "max_value": 2.0, "count": 100},
        {"name": "accuracy", "last_value": 0.95, "min_value": 0.1, "max_value": 0.95, "count": 100},
    ]
    db.get_metrics_trends.return_value = {
        "loss": [0.6, 0.5],  # prev=0.6, last=0.5 -> downward (good)
        "accuracy": [0.94, 0.95],  # prev=0.94, last=0.95 -> upward (good)
    }
    return db


@pytest.fixture
def mock_metadata_bus(tmp_path):
    bus = LocalMetadataBus(tmp_path / "metadata.json")
    return bus


@patch("goldfish.server_tools.execution_tools._get_db")
@patch("goldfish.server_tools.execution_tools._get_metadata_bus")
@patch("goldfish.server_tools.execution_tools._get_config")
def test_inspect_run_synthesis(mock_get_config, mock_get_bus, mock_get_db, mock_db, mock_metadata_bus):
    """Test that inspect_run correctly synthesizes dashboard and trends."""
    mock_get_db.return_value = mock_db
    mock_get_bus.return_value = mock_metadata_bus
    mock_get_config.return_value = MagicMock()

    result = inspect_run.fn("stage-123")

    assert result["run_id"] == "stage-123"
    assert "dashboard" in result
    dash = result["dashboard"]
    assert dash["progress"] == "Epoch 10/10"

    # Check trends
    metrics = dash["metrics"]
    assert metrics["loss"]["value"] == 0.5
    assert metrics["loss"]["trend"] == "downward"
    assert metrics["accuracy"]["trend"] == "upward"

    # Check that it triggered a sync signal (even if it's already completed,
    # inspect_run should trigger it for running runs)
    # Let's test a RUNNING run specifically for sync
    mock_db.get_stage_run.return_value["status"] = StageRunStatus.RUNNING

    inspect_run.fn("stage-123")

    sig = mock_metadata_bus.get_signal("goldfish")
    assert sig is not None
    assert sig.command == "sync"
    assert sig.payload["run_id"] == "stage-123"
