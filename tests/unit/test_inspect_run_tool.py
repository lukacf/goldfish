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
        "state": "completed",  # State machine column (source of truth)
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id)

    assert result["run_id"] == run_id
    assert result["state"] == "completed"  # State machine state (source of truth)
    assert result["dashboard"]["state"] == "completed"


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
        "state": "running",  # State machine column (source of truth)
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
    # Mock run_backend.get_zone() which is now used instead of gce_launcher._find_instance_zone
    mock_stage_exec.run_backend.get_zone.return_value = "us-west1-b"

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_stage_exec),
        patch("goldfish.server_tools.execution_tools._overdrive_ack_timeout", return_value=0.0),
        patch("time.sleep"),  # Avoid actual sleep delays
    ):
        inspect_run(run_id)

    mock_stage_exec.refresh_status_once.assert_called_once_with(run_id)
    # Verify sync signal was set with correct target URI
    mock_bus.set_signal.assert_called_once()
    args, kwargs = mock_bus.set_signal.call_args
    assert args[0] == "goldfish"
    assert isinstance(args[1], MetadataSignal)
    assert args[1].command == "sync"
    assert args[1].payload["run_id"] == run_id
    assert kwargs["target"] == "zones/us-west1-b/instances/instance-1"


def test_inspect_run_pending_when_ack_missing():
    """GCE running runs should report pending when ack not received."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-acde1234"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "running",
        "state": "running",  # State machine column (source of truth)
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "running",
        "reason_json": None,
        "backend_type": "gce",
        "backend_handle": "instance-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    mock_bus = MagicMock()
    mock_bus.get_ack.return_value = None

    mock_stage_exec = MagicMock()
    # Mock run_backend.get_zone() which is now used instead of gce_launcher._find_instance_zone
    mock_stage_exec.run_backend.get_zone.return_value = "us-west1-b"

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_stage_exec),
        patch("goldfish.server_tools.execution_tools._overdrive_ack_timeout", return_value=0.0),
        patch("time.sleep"),  # Avoid actual sleep delays
    ):
        result = inspect_run(run_id)

    assert result["dashboard"]["sync_status"] == "pending"


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
        "state": "launching",  # State machine column (source of truth)
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "backend_type": "gce",
        "backend_handle": "instance-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    mock_bus = MagicMock()

    mock_stage_exec = MagicMock()

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_stage_exec),
    ):
        result = inspect_run(run_id)

    assert result["dashboard"]["sync_status"] == "starting"
    mock_stage_exec.refresh_status_once.assert_called_once_with(run_id)
    mock_bus.set_signal.assert_not_called()


def test_inspect_run_includes_thoughts():
    """Test that inspect_run correctly fetches and includes thoughts."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-a1b2c3d4"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",  # State machine column (source of truth)
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

    # Mock thoughts in audit trail
    mock_db.get_run_thoughts.return_value = [
        {"timestamp": "2025-12-27T10:05:00Z", "reason": "Initial reasoning about hyperparameters."},
        {"timestamp": "2025-12-27T10:30:00Z", "reason": "Adjusting learning rate due to slow convergence."},
    ]

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id)

    assert "thoughts" in result
    assert len(result["thoughts"]) == 2
    assert result["thoughts"][0]["thought"] == "Initial reasoning about hyperparameters."
    assert result["thoughts"][1]["thought"] == "Adjusting learning rate due to slow convergence."
    mock_db.get_run_thoughts.assert_called_once_with(run_id)


def test_inspect_run_includes_attempt_info():
    """Test that inspect_run includes attempt context when requested."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "baseline",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",  # State machine column (source of truth)
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "100%",
        "reason_json": None,
        "attempt_num": 3,
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_attempt_context.return_value = {
        "attempt": 3,
        "runs_in_attempt": 5,
        "completed": 4,
        "failed": 1,
        "success": 0,
        "bad_results": 0,
        "status": "open",
    }

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["metadata", "attempt"])

    assert "attempt_context" in result
    assert result["attempt_context"]["attempt"] == 3
    assert result["attempt_context"]["runs_in_attempt"] == 5
    assert result["attempt_context"]["status"] == "open"


def test_inspect_run_refetches_row_after_sync():
    """Regression: inspect_run must re-fetch row after sync to get updated timestamps.

    Bug: inspect_run fetched the row before sync, then used it to build the response.
    This caused last_sync to show stale values even when sync succeeded.
    Fix: Re-fetch the row after sync_metrics_if_running() updates the database.
    """
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abcdef12"

    # Initial row with old timestamp
    initial_row = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "running",
        "state": "running",  # State machine column (source of truth)
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "progress": "running",
        "reason_json": None,
        "backend_type": "gce",
        "backend_handle": "instance-1",
        "last_metrics_sync_at": None,  # No sync yet
    }

    # Updated row after sync (what should be returned after re-fetch)
    updated_row = {
        **initial_row,
        "last_metrics_sync_at": "2025-12-27T10:30:00Z",  # Updated by sync
    }

    mock_db = MagicMock()
    # First call returns initial row, second call (after sync) returns updated row
    mock_db.get_stage_run.side_effect = [initial_row, initial_row, updated_row]
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    mock_bus = MagicMock()
    # get_ack will return our known req_id to simulate successful ACK
    mock_bus.get_ack.return_value = "abc12345"

    mock_stage_exec = MagicMock()
    # Mock run_backend.get_zone() which is now used instead of gce_launcher._find_instance_zone
    mock_stage_exec.run_backend.get_zone.return_value = "us-west1-b"

    # Mock UUID to return a known value so ack comparison works
    # uuid.uuid4() returns UUID, str(uuid) is like "abc12345-...", [:8] = "abc12345"
    mock_uuid_obj = MagicMock()
    mock_uuid_obj.__str__ = MagicMock(return_value="abc12345-1234-1234-1234-123456789abc")

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_metadata_bus", return_value=mock_bus),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_stage_exec),
        patch("goldfish.server_tools.execution_tools._overdrive_ack_timeout", return_value=0.5),
        patch("uuid.uuid4", return_value=mock_uuid_obj),
        patch("time.sleep"),  # Avoid actual sleep delays
    ):
        result = inspect_run(run_id)

    # Verify sync was called
    mock_stage_exec.sync_metrics_if_running.assert_called_once_with(run_id)

    # Verify row was re-fetched (3 calls: initial, refresh_status_once refresh, after sync)
    assert mock_db.get_stage_run.call_count == 3

    # The critical assertions: sync should be successful
    assert result["dashboard"]["sync_status"] == "synced"
    # sync_method is "none" because no metrics data in mock
    assert result["dashboard"]["sync_method"] == "none"
    assert result["dashboard"]["latest_metric_at"] is None  # No metrics data


def test_inspect_run_includes_svs_ml_outcome():
    """Test that inspect_run includes ml_outcome from SVS findings."""
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-aabbcc123"
    svs_findings = {
        "ai_review": {
            "decision": "approved",
            "findings": [],
            "duration_ms": 1500,
            "response_text": "Model achieved target accuracy.\n\nML_OUTCOME: val_accuracy=0.91, outcome=success",
            "ml_outcome": "success",
            "ml_metric_value": 0.91,
        }
    }
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "svs_findings_json": json.dumps(svs_findings),
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["svs"])

    assert "svs" in result
    assert result["svs"]["post_run"]["decision"] == "approved"
    assert result["svs"]["post_run"]["ml_outcome"] == "success"
    assert result["svs"]["post_run"]["ml_metric_value"] == 0.91
    assert "ML_OUTCOME:" in result["svs"]["post_run"]["full_text"]


def test_inspect_run_handles_missing_ml_outcome():
    """Test that inspect_run handles missing ml_outcome fields gracefully."""
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-ddeeff456"
    # SVS findings without ml_outcome fields (legacy or when not applicable)
    svs_findings = {
        "ai_review": {
            "decision": "approved",
            "findings": [],
            "duration_ms": 1000,
            "response_text": "Run completed without issues.",
        }
    }
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "svs_findings_json": json.dumps(svs_findings),
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["svs"])

    assert "svs" in result
    assert result["svs"]["post_run"]["decision"] == "approved"
    # Missing fields should be None
    assert result["svs"]["post_run"]["ml_outcome"] is None
    assert result["svs"]["post_run"]["ml_metric_value"] is None


def test_inspect_run_includes_svs_partial_outcome():
    """Test that inspect_run correctly handles partial ML outcome."""
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-aabbcc789"
    svs_findings = {
        "ai_review": {
            "decision": "warned",
            "findings": ["WARNING: Did not achieve goal value"],
            "duration_ms": 2000,
            "response_text": "Model achieved minimum but not goal.\n\nML_OUTCOME: accuracy=0.72, outcome=partial",
            "ml_outcome": "partial",
            "ml_metric_value": 0.72,
        }
    }
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "svs_findings_json": json.dumps(svs_findings),
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["svs"])

    assert result["svs"]["post_run"]["ml_outcome"] == "partial"
    assert result["svs"]["post_run"]["ml_metric_value"] == 0.72


def test_inspect_run_includes_reason():
    """Test that inspect_run includes reason from reason_json."""
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abc123def"
    reason_data = {"description": "Testing new learning rate schedule"}
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": json.dumps(reason_data),
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["metadata"])

    assert "reason" in result
    assert result["reason"] == "Testing new learning rate schedule"


def test_inspect_run_reason_null_when_missing():
    """Test that inspect_run returns null reason when reason_json is missing."""
    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-def456abc"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["metadata"])

    assert "reason" in result
    assert result["reason"] is None


def test_inspect_run_only_shows_latest_during_run_review():
    """Regression: inspect_run should only show the LATEST during_run SVS review.

    Bug: All unnotified during_run reviews were shown, flooding the output with
    redundant periodic monitoring updates.
    Fix: Only show the most recent during_run review (first one, since ordered DESC).
    Pre-run and post-run reviews should still all be shown.
    """
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-abc123def"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "running",
        "state": "running",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": None,
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "backend_type": "local",
        "backend_handle": "container-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    # Multiple during_run reviews (all for same run, ordered by reviewed_at DESC)
    mock_db.get_unnotified_svs_reviews.return_value = [
        {
            "id": 3,
            "stage_run_id": run_id,
            "review_type": "during_run",
            "decision": "warned",
            "parsed_findings": json.dumps([{"severity": "WARN", "summary": "Latest warning"}]),
            "response_text": "Latest during_run review",
            "reviewed_at": "2025-12-27T10:30:00Z",
        },
        {
            "id": 2,
            "stage_run_id": run_id,
            "review_type": "during_run",
            "decision": "warned",
            "parsed_findings": json.dumps([{"severity": "WARN", "summary": "Older warning"}]),
            "response_text": "Older during_run review",
            "reviewed_at": "2025-12-27T10:20:00Z",
        },
        {
            "id": 1,
            "stage_run_id": run_id,
            "review_type": "during_run",
            "decision": "warned",
            "parsed_findings": json.dumps([{"severity": "WARN", "summary": "Oldest warning"}]),
            "response_text": "Oldest during_run review",
            "reviewed_at": "2025-12-27T10:10:00Z",
        },
    ]

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["dashboard"])

    # Should only have ONE during_run review (the latest)
    svs_reviews = result["dashboard"]["new_svs_reviews"]
    assert len(svs_reviews) == 1, f"Expected 1 review, got {len(svs_reviews)}"
    assert svs_reviews[0]["full_text"] == "Latest during_run review"
    assert svs_reviews[0]["findings"][0]["summary"] == "Latest warning"

    # All 3 reviews should be marked as notified (even the ones not shown)
    mock_db.mark_svs_reviews_notified.assert_called_once()
    notified_ids = mock_db.mark_svs_reviews_notified.call_args[0][0]
    assert sorted(notified_ids) == [1, 2, 3], f"Expected all IDs to be notified, got {notified_ids}"


def test_inspect_run_keeps_all_pre_run_and_post_run_reviews():
    """Test that pre_run and post_run reviews are all shown (not filtered like during_run)."""
    import json

    from goldfish.server_tools.execution_tools import inspect_run

    run_id = "stage-def789abc"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = {
        "id": run_id,
        "workspace_name": "w1",
        "stage_name": "train",
        "status": "completed",
        "state": "completed",
        "started_at": "2025-12-27T10:00:00Z",
        "completed_at": "2025-12-27T11:00:00Z",
        "config_json": "{}",
        "inputs_json": "{}",
        "outputs_json": "[]",
        "reason_json": None,
        "backend_type": "local",
        "backend_handle": "container-1",
    }
    mock_db.get_metrics_trends.return_value = {}
    mock_db.get_metrics_summary.return_value = []

    # Mix of review types
    mock_db.get_unnotified_svs_reviews.return_value = [
        {
            "id": 4,
            "stage_run_id": run_id,
            "review_type": "post_run",
            "decision": "approved",
            "parsed_findings": json.dumps([]),
            "response_text": "Post-run review",
            "reviewed_at": "2025-12-27T11:00:00Z",
        },
        {
            "id": 3,
            "stage_run_id": run_id,
            "review_type": "during_run",
            "decision": "warned",
            "parsed_findings": json.dumps([{"severity": "WARN", "summary": "Latest during"}]),
            "response_text": "Latest during_run",
            "reviewed_at": "2025-12-27T10:30:00Z",
        },
        {
            "id": 2,
            "stage_run_id": run_id,
            "review_type": "during_run",
            "decision": "warned",
            "parsed_findings": json.dumps([{"severity": "WARN", "summary": "Older during"}]),
            "response_text": "Older during_run",
            "reviewed_at": "2025-12-27T10:20:00Z",
        },
        {
            "id": 1,
            "stage_run_id": run_id,
            "review_type": "pre_run",
            "decision": "approved",
            "parsed_findings": json.dumps([]),
            "response_text": "Pre-run review",
            "reviewed_at": "2025-12-27T10:00:00Z",
        },
    ]

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = inspect_run(run_id, include=["dashboard"])

    # Should have: 1 pre_run + 1 during_run (latest only) + 1 post_run = 3 reviews
    svs_reviews = result["dashboard"]["new_svs_reviews"]
    assert len(svs_reviews) == 3, f"Expected 3 reviews, got {len(svs_reviews)}"

    review_types = [r["review_type"] for r in svs_reviews]
    assert review_types.count("during_run") == 1, "Should only have 1 during_run review"
    assert review_types.count("pre_run") == 1, "Should have 1 pre_run review"
    assert review_types.count("post_run") == 1, "Should have 1 post_run review"

    # All 4 reviews should be marked as notified
    mock_db.mark_svs_reviews_notified.assert_called_once()
    notified_ids = mock_db.mark_svs_reviews_notified.call_args[0][0]
    assert sorted(notified_ids) == [1, 2, 3, 4]
