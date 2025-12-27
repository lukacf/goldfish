"""Unit tests for get_run_metrics MCP tool semantics."""

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
                _get_pipeline_executor=MagicMock(),
                _get_stage_executor=MagicMock(),
                _get_workspace_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("goldfish.server_tools"):
                sys.modules.pop(mod_name, None)
        yield


def _mock_stage_row(run_id: str) -> dict:
    return {
        "id": run_id,
        "workspace_name": "ws",
        "stage_name": "train",
        "status": "completed",
        "backend_type": "local",
        "backend_handle": run_id,
    }


def test_get_run_metrics_limit_none_returns_all():
    """limit=None should return all metrics (no implicit truncation)."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 2
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = [
        {"name": "loss", "value": 0.5, "step": 1, "timestamp": "2024-01-01T00:00:00+00:00"},
        {"name": "loss", "value": 0.4, "step": 2, "timestamp": "2024-01-01T00:00:01+00:00"},
    ]
    mock_db.get_metrics_summary.return_value = [
        {
            "stage_run_id": run_id,
            "name": "loss",
            "min_value": 0.4,
            "max_value": 0.5,
            "last_value": 0.4,
            "count": 2,
        }
    ]
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = get_run_metrics(run_id, limit=None)

    assert result["total_metrics"] == 2
    assert len(result["metrics"]) == 2
    assert isinstance(result["metrics"][0]["timestamp"], str)
    mock_db.get_run_metrics.assert_called_once_with(run_id, metric_name=None, metric_prefix=None, limit=None, offset=0)


def test_get_run_metrics_default_limit_applied():
    """Default limit should prevent unbounded fetches."""
    from goldfish.server_tools.execution_tools import DEFAULT_METRICS_LIMIT, get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 100
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = get_run_metrics(run_id)

    assert result["total_metrics"] == 100
    mock_db.get_run_metrics.assert_called_once_with(
        run_id, metric_name=None, metric_prefix=None, limit=DEFAULT_METRICS_LIMIT, offset=0
    )


def test_get_run_metrics_offset_without_limit():
    """offset should be honored even when limit is None."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 0
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = get_run_metrics(run_id, limit=None, offset=5)

    assert result["total_metrics"] == 0
    mock_db.get_run_metrics.assert_called_once_with(run_id, metric_name=None, metric_prefix=None, limit=None, offset=5)


def test_get_run_metrics_warns_when_unbounded_large_result():
    """Warn when limit=None and total exceeds warning threshold."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 20000
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = get_run_metrics(run_id, limit=None)

    assert "warnings" in result


def test_get_run_metrics_syncs_live_for_running_run():
    """Running runs should trigger a live sync before returning metrics."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-acde123"
    mock_db = MagicMock()
    row = _mock_stage_row(run_id)
    row["status"] = "running"
    mock_db.get_stage_run.return_value = row
    mock_db.count_run_metrics.return_value = 0
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    mock_executor = MagicMock()

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_executor),
    ):
        get_run_metrics(run_id)

    mock_executor.sync_metrics_if_running.assert_called_once_with(run_id)


def test_metrics_offset_cap_is_configurable(monkeypatch):
    """MAX_METRICS_OFFSET should honor env override."""
    from goldfish.server_tools import execution_tools

    monkeypatch.setenv("GOLDFISH_METRICS_MAX_OFFSET", "5")
    # Force reload to pick up env if cached
    import importlib

    importlib.reload(execution_tools)

    run_id = "stage-deadbeef"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 0
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    from goldfish.errors import GoldfishError

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        with pytest.raises(GoldfishError):
            execution_tools.get_run_metrics(run_id, offset=6)

    monkeypatch.delenv("GOLDFISH_METRICS_MAX_OFFSET", raising=False)
    importlib.reload(execution_tools)


def test_get_run_metrics_offset_too_large_raises():
    """Offsets above the max should be rejected to prevent abuse."""
    from goldfish.errors import GoldfishError
    from goldfish.server_tools.execution_tools import MAX_METRICS_OFFSET, get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        with pytest.raises(GoldfishError):
            get_run_metrics(run_id, offset=MAX_METRICS_OFFSET + 1)


def test_get_run_metrics_artifact_pagination():
    """Artifact pagination should be forwarded to the DB."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 0
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        get_run_metrics(run_id, artifact_limit=10, artifact_offset=5)

    mock_db.get_run_artifacts.assert_called_once_with(run_id, limit=10, offset=5)


def test_get_run_metrics_total_metrics_in_response():
    """total_metrics should be part of the response model."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 1
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = [
        {"name": "loss", "value": 0.5, "step": 1, "timestamp": "2024-01-01T00:00:00+00:00"},
    ]
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = get_run_metrics(run_id, limit=1)

    assert result["total_metrics"] == 1


def test_get_run_metrics_audit_reason_meets_min_length():
    """Audit reason should satisfy DB length constraint."""
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.count_run_metrics.return_value = 0
    mock_db.count_run_artifacts.return_value = 0
    mock_db.log_audit = MagicMock()
    mock_db.get_run_metrics.return_value = []
    mock_db.get_metrics_summary.return_value = []
    mock_db.get_run_artifacts.return_value = []

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        get_run_metrics(run_id)

    _, kwargs = mock_db.log_audit.call_args
    assert len(kwargs["reason"]) >= 15


def test_list_metric_names_tool():
    """list_metric_names should return distinct names."""
    from goldfish.server_tools.execution_tools import list_metric_names

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)
    mock_db.list_metric_names.return_value = ["loss", "accuracy"]
    mock_db.log_audit = MagicMock()

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        result = list_metric_names(run_id)

    assert result["run_id"] == run_id
    assert result["metric_names"] == ["loss", "accuracy"]
    assert result["count"] == 2


def test_get_run_metrics_workspace_mismatch_raises():
    """Workspace mismatch should be rejected when workspace is provided."""
    from goldfish.errors import GoldfishError
    from goldfish.server_tools.execution_tools import get_run_metrics

    run_id = "stage-abc123"
    mock_db = MagicMock()
    row = _mock_stage_row(run_id)
    row["workspace_name"] = "workspace-a"
    mock_db.get_stage_run.return_value = row

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        with pytest.raises(GoldfishError):
            get_run_metrics(run_id, workspace="workspace-b")


def test_list_metric_names_workspace_mismatch_raises():
    """Workspace mismatch should be rejected for list_metric_names."""
    from goldfish.errors import GoldfishError
    from goldfish.server_tools.execution_tools import list_metric_names

    run_id = "stage-abc123"
    mock_db = MagicMock()
    row = _mock_stage_row(run_id)
    row["workspace_name"] = "workspace-a"
    mock_db.get_stage_run.return_value = row

    with patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db):
        with pytest.raises(GoldfishError):
            list_metric_names(run_id, workspace="workspace-b")
