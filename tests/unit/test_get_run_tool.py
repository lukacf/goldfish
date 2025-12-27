"""Unit tests for get_run MCP tool semantics."""

import json
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
        "version": "v1",
        "status": "running",
        "started_at": "2025-01-01T00:00:00+00:00",
        "inputs_json": json.dumps({"tokens": "gs://bucket/tokens"}),
        "outputs_json": json.dumps([]),
        "config_json": json.dumps({"epochs": 1}),
        "reason_json": json.dumps({"description": "test run", "hypothesis": "loss drops", "goal": "baseline"}),
        "preflight_errors_json": json.dumps(["missing config param: hidden_dim"]),
        "preflight_warnings_json": json.dumps(["config file missing"]),
        "svs_findings_json": json.dumps(
            {
                "stats": {"tokens": {"entropy": 7.1}},
                "ai_review": {"decision": "warned", "findings": ["WARNING: Low entropy"], "duration_ms": 1200},
                "during_run": {
                    "decision": "blocked",
                    "history": [
                        {
                            "phase": "during_run",
                            "severity": "BLOCK",
                            "check": "metric_health",
                            "summary": "NaN detected",
                            "step": 10,
                            "timestamp": "2025-01-01T00:01:00Z",
                        }
                    ],
                },
            }
        ),
    }


def test_get_run_includes_reason_and_svs_summary():
    """get_run should surface reason and SVS pre/during/post findings."""
    from goldfish.server_tools.execution_tools import get_run

    run_id = "stage-abc123"
    mock_db = MagicMock()
    mock_db.get_stage_run.return_value = _mock_stage_row(run_id)

    mock_executor = MagicMock()
    mock_executor.refresh_status_once = MagicMock()
    mock_executor.sync_svs_if_running = MagicMock()

    with (
        patch("goldfish.server_tools.execution_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.execution_tools._get_stage_executor", return_value=mock_executor),
    ):
        result = get_run(run_id)

    assert result["reason"]["description"] == "test run"
    assert result["svs"]["preflight"]["errors"] == ["missing config param: hidden_dim"]
    assert result["svs"]["preflight"]["warnings"] == ["config file missing"]
    assert result["svs"]["during_run"]["decision"] == "blocked"
    assert result["svs"]["post_run"]["decision"] == "warned"
    mock_executor.sync_svs_if_running.assert_called_once_with(run_id)
