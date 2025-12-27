"""Unit tests for the consolidated inspect_workspace tool."""

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
                _get_pipeline_manager=MagicMock(),
                _get_workspace_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        sys.modules.pop("goldfish.server_tools.workspace_tools", None)
        yield


def test_inspect_workspace_basic():
    """Test inspect_workspace correctly aggregates info."""
    from datetime import datetime

    from goldfish.models import WorkspaceInfo
    from goldfish.server_tools.workspace_tools import inspect_workspace

    workspace_name = "baseline"
    mock_db = MagicMock()
    mock_db.get_workspace_goal.return_value = "Run baseline"
    mock_db.list_tags.return_value = []

    mock_wm = MagicMock()
    mock_wm.get_workspace_for_slot.return_value = None  # It's already a name
    mock_wm.get_workspace.return_value = WorkspaceInfo(
        name=workspace_name,
        created_at=datetime.now(),
        goal="Run baseline",
        snapshot_count=0,
        last_activity=datetime.now(),
        is_mounted=False,
        slot=None,
    )

    mock_pm = MagicMock()
    mock_pm.get_pipeline.return_value = {"stages": ["train"]}

    # We also mock LineageManager inside the function
    with (
        patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.workspace_tools._get_workspace_manager", return_value=mock_wm),
        patch("goldfish.server_tools.workspace_tools._get_pipeline_manager", return_value=mock_pm),
        patch("goldfish.server_tools.workspace_tools.LineageManager") as mock_lm_class,
    ):
        mock_lm = mock_lm_class.return_value
        mock_lm.get_workspace_lineage.return_value = {"history": []}

        result = inspect_workspace(name=workspace_name)

    assert result["name"] == workspace_name
    assert result["goal"] == "Run baseline"
    assert result["pipeline"]["stages"] == ["train"]
    assert result["lineage"]["history"] == []
