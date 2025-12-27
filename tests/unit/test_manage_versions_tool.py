"""Unit tests for the consolidated manage_versions tool."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from goldfish.errors import GoldfishError


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
                _get_pipeline_manager=MagicMock(),
                _get_workspace_manager=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        sys.modules.pop("goldfish.server_tools.workspace_tools", None)
        yield


def test_manage_versions_list():
    """Test action='list'."""
    from goldfish.server_tools.workspace_tools import manage_versions

    mock_db = MagicMock()
    mock_db.list_versions.return_value = [{"version": "v1"}, {"version": "v2"}]

    with patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db):
        result = manage_versions(workspace="w1", action="list")

    assert result["workspace"] == "w1"
    assert len(result["versions"]) == 2
    mock_db.list_versions.assert_called_once_with("w1", include_pruned=True)


def test_manage_versions_tag_success():
    """Test action='tag'."""
    from goldfish.server_tools.workspace_tools import manage_versions

    mock_db = MagicMock()
    mock_db.create_tag.return_value = {"tag_name": "best"}

    with patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db):
        result = manage_versions(workspace="w1", action="tag", version="v1", tag="best")

    assert result["success"] is True
    assert result["tag"]["tag_name"] == "best"
    mock_db.create_tag.assert_called_once_with("w1", "v1", "best")


def test_manage_versions_prune_range():
    """Test action='prune' with a range."""
    from goldfish.server_tools.workspace_tools import manage_versions

    mock_db = MagicMock()
    mock_db.prune_versions.return_value = {"pruned_count": 5}
    mock_config = MagicMock()
    mock_config.audit.min_reason_length = 15

    with (
        patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.workspace_tools._get_config", return_value=mock_config),
    ):
        result = manage_versions(
            workspace="w1", action="prune", from_version="v1", to_version="v5", reason="Cleanup old experiments"
        )

    assert result["success"] is True
    assert result["result"]["pruned_count"] == 5
    mock_db.prune_versions.assert_called_once_with("w1", "v1", "v5", "Cleanup old experiments")


def test_manage_versions_invalid_action():
    """Test that unknown actions raise GoldfishError."""
    from goldfish.server_tools.workspace_tools import manage_versions

    with pytest.raises(GoldfishError, match="Unknown action: jump"):
        manage_versions(workspace="w1", action="jump")
