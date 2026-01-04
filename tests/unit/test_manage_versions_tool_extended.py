"""Extended unit tests for manage_versions tool."""

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


def test_manage_versions_prune_range_validation():
    """Test validation for prune range."""
    from goldfish.server_tools.workspace_tools import manage_versions

    mock_db = MagicMock()
    mock_config = MagicMock()
    mock_config.audit.min_reason_length = 15

    with (
        patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.workspace_tools._get_config", return_value=mock_config),
    ):
        # Missing to_version
        with pytest.raises(GoldfishError, match="Both from_version and to_version are required"):
            manage_versions(
                workspace="w1", action="prune", from_version="v1", reason="Testing prune validation failure"
            )

        # Missing from_version
        with pytest.raises(GoldfishError, match="Both from_version and to_version are required"):
            manage_versions(workspace="w1", action="prune", to_version="v5", reason="Testing prune validation failure")


def test_manage_versions_prune_before_tag():
    """Test prune_before_tag action."""
    from goldfish.server_tools.workspace_tools import manage_versions

    mock_db = MagicMock()
    mock_db.prune_before_tag.return_value = {"pruned_count": 3}
    mock_config = MagicMock()
    mock_config.audit.min_reason_length = 15

    with (
        patch("goldfish.server_tools.workspace_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.workspace_tools._get_config", return_value=mock_config),
    ):
        result = manage_versions(
            workspace="w1", action="prune_before_tag", tag="milestone", reason="Cleanup before milestone"
        )

    assert result["success"] is True
    assert result["result"]["pruned_count"] == 3
    mock_db.prune_before_tag.assert_called_once_with("w1", "milestone", "Cleanup before milestone")
