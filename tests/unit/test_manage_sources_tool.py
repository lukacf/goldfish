"""Unit tests for the consolidated manage_sources tool."""

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
                _get_dataset_registry=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        sys.modules.pop("goldfish.server_tools.data_tools", None)
        yield


def test_manage_sources_list():
    """Test action='list'."""
    from goldfish.server_tools.data_tools import manage_sources

    mock_db = MagicMock()
    mock_db.count_sources.return_value = 1
    mock_db.list_sources.return_value = [
        {
            "id": "s1",
            "name": "s1",
            "description": "d",
            "created_at": "2025-12-27T10:00:00",
            "created_by": "ext",
            "gcs_location": "gs://",
            "size_bytes": 10,
            "status": "available",
            "metadata": "{}",
        }
    ]

    with patch("goldfish.server_tools.data_tools._get_db", return_value=mock_db):
        result = manage_sources(action="list")

    assert result["total"] == 1
    assert result["sources"][0].name == "s1"


def test_manage_sources_get_not_found():
    """Test action='get' error handling."""
    from goldfish.errors import SourceNotFoundError
    from goldfish.server_tools.data_tools import manage_sources

    mock_db = MagicMock()
    mock_db.get_source.return_value = None

    with patch("goldfish.server_tools.data_tools._get_db", return_value=mock_db):
        with pytest.raises(SourceNotFoundError):
            manage_sources(action="get", name="missing")


def test_manage_sources_delete():
    """Test action='delete'."""
    from goldfish.server_tools.data_tools import manage_sources

    mock_db = MagicMock()
    mock_config = MagicMock()
    mock_config.audit.min_reason_length = 15

    with (
        patch("goldfish.server_tools.data_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.data_tools._get_config", return_value=mock_config),
    ):
        result = manage_sources(action="delete", name="old-data", reason="No longer needed for study")

    assert result["success"] is True
    assert result["deleted"] == "old-data"
    mock_db.delete_source.assert_called_once_with("old-data")
    mock_db.log_audit.assert_called_once()
