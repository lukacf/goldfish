"""Unit tests for the consolidated manage_patterns tool."""

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
                _get_db=MagicMock(),
                mcp=mock_mcp,
            ),
        },
    ):
        sys.modules.pop("goldfish.server_tools.svs_tools", None)
        yield


def test_manage_patterns_list():
    """Test action='list'."""
    from goldfish.server_tools.svs_tools import manage_patterns

    mock_db = MagicMock()
    mock_db.list_failure_patterns.return_value = [
        {
            "id": "p1",
            "symptom": "OOM",
            "root_cause": "Big model",
            "status": "pending",
            "severity": "HIGH",
            "created_at": "ts",
        }
    ]
    mock_db.count_failure_patterns.return_value = 1

    with patch("goldfish.server_tools.svs_tools._get_db", return_value=mock_db):
        result = manage_patterns(action="list")

    assert result["total"] == 1
    assert result["patterns"][0]["symptom"] == "OOM"


def test_manage_patterns_approve():
    """Test action='approve'."""
    from goldfish.server_tools.svs_tools import manage_patterns

    mock_db = MagicMock()

    with (
        patch("goldfish.server_tools.svs_tools._get_db", return_value=mock_db),
        patch("goldfish.server_tools.svs_tools.FailurePatternManager") as mock_mgr_class,
    ):
        result = manage_patterns(action="approve", pattern_id="p1")

        mock_mgr = mock_mgr_class.return_value
        mock_mgr.approve_pattern.assert_called_once_with("p1")

    assert result["success"] is True
    assert result["status"] == "approved"


def test_manage_patterns_list_with_filters():
    """Regression test: list action must accept stage_type, severity, status filters.

    Ensures the tool signature matches the implementation and filters are passed
    through to the database layer correctly.
    """
    from goldfish.server_tools.svs_tools import manage_patterns

    mock_db = MagicMock()
    mock_db.list_failure_patterns.return_value = []
    mock_db.count_failure_patterns.return_value = 0

    with patch("goldfish.server_tools.svs_tools._get_db", return_value=mock_db):
        # Call with all filter parameters
        result = manage_patterns(
            action="list",
            stage_type="train",
            status="approved",
            severity="HIGH",
            limit=25,
            offset=10,
        )

    # Verify filters were passed to database method
    mock_db.list_failure_patterns.assert_called_once_with(
        status="approved",
        stage_type="train",
        severity="HIGH",
        limit=25,
        offset=10,
    )
    mock_db.count_failure_patterns.assert_called_once_with(
        status="approved",
        stage_type="train",
        severity="HIGH",
    )

    assert "patterns" in result
    assert "total" in result
