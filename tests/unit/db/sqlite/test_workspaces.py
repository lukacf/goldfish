"""Unit tests for SQLiteWorkspaceStore.

These tests define the expected behavior for the SQLite-backed implementation
of the WorkspaceStore protocol.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.workspaces import SQLiteWorkspaceStore


def _is_iso8601(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _settings() -> GoldfishSettings:
    return GoldfishSettings(
        project_name="test",
        dev_repo_path=Path("/tmp/dev-repo"),
        workspaces_path=Path("/tmp/workspaces"),
        backend="local",
        db_path=Path("/tmp/goldfish.db"),
        db_backend="sqlite",
        log_format="console",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=60,
    )


def test_get_workspace_when_missing_returns_none(test_db: Database) -> None:
    """get_workspace returns None when the workspace does not exist."""
    settings = _settings()
    store = SQLiteWorkspaceStore(db=test_db, settings=settings)
    assert store.settings is settings

    assert store.get_workspace("missing") is None


def test_create_workspace_when_new_persists_and_returns_row(test_db: Database) -> None:
    """create_workspace persists workspace and returns the stored row."""
    store = SQLiteWorkspaceStore(db=test_db, settings=_settings())

    row = store.create_workspace(name="w1", goal="test goal")

    assert row["workspace_name"] == "w1"
    assert row["goal"] == "test goal"
    assert _is_iso8601(row["created_at"])
    assert _is_iso8601(row["updated_at"])

    fetched = store.get_workspace("w1")

    assert fetched == row


def test_list_workspaces_when_multiple_returns_sorted_rows(test_db: Database) -> None:
    """list_workspaces returns all workspaces ordered by workspace_name."""
    store = SQLiteWorkspaceStore(db=test_db, settings=_settings())

    store.create_workspace(name="b", goal="goal b")
    store.create_workspace(name="a", goal="goal a")

    rows = store.list_workspaces()

    assert [row["workspace_name"] for row in rows] == ["a", "b"]


def test_delete_workspace_when_present_removes_workspace(test_db: Database) -> None:
    """delete_workspace removes workspace state and becomes a no-op after deletion."""
    store = SQLiteWorkspaceStore(db=test_db, settings=_settings())

    store.create_workspace(name="w1", goal="test goal")

    store.delete_workspace("w1")

    assert store.get_workspace("w1") is None

    # No-op if missing
    store.delete_workspace("w1")
