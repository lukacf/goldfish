"""Unit tests for SQLiteStageRunStore."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.runs import SQLiteStageRunStore
from goldfish.db.sqlite.workspaces import SQLiteWorkspaceStore


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


def _create_workspace_version(db: Database, workspace_name: str, version: str) -> None:
    timestamp = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO workspace_versions
                (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (workspace_name, version, f"{workspace_name}-{version}", "deadbeef", timestamp, "manual"),
        )


def test_get_stage_run_when_missing_returns_none(test_db: Database) -> None:
    """get_stage_run returns None when run_id does not exist."""
    settings = _settings()
    store = SQLiteStageRunStore(db=test_db, settings=settings)
    assert store.settings is settings

    assert store.get_stage_run("stage-missing") is None


def test_create_stage_run_when_new_persists_and_returns_row(test_db: Database) -> None:
    """create_stage_run inserts stage_runs row and returns the stored view."""
    settings = _settings()
    workspace_store = SQLiteWorkspaceStore(db=test_db, settings=settings)
    workspace_store.create_workspace(name="w1", goal="goal")
    _create_workspace_version(test_db, "w1", "v1")

    store = SQLiteStageRunStore(db=test_db, settings=settings)

    row = store.create_stage_run(
        id="stage-1",
        workspace_name="w1",
        version="v1",
        stage_name="train",
    )

    assert row["id"] == "stage-1"
    assert row["workspace_name"] == "w1"
    assert row["version"] == "v1"
    assert row["stage_name"] == "train"
    assert row["status"] == "pending"
    assert datetime.fromisoformat(row["started_at"])
    assert row["completed_at"] is None

    fetched = store.get_stage_run("stage-1")
    assert fetched == row


def test_update_status_when_present_updates_status(test_db: Database) -> None:
    """update_status updates the status column for the given run_id."""
    settings = _settings()
    workspace_store = SQLiteWorkspaceStore(db=test_db, settings=settings)
    workspace_store.create_workspace(name="w1", goal="goal")
    _create_workspace_version(test_db, "w1", "v1")

    store = SQLiteStageRunStore(db=test_db, settings=settings)
    store.create_stage_run(
        id="stage-1",
        workspace_name="w1",
        version="v1",
        stage_name="train",
        status="pending",
        started_at="2026-01-01T00:00:00+00:00",
    )

    store.update_status("stage-1", status="running")

    updated = store.get_stage_run("stage-1")
    assert updated is not None
    assert updated["status"] == "running"


def test_get_stage_runs_by_workspace_when_multiple_filters_and_orders(test_db: Database) -> None:
    """get_stage_runs_by_workspace returns only runs for the workspace, ordered by started_at desc."""
    settings = _settings()
    workspace_store = SQLiteWorkspaceStore(db=test_db, settings=settings)
    workspace_store.create_workspace(name="w1", goal="goal")
    workspace_store.create_workspace(name="w2", goal="goal")
    _create_workspace_version(test_db, "w1", "v1")
    _create_workspace_version(test_db, "w2", "v1")

    store = SQLiteStageRunStore(db=test_db, settings=settings)
    store.create_stage_run(
        id="stage-w1-old",
        workspace_name="w1",
        version="v1",
        stage_name="train",
        started_at="2026-01-01T00:00:00+00:00",
    )
    store.create_stage_run(
        id="stage-w1-new",
        workspace_name="w1",
        version="v1",
        stage_name="train",
        started_at="2026-01-02T00:00:00+00:00",
    )
    store.create_stage_run(
        id="stage-w2",
        workspace_name="w2",
        version="v1",
        stage_name="train",
        started_at="2026-01-03T00:00:00+00:00",
    )

    rows = store.get_stage_runs_by_workspace("w1")
    assert [row["id"] for row in rows] == ["stage-w1-new", "stage-w1-old"]
