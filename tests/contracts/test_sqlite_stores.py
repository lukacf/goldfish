"""Contract test implementations for SQLite stores."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.metrics import SQLiteMetricsStore
from goldfish.db.sqlite.runs import SQLiteStageRunStore
from goldfish.db.sqlite.workspaces import SQLiteWorkspaceStore
from tests.contracts.test_stage_run_store import StageRunStoreContract
from tests.contracts.test_workspace_store import WorkspaceStoreContract


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


class TestSQLiteWorkspaceStore(WorkspaceStoreContract):
    """WorkspaceStoreContract for SQLiteWorkspaceStore."""

    @pytest.fixture
    def workspace_store(self, test_db: Database) -> SQLiteWorkspaceStore:
        return SQLiteWorkspaceStore(test_db, settings=_settings())


class TestSQLiteStageRunStore(StageRunStoreContract):
    """StageRunStoreContract for SQLiteStageRunStore."""

    @pytest.fixture
    def stage_run_workspace_name(self) -> str:
        return "ws-1"

    @pytest.fixture
    def stage_run_other_workspace_name(self) -> str:
        return "ws-other"

    @pytest.fixture
    def stage_run_version(self) -> str:
        return "v1"

    @pytest.fixture
    def stage_run_store(
        self,
        test_db: Database,
        stage_run_workspace_name: str,
        stage_run_other_workspace_name: str,
        stage_run_version: str,
    ) -> SQLiteStageRunStore:
        settings = _settings()
        workspace_store = SQLiteWorkspaceStore(test_db, settings=settings)
        workspace_store.create_workspace(name=stage_run_workspace_name, goal="goal")
        workspace_store.create_workspace(name=stage_run_other_workspace_name, goal="goal")
        _create_workspace_version(test_db, stage_run_workspace_name, stage_run_version)
        _create_workspace_version(test_db, stage_run_other_workspace_name, stage_run_version)
        return SQLiteStageRunStore(test_db, settings=settings)


class TestSQLiteMetricsStore:
    """MetricsStore round-trip contract for SQLiteMetricsStore."""

    def test_record_metric_when_recorded_then_get_metrics_returns_values(self, test_db: Database) -> None:
        """record_metric round-trips through get_metrics."""
        settings = _settings()
        workspace_store = SQLiteWorkspaceStore(test_db, settings=settings)
        workspace_store.create_workspace(name="ws-1", goal="goal")
        _create_workspace_version(test_db, "ws-1", "v1")

        runs = SQLiteStageRunStore(test_db, settings=settings)
        runs.create_stage_run(
            id="stage-1",
            workspace_name="ws-1",
            version="v1",
            stage_name="train",
            status="pending",
            started_at="2026-01-01T00:00:00+00:00",
            completed_at=None,
        )

        metrics = SQLiteMetricsStore(test_db, settings=settings)
        metrics.record_metric("stage-1", "accuracy", 0.5, step=None)

        rows = metrics.get_metrics("stage-1")
        assert len(rows) == 1
        assert rows[0]["stage_run_id"] == "stage-1"
        assert rows[0]["name"] == "accuracy"
        assert rows[0]["value"] == 0.5
