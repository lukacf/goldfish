"""Unit tests for SQLiteMetricsStore."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.metrics import SQLiteMetricsStore
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


def _setup_stage_run(db: Database, run_id: str) -> None:
    settings = _settings()
    workspace_store = SQLiteWorkspaceStore(db=db, settings=settings)
    workspace_store.create_workspace(name="w1", goal="goal")

    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO workspace_versions
                (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("w1", "v1", "w1-v1", "deadbeef", "2026-01-01T00:00:00+00:00", "manual"),
        )

    runs_store = SQLiteStageRunStore(db=db, settings=settings)
    runs_store.create_stage_run(
        id=run_id,
        workspace_name="w1",
        version="v1",
        stage_name="train",
        started_at="2026-01-01T00:00:00+00:00",
    )


def test_record_metric_when_called_inserts_row(test_db: Database) -> None:
    """record_metric persists a metric that is returned by get_metrics."""
    _setup_stage_run(test_db, run_id="stage-1")
    settings = _settings()
    store = SQLiteMetricsStore(db=test_db, settings=settings)
    assert store.settings is settings

    store.record_metric(run_id="stage-1", name="loss", value=1.23, step=1)

    metrics = store.get_metrics("stage-1")
    assert len(metrics) == 1
    metric = metrics[0]
    assert metric["stage_run_id"] == "stage-1"
    assert metric["name"] == "loss"
    assert metric["value"] == 1.23
    assert metric["step"] == 1
    assert datetime.fromisoformat(metric["timestamp"])


def test_get_metrics_when_multiple_returns_all_rows(test_db: Database) -> None:
    """get_metrics returns all metric rows for a stage run."""
    _setup_stage_run(test_db, run_id="stage-1")
    store = SQLiteMetricsStore(db=test_db, settings=_settings())

    store.record_metric(run_id="stage-1", name="loss", value=1.23, step=1)
    store.record_metric(run_id="stage-1", name="acc", value=0.5, step=1)

    metrics = store.get_metrics("stage-1")
    assert [m["name"] for m in metrics] == ["loss", "acc"]
