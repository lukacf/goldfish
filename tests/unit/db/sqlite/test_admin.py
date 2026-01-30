"""Unit tests for SQLiteAuditStore."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.admin import SQLiteAuditStore


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


def test_record_audit_when_called_inserts_row(test_db: Database) -> None:
    """record_audit inserts an audit row that is returned by get_audit_log."""
    settings = _settings()
    store = SQLiteAuditStore(db=test_db, settings=settings)
    assert store.settings is settings

    store.record_audit(operation="op", workspace="w1", details={"k": "v"})

    rows = store.get_audit_log(limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row["operation"] == "op"
    assert row["workspace"] == "w1"
    assert row["slot"] is None
    assert len(row["reason"]) >= 15
    assert datetime.fromisoformat(row["timestamp"])
    assert json.loads(row["details"] or "{}") == {"k": "v"}


def test_get_audit_log_when_multiple_returns_most_recent_first(test_db: Database) -> None:
    """get_audit_log returns rows in reverse chronological order."""
    store = SQLiteAuditStore(db=test_db, settings=_settings())

    store.record_audit(operation="first", workspace=None, details={})
    store.record_audit(operation="second", workspace=None, details={})

    rows = store.get_audit_log(limit=2)
    assert [row["operation"] for row in rows] == ["second", "first"]
