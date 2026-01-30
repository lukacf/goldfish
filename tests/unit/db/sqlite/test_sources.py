"""Unit tests for SQLiteSourceStore."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.sqlite.sources import SQLiteSourceStore


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


def test_get_source_when_missing_returns_none(test_db: Database) -> None:
    """get_source returns None when source id does not exist."""
    settings = _settings()
    store = SQLiteSourceStore(db=test_db, settings=settings)
    assert store.settings is settings

    assert store.get_source("missing") is None


def test_register_source_when_new_persists_and_returns_row(test_db: Database) -> None:
    """register_source inserts sources row and returns the stored row."""
    store = SQLiteSourceStore(db=test_db, settings=_settings())

    row = store.register_source(
        id="s1",
        name="source 1",
        description=None,
        created_by="external",
        gcs_location="gs://bucket/s1",
        size_bytes=None,
        metadata={"k": "v"},
    )

    assert row["id"] == "s1"
    assert row["name"] == "source 1"
    assert row["created_by"] == "external"
    assert row["gcs_location"] == "gs://bucket/s1"
    assert row["status"] == "available"
    assert datetime.fromisoformat(row["created_at"])
    assert row["metadata"] is not None

    fetched = store.get_source("s1")
    assert fetched == row


def test_list_sources_when_multiple_orders_by_created_at_desc(test_db: Database) -> None:
    """list_sources returns all sources ordered by created_at desc."""
    store = SQLiteSourceStore(db=test_db, settings=_settings())

    store.register_source(
        id="old",
        name="old",
        created_at="2026-01-01T00:00:00+00:00",
        created_by="external",
        gcs_location="gs://bucket/old",
    )
    store.register_source(
        id="new",
        name="new",
        created_at="2026-01-02T00:00:00+00:00",
        created_by="external",
        gcs_location="gs://bucket/new",
    )

    rows = store.list_sources()
    assert [row["id"] for row in rows] == ["new", "old"]
