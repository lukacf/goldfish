"""Unit tests for goldfish.db.sqlite package exports."""

from __future__ import annotations


def test_sqlite_package_when_imported_exports_store_classes() -> None:
    """goldfish.db.sqlite exports the concrete SQLite store classes."""
    from goldfish.db.sqlite import (
        SQLiteAuditStore,
        SQLiteMetricsStore,
        SQLiteSourceStore,
        SQLiteStageRunStore,
        SQLiteWorkspaceStore,
    )

    assert SQLiteWorkspaceStore is not None
    assert SQLiteStageRunStore is not None
    assert SQLiteMetricsStore is not None
    assert SQLiteSourceStore is not None
    assert SQLiteAuditStore is not None
