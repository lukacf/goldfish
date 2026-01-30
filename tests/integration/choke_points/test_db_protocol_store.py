"""Choke-point integration tests for DB Protocol → Store boundary."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from types import ModuleType
from typing import TypeVar

import pytest

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database

T = TypeVar("T")


def _optional_import(module_name: str) -> ModuleType | None:
    try:
        if find_spec(module_name) is None:
            return None
    except ModuleNotFoundError:
        return None
    return import_module(module_name)


def _call_no_raise(fn: Callable[[], T], *, label: str) -> T:
    try:
        return fn()
    except Exception as e:  # pragma: no cover - this is a RED choke-point test
        pytest.fail(f"{label} raised {type(e).__name__}: {e}")


def test_workspace_store_roundtrip_when_implemented_returns_typed_dict(test_db: Database, tmp_path: Path) -> None:
    """Define expected DB store behavior (Gate 2: RED OK)."""
    module = _optional_import("goldfish.db.sqlite.workspaces")
    assert module is not None, "Expected `goldfish.db.sqlite.workspaces` to exist (SQLiteWorkspaceStore implementation)"

    store_type = getattr(module, "SQLiteWorkspaceStore", None)
    assert store_type is not None, "Expected `SQLiteWorkspaceStore` to be defined in `goldfish.db.sqlite.workspaces`"

    settings = GoldfishSettings(
        project_name="choke-db-protocol-store",
        dev_repo_path=tmp_path / "dev-repo",
        workspaces_path=tmp_path / "workspaces",
        backend="local",
        db_path=test_db.db_path,
        db_backend="sqlite",
        log_format="console",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=60,
    )
    store = store_type(db=test_db, settings=settings)

    created = _call_no_raise(
        lambda: store.create_workspace(name="ws-choke-1", goal="goal"),
        label="create_workspace",
    )
    assert created["workspace_name"] == "ws-choke-1"
    assert created["goal"] == "goal"
    assert created["created_at"]
    assert created["updated_at"]

    fetched = _call_no_raise(
        lambda: store.get_workspace("ws-choke-1"),
        label="get_workspace(existing)",
    )
    assert fetched == created

    missing = _call_no_raise(
        lambda: store.get_workspace("ws-does-not-exist"),
        label="get_workspace(missing)",
    )
    assert missing is None
