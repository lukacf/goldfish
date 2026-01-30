"""WorkspaceStore contract tests.

These tests are abstract: concrete store implementations should subclass
WorkspaceStoreContract and provide a WorkspaceStore fixture.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pytest

from goldfish.db.protocols import WorkspaceStore


class WorkspaceStoreContract(ABC):
    """Abstract contract tests for WorkspaceStore implementations."""

    @pytest.fixture
    @abstractmethod
    def workspace_store(self) -> WorkspaceStore:
        """Return a fresh WorkspaceStore instance for each test."""

    def test_get_workspace_when_missing_returns_none(self, workspace_store: WorkspaceStore) -> None:
        """Missing workspace returns None (not exception)."""
        assert workspace_store.get_workspace("missing-workspace") is None

    def test_create_workspace_when_created_then_get_workspace_returns_equal(
        self, workspace_store: WorkspaceStore
    ) -> None:
        """create_workspace round-trips through get_workspace."""
        created = workspace_store.create_workspace("ws-1", "goal")
        fetched = workspace_store.get_workspace("ws-1")
        assert fetched == created

    def test_list_workspaces_when_created_then_contains_workspace(self, workspace_store: WorkspaceStore) -> None:
        """list_workspaces includes created workspace."""
        workspace_store.create_workspace("ws-1", "goal")
        rows = workspace_store.list_workspaces()
        assert any(row["workspace_name"] == "ws-1" for row in rows)

    def test_delete_workspace_when_called_then_get_workspace_returns_none(
        self, workspace_store: WorkspaceStore
    ) -> None:
        """delete_workspace removes workspace."""
        workspace_store.create_workspace("ws-1", "goal")
        workspace_store.delete_workspace("ws-1")
        assert workspace_store.get_workspace("ws-1") is None
