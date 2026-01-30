"""StageRunStore contract tests.

These tests are abstract: concrete store implementations should subclass
StageRunStoreContract and provide a StageRunStore fixture.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pytest

from goldfish.db.protocols import StageRunStore


class StageRunStoreContract(ABC):
    """Abstract contract tests for StageRunStore implementations."""

    @pytest.fixture
    @abstractmethod
    def stage_run_store(self) -> StageRunStore:
        """Return a fresh StageRunStore instance for each test."""

    @pytest.fixture
    @abstractmethod
    def stage_run_workspace_name(self) -> str:
        """Workspace name that exists (and has the version under test)."""

    @pytest.fixture
    @abstractmethod
    def stage_run_other_workspace_name(self) -> str:
        """Second workspace name that exists (and has the version under test)."""

    @pytest.fixture
    @abstractmethod
    def stage_run_version(self) -> str:
        """Workspace version that exists for both workspaces."""

    def test_get_stage_run_when_missing_returns_none(self, stage_run_store: StageRunStore) -> None:
        """Missing stage run returns None (not exception)."""
        assert stage_run_store.get_stage_run("stage-missing") is None

    def test_create_stage_run_when_created_then_get_stage_run_returns_equal(
        self, stage_run_store: StageRunStore, stage_run_workspace_name: str, stage_run_version: str
    ) -> None:
        """create_stage_run round-trips through get_stage_run."""
        created = stage_run_store.create_stage_run(
            id="stage-1",
            workspace_name=stage_run_workspace_name,
            version=stage_run_version,
            stage_name="train",
            status="pending",
            started_at="2026-01-01T00:00:00+00:00",
            completed_at=None,
        )
        fetched = stage_run_store.get_stage_run("stage-1")
        assert fetched == created

    def test_update_status_when_called_then_status_updated(
        self, stage_run_store: StageRunStore, stage_run_workspace_name: str, stage_run_version: str
    ) -> None:
        """update_status persists the new status."""
        stage_run_store.create_stage_run(
            id="stage-1",
            workspace_name=stage_run_workspace_name,
            version=stage_run_version,
            stage_name="train",
            status="pending",
            started_at="2026-01-01T00:00:00+00:00",
            completed_at=None,
        )
        stage_run_store.update_status("stage-1", "running")
        fetched = stage_run_store.get_stage_run("stage-1")
        assert fetched is not None
        assert fetched["status"] == "running"

    def test_get_stage_runs_by_workspace_when_runs_exist_then_returns_them(
        self,
        stage_run_store: StageRunStore,
        stage_run_workspace_name: str,
        stage_run_other_workspace_name: str,
        stage_run_version: str,
    ) -> None:
        """get_stage_runs_by_workspace returns runs scoped to the workspace."""
        stage_run_store.create_stage_run(
            id="stage-1",
            workspace_name=stage_run_workspace_name,
            version=stage_run_version,
            stage_name="train",
            status="pending",
            started_at="2026-01-01T00:00:00+00:00",
            completed_at=None,
        )
        stage_run_store.create_stage_run(
            id="stage-2",
            workspace_name=stage_run_workspace_name,
            version=stage_run_version,
            stage_name="eval",
            status="pending",
            started_at="2026-01-01T00:00:01+00:00",
            completed_at=None,
        )
        stage_run_store.create_stage_run(
            id="stage-other",
            workspace_name=stage_run_other_workspace_name,
            version=stage_run_version,
            stage_name="train",
            status="pending",
            started_at="2026-01-01T00:00:00+00:00",
            completed_at=None,
        )
        rows = stage_run_store.get_stage_runs_by_workspace(stage_run_workspace_name)
        ids = {row["id"] for row in rows}
        assert ids == {"stage-1", "stage-2"}
