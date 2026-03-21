"""Integration tests for Goldfish workspace branching behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def branching_setup(temp_dir: Path, temp_git_repo: Path) -> dict[str, object]:
    """Create a project + manager pair for branching tests."""
    project_root = temp_dir / "project"
    project_root.mkdir()
    (project_root / "workspaces").mkdir()
    (project_root / ".goldfish").mkdir()

    config = GoldfishConfig(
        project_name="test-project",
        dev_repo_path=str(temp_git_repo.relative_to(temp_dir)),
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )
    db = Database(project_root / ".goldfish" / "goldfish.db")
    manager = WorkspaceManager(config=config, project_root=project_root, db=db)

    return {
        "project_root": project_root,
        "dev_repo": temp_git_repo,
        "db": db,
        "manager": manager,
    }


def test_branch_from_workspace_head_creates_real_fork_version_and_keeps_diff_baseline(branching_setup) -> None:
    """Branching from a dirty mounted workspace should create a usable fork marker."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.create_workspace("parent", goal="Parent workspace", reason="Creating parent workspace")
    manager.mount("parent", "w1", reason="Mounting parent workspace")

    slot_path = manager._slot_path("w1")
    (slot_path / "v1.txt").write_text("saved version\n")
    manager.save_version("w1", "Create explicit parent version")

    (slot_path / "head_only.txt").write_text("dirty head change\n")
    child = manager.create_workspace(
        "child",
        goal="Child workspace",
        reason="Branching from current parent head",
        from_workspace="parent",
    )

    parent_versions = db.list_versions("parent")
    latest_parent_version = parent_versions[-1]
    child_lineage = db.get_workspace_lineage("child")

    assert child.forked_from == f"parent@{latest_parent_version['version']}"
    assert latest_parent_version["created_by"] == "fork"
    assert child_lineage is not None
    assert child_lineage["parent_workspace"] == "parent"
    assert child_lineage["parent_version"] == latest_parent_version["version"]
    assert manager.git.get_tag_sha(latest_parent_version["git_tag"]) == latest_parent_version["git_sha"]

    diff_result = manager.diff("w1")
    assert diff_result.right == "parent@v1"


def test_sync_and_version_does_not_reuse_fork_marker(branching_setup) -> None:
    """A later run on the same SHA should create a run version, not reuse the fork marker."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.create_workspace("parent", goal="Parent workspace", reason="Creating parent workspace")
    manager.mount("parent", "w1", reason="Mounting parent workspace")

    slot_path = manager._slot_path("w1")
    (slot_path / "base.txt").write_text("base\n")
    manager.save_version("w1", "Create explicit parent version")

    (slot_path / "head_only.txt").write_text("head change\n")
    manager.create_workspace(
        "child",
        goal="Child workspace",
        reason="Branching from current parent head",
        from_workspace="parent",
    )

    fork_version = db.list_versions("parent")[-1]
    version, _ = manager.sync_and_version("w1", "train")
    run_version = db.get_version("parent", version)

    assert fork_version["created_by"] == "fork"
    assert version != fork_version["version"]
    assert run_version is not None
    assert run_version["created_by"] == "run"


def test_branch_from_saved_version_still_works_after_source_branch_deleted(branching_setup) -> None:
    """Version-based branching should work even when the source branch is gone."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.create_workspace("archived", goal="Archived workspace", reason="Creating archived workspace")
    manager.mount("archived", "w1", reason="Mounting archived workspace")

    slot_path = manager._slot_path("w1")
    (slot_path / "saved.txt").write_text("saved state\n")
    saved = manager.save_version("w1", "Create archived source version")
    manager.hibernate("w1", reason="Hibernating archived workspace")

    manager.git.delete_branch("archived", force=True)

    child = manager.create_workspace(
        "restored",
        goal="Restored child",
        reason="Branching from archived immutable version",
        from_workspace="archived",
        from_version=saved.version,
    )
    child_lineage = db.get_workspace_lineage("restored")

    assert child.success is True
    assert child.forked_from == f"archived@{saved.version}"
    assert child_lineage is not None
    assert child_lineage["parent_workspace"] == "archived"
    assert child_lineage["parent_version"] == saved.version


def test_branch_from_legacy_workspace_branch_auto_creates_lineage(branching_setup) -> None:
    """Existing goldfish branches without lineage rows should still be branchable."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.git.create_branch("legacy-parent", "main")
    assert db.get_workspace_lineage("legacy-parent") is None

    manager.create_workspace(
        "child-from-legacy",
        goal="Child from legacy workspace",
        reason="Branching from legacy workspace branch",
        from_workspace="legacy-parent",
    )

    legacy_lineage = db.get_workspace_lineage("legacy-parent")
    child_lineage = db.get_workspace_lineage("child-from-legacy")

    assert legacy_lineage is not None
    assert child_lineage is not None
    assert child_lineage["parent_workspace"] == "legacy-parent"
    assert child_lineage["parent_version"] is not None


def test_branch_from_head_does_not_reuse_pruned_version(branching_setup) -> None:
    """If the only matching SHA version was pruned, branching should mint a new fork marker."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.create_workspace("parent", goal="Parent workspace", reason="Creating parent workspace")
    manager.mount("parent", "w1", reason="Mounting parent workspace")

    slot_path = manager._slot_path("w1")
    (slot_path / "same-sha.txt").write_text("same sha\n")
    saved = manager.save_version("w1", "Create explicit version to prune")
    db.prune_version("parent", saved.version, "Pruning explicit version before branching")

    child = manager.create_workspace(
        "child-after-prune",
        goal="Child after prune",
        reason="Branching from parent head after pruning old version",
        from_workspace="parent",
    )
    child_lineage = db.get_workspace_lineage("child-after-prune")
    parent_versions = db.list_versions("parent")

    assert child.success is True
    assert child_lineage is not None
    assert child_lineage["parent_version"] == "v2"
    assert parent_versions[-1]["version"] == "v2"
    assert parent_versions[-1]["created_by"] == "fork"


def test_branch_from_workspace_head_ignores_stale_crash_mount(branching_setup) -> None:
    """Stale slot contents without an active mount row must not be auto-synced."""
    manager = branching_setup["manager"]
    db = branching_setup["db"]

    assert isinstance(manager, WorkspaceManager)
    assert isinstance(db, Database)

    manager.create_workspace("parent", goal="Parent workspace", reason="Creating parent workspace")
    manager.mount("parent", "w1", reason="Mounting parent workspace")

    slot_path = manager._slot_path("w1")
    (slot_path / "stale.txt").write_text("stale crash content\n")

    # Simulate a crash: the slot still has files and .goldfish-mount, but the DB
    # no longer considers it an active mount.
    assert db.delete_mount("w1") is True

    manager.create_workspace(
        "child",
        goal="Child workspace",
        reason="Branching from parent after stale crash mount",
        from_workspace="parent",
    )

    manager.mount("child", "w2", reason="Inspecting child workspace after branching")
    child_path = manager._slot_path("w2")

    assert not (child_path / "stale.txt").exists()
