"""End-to-end workflow tests - P1.

These tests verify complete workflows using real components (no mocks).
They test the integration between workspace manager, git layer, and database.
"""

import subprocess
from pathlib import Path

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.models import DirtyState
from goldfish.workspace.manager import WorkspaceManager


def run_git(cmd: list[str], cwd: Path) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git"] + cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


@pytest.fixture
def e2e_setup(temp_dir):
    """Create a full project setup for e2e testing.

    Returns:
        Dict with project_root, dev_repo, db, git, and manager
    """
    # Create dev repo
    dev_repo = temp_dir / "project-dev"
    dev_repo.mkdir()
    run_git(["init"], dev_repo)
    run_git(["config", "user.email", "test@example.com"], dev_repo)
    run_git(["config", "user.name", "Test User"], dev_repo)
    (dev_repo / "README.md").write_text("# Test Project")
    (dev_repo / "code.py").write_text("# Initial code")
    run_git(["add", "."], dev_repo)
    run_git(["commit", "-m", "Initial commit"], dev_repo)

    # Create project structure
    project_root = temp_dir / "project"
    project_root.mkdir()
    (project_root / "workspaces").mkdir()
    (project_root / ".goldfish").mkdir()
    (project_root / "experiments").mkdir()

    # Create database
    db = Database(project_root / ".goldfish" / "goldfish.db")

    # Create config - dev_repo_path is relative to project_root.parent
    config = GoldfishConfig(
        project_name="test-project",
        dev_repo_path="project-dev",  # Sibling of project_root (relative to parent)
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )

    # Create WorkspaceManager
    manager = WorkspaceManager(
        config=config,
        project_root=project_root,
        db=db,
    )

    # Also expose git layer for direct testing
    git = manager.git

    return {
        "project_root": project_root,
        "dev_repo": dev_repo,
        "config": config,
        "db": db,
        "git": git,
        "manager": manager,
    }


class TestWorkspaceLifecycleWorkflow:
    """Test complete workspace lifecycle: create → mount → edit → checkpoint → rollback."""

    def test_create_workspace_workflow(self, e2e_setup):
        """Create a workspace and verify it exists."""
        manager = e2e_setup["manager"]
        git = e2e_setup["git"]

        # Create workspace
        result = manager.create_workspace(
            name="feature-x", goal="Implement feature X", reason="Starting work on feature X implementation"
        )

        # Verify response
        assert result.success is True
        assert result.workspace == "feature-x"
        assert "main" in result.forked_from

        # Verify branch exists
        assert git.branch_exists("feature-x")

    def test_mount_unmount_workflow(self, e2e_setup):
        """Create, mount, and unmount a workspace."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Create workspace
        manager.create_workspace(name="mount-test", goal="Test mounting", reason="Testing mount workflow")

        # Mount to slot
        mount_result = manager.mount(workspace="mount-test", slot="w1", reason="Mounting for testing")

        assert mount_result.success is True
        assert mount_result.slot == "w1"
        assert mount_result.workspace == "mount-test"

        # Verify directory exists
        slot_path = project_root / "workspaces" / "w1"
        assert slot_path.exists()
        assert (slot_path / "README.md").exists()

        # Hibernate (unmount)
        hibernate_result = manager.hibernate(slot="w1", reason="Done with testing")

        assert hibernate_result.success is True
        assert hibernate_result.slot == "w1"
        assert hibernate_result.workspace == "mount-test"

        # Directory should be removed
        assert not slot_path.exists()

    def test_edit_and_checkpoint_workflow(self, e2e_setup):
        """Mount workspace, make edits, create checkpoint."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Setup: create and mount
        manager.create_workspace(name="checkpoint-test", goal="Test checkpoints", reason="Testing checkpoint workflow")
        manager.mount(workspace="checkpoint-test", slot="w1", reason="Mounting for checkpoint test")

        slot_path = project_root / "workspaces" / "w1"

        # Make changes
        (slot_path / "new_feature.py").write_text("def new_feature():\n    pass")
        (slot_path / "code.py").write_text("# Modified code")

        # Verify dirty state
        slot_info = manager.get_slot_info("w1")
        assert slot_info.dirty == DirtyState.DIRTY

        # Create checkpoint
        checkpoint_result = manager.checkpoint(slot="w1", message="Add new feature to codebase")

        assert checkpoint_result.success is True
        assert checkpoint_result.snapshot_id.startswith("snap-")

        # Verify clean state after checkpoint
        slot_info = manager.get_slot_info("w1")
        assert slot_info.dirty == DirtyState.CLEAN

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with checkpoint test")

    def test_rollback_workflow(self, e2e_setup):
        """Create checkpoints, rollback to earlier state."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Setup
        manager.create_workspace(name="rollback-test", goal="Test rollback", reason="Testing rollback workflow")
        manager.mount(workspace="rollback-test", slot="w1", reason="Mounting for rollback test")

        slot_path = project_root / "workspaces" / "w1"

        # Create first checkpoint (good state)
        (slot_path / "feature.py").write_text("# Version 1 - Good")
        first_checkpoint = manager.checkpoint(slot="w1", message="Good version of feature")

        # Create second checkpoint (bad state)
        (slot_path / "feature.py").write_text("# Version 2 - Bad")
        (slot_path / "extra.py").write_text("# Extra file")
        manager.checkpoint(slot="w1", message="Bad version with bugs")

        # Rollback to first checkpoint
        rollback_result = manager.rollback(
            slot="w1", snapshot_id=first_checkpoint.snapshot_id, reason="Rolling back to fix bugs"
        )

        assert rollback_result.success is True
        assert rollback_result.snapshot_id == first_checkpoint.snapshot_id

        # Verify content is restored
        assert (slot_path / "feature.py").read_text() == "# Version 1 - Good"

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with rollback test")


class TestMultiSlotWorkflow:
    """Test workflows involving multiple slots."""

    def test_multiple_workspaces_parallel(self, e2e_setup):
        """Work on multiple workspaces simultaneously."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Create two workspaces
        manager.create_workspace(name="feature-a", goal="Feature A", reason="Working on feature A")
        manager.create_workspace(name="feature-b", goal="Feature B", reason="Working on feature B")

        # Mount both
        manager.mount(workspace="feature-a", slot="w1", reason="Working on feature A")
        manager.mount(workspace="feature-b", slot="w2", reason="Working on feature B")

        # Make different changes in each
        slot1 = project_root / "workspaces" / "w1"
        slot2 = project_root / "workspaces" / "w2"

        (slot1 / "feature_a.py").write_text("# Feature A code")
        (slot2 / "feature_b.py").write_text("# Feature B code")

        # Checkpoint both
        cp1 = manager.checkpoint(slot="w1", message="Feature A progress made")
        cp2 = manager.checkpoint(slot="w2", message="Feature B progress made")

        assert cp1.snapshot_id != cp2.snapshot_id

        # Verify isolation - files don't cross over
        assert not (slot1 / "feature_b.py").exists()
        assert not (slot2 / "feature_a.py").exists()

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with feature A")
        manager.hibernate(slot="w2", reason="Done with feature B")

    def test_switch_workspace_in_slot(self, e2e_setup):
        """Switch between workspaces in the same slot."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Create two workspaces
        manager.create_workspace(name="ws-alpha", goal="Alpha workspace", reason="Creating alpha workspace")
        manager.create_workspace(name="ws-beta", goal="Beta workspace", reason="Creating beta workspace")

        # Mount first, make changes, checkpoint
        manager.mount(workspace="ws-alpha", slot="w1", reason="Working on alpha")
        slot_path = project_root / "workspaces" / "w1"
        (slot_path / "alpha.py").write_text("# Alpha")
        manager.checkpoint(slot="w1", message="Alpha work completed")
        manager.hibernate(slot="w1", reason="Switching to beta")

        # Mount second, make changes, checkpoint
        manager.mount(workspace="ws-beta", slot="w1", reason="Working on beta")
        (slot_path / "beta.py").write_text("# Beta")
        manager.checkpoint(slot="w1", message="Beta work completed")
        manager.hibernate(slot="w1", reason="Switching back to alpha")

        # Re-mount first and verify alpha.py exists, beta.py doesn't
        manager.mount(workspace="ws-alpha", slot="w1", reason="Resuming alpha work")
        assert (slot_path / "alpha.py").exists()
        assert not (slot_path / "beta.py").exists()

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with slot switching")


class TestSnapshotWorkflow:
    """Test snapshot listing and navigation."""

    def test_list_snapshots_workflow(self, e2e_setup):
        """Create multiple snapshots and list them."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Setup
        manager.create_workspace(name="snapshot-list", goal="Test listing", reason="Testing snapshot listing")
        manager.mount(workspace="snapshot-list", slot="w1", reason="Creating snapshots for listing")

        slot_path = project_root / "workspaces" / "w1"

        # Create several snapshots
        snapshots = []
        for i in range(3):
            (slot_path / f"file{i}.py").write_text(f"# File {i}")
            result = manager.checkpoint(slot="w1", message=f"Checkpoint number {i}")
            snapshots.append(result.snapshot_id)

        # List snapshots
        snapshot_list = manager.list_snapshots(workspace="snapshot-list")

        # All snapshots should be present (list_snapshots returns list of dicts)
        snapshot_ids = [s["snapshot_id"] for s in snapshot_list]
        for snap_id in snapshots:
            assert snap_id in snapshot_ids

        # Cleanup
        manager.hibernate(slot="w1", reason="Done listing snapshots")


class TestDatabasePersistenceWorkflow:
    """Test that workspace data persists correctly."""

    def test_workspace_audit_log_persistence(self, e2e_setup):
        """Workspace operations should be logged to audit."""
        db = e2e_setup["db"]
        manager = e2e_setup["manager"]

        # Create workspace
        manager.create_workspace(name="persist-test", goal="My important goal", reason="Testing goal persistence")

        # Verify operation is in audit log
        audits = db.get_recent_audit(limit=10)
        create_ops = [a for a in audits if a["operation"] == "create_workspace"]
        assert len(create_ops) >= 1
        # The most recent create_workspace should be our operation
        assert create_ops[0]["workspace"] == "persist-test"

    def test_workspace_survives_mount_unmount(self, e2e_setup):
        """Workspace state survives mount/unmount cycles."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Create and mount
        manager.create_workspace(name="survive-test", goal="Survival test", reason="Testing workspace survival")
        manager.mount(workspace="survive-test", slot="w1", reason="Testing workspace survival")

        slot_path = project_root / "workspaces" / "w1"

        # Make changes and checkpoint
        (slot_path / "important.py").write_text("# Important work")
        manager.checkpoint(slot="w1", message="Important checkpoint saved")

        # Unmount
        manager.hibernate(slot="w1", reason="Unmounting to test survival")

        # Re-mount and verify
        manager.mount(workspace="survive-test", slot="w2", reason="Re-mounting to verify")  # Different slot
        new_slot = project_root / "workspaces" / "w2"

        assert (new_slot / "important.py").exists()
        assert (new_slot / "important.py").read_text() == "# Important work"

        # Cleanup
        manager.hibernate(slot="w2", reason="Done with survival test")


class TestDiffWorkflow:
    """Test diff functionality in workflows."""

    def test_diff_shows_uncommitted_changes(self, e2e_setup):
        """Diff should show uncommitted changes."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Setup
        manager.create_workspace(name="diff-workflow", goal="Test diff", reason="Testing diff functionality")
        manager.mount(workspace="diff-workflow", slot="w1", reason="Testing diff functionality")

        slot_path = project_root / "workspaces" / "w1"

        # Make changes without checkpointing
        (slot_path / "code.py").write_text("# Modified code significantly")

        # Get diff
        diff_result = manager.diff(slot="w1")

        assert diff_result.has_changes is True
        assert "code.py" in diff_result.files_changed
        assert len(diff_result.diff_text) > 0

        # Cleanup - hibernate auto-checkpoints dirty changes
        manager.hibernate(slot="w1", reason="Done with diff test")

    def test_diff_empty_after_checkpoint(self, e2e_setup):
        """Diff should be empty after checkpoint."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Setup
        manager.create_workspace(name="diff-clean", goal="Test clean diff", reason="Testing clean diff state")
        manager.mount(workspace="diff-clean", slot="w1", reason="Testing clean diff")

        slot_path = project_root / "workspaces" / "w1"

        # Make changes and checkpoint
        (slot_path / "new.py").write_text("# New file")
        manager.checkpoint(slot="w1", message="Add new file to codebase")

        # Diff should be empty
        diff_result = manager.diff(slot="w1")

        assert diff_result.has_changes is False
        assert len(diff_result.files_changed) == 0

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with clean diff test")


class TestErrorRecoveryWorkflow:
    """Test error handling and recovery in workflows."""

    def test_mount_nonexistent_workspace_fails(self, e2e_setup):
        """Mounting nonexistent workspace should fail gracefully."""
        manager = e2e_setup["manager"]

        from goldfish.errors import WorkspaceNotFoundError

        with pytest.raises(WorkspaceNotFoundError):
            manager.mount(workspace="nonexistent", slot="w1", reason="This should fail")

    def test_double_mount_same_slot_fails(self, e2e_setup):
        """Mounting to an occupied slot should fail."""
        manager = e2e_setup["manager"]

        from goldfish.errors import SlotNotEmptyError

        # Create and mount first workspace
        manager.create_workspace(name="first-ws", goal="First", reason="Creating first workspace")
        manager.mount(workspace="first-ws", slot="w1", reason="Mounting first workspace")

        # Try to mount another workspace to same slot
        manager.create_workspace(name="second-ws", goal="Second", reason="Creating second workspace")

        with pytest.raises(SlotNotEmptyError):
            manager.mount(workspace="second-ws", slot="w1", reason="This should fail")

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with double mount test")

    def test_rollback_to_invalid_snapshot_fails(self, e2e_setup):
        """Rollback to invalid snapshot should fail."""
        manager = e2e_setup["manager"]

        from goldfish.errors import GoldfishError

        # Setup
        manager.create_workspace(name="bad-rollback", goal="Test", reason="Testing bad rollback")
        manager.mount(workspace="bad-rollback", slot="w1", reason="Testing invalid rollback")

        with pytest.raises(GoldfishError):
            manager.rollback(slot="w1", snapshot_id="snap-nonexistent-00000000-000000", reason="This should fail")

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with bad rollback test")
