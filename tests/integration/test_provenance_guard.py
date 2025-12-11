"""Tests for Phase 3: Provenance Guard.

Ensures that run() syncs slot changes to branch BEFORE creating version tag,
guaranteeing 100% provenance - every run executes against committed code.
"""

import subprocess

import pytest

from goldfish.config import (
    AuditConfig,
    GoldfishConfig,
    JobsConfig,
    StateMdConfig,
)
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def provenance_setup(temp_dir):
    """Set up isolated environment for provenance guard tests."""
    # Resolve to handle macOS symlinks
    temp_dir = temp_dir.resolve()

    # Create user project directory
    user_project = temp_dir / "myproject"
    user_project.mkdir()

    # Create dev repo with git
    dev_repo = temp_dir / "myproject-dev"
    dev_repo.mkdir()
    subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create initial structure in dev repo
    (dev_repo / "code").mkdir()
    (dev_repo / "code" / "main.py").write_text("# initial")
    (dev_repo / ".goldfish").mkdir()

    subprocess.run(["git", "add", "."], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create workspaces directory in user project
    workspaces_dir = user_project / "workspaces"
    workspaces_dir.mkdir()

    # Initialize database (auto-initializes schema on creation)
    db_path = dev_repo / ".goldfish" / "goldfish.db"
    db = Database(db_path)

    # Create config - dev_repo is sibling to user_project
    config = GoldfishConfig(
        project_name="test-provenance",
        dev_repo_path="myproject-dev",  # Sibling to myproject
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="exp"),
        invariants=[],
    )

    # Initialize manager (it creates GitLayer internally)
    manager = WorkspaceManager(
        db=db,
        config=config,
        project_root=user_project,
    )
    git = manager.git

    return {
        "user_project": user_project,
        "dev_repo": dev_repo,
        "workspaces_dir": workspaces_dir,
        "db": db,
        "config": config,
        "git": git,
        "manager": manager,
    }


class TestProvenanceGuard:
    """Tests for provenance guard - sync before versioning."""

    def test_auto_version_syncs_slot_changes(self, provenance_setup):
        """Auto-versioning should sync slot changes to branch before creating tag."""
        manager = provenance_setup["manager"]
        db = provenance_setup["db"]
        git = provenance_setup["git"]
        dev_repo = provenance_setup["dev_repo"]
        workspaces_dir = provenance_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace("test-ws", "Test provenance", "Creating for provenance test")
        manager.mount("test-ws", "w1", "Mount for testing provenance")

        slot_path = workspaces_dir / "w1"

        # Make changes in slot (user edits)
        (slot_path / "code" / "main.py").write_text("# edited by user")
        (slot_path / "code" / "new_file.py").write_text("# new file")

        # Get branch SHA before auto_version
        branch_sha_before = git.get_head_sha_from_branch("goldfish/test-ws")

        # Call sync_and_version (the new method we'll create)
        version, sha = manager.sync_and_version("w1", "test-stage", "Auto-version test")

        # Get branch SHA after auto_version
        branch_sha_after = git.get_head_sha_from_branch("goldfish/test-ws")

        # Branch should have advanced (changes were committed)
        assert branch_sha_after != branch_sha_before

        # The version SHA should match the new branch head
        assert sha == branch_sha_after

        # Verify version was recorded in database
        versions = db.list_versions("test-ws")
        assert len(versions) == 1
        assert versions[0]["version"] == version
        assert versions[0]["git_sha"] == sha

    def test_auto_version_creates_tag_against_synced_commit(self, provenance_setup):
        """Version tag should point to the synced commit, not stale state."""
        manager = provenance_setup["manager"]
        git = provenance_setup["git"]
        dev_repo = provenance_setup["dev_repo"]
        workspaces_dir = provenance_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace("tagged-ws", "Test tagging", "Creating for tag testing")
        manager.mount("tagged-ws", "w1", "Mount for tag testing")

        slot_path = workspaces_dir / "w1"

        # Make changes in slot
        (slot_path / "code" / "main.py").write_text("# version 1 code")

        # Create version
        version, sha = manager.sync_and_version("w1", "train", "First version")

        # Verify tag exists and points to correct SHA
        git_tag = f"tagged-ws-{version}"
        result = subprocess.run(
            ["git", "rev-parse", git_tag],
            cwd=dev_repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        tag_sha = result.stdout.strip()
        assert tag_sha == sha

    def test_auto_version_no_changes_still_versions(self, provenance_setup):
        """Auto-version should work even with no slot changes (empty commit or existing)."""
        manager = provenance_setup["manager"]
        db = provenance_setup["db"]
        workspaces_dir = provenance_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace("no-changes-ws", "Test no changes", "Creating for no-change test")
        manager.mount("no-changes-ws", "w1", "Mount without changes")

        # No changes made to slot

        # Should still create version successfully
        version, sha = manager.sync_and_version("w1", "eval", "No changes version")

        # Version should be recorded
        versions = db.list_versions("no-changes-ws")
        assert len(versions) == 1
        assert versions[0]["version"] == version

    def test_auto_version_sequential_versions(self, provenance_setup):
        """Sequential auto-versions should each capture changes at that point."""
        manager = provenance_setup["manager"]
        db = provenance_setup["db"]
        workspaces_dir = provenance_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace("seq-ws", "Sequential versions test", "Creating for sequential test")
        manager.mount("seq-ws", "w1", "Mount for sequential test")

        slot_path = workspaces_dir / "w1"

        # Version 1
        (slot_path / "code" / "main.py").write_text("# v1")
        v1, sha1 = manager.sync_and_version("w1", "train", "Version 1")

        # Version 2 with more changes
        (slot_path / "code" / "main.py").write_text("# v2")
        v2, sha2 = manager.sync_and_version("w1", "train", "Version 2")

        # Version 3 with even more changes
        (slot_path / "code" / "utils.py").write_text("# utils")
        v3, sha3 = manager.sync_and_version("w1", "train", "Version 3")

        # All versions should be different
        assert v1 != v2 != v3
        assert sha1 != sha2 != sha3

        # All should be recorded
        versions = db.list_versions("seq-ws")
        assert len(versions) == 3

    def test_auto_version_updates_slot_metadata(self, provenance_setup):
        """After sync_and_version, slot metadata should reflect current state."""
        manager = provenance_setup["manager"]
        git = provenance_setup["git"]
        workspaces_dir = provenance_setup["workspaces_dir"]

        import json

        # Create and mount workspace
        manager.create_workspace("meta-ws", "Metadata update test", "Creating for metadata test")
        manager.mount("meta-ws", "w1", "Mount for metadata test")

        slot_path = workspaces_dir / "w1"

        # Get initial mounted_sha from metadata
        metadata_file = slot_path / ".goldfish-mount"
        initial_metadata = json.loads(metadata_file.read_text())
        initial_sha = initial_metadata["mounted_sha"]

        # Make changes
        (slot_path / "code" / "main.py").write_text("# changed")

        # Sync and version
        version, sha = manager.sync_and_version("w1", "stage", "Update metadata test")

        # Metadata should be updated to new SHA
        updated_metadata = json.loads(metadata_file.read_text())
        assert updated_metadata["mounted_sha"] == sha
        assert updated_metadata["mounted_sha"] != initial_sha

    def test_sync_and_version_requires_mounted_workspace(self, provenance_setup):
        """sync_and_version should fail if slot is empty."""
        manager = provenance_setup["manager"]

        from goldfish.errors import SlotEmptyError

        with pytest.raises(SlotEmptyError):
            manager.sync_and_version("w1", "stage", "Should fail")

    def test_sync_and_version_records_audit(self, provenance_setup):
        """sync_and_version should record in audit log."""
        manager = provenance_setup["manager"]
        db = provenance_setup["db"]

        # Create and mount workspace
        manager.create_workspace("audit-ws", "Audit test workspace", "Creating for audit test")
        manager.mount("audit-ws", "w1", "Mount for audit test")

        # Sync and version
        manager.sync_and_version("w1", "train", "Audit logging test")

        # Check audit log
        audits = db.get_recent_audit(limit=10)
        version_audit = next(
            (a for a in audits if a["operation"] == "sync_and_version"),
            None,
        )
        assert version_audit is not None
        assert version_audit["workspace"] == "audit-ws"
