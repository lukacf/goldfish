"""Tests for copy-based workspace mounting (Phase 2).

Tests cover:
- Copy-based mount creates plain directory with .goldfish-mount metadata
- Mount copies files from branch correctly
- Mount records mount in database
- Mount rejects workspace already mounted elsewhere
- Hibernate syncs changes back to branch
- Hibernate removes slot directory
- Hibernate deletes mount from database
- Round-trip: changes survive hibernate/re-mount cycle
"""

import json
import subprocess

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, SlotNotEmptyError
from goldfish.models import SlotState
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def copy_mount_setup(temp_dir):
    """Setup for copy-based mounting tests.

    Creates:
    - User project directory with goldfish.yaml
    - Dev repo with git initialized and initial code
    - Database with schema
    - WorkspaceManager configured for the setup
    """
    # Resolve temp_dir to handle macOS symlinks (/var -> /private/var)
    temp_dir = temp_dir.resolve()

    # Create user project directory
    user_project = temp_dir / "my-project"
    user_project.mkdir()
    workspaces_dir = user_project / "workspaces"
    workspaces_dir.mkdir()

    # Create dev repo with git
    dev_repo = temp_dir / "my-project-dev"
    dev_repo.mkdir()

    # Initialize git repo
    subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Ensure branch is named 'main' (git default may vary)
    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create initial structure and commit
    (dev_repo / "code").mkdir()
    (dev_repo / "code" / "main.py").write_text("# Main module\nprint('hello')")
    (dev_repo / "code" / "utils.py").write_text("# Utilities\ndef helper(): pass")
    (dev_repo / "README.md").write_text("# My Project\n\nA test project.")

    subprocess.run(["git", "add", "."], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create .goldfish directory in dev repo
    goldfish_dir = dev_repo / ".goldfish"
    goldfish_dir.mkdir()

    # Create database
    db = Database(goldfish_dir / "goldfish.db")

    # Create config - use relative path from user_project parent
    config = GoldfishConfig(
        project_name="my-project",
        dev_repo_path="my-project-dev",  # Relative to user_project.parent
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )

    # Create manager
    manager = WorkspaceManager(
        config=config,
        project_root=user_project,
        db=db,
    )

    return {
        "user_project": user_project,
        "dev_repo": dev_repo,
        "workspaces_dir": workspaces_dir,
        "db": db,
        "config": config,
        "manager": manager,
        "git": manager.git,
    }


class TestCopyBasedMount:
    """Tests for copy-based mount() operation."""

    def test_mount_creates_goldfish_mount_metadata(self, copy_mount_setup):
        """Mount should create .goldfish-mount metadata file in slot."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create workspace
        manager.create_workspace(
            name="feature-a",
            goal="Test feature A",
            reason="Testing copy-based mounting",
        )

        # Mount workspace
        result = manager.mount(
            workspace="feature-a",
            slot="w1",
            reason="Testing mount metadata creation",
        )

        assert result.success is True

        # Check .goldfish-mount exists
        slot_path = workspaces_dir / "w1"
        metadata_file = slot_path / ".goldfish-mount"
        assert metadata_file.exists(), ".goldfish-mount metadata file should exist"

        # Check metadata content
        metadata = json.loads(metadata_file.read_text())
        assert metadata["workspace_name"] == "feature-a"
        # Branch includes goldfish/ prefix (Goldfish convention for scalability)
        assert metadata["branch"] == "goldfish/feature-a"
        assert "mounted_sha" in metadata
        assert "mounted_at" in metadata

    def test_mount_copies_files_from_branch(self, copy_mount_setup):
        """Mount should copy all tracked files from branch to slot."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create workspace
        manager.create_workspace(
            name="feature-b",
            goal="Test feature B",
            reason="Testing file copy on mount",
        )

        # Mount workspace
        manager.mount(
            workspace="feature-b",
            slot="w1",
            reason="Testing file copying from branch",
        )

        # Check files were copied
        slot_path = workspaces_dir / "w1"
        assert (slot_path / "code" / "main.py").exists()
        assert (slot_path / "code" / "utils.py").exists()
        assert (slot_path / "README.md").exists()

        # Check file contents are correct
        assert "hello" in (slot_path / "code" / "main.py").read_text()
        assert "helper" in (slot_path / "code" / "utils.py").read_text()

    def test_mount_no_git_directory_in_slot(self, copy_mount_setup):
        """Slot should NOT have .git directory - it's plain files only."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace(
            name="feature-c",
            goal="Test no .git",
            reason="Verifying no git in slot",
        )
        manager.mount(
            workspace="feature-c",
            slot="w1",
            reason="Testing that slot has no .git dir",
        )

        # Check NO .git directory
        slot_path = workspaces_dir / "w1"
        assert not (slot_path / ".git").exists(), "Slot should NOT have .git directory"

    def test_mount_records_in_database(self, copy_mount_setup):
        """Mount should record the mount in workspace_mounts table."""
        manager = copy_mount_setup["manager"]
        db = copy_mount_setup["db"]

        # Create and mount workspace
        manager.create_workspace(
            name="feature-d",
            goal="Test DB recording",
            reason="Testing mount database recording",
        )
        manager.mount(
            workspace="feature-d",
            slot="w1",
            reason="Testing database mount recording",
        )

        # Check database record
        mount = db.get_mount("w1")
        assert mount is not None, "Mount should be recorded in database"
        assert mount["workspace_name"] == "feature-d"
        assert mount["slot"] == "w1"
        assert mount["status"] == "active"
        assert mount["mounted_sha"] is not None

    def test_mount_rejects_workspace_already_mounted_elsewhere(self, copy_mount_setup):
        """Mount should reject if workspace is already mounted in another slot."""
        manager = copy_mount_setup["manager"]

        # Create and mount workspace to w1
        manager.create_workspace(
            name="feature-e",
            goal="Test double mount rejection",
            reason="Testing mount rejection for already-mounted workspace",
        )
        manager.mount(
            workspace="feature-e",
            slot="w1",
            reason="First mount of workspace to w1",
        )

        # Try to mount same workspace to w2 - should fail
        with pytest.raises(GoldfishError) as exc_info:
            manager.mount(
                workspace="feature-e",
                slot="w2",
                reason="Attempting second mount to w2",
            )

        assert "already mounted" in str(exc_info.value).lower()

    def test_mount_to_occupied_slot_fails(self, copy_mount_setup):
        """Mount should fail if slot already has a workspace."""
        manager = copy_mount_setup["manager"]

        # Create two workspaces
        manager.create_workspace(
            name="workspace-1",
            goal="First workspace",
            reason="Creating first workspace for slot test",
        )
        manager.create_workspace(
            name="workspace-2",
            goal="Second workspace",
            reason="Creating second workspace for slot test",
        )

        # Mount first to w1
        manager.mount(
            workspace="workspace-1",
            slot="w1",
            reason="Mounting first workspace to w1",
        )

        # Try to mount second to same slot - should fail
        with pytest.raises(SlotNotEmptyError):
            manager.mount(
                workspace="workspace-2",
                slot="w1",
                reason="Attempting to mount to occupied slot",
            )


class TestCopyBasedHibernate:
    """Tests for copy-based hibernate() operation."""

    def test_hibernate_syncs_changes_to_branch(self, copy_mount_setup):
        """Hibernate should sync changes back to the git branch."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]
        git = copy_mount_setup["git"]
        dev_repo = copy_mount_setup["dev_repo"]

        # Create, mount, and modify
        manager.create_workspace(
            name="feature-sync",
            goal="Test sync on hibernate",
            reason="Testing hibernate sync functionality",
        )
        manager.mount(
            workspace="feature-sync",
            slot="w1",
            reason="Mounting for hibernate sync test",
        )

        # Make changes in the slot
        slot_path = workspaces_dir / "w1"
        (slot_path / "new_file.py").write_text("# New feature code")
        (slot_path / "code" / "main.py").write_text("# Modified main")

        # Hibernate
        result = manager.hibernate(
            slot="w1",
            reason="Testing sync back to branch",
        )

        assert result.success is True

        # Verify changes were synced to branch by checking out the branch
        # Use git show to verify without needing a worktree
        def git_show(path: str) -> str:
            res = subprocess.run(
                ["git", "show", f"goldfish/feature-sync:{path}"],
                cwd=dev_repo,
                capture_output=True,
                text=True,
                check=True,
            )
            return res.stdout

        assert git_show("new_file.py") == "# New feature code"
        assert "Modified main" in git_show("code/main.py")

    def test_hibernate_removes_slot_directory(self, copy_mount_setup):
        """Hibernate should remove the slot directory."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create and mount
        manager.create_workspace(
            name="feature-remove",
            goal="Test slot removal",
            reason="Testing slot directory removal",
        )
        manager.mount(
            workspace="feature-remove",
            slot="w1",
            reason="Mounting for removal test",
        )

        slot_path = workspaces_dir / "w1"
        assert slot_path.exists()

        # Hibernate
        manager.hibernate(
            slot="w1",
            reason="Testing slot directory removal on hibernate",
        )

        # Slot should be removed
        assert not slot_path.exists(), "Slot directory should be removed after hibernate"

    def test_hibernate_deletes_mount_from_database(self, copy_mount_setup):
        """Hibernate should delete mount record from database."""
        manager = copy_mount_setup["manager"]
        db = copy_mount_setup["db"]

        # Create and mount
        manager.create_workspace(
            name="feature-dbdel",
            goal="Test DB deletion",
            reason="Testing database mount deletion",
        )
        manager.mount(
            workspace="feature-dbdel",
            slot="w1",
            reason="Mounting for DB deletion test",
        )

        # Verify mount exists in DB
        assert db.get_mount("w1") is not None

        # Hibernate
        manager.hibernate(
            slot="w1",
            reason="Testing mount deletion from database",
        )

        # Mount should be deleted from DB
        assert db.get_mount("w1") is None, "Mount record should be deleted after hibernate"

    def test_hibernate_handles_file_deletions(self, copy_mount_setup):
        """Hibernate should sync file deletions back to branch."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]
        dev_repo = copy_mount_setup["dev_repo"]

        # Create, mount
        manager.create_workspace(
            name="feature-del",
            goal="Test deletion sync",
            reason="Testing file deletion synchronization",
        )
        manager.mount(
            workspace="feature-del",
            slot="w1",
            reason="Mounting for deletion sync test",
        )

        # Delete a file in the slot
        slot_path = workspaces_dir / "w1"
        utils_file = slot_path / "code" / "utils.py"
        assert utils_file.exists()
        utils_file.unlink()

        # Hibernate
        manager.hibernate(
            slot="w1",
            reason="Testing deleted file sync to branch",
        )

        # Verify deletion synced to branch (branch has goldfish/ prefix)
        # Use git ls-tree to verify file is gone without needing a worktree
        res = subprocess.run(
            ["git", "ls-tree", "-r", "goldfish/feature-del", "--name-only"],
            cwd=dev_repo,
            capture_output=True,
            text=True,
            check=True,
        )
        files = res.stdout.strip().split("\n")
        assert "code/utils.py" not in files, "Deleted file should be removed from branch"


class TestCopyBasedRoundTrip:
    """Tests for mount/hibernate/re-mount cycle preserving changes."""

    def test_changes_survive_hibernate_remount_cycle(self, copy_mount_setup):
        """Changes made in slot should survive hibernate and re-mount."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create and mount
        manager.create_workspace(
            name="feature-roundtrip",
            goal="Test round-trip",
            reason="Testing mount/hibernate/remount cycle",
        )
        manager.mount(
            workspace="feature-roundtrip",
            slot="w1",
            reason="First mount for round-trip test",
        )

        # Make changes
        slot_path = workspaces_dir / "w1"
        (slot_path / "my_feature.py").write_text("# My awesome feature\nclass Feature: pass")
        (slot_path / "code" / "main.py").write_text("# Updated main\nimport my_feature")

        # Hibernate
        manager.hibernate(
            slot="w1",
            reason="Hibernating to test round-trip",
        )

        # Re-mount (could be to different slot)
        manager.mount(
            workspace="feature-roundtrip",
            slot="w2",
            reason="Re-mounting to verify changes preserved",
        )

        # Verify changes are there
        slot_path_w2 = workspaces_dir / "w2"
        assert (slot_path_w2 / "my_feature.py").exists()
        assert "My awesome feature" in (slot_path_w2 / "my_feature.py").read_text()
        assert "Updated main" in (slot_path_w2 / "code" / "main.py").read_text()

        # Cleanup
        manager.hibernate(
            slot="w2",
            reason="Final cleanup after round-trip test",
        )

    def test_slot_state_reflects_mount_status(self, copy_mount_setup):
        """Slot state should correctly reflect mounted status."""
        manager = copy_mount_setup["manager"]

        # Initially empty
        slot_info = manager.get_slot_info("w1")
        assert slot_info.state == SlotState.EMPTY

        # Create and mount
        manager.create_workspace(
            name="feature-state",
            goal="Test slot state",
            reason="Testing slot state reflection",
        )
        manager.mount(
            workspace="feature-state",
            slot="w1",
            reason="Mounting to test state reflection",
        )

        # Should be mounted
        slot_info = manager.get_slot_info("w1")
        assert slot_info.state == SlotState.MOUNTED
        assert slot_info.workspace == "feature-state"

        # Hibernate
        manager.hibernate(
            slot="w1",
            reason="Hibernating to test state change",
        )

        # Should be empty again
        slot_info = manager.get_slot_info("w1")
        assert slot_info.state == SlotState.EMPTY


class TestCopySafetyGuards:
    """Tests for safety guards in copy-based mounting."""

    def test_mount_rejects_non_empty_non_goldfish_directory(self, copy_mount_setup):
        """Mount should refuse to overwrite non-Goldfish directory."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]

        # Create workspace
        manager.create_workspace(
            name="feature-safe",
            goal="Test safety guard",
            reason="Testing safety guard for non-empty directory",
        )

        # Pre-create slot with non-Goldfish content
        slot_path = workspaces_dir / "w1"
        slot_path.mkdir(exist_ok=True)
        (slot_path / "important_user_file.txt").write_text("DO NOT OVERWRITE ME")

        # Mount should fail with safety error
        with pytest.raises(GoldfishError) as exc_info:
            manager.mount(
                workspace="feature-safe",
                slot="w1",
                reason="Attempting mount to non-Goldfish directory",
            )

        assert "not empty" in str(exc_info.value).lower() or "data loss" in str(exc_info.value).lower()

        # Original file should still be there
        assert (slot_path / "important_user_file.txt").exists()

    def test_mount_can_remount_to_existing_goldfish_slot(self, copy_mount_setup):
        """Mount should succeed if slot has leftover .goldfish-mount from crash."""
        manager = copy_mount_setup["manager"]
        workspaces_dir = copy_mount_setup["workspaces_dir"]
        db = copy_mount_setup["db"]

        # Create workspace
        manager.create_workspace(
            name="feature-recovery",
            goal="Test crash recovery",
            reason="Testing remount after simulated crash",
        )

        # Simulate crashed mount - create slot with metadata but no DB record
        slot_path = workspaces_dir / "w1"
        slot_path.mkdir(exist_ok=True)
        (slot_path / ".goldfish-mount").write_text(
            json.dumps(
                {
                    "workspace_name": "feature-recovery",
                    "branch": "feature-recovery",
                    "mounted_sha": "abc123",
                    "mounted_at": "2024-01-01T00:00:00",
                }
            )
        )
        (slot_path / "some_file.txt").write_text("leftover")

        # Mount should work (overwrite crashed state)
        result = manager.mount(
            workspace="feature-recovery",
            slot="w1",
            reason="Remounting after simulated crash",
        )

        assert result.success is True

        # Cleanup
        manager.hibernate(
            slot="w1",
            reason="Cleaning up crash recovery test",
        )
