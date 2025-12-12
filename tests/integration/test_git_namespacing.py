"""Tests for git ref namespacing.

The goldfish/* namespace allows scaling to 1000+ workspaces without
performance degradation from excessive branch count under experiment/*.
"""

import json

import pytest

from goldfish.workspace.git_layer import GitLayer


@pytest.fixture
def git_layer(temp_git_repo, tmp_path):
    """Create GitLayer with temp git repo."""
    workspaces_dir = tmp_path / "workspaces"
    workspaces_dir.mkdir()
    return GitLayer(
        dev_repo_path=temp_git_repo,
        project_root=tmp_path,
        workspaces_dir="workspaces",
    )


class TestBranchNamingConvention:
    """Verify branches use goldfish/ prefix."""

    def test_create_workspace_uses_goldfish_prefix(self, git_layer, temp_git_repo):
        """New workspace branches should be created under goldfish/*."""
        git_layer.create_branch("my_experiment")

        # Verify branch exists with goldfish/ prefix
        import subprocess

        result = subprocess.run(
            ["git", "branch", "--list", "goldfish/*"],
            cwd=temp_git_repo,
            capture_output=True,
            text=True,
        )
        assert "goldfish/my_experiment" in result.stdout

    def test_branch_exists_checks_goldfish_namespace(self, git_layer):
        """branch_exists should check goldfish/* namespace."""
        # Create branch
        git_layer.create_branch("test_ws")

        # Should find it
        assert git_layer.branch_exists("test_ws") is True

        # Non-existent should return False
        assert git_layer.branch_exists("nonexistent") is False

    def test_delete_branch_removes_goldfish_namespace(self, git_layer, temp_git_repo):
        """delete_branch should remove goldfish/* branch."""
        git_layer.create_branch("to_delete")
        assert git_layer.branch_exists("to_delete") is True

        git_layer.delete_branch("to_delete", force=True)

        # Verify branch is gone
        import subprocess

        result = subprocess.run(
            ["git", "branch", "--list", "goldfish/to_delete"],
            cwd=temp_git_repo,
            capture_output=True,
            text=True,
        )
        assert "goldfish/to_delete" not in result.stdout

    def test_list_branches_returns_workspace_names(self, git_layer):
        """list_branches should return workspace names without prefix."""
        git_layer.create_branch("ws_one")
        git_layer.create_branch("ws_two")

        branches = git_layer.list_branches()

        # Should return clean workspace names
        assert "ws_one" in branches
        assert "ws_two" in branches
        # Should NOT include the prefix
        assert "goldfish/ws_one" not in branches


class TestMountWithNewNamespace:
    """Verify mount operations work with goldfish/ namespace."""

    def test_add_worktree_uses_goldfish_branch(self, git_layer, tmp_path, temp_git_repo):
        """Worktree should reference goldfish/* branch."""
        import subprocess

        git_layer.create_branch("mount_test")
        slot_path = tmp_path / "workspaces" / "w1"

        git_layer.add_worktree("mount_test", slot_path)

        # Verify worktree points to goldfish branch
        result = subprocess.run(
            ["git", "worktree", "list", "--porcelain"],
            cwd=temp_git_repo,
            capture_output=True,
            text=True,
        )
        assert "goldfish/mount_test" in result.stdout

        # Cleanup
        git_layer.remove_worktree(slot_path, force=True)

    def test_copy_mount_uses_goldfish_branch(self, git_layer, tmp_path):
        """Copy-based mount should work with goldfish/* namespace."""
        git_layer.create_branch("copy_mount_test")
        slot_path = tmp_path / "workspaces" / "w2"

        metadata = git_layer.copy_mount_workspace("copy_mount_test", slot_path)

        # Verify metadata records goldfish branch
        assert metadata["branch"] == "goldfish/copy_mount_test"
        assert metadata["workspace_name"] == "copy_mount_test"

        # Verify metadata file
        meta_file = slot_path / ".goldfish-mount"
        assert meta_file.exists()
        saved_meta = json.loads(meta_file.read_text())
        assert saved_meta["branch"] == "goldfish/copy_mount_test"


class TestSyncWithNewNamespace:
    """Verify sync operations work with goldfish/ namespace."""

    def test_sync_slot_to_branch_works_with_goldfish_namespace(self, git_layer, tmp_path):
        """sync_slot_to_branch should work with goldfish/* branches."""
        git_layer.create_branch("sync_test")
        slot_path = tmp_path / "workspaces" / "w3"

        # Mount and create a file
        metadata = git_layer.copy_mount_workspace("sync_test", slot_path)
        (slot_path / "new_file.py").write_text("print('hello')")

        # Sync should succeed
        new_sha = git_layer.sync_slot_to_branch(slot_path, "sync_test", "Add new file")

        # SHA should change
        assert new_sha != metadata["mounted_sha"]

    def test_create_snapshot_copy_based_works_with_goldfish_namespace(self, git_layer, tmp_path):
        """Snapshot creation should work with goldfish/* branches."""
        git_layer.create_branch("snapshot_test")
        slot_path = tmp_path / "workspaces" / "w4"

        git_layer.copy_mount_workspace("snapshot_test", slot_path)
        (slot_path / "model.py").write_text("# model code")

        snapshot_id, git_sha = git_layer.create_snapshot_copy_based(slot_path, "snapshot_test", "Checkpoint model")

        # Snapshot should be created
        assert snapshot_id.startswith("snap-")
        assert len(git_sha) == 40  # Full SHA


class TestHibernateWithNewNamespace:
    """Verify hibernate operations work with goldfish/ namespace."""

    def test_remove_worktree_works_with_goldfish_namespace(self, git_layer, tmp_path, temp_git_repo):
        """Worktree removal should work with goldfish/* branches."""
        git_layer.create_branch("hibernate_test")
        slot_path = tmp_path / "workspaces" / "w5"

        git_layer.add_worktree("hibernate_test", slot_path)
        assert slot_path.exists()

        git_layer.remove_worktree(slot_path, force=True)
        assert not slot_path.exists()


class TestListFilteringWithNewNamespace:
    """Verify list operations filter correctly."""

    def test_list_snapshots_filters_goldfish_branches(self, git_layer, tmp_path):
        """list_snapshots should work with goldfish/* branches."""
        git_layer.create_branch("list_snap_test")
        slot_path = tmp_path / "workspaces" / "w6"

        git_layer.copy_mount_workspace("list_snap_test", slot_path)
        (slot_path / "code.py").write_text("# code")

        git_layer.create_snapshot_copy_based(slot_path, "list_snap_test", "Snap 1")
        (slot_path / "code.py").write_text("# updated code")
        git_layer.create_snapshot_copy_based(slot_path, "list_snap_test", "Snap 2")

        snapshots = git_layer.list_snapshots("list_snap_test")
        assert len(snapshots) >= 2

    def test_get_branch_info_works_with_goldfish_namespace(self, git_layer, tmp_path):
        """get_branch_info should work with goldfish/* branches."""
        git_layer.create_branch("info_test")
        slot_path = tmp_path / "workspaces" / "w7"

        git_layer.copy_mount_workspace("info_test", slot_path)
        (slot_path / "file.txt").write_text("content")
        git_layer.create_snapshot_copy_based(slot_path, "info_test", "Test snapshot")

        info = git_layer.get_branch_info("info_test")

        # Should return metadata
        assert "created_at" in info
        assert "last_activity" in info
        assert "snapshot_count" in info
