"""Tests for git divergence detection in sync operations.

These tests verify that Goldfish correctly handles scenarios where the workspace
branch moves while a slot is mounted:

1. No change: Branch SHA matches mount SHA → proceed
2. Forward move: Branch advanced (mount SHA is ancestor) → update metadata and proceed
3. True divergence: Branch changed incompatibly → reject with error
"""

import json
import pathlib
import subprocess

import pytest

from goldfish.errors import GoldfishError
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


def make_commit(repo_path, message="test commit", branch=None):
    """Helper to make a commit in a repo on a specific branch.

    Uses a temporary worktree to avoid conflicts with the main worktree.
    """
    import tempfile

    if branch:
        # Use a temp worktree to make commits without affecting main worktree
        with tempfile.TemporaryDirectory() as tmp_dir:
            worktree_path = tmp_dir
            subprocess.run(
                ["git", "worktree", "add", worktree_path, branch],
                cwd=repo_path,
                check=True,
                capture_output=True,
            )

            # Create or modify a file in the worktree
            test_file = (
                (repo_path.parent / worktree_path / "test_file.txt")
                if not str(worktree_path).startswith("/")
                else (pathlib.Path(worktree_path) / "test_file.txt")
            )
            test_file = pathlib.Path(worktree_path) / "test_file.txt"
            current_content = test_file.read_text() if test_file.exists() else ""
            test_file.write_text(current_content + f"\n{message}")

            subprocess.run(["git", "add", "-A"], cwd=worktree_path, check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", message],
                cwd=worktree_path,
                check=True,
                capture_output=True,
            )

            # Get the new SHA
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=worktree_path,
                capture_output=True,
                text=True,
                check=True,
            )
            new_sha = result.stdout.strip()

            # Remove worktree
            subprocess.run(
                ["git", "worktree", "remove", "--force", worktree_path],
                cwd=repo_path,
                check=True,
                capture_output=True,
            )

            return new_sha
    else:
        # Original behavior for main worktree
        test_file = repo_path / "test_file.txt"
        current_content = test_file.read_text() if test_file.exists() else ""
        test_file.write_text(current_content + f"\n{message}")

        subprocess.run(["git", "add", "-A"], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", message],
            cwd=repo_path,
            check=True,
            capture_output=True,
        )

        # Get the new SHA
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()


class TestIsAncestor:
    """Tests for the is_ancestor helper method."""

    def test_is_ancestor_returns_true_for_parent_child(self, git_layer, temp_git_repo):
        """Parent commit should be ancestor of child commit."""
        git_layer.create_branch("ancestor_test")

        # Get initial SHA
        parent_sha = git_layer.get_head_sha_from_branch("goldfish/ancestor_test")

        # Make a commit on the branch using worktree
        child_sha = make_commit(temp_git_repo, "child commit", branch="goldfish/ancestor_test")

        # Parent should be ancestor of child
        assert git_layer.is_ancestor(parent_sha, child_sha) is True

    def test_is_ancestor_returns_false_for_child_parent(self, git_layer, temp_git_repo):
        """Child commit should NOT be ancestor of parent commit."""
        git_layer.create_branch("ancestor_test2")

        parent_sha = git_layer.get_head_sha_from_branch("goldfish/ancestor_test2")

        # Make a commit on the branch using worktree
        child_sha = make_commit(temp_git_repo, "child commit", branch="goldfish/ancestor_test2")

        # Child should NOT be ancestor of parent
        assert git_layer.is_ancestor(child_sha, parent_sha) is False

    def test_is_ancestor_returns_true_for_same_commit(self, git_layer, temp_git_repo):
        """A commit is considered its own ancestor."""
        git_layer.create_branch("ancestor_test3")
        sha = git_layer.get_head_sha_from_branch("goldfish/ancestor_test3")

        # Same commit should be its own ancestor
        assert git_layer.is_ancestor(sha, sha) is True

    def test_is_ancestor_returns_false_for_diverged_commits(self, git_layer, temp_git_repo):
        """Commits on diverged branches should not be ancestors of each other."""
        git_layer.create_branch("diverge_base")

        # Get base SHA
        base_sha = git_layer.get_head_sha_from_branch("goldfish/diverge_base")

        # Create two branches from same point (using git branch, not checkout)
        subprocess.run(
            ["git", "branch", "goldfish/diverge_a", "goldfish/diverge_base"],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
        )
        sha_a = make_commit(temp_git_repo, "commit on branch A", branch="goldfish/diverge_a")

        subprocess.run(
            ["git", "branch", "goldfish/diverge_b", "goldfish/diverge_base"],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
        )
        sha_b = make_commit(temp_git_repo, "commit on branch B", branch="goldfish/diverge_b")

        # Neither should be ancestor of the other
        assert git_layer.is_ancestor(sha_a, sha_b) is False
        assert git_layer.is_ancestor(sha_b, sha_a) is False

        # But base should be ancestor of both
        assert git_layer.is_ancestor(base_sha, sha_a) is True
        assert git_layer.is_ancestor(base_sha, sha_b) is True


class TestSyncDivergenceDetection:
    """Tests for divergence detection in sync_slot_to_branch."""

    def test_sync_succeeds_when_branch_unchanged(self, git_layer, tmp_path):
        """Sync should succeed when branch SHA matches mount SHA."""
        git_layer.create_branch("sync_unchanged")
        slot_path = tmp_path / "workspaces" / "w1"

        # Mount workspace
        metadata = git_layer.copy_mount_workspace("sync_unchanged", slot_path)
        original_sha = metadata["mounted_sha"]

        # Make a local change
        (slot_path / "local_file.py").write_text("# local change")

        # Sync should succeed
        new_sha = git_layer.sync_slot_to_branch(slot_path, "sync_unchanged", "Local change")

        # SHA should change (we made a commit)
        assert new_sha != original_sha

    def test_sync_succeeds_when_branch_moved_forward(self, git_layer, tmp_path, temp_git_repo):
        """Sync should succeed when branch moved forward (mount SHA is ancestor of current)."""
        git_layer.create_branch("sync_forward")
        slot_path = tmp_path / "workspaces" / "w2"

        # Mount workspace
        metadata = git_layer.copy_mount_workspace("sync_forward", slot_path)
        original_mount_sha = metadata["mounted_sha"]

        # Simulate another process committing to the branch (forward move)
        # Use worktree to avoid checkout conflicts
        external_sha = make_commit(temp_git_repo, "External commit (forward move)", branch="goldfish/sync_forward")

        # Branch has moved forward
        current_branch_sha = git_layer.get_head_sha_from_branch("goldfish/sync_forward")
        assert current_branch_sha == external_sha
        assert current_branch_sha != original_mount_sha

        # Make a local change
        (slot_path / "local_file.py").write_text("# local change after forward move")

        # Sync should succeed (forward move is safe)
        new_sha = git_layer.sync_slot_to_branch(slot_path, "sync_forward", "Local change after forward")

        # Verify metadata was updated
        updated_metadata = json.loads((slot_path / ".goldfish-mount").read_text())
        # The mounted_sha should have been updated during sync
        assert updated_metadata["mounted_sha"] != original_mount_sha

    def test_sync_fails_on_true_divergence(self, git_layer, tmp_path, temp_git_repo):
        """Sync should fail when branch truly diverged (mount SHA not ancestor of current)."""
        git_layer.create_branch("sync_diverge")
        slot_path = tmp_path / "workspaces" / "w3"

        # First, make a commit on the branch so it diverges from main
        make_commit(temp_git_repo, "Initial branch commit", branch="goldfish/sync_diverge")

        # Mount workspace
        metadata = git_layer.copy_mount_workspace("sync_diverge", slot_path)
        original_mount_sha = metadata["mounted_sha"]

        # Create true divergence by making an orphan commit (no parent)
        # This simulates force-pushing a completely rewritten history
        result = subprocess.run(
            ["git", "commit-tree", "-m", "Orphan commit", "HEAD^{tree}"],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
            text=True,
        )
        orphan_sha = result.stdout.strip()

        # Force reset sync_diverge to point to this orphan commit
        subprocess.run(
            ["git", "branch", "-f", "goldfish/sync_diverge", orphan_sha],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
        )

        # Verify the branch moved to an unrelated commit (orphan has no ancestors)
        current_branch_sha = git_layer.get_head_sha_from_branch("goldfish/sync_diverge")
        assert current_branch_sha != original_mount_sha
        assert not git_layer.is_ancestor(original_mount_sha, current_branch_sha)

        # Sync should fail with divergence error
        with pytest.raises(GoldfishError) as exc_info:
            git_layer.sync_slot_to_branch(slot_path, "sync_diverge", "Should fail")

        assert "diverged" in str(exc_info.value).lower()
        assert "not a forward move" in str(exc_info.value).lower()

    def test_sync_updates_metadata_on_forward_move(self, git_layer, tmp_path, temp_git_repo):
        """Verify metadata file is updated when branch moved forward."""
        git_layer.create_branch("sync_meta_update")
        slot_path = tmp_path / "workspaces" / "w4"

        # Mount workspace
        git_layer.copy_mount_workspace("sync_meta_update", slot_path)
        original_metadata = json.loads((slot_path / ".goldfish-mount").read_text())
        original_sha = original_metadata["mounted_sha"]

        # Simulate forward move using worktree
        make_commit(temp_git_repo, "External forward commit", branch="goldfish/sync_meta_update")

        # Make a local change and sync
        (slot_path / "file.py").write_text("# content")
        git_layer.sync_slot_to_branch(slot_path, "sync_meta_update", "After forward move")

        # Metadata should be updated
        updated_metadata = json.loads((slot_path / ".goldfish-mount").read_text())
        assert updated_metadata["mounted_sha"] != original_sha
        # The metadata should now reflect the new branch state
        assert updated_metadata["workspace_name"] == "sync_meta_update"


class TestSyncNoChanges:
    """Tests for sync when no local changes were made."""

    def test_sync_returns_same_sha_when_no_changes(self, git_layer, tmp_path):
        """Sync should return existing SHA when no changes were made."""
        git_layer.create_branch("sync_no_change")
        slot_path = tmp_path / "workspaces" / "w5"

        metadata = git_layer.copy_mount_workspace("sync_no_change", slot_path)
        original_sha = metadata["mounted_sha"]

        # Sync without making any changes
        result_sha = git_layer.sync_slot_to_branch(slot_path, "sync_no_change", "No changes")

        # Should return the same SHA
        assert result_sha == original_sha

    def test_sync_creates_commit_after_forward_move_even_without_local_changes(
        self, git_layer, tmp_path, temp_git_repo
    ):
        """Sync after forward move creates new commit because slot lacks external changes.

        When the branch moved forward with external changes (e.g., new files),
        but the slot doesn't have those changes, syncing will create a commit
        that reverts those external changes (since sync mirrors slot → branch).

        This is expected behavior - the slot is the source of truth for content.
        """
        git_layer.create_branch("sync_forward_no_local")
        slot_path = tmp_path / "workspaces" / "w6"

        git_layer.copy_mount_workspace("sync_forward_no_local", slot_path)

        # Simulate forward move with external changes using worktree
        external_sha = make_commit(temp_git_repo, "External commit", branch="goldfish/sync_forward_no_local")

        # Sync without local changes - will create a NEW commit because
        # slot lacks the external changes (test_file.txt from make_commit)
        result_sha = git_layer.sync_slot_to_branch(slot_path, "sync_forward_no_local", "Sync from slot")

        # Should create a new commit on top of the external commit
        # (reverting the external changes since slot doesn't have them)
        assert result_sha != external_sha
        # The new commit should descend from the external commit
        assert git_layer.is_ancestor(external_sha, result_sha)


class TestMultipleForwardMoves:
    """Tests for handling multiple sequential forward moves."""

    def test_sync_handles_multiple_forward_commits(self, git_layer, tmp_path, temp_git_repo):
        """Sync should handle branch that moved forward multiple commits."""
        git_layer.create_branch("sync_multi_forward")
        slot_path = tmp_path / "workspaces" / "w7"

        metadata = git_layer.copy_mount_workspace("sync_multi_forward", slot_path)
        original_sha = metadata["mounted_sha"]

        # Make multiple external commits using worktree
        make_commit(temp_git_repo, "External commit 1", branch="goldfish/sync_multi_forward")
        make_commit(temp_git_repo, "External commit 2", branch="goldfish/sync_multi_forward")
        final_external_sha = make_commit(temp_git_repo, "External commit 3", branch="goldfish/sync_multi_forward")

        # Verify branch moved forward by 3 commits
        current_sha = git_layer.get_head_sha_from_branch("goldfish/sync_multi_forward")
        assert current_sha == final_external_sha
        assert git_layer.is_ancestor(original_sha, current_sha)

        # Make local change and sync
        (slot_path / "local.py").write_text("# local")
        new_sha = git_layer.sync_slot_to_branch(slot_path, "sync_multi_forward", "After 3 forward commits")

        # Should succeed
        assert new_sha != original_sha
