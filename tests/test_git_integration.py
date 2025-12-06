"""Real git integration tests - P1.

These tests use actual git repositories to verify git_layer operations.
No mocks - real git commands on real (temporary) repos.
"""

import subprocess
from pathlib import Path

import pytest

from goldfish.workspace.git_layer import GitLayer
from goldfish.errors import GoldfishError


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
def git_setup(temp_dir):
    """Create a real git repository setup for testing.

    Returns:
        Tuple of (dev_repo_path, project_root, GitLayer instance)
    """
    # Create dev repo (the git repository)
    dev_repo = temp_dir / "project-dev"
    dev_repo.mkdir()

    # Initialize a git repo
    run_git(["init"], dev_repo)
    run_git(["config", "user.email", "test@example.com"], dev_repo)
    run_git(["config", "user.name", "Test User"], dev_repo)

    # Create initial commit
    (dev_repo / "README.md").write_text("# Test Repo")
    run_git(["add", "."], dev_repo)
    run_git(["commit", "-m", "Initial commit"], dev_repo)

    # Create project root
    project_root = temp_dir / "project"
    project_root.mkdir()
    (project_root / "workspaces").mkdir()

    # Create GitLayer
    git = GitLayer(dev_repo, project_root, "workspaces")

    return dev_repo, project_root, git


# Legacy fixture for backward compatibility
@pytest.fixture
def git_repo(git_setup):
    """Return just the dev repo path."""
    return git_setup[0]


class TestGitLayerBranchOperations:
    """Tests for branch operations with real git."""

    def test_branch_exists_returns_true_for_existing_branch(self, git_setup):
        """branch_exists should return True for created workspace branch."""
        dev_repo, project_root, git = git_setup

        # Get current branch to use as base
        current = run_git(["branch", "--show-current"], dev_repo)

        # Create a workspace branch (GitLayer adds experiment/ prefix)
        git.create_branch("test-workspace", from_ref=current)

        # Now it should exist
        assert git.branch_exists("test-workspace")

    def test_branch_exists_returns_false_for_nonexistent_branch(self, git_setup):
        """branch_exists should return False for nonexistent branch."""
        dev_repo, project_root, git = git_setup

        assert git.branch_exists("nonexistent-branch") is False

    def test_create_branch_creates_new_branch(self, git_setup):
        """create_branch should create a new branch."""
        dev_repo, project_root, git = git_setup

        # Get current branch name
        current = run_git(["branch", "--show-current"], dev_repo)

        # Create new branch (don't include experiment/ - GitLayer adds it)
        git.create_branch("new-feature", from_ref=current)

        # Verify branch exists
        assert git.branch_exists("new-feature")

        # Verify the actual branch name in git
        branches = run_git(["branch", "--list"], dev_repo)
        assert "experiment/new-feature" in branches

    def test_create_branch_from_specific_ref(self, git_setup):
        """create_branch should create from specified ref."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)

        # Make a second commit
        (dev_repo / "file2.txt").write_text("content")
        run_git(["add", "."], dev_repo)
        run_git(["commit", "-m", "Second commit"], dev_repo)

        # Get the first commit SHA
        first_sha = run_git(["rev-list", "--max-parents=0", "HEAD"], dev_repo)

        # Create branch from first commit (don't include experiment/)
        git.create_branch("from-first", from_ref=first_sha[:7])

        # Branch should exist
        assert git.branch_exists("from-first")


class TestGitLayerWorktreeOperations:
    """Tests for worktree operations with real git."""

    def test_add_worktree_creates_worktree(self, git_setup):
        """add_worktree should create a new worktree."""
        dev_repo, project_root, git = git_setup

        # Create branch first (don't include experiment/)
        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("test-ws", from_ref=current)

        # Add worktree - first arg is workspace_name, second is slot_path
        worktree_path = project_root / "workspaces" / "w1"
        git.add_worktree("test-ws", worktree_path)

        # Worktree should exist and have files
        assert worktree_path.exists()
        assert (worktree_path / "README.md").exists()

        # Cleanup
        git.remove_worktree(worktree_path)

    def test_remove_worktree_removes_worktree(self, git_setup):
        """remove_worktree should remove the worktree."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("remove-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "w2"
        git.add_worktree("remove-test", worktree_path)

        # Remove the worktree
        git.remove_worktree(worktree_path)

        # Directory should be gone
        assert not worktree_path.exists()

    def test_worktree_is_isolated(self, git_setup):
        """Changes in worktree should not affect main repo."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("isolated", from_ref=current)

        worktree_path = project_root / "workspaces" / "isolated"
        git.add_worktree("isolated", worktree_path)

        # Make changes in worktree
        (worktree_path / "new_file.txt").write_text("worktree content")

        # Main repo should not have the file
        assert not (dev_repo / "new_file.txt").exists()

        # Cleanup (force=True because we have untracked files)
        git.remove_worktree(worktree_path, force=True)


class TestGitLayerSnapshotOperations:
    """Tests for snapshot (tag) operations with real git."""

    def test_create_snapshot_creates_tag(self, git_setup):
        """create_snapshot should create a snapshot tag."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("snap-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "snap"
        git.add_worktree("snap-test", worktree_path)

        try:
            # Make a change
            (worktree_path / "code.py").write_text("print('hello')")

            # Create snapshot
            snapshot_id = git.create_snapshot(worktree_path, "Test checkpoint")

            # Snapshot ID should follow format
            assert snapshot_id.startswith("snap-")

            # Tag should exist
            tags = run_git(["tag", "--list", snapshot_id], dev_repo)
            assert snapshot_id in tags
        finally:
            git.remove_worktree(worktree_path)

    def test_get_latest_snapshot_returns_most_recent(self, git_setup):
        """get_latest_snapshot should return most recent snapshot."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("latest-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "latest"
        git.add_worktree("latest-test", worktree_path)

        try:
            # Create first snapshot
            (worktree_path / "v1.txt").write_text("v1")
            snap1 = git.create_snapshot(worktree_path, "First snapshot")

            # Create second snapshot
            (worktree_path / "v2.txt").write_text("v2")
            snap2 = git.create_snapshot(worktree_path, "Second snapshot")

            # Latest should be snap2
            latest = git.get_latest_snapshot(worktree_path)
            assert latest == snap2
        finally:
            git.remove_worktree(worktree_path)

    def test_list_snapshots_returns_all_snapshots(self, git_setup):
        """list_snapshots should return all snapshots for workspace."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("list-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "list"
        git.add_worktree("list-test", worktree_path)

        try:
            # Create multiple snapshots
            (worktree_path / "a.txt").write_text("a")
            snap1 = git.create_snapshot(worktree_path, "Snapshot A")

            (worktree_path / "b.txt").write_text("b")
            snap2 = git.create_snapshot(worktree_path, "Snapshot B")

            # List snapshots
            snapshots = git.list_snapshots("list-test")

            assert snap1 in snapshots
            assert snap2 in snapshots
        finally:
            git.remove_worktree(worktree_path)

    def test_checkout_snapshot_reverts_changes(self, git_setup):
        """checkout_snapshot should revert to snapshot state."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("checkout-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "checkout"
        git.add_worktree("checkout-test", worktree_path)

        try:
            # Create initial file and snapshot
            (worktree_path / "keep.txt").write_text("keep this")
            snap1 = git.create_snapshot(worktree_path, "Good state")

            # Make more changes
            (worktree_path / "keep.txt").write_text("modified")
            (worktree_path / "new.txt").write_text("new file")
            git.create_snapshot(worktree_path, "Bad state")

            # Checkout first snapshot
            git.checkout_snapshot(worktree_path, snap1)

            # Should be reverted
            assert (worktree_path / "keep.txt").read_text() == "keep this"
            # new.txt should be gone (untracked files removed by reset --hard)
        finally:
            git.remove_worktree(worktree_path)


class TestGitLayerBranchInfo:
    """Tests for branch info operations with real git."""

    def test_get_branch_info_returns_snapshot_count(self, git_setup):
        """get_branch_info should return snapshot count."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("info-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "info"
        git.add_worktree("info-test", worktree_path)

        try:
            # Initially no snapshots
            info = git.get_branch_info("info-test")
            initial_count = info["snapshot_count"]

            # Create a snapshot
            (worktree_path / "file.txt").write_text("content")
            git.create_snapshot(worktree_path, "New snapshot")

            # Count should increase
            info = git.get_branch_info("info-test")
            assert info["snapshot_count"] == initial_count + 1
        finally:
            git.remove_worktree(worktree_path)

    def test_get_branch_info_returns_last_activity(self, git_setup):
        """get_branch_info should return last activity timestamp."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("activity-test", from_ref=current)

        # Check for the created workspace
        info = git.get_branch_info("activity-test")

        # Should have a last_activity timestamp
        assert "last_activity" in info
        assert info["last_activity"] is not None


class TestGitLayerDiff:
    """Tests for diff operations with real git."""

    def test_get_diff_returns_changes(self, git_setup):
        """get_diff should return uncommitted changes."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("diff-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "diff"
        git.add_worktree("diff-test", worktree_path)

        try:
            # Create a snapshot first
            (worktree_path / "original.txt").write_text("original")
            git.create_snapshot(worktree_path, "Original")

            # Make changes
            (worktree_path / "original.txt").write_text("modified content")
            (worktree_path / "new_file.txt").write_text("new")

            # Get diff text
            diff = git.get_diff_text(worktree_path)

            # Should show changes
            assert "original.txt" in diff or "modified" in diff
        finally:
            # force=True because we have uncommitted changes
            git.remove_worktree(worktree_path, force=True)

    def test_get_diff_empty_when_no_changes(self, git_setup):
        """get_diff should return empty string when no changes."""
        dev_repo, project_root, git = git_setup

        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("clean-test", from_ref=current)

        worktree_path = project_root / "workspaces" / "clean"
        git.add_worktree("clean-test", worktree_path)

        try:
            # Create snapshot - no uncommitted changes after
            (worktree_path / "file.txt").write_text("content")
            git.create_snapshot(worktree_path, "Clean state")

            # Diff should be empty
            diff = git.get_diff_text(worktree_path)
            assert diff.strip() == ""
        finally:
            git.remove_worktree(worktree_path)


class TestGitLayerPushBranch:
    """Tests for push_branch operations with real git remotes."""

    @pytest.fixture
    def git_setup_with_remote(self, temp_dir):
        """Create a git setup with a local bare remote for testing push."""
        # Create bare remote repo (simulates GitHub/GitLab)
        remote_repo = temp_dir / "remote.git"
        remote_repo.mkdir()
        run_git(["init", "--bare"], remote_repo)

        # Create dev repo
        dev_repo = temp_dir / "project-dev"
        dev_repo.mkdir()
        run_git(["init"], dev_repo)
        run_git(["config", "user.email", "test@example.com"], dev_repo)
        run_git(["config", "user.name", "Test User"], dev_repo)

        # Create initial commit
        (dev_repo / "README.md").write_text("# Test Repo")
        run_git(["add", "."], dev_repo)
        run_git(["commit", "-m", "Initial commit"], dev_repo)

        # Get the default branch name (main or master depending on git version)
        default_branch = run_git(["branch", "--show-current"], dev_repo)

        # Add remote and push
        run_git(["remote", "add", "origin", str(remote_repo)], dev_repo)
        run_git(["push", "-u", "origin", default_branch], dev_repo)

        # Create project structure
        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        # Create GitLayer
        git = GitLayer(dev_repo, project_root, "workspaces")

        return {
            "dev_repo": dev_repo,
            "remote_repo": remote_repo,
            "project_root": project_root,
            "git": git,
            "default_branch": default_branch,
        }

    def test_has_remote_returns_true_with_remote(self, git_setup_with_remote):
        """has_remote should return True when remote is configured."""
        git = git_setup_with_remote["git"]

        assert git.has_remote() is True

    def test_has_remote_returns_false_without_remote(self, git_setup):
        """has_remote should return False when no remote."""
        dev_repo, project_root, git = git_setup

        assert git.has_remote() is False

    def test_push_branch_pushes_to_remote(self, git_setup_with_remote):
        """push_branch should push workspace branch to remote."""
        git = git_setup_with_remote["git"]
        dev_repo = git_setup_with_remote["dev_repo"]
        remote_repo = git_setup_with_remote["remote_repo"]
        project_root = git_setup_with_remote["project_root"]
        default_branch = git_setup_with_remote["default_branch"]

        # Create a workspace branch
        git.create_branch("push-test", from_ref=default_branch)

        # Add worktree and make changes
        worktree_path = project_root / "workspaces" / "push"
        git.add_worktree("push-test", worktree_path)

        try:
            # Make changes and create snapshot
            (worktree_path / "new_file.py").write_text("# New code")
            git.create_snapshot(worktree_path, "Add new code")

            # Push the branch
            git.push_branch("push-test")

            # Verify branch exists on remote
            remote_branches = run_git(["branch", "-r"], dev_repo)
            assert "origin/experiment/push-test" in remote_branches
        finally:
            git.remove_worktree(worktree_path)

    def test_push_branch_with_tags(self, git_setup_with_remote):
        """push_branch should also push snapshot tags."""
        git = git_setup_with_remote["git"]
        dev_repo = git_setup_with_remote["dev_repo"]
        remote_repo = git_setup_with_remote["remote_repo"]
        project_root = git_setup_with_remote["project_root"]
        default_branch = git_setup_with_remote["default_branch"]

        # Create a workspace branch
        git.create_branch("tag-test", from_ref=default_branch)

        worktree_path = project_root / "workspaces" / "tags"
        git.add_worktree("tag-test", worktree_path)

        try:
            # Create snapshots
            (worktree_path / "v1.py").write_text("# V1")
            snap1 = git.create_snapshot(worktree_path, "Version 1")

            # Push - should include tags
            git.push_branch("tag-test")

            # Check tag was pushed by fetching from remote
            remote_tags = run_git(["ls-remote", "--tags", str(remote_repo)], dev_repo)
            assert snap1 in remote_tags
        finally:
            git.remove_worktree(worktree_path)

    def test_push_branch_raises_on_failure(self, git_setup):
        """push_branch should raise SyncError when push fails."""
        from goldfish.errors import SyncError

        dev_repo, project_root, git = git_setup

        # Create a branch but don't set up remote
        current = run_git(["branch", "--show-current"], dev_repo)
        git.create_branch("no-remote", from_ref=current)

        # Try to push - should fail because no remote
        with pytest.raises(SyncError):
            git.push_branch("no-remote")
