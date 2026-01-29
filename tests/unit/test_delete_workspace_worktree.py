"""Regression tests for delete_workspace worktree cleanup.

Bug context: delete_workspace() failed with "cannot delete branch checked out"
because sync_slot_to_branch() creates a worktree at .goldfish/tmp-sync/
that was never cleaned up before branch deletion.
"""

from pathlib import Path
from unittest.mock import patch


def test_delete_branch_cleans_up_tmp_sync_worktree():
    """Regression test: delete_branch must clean up stale tmp-sync worktree.

    Bug: sync_slot_to_branch() creates a worktree at .goldfish/tmp-sync/{workspace}
    but this was never cleaned up. When delete_workspace() tried to delete the
    branch, git refused because the branch was still checked out in that worktree.
    """
    from goldfish.workspace.git_layer import GitLayer

    # Create a GitLayer with mocked git commands
    with patch.object(GitLayer, "_run_git") as mock_run_git:
        # Setup mock to return success for all git commands
        mock_run_git.return_value = ("", "")

        # Create GitLayer instance with mocked initialization
        with patch.object(GitLayer, "__init__", lambda self, *args, **kwargs: None):
            git_layer = GitLayer.__new__(GitLayer)
            git_layer.dev_repo = Path("/tmp/test-dev")
            git_layer.project_root = Path("/tmp/test")
            git_layer.workspaces_dir = "workspaces"

        # Simulate tmp-sync worktree existing
        workspace_name = "test_workspace"
        temp_worktree = git_layer.dev_repo / ".goldfish" / "tmp-sync" / workspace_name

        with patch.object(Path, "exists", return_value=True):
            with patch("shutil.rmtree") as mock_rmtree:
                git_layer.delete_branch(workspace_name, force=True)

        # Verify worktree cleanup was attempted
        cleanup_calls = [c for c in mock_run_git.call_args_list if "worktree" in c[0]]
        assert any(
            "remove" in str(c) for c in cleanup_calls
        ), "delete_branch should attempt to remove tmp-sync worktree"
        assert any("prune" in str(c) for c in cleanup_calls), "delete_branch should prune worktree metadata"


def test_delete_branch_succeeds_without_tmp_sync_worktree():
    """Verify delete_branch works when no tmp-sync worktree exists."""
    from goldfish.workspace.git_layer import GitLayer

    with patch.object(GitLayer, "_run_git") as mock_run_git:
        mock_run_git.return_value = ("", "")

        with patch.object(GitLayer, "__init__", lambda self, *args, **kwargs: None):
            git_layer = GitLayer.__new__(GitLayer)
            git_layer.dev_repo = Path("/tmp/test-dev")
            git_layer.project_root = Path("/tmp/test")
            git_layer.workspaces_dir = "workspaces"

        # No tmp-sync worktree exists
        with patch.object(Path, "exists", return_value=False):
            git_layer.delete_branch("test_workspace", force=True)

        # Should still call git branch -D
        branch_delete_calls = [c for c in mock_run_git.call_args_list if c[0][0] == "branch" and "-D" in c[0]]
        assert len(branch_delete_calls) == 1, "Should delete the branch"
