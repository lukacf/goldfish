"""Tests for subprocess timeout handling - P1.

TDD: Write failing tests first, then implement.
"""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from goldfish.workspace.git_layer import GitLayer


class TestGitLayerTimeouts:
    """Tests for GitLayer subprocess timeout handling."""

    def test_run_git_has_timeout_parameter(self, temp_dir):
        """_run_git should have a timeout parameter."""
        # Create a minimal git repo for testing
        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=dev_repo, capture_output=True, check=True)

        project_root = temp_dir / "project"
        project_root.mkdir()
        workspaces = project_root / "workspaces"
        workspaces.mkdir()

        git_layer = GitLayer(dev_repo, project_root, "workspaces")

        # Should have timeout in subprocess call
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git_layer._run_git("status")

            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert "timeout" in call_kwargs
            assert call_kwargs["timeout"] > 0

    def test_run_git_uses_reasonable_timeout(self, temp_dir):
        """Timeout should be reasonable (30-120 seconds)."""
        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=dev_repo, capture_output=True, check=True)

        project_root = temp_dir / "project"
        project_root.mkdir()
        workspaces = project_root / "workspaces"
        workspaces.mkdir()

        git_layer = GitLayer(dev_repo, project_root, "workspaces")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git_layer._run_git("status")

            call_kwargs = mock_run.call_args[1]
            timeout = call_kwargs["timeout"]
            # Should be reasonable - at least 30s, at most 120s for git ops
            assert 30 <= timeout <= 120

    def test_run_git_raises_on_timeout(self, temp_dir):
        """Should raise GoldfishError on timeout."""
        from goldfish.errors import GoldfishError

        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=dev_repo, capture_output=True, check=True)

        project_root = temp_dir / "project"
        project_root.mkdir()
        workspaces = project_root / "workspaces"
        workspaces.mkdir()

        git_layer = GitLayer(dev_repo, project_root, "workspaces")

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="git", timeout=60)

            with pytest.raises(GoldfishError) as exc_info:
                git_layer._run_git("status")

            assert "timed out" in str(exc_info.value).lower()

    def test_has_staged_changes_has_timeout(self, temp_dir):
        """has_staged_changes should use timeout."""
        dev_repo = temp_dir / "dev-repo"
        dev_repo.mkdir()
        subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=dev_repo, capture_output=True, check=True)

        project_root = temp_dir / "project"
        project_root.mkdir()
        workspaces = project_root / "workspaces"
        workspaces.mkdir()

        git_layer = GitLayer(dev_repo, project_root, "workspaces")

        # Create a worktree to test on
        slot_path = workspaces / "w1"
        subprocess.run(["git", "worktree", "add", str(slot_path), "HEAD"], cwd=dev_repo, capture_output=True)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git_layer.has_staged_changes(slot_path)

            # Check that timeout was passed
            call_kwargs = mock_run.call_args[1]
            assert "timeout" in call_kwargs


class TestInitTimeouts:
    """Tests for init.py subprocess timeout handling."""

    def test_create_dev_repo_has_timeouts(self, temp_dir):
        """All git commands in init should have timeouts."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "test-dev"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            _create_dev_repo(dev_repo_path, "test-project")

            # Should have been called multiple times (init, add, commit)
            assert mock_run.call_count >= 3

            # Each call should have timeout
            for call in mock_run.call_args_list:
                call_kwargs = call[1]
                assert "timeout" in call_kwargs, f"Missing timeout in call: {call}"


class TestDefaultTimeout:
    """Tests for configurable default timeout."""

    def test_git_timeout_constant_exists(self):
        """Should have a configurable timeout constant."""
        from goldfish.workspace import git_layer

        assert hasattr(git_layer, "GIT_TIMEOUT")
        assert git_layer.GIT_TIMEOUT > 0

    def test_timeout_is_reasonable(self):
        """Default timeout should be between 30-120 seconds."""
        from goldfish.workspace import git_layer

        assert 30 <= git_layer.GIT_TIMEOUT <= 120
