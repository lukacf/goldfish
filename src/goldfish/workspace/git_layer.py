"""Git worktree operations - INTERNAL MODULE.

Claude should never see git commands or error messages from this layer.
All errors are translated to GoldfishError before leaving this module.
"""

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Default timeout for git operations (60 seconds)
# Long enough for remote operations, short enough to catch hangs
GIT_TIMEOUT = 60

logger = logging.getLogger(__name__)

from goldfish.errors import (
    GoldfishError,
    WorkspaceAlreadyExistsError,
    WorkspaceNotFoundError,
    SlotNotEmptyError,
    SyncError,
    translate_git_error,
)


class GitLayer:
    """Low-level git worktree operations.

    This class is INTERNAL - it should never be exposed to Claude.
    All git terminology and errors are translated to Goldfish concepts.
    """

    def __init__(self, dev_repo_path: Path, project_root: Path, workspaces_dir: str):
        """Initialize git layer.

        Args:
            dev_repo_path: Path to the {project}-dev repository
            project_root: Path to the project directory
            workspaces_dir: Name of workspaces directory within project
        """
        self.dev_repo = dev_repo_path.resolve()
        self.project_root = project_root.resolve()
        self.workspaces_dir = self.project_root / workspaces_dir

        # Verify dev repo exists
        if not (self.dev_repo / ".git").exists():
            raise GoldfishError(
                f"Project not initialized. Expected repository at {dev_repo_path}"
            )

    def _run_git(
        self, *args: str, cwd: Optional[Path] = None, check: bool = True
    ) -> tuple[str, str]:
        """Run git command, translating errors.

        Args:
            *args: Git command arguments
            cwd: Working directory (default: dev_repo)
            check: Whether to raise on non-zero exit

        Returns:
            Tuple of (stdout, stderr)

        Raises:
            GoldfishError: On git errors or timeouts (translated to platform-speak)
        """
        cmd = ["git"] + list(args)
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd or self.dev_repo,
                capture_output=True,
                text=True,
                timeout=GIT_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            raise GoldfishError(
                f"Operation timed out after {GIT_TIMEOUT} seconds. "
                f"The repository may be slow or unresponsive."
            )

        if check and result.returncode != 0:
            error_msg = translate_git_error(result.stderr)
            stderr_lower = result.stderr.lower()

            # Map to specific error types
            if "already exists" in stderr_lower:
                raise WorkspaceAlreadyExistsError(error_msg)
            if "not found" in stderr_lower or "does not exist" in stderr_lower:
                raise WorkspaceNotFoundError(error_msg)
            if "is not an empty directory" in stderr_lower:
                raise SlotNotEmptyError(error_msg)

            raise GoldfishError(f"Operation failed: {error_msg}")

        return result.stdout.strip(), result.stderr.strip()

    # --- Branch operations (workspaces) ---

    def branch_exists(self, workspace_name: str) -> bool:
        """Check if a workspace (branch) exists."""
        branch = f"experiment/{workspace_name}"
        try:
            self._run_git("show-ref", "--verify", f"refs/heads/{branch}")
            return True
        except GoldfishError:
            return False

    def create_branch(self, workspace_name: str, from_ref: str = "main") -> None:
        """Create a new workspace branch from a reference."""
        branch = f"experiment/{workspace_name}"
        self._run_git("branch", branch, from_ref)

    def delete_branch(self, workspace_name: str, force: bool = False) -> None:
        """Delete a workspace branch."""
        branch = f"experiment/{workspace_name}"
        flag = "-D" if force else "-d"
        self._run_git("branch", flag, branch)

    def list_branches(self) -> list[str]:
        """List all workspace branches (experiment/*)."""
        stdout, _ = self._run_git(
            "branch", "--list", "experiment/*", "--format=%(refname:short)"
        )
        branches = []
        for line in stdout.split("\n"):
            line = line.strip()
            if line:
                # Strip "experiment/" prefix
                name = line.replace("experiment/", "")
                branches.append(name)
        return branches

    def get_branch_info(self, workspace_name: str) -> dict:
        """Get metadata about a workspace branch."""
        branch = f"experiment/{workspace_name}"

        # Get creation time (first commit on branch after diverging from main)
        try:
            stdout, _ = self._run_git(
                "log", branch, "--not", "main", "--format=%aI", "--reverse", "-1"
            )
            created_at = stdout.strip() if stdout.strip() else None
        except GoldfishError as e:
            logger.warning(f"Failed to get git metadata for workspace '{workspace_name}': {e}")
            created_at = None

        # Fall back to first commit if above fails
        if not created_at:
            try:
                stdout, _ = self._run_git(
                    "log", branch, "--format=%aI", "--reverse", "-1"
                )
                created_at = stdout.strip() if stdout.strip() else None
            except GoldfishError:
                created_at = None

        # Get last activity
        try:
            stdout, _ = self._run_git("log", branch, "--format=%aI", "-1")
            last_activity = stdout.strip() if stdout.strip() else None
        except GoldfishError:
            last_activity = None

        # Count snapshots (tags matching snap-*)
        try:
            stdout, _ = self._run_git("tag", "--list", "snap-*", "--merged", branch)
            snapshot_count = len([t for t in stdout.split("\n") if t.strip()])
        except GoldfishError:
            snapshot_count = 0

        return {
            "created_at": created_at,
            "last_activity": last_activity,
            "snapshot_count": snapshot_count,
        }

    # --- Worktree operations (slots) ---

    def add_worktree(self, workspace_name: str, slot_path: Path) -> None:
        """Mount workspace to slot via git worktree.

        This operation should be atomic - it will fail if slot_path exists.
        This prevents TOCTOU races in concurrent mount operations.
        """
        # Ensure parent exists
        slot_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if path already exists and is non-empty
        # This provides early detection before calling git
        if slot_path.exists():
            try:
                if any(slot_path.iterdir()):
                    from goldfish.errors import GoldfishError
                    raise GoldfishError(
                        f"Cannot add worktree: {slot_path} is not empty"
                    )
            except (PermissionError, OSError):
                # If we can't check, let git fail naturally
                pass

        branch = f"experiment/{workspace_name}"
        # Git worktree add is atomic - either succeeds completely or fails
        self._run_git("worktree", "add", str(slot_path), branch)

    def remove_worktree(self, slot_path: Path, force: bool = False) -> None:
        """Remove worktree (hibernate slot)."""
        args = ["worktree", "remove", str(slot_path)]
        if force:
            args.append("--force")
        self._run_git(*args)

    def list_worktrees(self) -> list[dict]:
        """List all worktrees with their branches."""
        stdout, _ = self._run_git("worktree", "list", "--porcelain")
        worktrees = []
        current: dict = {}

        for line in stdout.split("\n"):
            if line.startswith("worktree "):
                if current:
                    worktrees.append(current)
                current = {"path": line[9:]}
            elif line.startswith("branch "):
                current["branch"] = line[7:]
            elif line == "bare":
                current["bare"] = True
            elif line.startswith("HEAD "):
                current["head"] = line[5:]

        if current:
            worktrees.append(current)

        return worktrees

    def get_worktree_for_slot(self, slot_path: Path) -> Optional[dict]:
        """Get worktree info for a specific slot path."""
        worktrees = self.list_worktrees()
        slot_str = str(slot_path.resolve())

        for wt in worktrees:
            if wt.get("path") == slot_str:
                return wt
        return None

    # --- Working directory operations ---

    def is_dirty(self, slot_path: Path) -> bool:
        """Check if worktree has uncommitted changes."""
        stdout, _ = self._run_git("status", "--porcelain", cwd=slot_path)
        return bool(stdout.strip())

    def get_head_sha(self, slot_path: Path, short: bool = True) -> str:
        """Get current HEAD SHA."""
        args = ["rev-parse"]
        if short:
            args.append("--short")
        args.append("HEAD")
        stdout, _ = self._run_git(*args, cwd=slot_path)
        return stdout.strip()

    def stage_all(self, slot_path: Path) -> None:
        """Stage all changes in worktree."""
        self._run_git("add", "-A", cwd=slot_path)

    def commit(self, slot_path: Path, message: str) -> str:
        """Commit staged changes. Returns commit SHA."""
        self._run_git("commit", "-m", message, cwd=slot_path)
        return self.get_head_sha(slot_path)

    def has_staged_changes(self, slot_path: Path) -> bool:
        """Check if there are staged changes to commit."""
        # git diff --quiet returns 1 if there are differences
        try:
            result = subprocess.run(
                ["git", "diff", "--cached", "--quiet"],
                cwd=slot_path,
                capture_output=True,
                timeout=GIT_TIMEOUT,
            )
            return result.returncode != 0
        except subprocess.TimeoutExpired:
            raise GoldfishError(
                f"Operation timed out after {GIT_TIMEOUT} seconds. "
                f"The repository may be slow or unresponsive."
            )

    def get_changed_files(self, slot_path: Path) -> list[str]:
        """Get list of changed files (staged and unstaged)."""
        stdout, _ = self._run_git("status", "--porcelain", cwd=slot_path)
        files = []
        for line in stdout.split("\n"):
            if line.strip():
                # Format: XY filename (or XY -> newname for renames)
                # Note: _run_git strips leading space, so lines that started
                # with ' M' become 'M' after strip. We need to find the filename
                # by looking for the space after the status chars.
                # Status chars are 1-2 characters, followed by space, then filename.
                parts = line.split(" ", 1)
                if len(parts) >= 2:
                    filename = parts[1].strip()
                else:
                    # Fallback: treat entire line as filename (shouldn't happen)
                    filename = line.strip()
                if " -> " in filename:
                    filename = filename.split(" -> ")[1]
                files.append(filename)
        return files

    def get_diff_stats(self, slot_path: Path) -> str:
        """Get diff statistics summary."""
        stdout, _ = self._run_git("diff", "--stat", "HEAD", cwd=slot_path)
        # Return just the summary line (last line)
        lines = [l for l in stdout.strip().split("\n") if l.strip()]
        if lines:
            return lines[-1].strip()
        return "No changes"

    def get_diff_text(self, slot_path: Path) -> str:
        """Get full diff text."""
        stdout, _ = self._run_git("diff", "HEAD", cwd=slot_path)
        return stdout

    def checkout_snapshot(self, slot_path: Path, snapshot_id: str) -> int:
        """Checkout a snapshot, discarding current changes.

        Args:
            slot_path: Path to the worktree
            snapshot_id: Snapshot tag to checkout

        Returns:
            Number of files changed
        """
        # Get list of changed files before checkout
        changed_before = self.get_changed_files(slot_path)

        # Hard reset to the snapshot
        self._run_git("reset", "--hard", snapshot_id, cwd=slot_path)

        # Clean untracked files
        self._run_git("clean", "-fd", cwd=slot_path)

        return len(changed_before)

    # --- Snapshot operations ---

    def create_snapshot(self, slot_path: Path, message: str) -> str:
        """Commit all changes and create a tag. Returns snapshot ID."""
        # Stage all changes
        self.stage_all(slot_path)

        # Check if there's anything to commit
        if self.is_dirty(slot_path) or self.has_staged_changes(slot_path):
            self.commit(slot_path, message)

        # Create snapshot tag
        sha = self.get_head_sha(slot_path)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        snapshot_id = f"snap-{sha}-{timestamp}"

        self._run_git("tag", snapshot_id, cwd=slot_path)

        return snapshot_id

    def get_latest_snapshot(self, slot_path: Path) -> Optional[str]:
        """Get the most recent snapshot tag."""
        try:
            stdout, _ = self._run_git(
                "describe", "--tags", "--abbrev=0", "--match=snap-*", cwd=slot_path
            )
            return stdout.strip() if stdout.strip() else None
        except GoldfishError:
            return None

    def list_snapshots(self, workspace_name: str) -> list[str]:
        """List all snapshot IDs for a workspace."""
        branch = f"experiment/{workspace_name}"
        try:
            stdout, _ = self._run_git("tag", "--list", "snap-*", "--merged", branch)
            return [t.strip() for t in stdout.split("\n") if t.strip()]
        except GoldfishError as e:
            logger.warning(f"Failed to list snapshots for workspace '{workspace_name}': {e}")
            return []

    def get_snapshot_info(self, snapshot_id: str) -> dict:
        """Get detailed info about a snapshot.

        Args:
            snapshot_id: The snapshot tag

        Returns:
            Dict with 'commit_date' and 'message' keys
        """
        try:
            # Get the commit that the tag points to
            stdout, _ = self._run_git("rev-list", "-1", snapshot_id)
            commit_sha = stdout.strip()

            # Get commit date (ISO format)
            date_out, _ = self._run_git(
                "log", "-1", "--format=%cI", commit_sha
            )

            # Get commit message (first line)
            msg_out, _ = self._run_git(
                "log", "-1", "--format=%s", commit_sha
            )

            return {
                "commit_date": date_out.strip(),
                "message": msg_out.strip(),
            }
        except GoldfishError as e:
            logger.warning(f"Failed to get snapshot info for '{snapshot_id}': {e}")
            return {
                "commit_date": None,
                "message": "",
            }

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot tag.

        Args:
            snapshot_id: The snapshot tag to delete

        Returns:
            True if deleted successfully
        """
        try:
            self._run_git("tag", "-d", snapshot_id)
            return True
        except GoldfishError:
            return False

    # --- Sync operations ---

    def push_branch(self, workspace_name: str) -> None:
        """Push workspace to remote (if configured)."""
        branch = f"experiment/{workspace_name}"
        try:
            # Check if remote exists
            self._run_git("remote", "get-url", "origin", check=False)
            self._run_git("push", "origin", branch, "--tags")
        except GoldfishError as e:
            raise SyncError(f"Failed to sync workspace: {e.message}")

    def fetch(self) -> None:
        """Fetch from remote."""
        try:
            self._run_git("fetch", "--all", "--tags")
        except GoldfishError:
            pass  # Ignore fetch errors (might not have remote)

    def has_remote(self) -> bool:
        """Check if a remote is configured."""
        try:
            stdout, _ = self._run_git("remote", check=False)
            return bool(stdout.strip())
        except GoldfishError:
            return False
