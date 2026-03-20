"""Git worktree operations - INTERNAL MODULE.

Claude should never see git commands or error messages from this layer.
All errors are translated to GoldfishError before leaving this module.
"""

import fnmatch
import json
import logging
import shutil
import subprocess
import tarfile
from datetime import UTC, datetime
from pathlib import Path

from goldfish.errors import (
    GoldfishError,
    SlotNotEmptyError,
    SyncError,
    WorkspaceAlreadyExistsError,
    WorkspaceNotFoundError,
    translate_git_error,
)

# Default timeout for git operations (60 seconds)
# Long enough for remote operations, short enough to catch hangs
GIT_TIMEOUT = 60

logger = logging.getLogger(__name__)


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
            raise GoldfishError(f"Project not initialized. Expected repository at {dev_repo_path}")

    def _run_git(self, *args: str, cwd: Path | None = None, check: bool = True) -> tuple[str, str]:
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
        except subprocess.TimeoutExpired as err:
            raise GoldfishError(
                f"Operation timed out after {GIT_TIMEOUT} seconds. The repository may be slow or unresponsive."
            ) from err

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

    # --- Branch naming convention ---
    # Workspace branches are stored under goldfish/* namespace for scalability.
    # This allows git to efficiently handle 1000+ workspaces without branch
    # enumeration performance issues.

    def _workspace_branch(self, workspace: str) -> str:
        """Get branch name for workspace (under refs/heads for git compatibility)."""
        return f"goldfish/{workspace}"

    def _is_workspace_branch(self, branch_name: str) -> bool:
        """Check if branch is a goldfish workspace branch."""
        return branch_name.startswith("goldfish/")

    def _workspace_from_branch(self, branch_name: str) -> str:
        """Extract workspace name from branch."""
        return branch_name.removeprefix("goldfish/")

    def branch_exists(self, workspace_name: str) -> bool:
        """Check if a workspace (branch) exists."""
        branch = self._workspace_branch(workspace_name)
        try:
            self._run_git("show-ref", "--verify", f"refs/heads/{branch}")
            return True
        except GoldfishError:
            return False

    def create_branch(self, workspace_name: str, from_ref: str = "main") -> None:
        """Create a new workspace branch from a reference.

        The from_ref must be a git-resolvable ref (main, a SHA, or a full
        goldfish/* branch name). Callers are responsible for translating
        workspace names to goldfish/* — this layer does NOT guess.
        """
        branch = self._workspace_branch(workspace_name)
        self._run_git("branch", branch, from_ref)

    def delete_branch(self, workspace_name: str, force: bool = False) -> None:
        """Delete a workspace branch.

        Cleans up any stale tmp-sync worktree before deletion to avoid
        "cannot delete branch checked out at" errors.
        """
        # Clean up any stale tmp-sync worktree for this workspace
        temp_worktree = self.dev_repo / ".goldfish" / "tmp-sync" / workspace_name
        if temp_worktree.exists():
            try:
                self._run_git("worktree", "remove", str(temp_worktree), "--force", check=False)
            except GoldfishError:
                pass
            # If git worktree remove didn't work, force remove the directory
            if temp_worktree.exists():
                import shutil

                shutil.rmtree(temp_worktree, ignore_errors=True)
            # Prune worktree metadata
            self._run_git("worktree", "prune", check=False)

        branch = self._workspace_branch(workspace_name)
        flag = "-D" if force else "-d"
        self._run_git("branch", flag, branch)

    def list_branches(self) -> list[str]:
        """List all workspace branches (goldfish/*)."""
        stdout, _ = self._run_git("branch", "--list", "goldfish/*", "--format=%(refname:short)")
        branches = []
        for line in stdout.split("\n"):
            line = line.strip()
            if line:
                # Strip "goldfish/" prefix
                name = self._workspace_from_branch(line)
                branches.append(name)
        return branches

    def get_branch_info(self, workspace_name: str) -> dict:
        """Get metadata about a workspace branch."""
        branch = self._workspace_branch(workspace_name)

        # Get creation time (first commit on branch after diverging from main)
        try:
            stdout, _ = self._run_git("log", branch, "--not", "main", "--format=%aI", "--reverse", "-1")
            created_at = stdout.strip() if stdout.strip() else None
        except GoldfishError as e:
            logger.warning(f"Failed to get git metadata for workspace '{workspace_name}': {e}")
            created_at = None

        # Fall back to first commit if above fails
        if not created_at:
            try:
                stdout, _ = self._run_git("log", branch, "--format=%aI", "--reverse", "-1")
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

                    raise GoldfishError(f"Cannot add worktree: {slot_path} is not empty")
            except (PermissionError, OSError):
                # If we can't check, let git fail naturally
                pass

        branch = self._workspace_branch(workspace_name)
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

    def get_worktree_for_slot(self, slot_path: Path) -> dict | None:
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
        except subprocess.TimeoutExpired as err:
            raise GoldfishError(
                f"Operation timed out after {GIT_TIMEOUT} seconds. The repository may be slow or unresponsive."
            ) from err

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
        lines = [line for line in stdout.strip().split("\n") if line.strip()]
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
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        snapshot_id = f"snap-{sha}-{timestamp}"

        self._run_git("tag", snapshot_id, cwd=slot_path)

        return snapshot_id

    def create_tag(self, branch_or_slot: str, tag_name: str, commit_sha: str) -> None:
        """Create a git tag on a specific commit.

        Args:
            branch_or_slot: Branch name or slot (unused, for API compatibility)
            tag_name: Name of the tag to create
            commit_sha: SHA of the commit to tag

        This is used for workspace versioning (e.g., "workspace-v1").
        """
        # Create tag in the dev repo (not in worktree)
        self._run_git("tag", tag_name, commit_sha, cwd=self.dev_repo)

    def get_tag_sha(self, tag_name: str) -> str | None:
        """Get the SHA that a tag points to.

        Args:
            tag_name: Name of the tag

        Returns:
            The SHA the tag points to, or None if tag doesn't exist
        """
        try:
            stdout, _ = self._run_git("rev-parse", tag_name, cwd=self.dev_repo)
            return stdout.strip()
        except GoldfishError:
            return None

    def get_latest_snapshot(self, slot_path: Path) -> str | None:
        """Get the most recent snapshot tag."""
        try:
            stdout, _ = self._run_git("describe", "--tags", "--abbrev=0", "--match=snap-*", cwd=slot_path)
            return stdout.strip() if stdout.strip() else None
        except GoldfishError:
            return None

    def list_snapshots(self, workspace_name: str) -> list[str]:
        """List all snapshot IDs for a workspace."""
        branch = self._workspace_branch(workspace_name)
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
            date_out, _ = self._run_git("log", "-1", "--format=%cI", commit_sha)

            # Get commit message (first line)
            msg_out, _ = self._run_git("log", "-1", "--format=%s", commit_sha)

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
        branch = self._workspace_branch(workspace_name)
        try:
            # Check if remote exists
            self._run_git("remote", "get-url", "origin", check=False)
            self._run_git("push", "origin", branch, "--tags")
        except GoldfishError as e:
            raise SyncError(f"Failed to sync workspace: {e.message}") from e

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

    def has_remote_branch(self, branch: str) -> bool:
        """Check if a branch exists on the remote (origin).

        Args:
            branch: Local branch name (e.g., 'goldfish/my-workspace')

        Returns:
            True if origin/{branch} exists, False otherwise.
        """
        if not self.has_remote():
            return False
        try:
            # Check if the remote branch ref exists
            self._run_git("rev-parse", "--verify", f"origin/{branch}", check=True)
            return True
        except GoldfishError:
            return False

    def diff_commits(self, from_sha: str, to_sha: str) -> dict:
        """Get diff between two commits.

        Args:
            from_sha: Starting commit SHA
            to_sha: Ending commit SHA

        Returns:
            Dict with:
            - commits: List of commits between from_sha and to_sha
            - files: Dict of file changes {file_path: change_type}
        """
        # Get commits between the two SHAs
        stdout, _ = self._run_git("log", "--oneline", "--format=%H|%s", f"{from_sha}..{to_sha}")
        commits = []
        for line in stdout.strip().split("\n"):
            if line and "|" in line:
                sha, message = line.split("|", 1)
                commits.append({"sha": sha, "message": message})

        # Get file changes
        stdout, _ = self._run_git("diff", "--name-status", from_sha, to_sha)
        files = {}
        for line in stdout.strip().split("\n"):
            if line:
                parts = line.split("\t")
                if len(parts) >= 2:
                    change_type, file_path = parts[0], parts[1]
                    files[file_path] = change_type

        return {
            "commits": commits,
            "files": files,
        }

    def diff_shas(self, sha1: str, sha2: str) -> dict:
        """Diff two git SHAs.

        Args:
            sha1: First commit SHA
            sha2: Second commit SHA

        Returns:
            Dict with has_changes, summary, files_changed, diff_text
        """
        # Get list of changed files
        stdout, _ = self._run_git("diff", "--name-status", sha1, sha2)
        files_changed = []
        for line in stdout.strip().split("\n"):
            if line:
                parts = line.split("\t")
                if len(parts) >= 2:
                    files_changed.append(parts[1])

        has_changes = len(files_changed) > 0

        # Get diff text
        diff_text = ""
        if has_changes:
            stdout, _ = self._run_git("diff", sha1, sha2)
            diff_text = stdout

        return {
            "has_changes": has_changes,
            "summary": f"{len(files_changed)} file(s) changed" if has_changes else "No differences",
            "files_changed": files_changed,
            "diff_text": diff_text,
        }

    # --- Copy-based mounting operations (Phase 2) ---

    def diff_slot_against_sha(self, slot_path: Path, workspace: str, mounted_sha: str) -> dict:
        """Diff a copy-based slot against its mounted SHA.

        Re-uses or creates a temp worktree at mounted_sha, diffs the slot against it,
        and returns the result.

        Args:
            slot_path: Path to the slot directory
            workspace: Workspace name (for branch context)
            mounted_sha: The SHA the slot was mounted from

        Returns:
            Dict with has_changes, summary, files_changed, diff_text
        """
        # Create temp worktree at mounted_sha
        temp_worktree = self.dev_repo / ".goldfish" / "tmp-diff" / workspace
        temp_worktree.parent.mkdir(parents=True, exist_ok=True)

        try:
            if not temp_worktree.exists():
                self._run_git("worktree", "add", "--detach", str(temp_worktree), mounted_sha)
            else:
                # Ensure it's on the correct SHA
                self._run_git("checkout", "--detach", mounted_sha, cwd=temp_worktree)
                self._run_git("reset", "--hard", mounted_sha, cwd=temp_worktree)
                self._run_git("clean", "-fd", cwd=temp_worktree)

            # Use diff to compare slot against worktree
            # Exclude .goldfish-mount, STATE.md, and other goldfish files
            exclude_patterns = [
                ".goldfish-mount",
                "STATE.md",
                ".git",
                "__pycache__",
                "*.pyc",
                ".pytest_cache",
            ]

            # Build diff command with excludes
            diff_cmd = ["diff", "-rq"]  # recursive, brief (just show which files differ)
            for pattern in exclude_patterns:
                diff_cmd.extend(["--exclude", pattern])
            diff_cmd.extend([str(temp_worktree), str(slot_path)])

            result = subprocess.run(diff_cmd, capture_output=True, text=True, timeout=30)

            # Parse diff output
            # Format: "Files /path/a/file and /path/b/file differ" or "Only in /path: file"
            files_changed = []
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                if line.startswith("Files "):
                    # "Files /tmp/.../file and /slot/.../file differ"
                    parts = line.split(" and ")
                    if len(parts) >= 2:
                        # Extract relative path from slot_path
                        file_path = parts[1].replace(" differ", "").replace(str(slot_path) + "/", "")
                        files_changed.append(f"M {file_path}")
                elif line.startswith("Only in "):
                    # "Only in /path: filename"
                    if str(slot_path) in line:
                        # File only in slot = added
                        filename = line.split(": ")[-1]
                        dir_part = line.split(": ")[0].replace("Only in ", "").replace(str(slot_path), "").strip("/")
                        rel_path = f"{dir_part}/{filename}".strip("/")
                        files_changed.append(f"A {rel_path}")
                    else:
                        # File only in worktree = deleted from slot
                        filename = line.split(": ")[-1]
                        dir_part = (
                            line.split(": ")[0].replace("Only in ", "").replace(str(temp_worktree), "").strip("/")
                        )
                        rel_path = f"{dir_part}/{filename}".strip("/")
                        files_changed.append(f"D {rel_path}")

            has_changes = len(files_changed) > 0

            if not has_changes:
                return {
                    "has_changes": False,
                    "summary": "No changes",
                    "files_changed": [],
                    "diff_text": "",
                }

            # Get actual diff text for changed files (limit to avoid huge output)
            diff_text_cmd = ["diff", "-u"]
            for pattern in exclude_patterns:
                diff_text_cmd.extend(["--exclude", pattern])
            diff_text_cmd.extend([str(temp_worktree), str(slot_path)])

            diff_result = subprocess.run(diff_text_cmd, capture_output=True, text=True, timeout=30)
            diff_text = diff_result.stdout

            summary = f"{len(files_changed)} file(s) changed"

            return {
                "has_changes": has_changes,
                "summary": summary,
                "files_changed": files_changed,
                "diff_text": diff_text,
            }

        except Exception as e:
            logger.warning(f"Diff failed for {workspace}: {e}")
            if temp_worktree.exists():
                try:
                    self._run_git("worktree", "remove", str(temp_worktree), "--force", check=False)
                except GoldfishError:
                    pass
                if temp_worktree.exists():
                    shutil.rmtree(temp_worktree, ignore_errors=True)
            raise

    def is_slot_dirty(self, slot_path: Path, workspace: str, compare_sha: str) -> bool:
        """Check if slot has changes compared to a git SHA.

        Re-uses or creates a temp worktree at compare_sha, diffs the slot against it,
        and returns the result.

        Args:
            slot_path: Path to the slot directory
            workspace: Workspace name (for temp worktree naming)
            compare_sha: The SHA to compare against

        Returns:
            True if slot has changes, False if clean
        """
        # Create temp worktree at compare_sha
        temp_worktree = self.dev_repo / ".goldfish" / "tmp-dirty" / workspace
        temp_worktree.parent.mkdir(parents=True, exist_ok=True)

        try:
            if not temp_worktree.exists():
                self._run_git("worktree", "add", "--detach", str(temp_worktree), compare_sha)
            else:
                # Ensure it's on the correct SHA
                self._run_git("checkout", "--detach", compare_sha, cwd=temp_worktree)
                self._run_git("reset", "--hard", compare_sha, cwd=temp_worktree)
                self._run_git("clean", "-fd", cwd=temp_worktree)

            # Exclude goldfish metadata and common artifacts
            exclude_patterns = [
                ".goldfish-mount",
                "STATE.md",
                ".git",
                "__pycache__",
                "*.pyc",
                ".pytest_cache",
            ]

            # Quick diff - just check if any files differ
            diff_cmd = ["diff", "-rq"]
            for pattern in exclude_patterns:
                diff_cmd.extend(["--exclude", pattern])
            diff_cmd.extend([str(temp_worktree), str(slot_path)])

            result = subprocess.run(diff_cmd, capture_output=True, text=True, timeout=30)

            # diff returns 0 if identical, 1 if differences
            return result.returncode != 0

        except Exception as e:
            logger.warning(f"Dirty check failed for {workspace}: {e}")
            if temp_worktree.exists():
                try:
                    self._run_git("worktree", "remove", str(temp_worktree), "--force", check=False)
                except GoldfishError:
                    pass
                if temp_worktree.exists():
                    shutil.rmtree(temp_worktree, ignore_errors=True)
            # On any error, assume dirty to be safe
            return True

    def get_head_sha_from_branch(self, branch: str) -> str:
        """Get HEAD SHA from a branch (without needing a worktree).

        Args:
            branch: Full branch name (e.g., "goldfish/workspace_name")

        Returns:
            The commit SHA at the head of the branch
        """
        stdout, _ = self._run_git("rev-parse", branch)
        return stdout.strip()

    def is_ancestor(self, potential_ancestor: str, descendant: str) -> bool:
        """Check if one commit is an ancestor of another.

        Uses `git merge-base --is-ancestor` which returns exit code 0 if true.

        Args:
            potential_ancestor: SHA that might be an ancestor
            descendant: SHA that might be a descendant

        Returns:
            True if potential_ancestor is an ancestor of (or equal to) descendant
        """
        try:
            self._run_git("merge-base", "--is-ancestor", potential_ancestor, descendant)
            return True
        except GoldfishError:
            # Exit code 1 means not an ancestor, which is a normal case
            return False

    def copy_mount_workspace(self, workspace_name: str, slot_path: Path) -> dict:
        """Copy workspace branch content to slot directory.

        The slot is a plain directory with NO git - just files.
        This uses git archive to extract files cleanly.

        Args:
            workspace_name: Name of the workspace to mount
            slot_path: Path to the slot directory

        Returns:
            Metadata dict with workspace_name, branch, mounted_sha, mounted_at

        Raises:
            WorkspaceNotFoundError: If workspace doesn't exist
            GoldfishError: If slot exists and is not empty/not a Goldfish workspace
        """
        branch = self._workspace_branch(workspace_name)
        if not self.branch_exists(workspace_name):
            raise WorkspaceNotFoundError(f"Workspace '{workspace_name}' not found")

        # CRITICAL: Safety check - refuse to mount to non-empty non-Goldfish directory
        if slot_path.exists():
            if any(slot_path.iterdir()):  # Non-empty
                metadata_file = slot_path / ".goldfish-mount"
                if not metadata_file.exists():
                    raise GoldfishError(
                        f"Target directory '{slot_path}' is not empty and is not a Goldfish workspace.\n"
                        f"Refusing to mount to avoid data loss."
                    )
                # If it's a Goldfish workspace, we'll overwrite it (re-mount)
                shutil.rmtree(slot_path)

        # Get the tree content from the branch via git archive
        slot_path.mkdir(parents=True, exist_ok=True)
        tar_path = slot_path.parent / f".{slot_path.name}.tar"

        try:
            # Export branch content to tar
            self._run_git("archive", branch, "--format=tar", f"--output={tar_path}")

            # Extract to slot
            with tarfile.open(tar_path) as tar:
                tar.extractall(slot_path)
        finally:
            # Clean up tar file
            if tar_path.exists():
                tar_path.unlink()

        # Create metadata file (the ONLY goldfish file in slot)
        head_sha = self.get_head_sha_from_branch(branch)
        metadata = {
            "workspace_name": workspace_name,
            "branch": branch,
            "mounted_sha": head_sha,
            "mounted_at": datetime.now(UTC).isoformat(),
        }
        (slot_path / ".goldfish-mount").write_text(json.dumps(metadata, indent=2))

        return metadata

    def checkout_snapshot_copy_based(self, slot_path: Path, git_tag: str) -> int:
        """Checkout a snapshot/version in a copy-based workspace.

        Replaces slot contents with the contents at the specified git tag.
        Works with copy-based workspaces (no .git in slot).

        Args:
            slot_path: Path to the user workspace slot
            git_tag: Git tag/version to checkout (e.g., "snap-xxx" or branch ref)

        Returns:
            Number of files changed (approximate)
        """
        # Read current metadata to preserve workspace_name
        metadata_file = slot_path / ".goldfish-mount"
        if not metadata_file.exists():
            raise GoldfishError(f"Slot '{slot_path}' is not a Goldfish workspace")

        metadata = json.loads(metadata_file.read_text())

        # Count files before
        files_before = set()
        for f in slot_path.rglob("*"):
            if f.is_file() and ".goldfish-mount" not in str(f):
                files_before.add(f.relative_to(slot_path))

        # Clear slot contents (except metadata)
        for item in slot_path.iterdir():
            if item.name != ".goldfish-mount":
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()

        # Extract contents from git tag
        tar_path = slot_path.parent / f".{slot_path.name}.tar"

        try:
            # Export tag content to tar
            self._run_git("archive", git_tag, "--format=tar", f"--output={tar_path}", cwd=self.dev_repo)

            # Extract to slot
            with tarfile.open(tar_path) as tar:
                tar.extractall(slot_path)
        finally:
            # Clean up tar file
            if tar_path.exists():
                tar_path.unlink()

        # Count files after
        files_after = set()
        for f in slot_path.rglob("*"):
            if f.is_file() and ".goldfish-mount" not in str(f):
                files_after.add(f.relative_to(slot_path))

        # Update metadata with new SHA
        tag_sha = self._run_git("rev-parse", git_tag, cwd=self.dev_repo)[0].strip()
        metadata["mounted_sha"] = tag_sha
        metadata["mounted_at"] = datetime.now(UTC).isoformat()
        metadata["rolledback_to"] = git_tag
        metadata_file.write_text(json.dumps(metadata, indent=2))

        # Return count of changed files
        return len(files_before.symmetric_difference(files_after))

    def _sync_directory(
        self,
        src: Path,
        dst: Path,
        exclude: list[str] | None = None,
    ) -> None:
        """Sync src to dst with delete semantics (like rsync --delete).

        CRITICAL: This deletes files in dst that don't exist in src.
        Also respects .gitignore patterns from dst.

        Args:
            src: Source directory (user workspace)
            dst: Destination directory (git worktree)
            exclude: List of patterns to exclude from sync
        """
        if exclude is None:
            exclude = []

        # Load .gitignore patterns from destination (if exists)
        gitignore_patterns = set(exclude)
        gitignore_file = dst / ".gitignore"
        if gitignore_file.exists():
            for line in gitignore_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    gitignore_patterns.add(line)

        def should_exclude(path: Path) -> bool:
            """Check if path matches any exclusion pattern."""
            name = path.name
            for pattern in gitignore_patterns:
                if fnmatch.fnmatch(name, pattern):
                    return True
            return False

        # 1. Delete files in dst that don't exist in src
        if dst.exists():
            for dst_item in list(dst.rglob("*")):
                if dst_item.is_file():
                    rel_path = dst_item.relative_to(dst)
                    src_item = src / rel_path
                    # Don't delete .git directory or excluded files
                    if ".git" in dst_item.parts:
                        continue
                    if not src_item.exists() and not should_exclude(dst_item):
                        dst_item.unlink()

            # Clean up empty directories (except .git)
            for dst_item in sorted(dst.rglob("*"), reverse=True):
                if dst_item.is_dir() and ".git" not in dst_item.parts:
                    try:
                        if not any(dst_item.iterdir()):
                            dst_item.rmdir()
                    except OSError:
                        pass  # Directory not empty or other error

        # 2. Copy/update files from src to dst
        for src_item in src.rglob("*"):
            if src_item.is_file():
                rel_path = src_item.relative_to(src)
                # Skip excluded files
                if should_exclude(src_item):
                    continue
                # Skip .goldfish-mount metadata file
                if src_item.name == ".goldfish-mount":
                    continue
                dst_item = dst / rel_path
                dst_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_item, dst_item)

    def sync_slot_to_branch(
        self,
        slot_path: Path,
        workspace_name: str,
        commit_msg: str,
    ) -> str:
        """Sync slot changes back to branch and commit.

        This is called before run() or on unmount.

        Args:
            slot_path: Path to the user workspace slot
            workspace_name: Name of the workspace
            commit_msg: Commit message for the changes

        Returns:
            The new commit SHA (or existing SHA if no changes)

        Raises:
            GoldfishError: If workspace diverged on remote
        """
        branch = self._workspace_branch(workspace_name)
        metadata_file = slot_path / ".goldfish-mount"

        if not metadata_file.exists():
            raise GoldfishError(f"Slot '{slot_path}' is not a mounted Goldfish workspace")

        metadata = json.loads(metadata_file.read_text())

        # 1. Check if branch moved - allow forward moves, reject true divergence
        current_branch_sha = self.get_head_sha_from_branch(branch)
        mounted_sha = metadata["mounted_sha"]

        if current_branch_sha != mounted_sha:
            # Branch moved - check if it's a forward move (safe) or true divergence
            if self.is_ancestor(mounted_sha, current_branch_sha):
                # Forward move: mounted_sha is ancestor of current_branch_sha
                # This is safe - the branch just advanced (e.g., from another session)
                logger.info(
                    f"Workspace '{workspace_name}' branch moved forward from "
                    f"{mounted_sha[:8]} to {current_branch_sha[:8]}. Updating metadata."
                )
                # Update metadata to new branch position
                metadata["mounted_sha"] = current_branch_sha
                metadata_file.write_text(json.dumps(metadata, indent=2))
            else:
                # True divergence: branch was rewritten/rebased
                raise GoldfishError(
                    f"Workspace '{workspace_name}' diverged - not a forward move.\n"
                    f"Branch moved from {mounted_sha[:8]} to {current_branch_sha[:8]}.\n"
                    f"Options: rollback() or branch_workspace()"
                )

        # 2. Sync files from slot to branch using a temporary worktree
        temp_worktree = self.dev_repo / ".goldfish" / "tmp-sync" / workspace_name
        temp_worktree.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Re-use or create temp worktree
            if not temp_worktree.exists():
                try:
                    self._run_git("worktree", "add", str(temp_worktree), branch)
                except GoldfishError as e:
                    if "already checked out" in str(e).lower() or "already exists" in str(e).lower():
                        # Try to find where it's checked out
                        stdout, _ = self._run_git("worktree", "list", "--porcelain")
                        # Format: worktree /path/to/dir\nHEAD sha\nbranch refs/heads/branch\n\n
                        # We just want to know if temp_worktree is indeed a worktree
                        if temp_worktree.exists() and (temp_worktree / ".git").exists():
                            logger.info(f"Reusing existing worktree at {temp_worktree} for {branch}")
                        else:
                            # Not where we expected, or corrupted. Force remove and retry.
                            logger.warning(f"Worktree for {branch} exists elsewhere or is corrupt. Cleaning up.")
                            self._run_git("worktree", "prune")
                            if temp_worktree.exists():
                                shutil.rmtree(temp_worktree, ignore_errors=True)
                            self._run_git("worktree", "add", str(temp_worktree), branch)
                    else:
                        raise

            # Ensure it's on the correct branch and clean
            self._run_git("checkout", "-B", branch, cwd=temp_worktree)
            # Only reset to origin/branch if the remote branch exists
            # (new workspaces don't have remote branches until first push)
            reset_target = f"origin/{branch}" if self.has_remote_branch(branch) else branch
            self._run_git("reset", "--hard", reset_target, cwd=temp_worktree)
            self._run_git("clean", "-fd", cwd=temp_worktree)

            # Sync with delete: mirror slot to worktree (respecting .gitignore)
            self._sync_directory(
                src=slot_path,
                dst=temp_worktree,
                exclude=[".goldfish-mount", "STATE.md", ".git", "__pycache__", "*.pyc", ".pytest_cache"],
            )

            # Commit in the worktree (this advances the branch)
            self._run_git("add", "-A", cwd=temp_worktree)

            # Check if there are actual changes
            status, _ = self._run_git("status", "--porcelain", cwd=temp_worktree)
            if not status.strip():
                return current_branch_sha  # No changes

            self._run_git("commit", "-m", commit_msg, cwd=temp_worktree)
            # Use full SHA (not short) to match get_head_sha_from_branch
            new_sha = self.get_head_sha(temp_worktree, short=False)

            # Update metadata with new SHA
            metadata["mounted_sha"] = new_sha
            metadata_file.write_text(json.dumps(metadata, indent=2))

            return new_sha

        except Exception as e:
            logger.warning(f"Failed to sync {workspace_name}: {e}")
            # If sync fails, best to remove the worktree so next attempt starts fresh
            if temp_worktree.exists():
                try:
                    self._run_git("worktree", "remove", str(temp_worktree), "--force", check=False)
                except GoldfishError:
                    pass
                if temp_worktree.exists():
                    shutil.rmtree(temp_worktree, ignore_errors=True)
            raise

    def copy_unmount_workspace(self, slot_path: Path, workspace_name: str, commit_msg: str) -> str:
        """Sync changes and remove slot directory.

        Args:
            slot_path: Path to the user workspace slot
            workspace_name: Name of the workspace
            commit_msg: Commit message for any pending changes

        Returns:
            The final commit SHA
        """
        # Sync any pending changes
        sha = self.sync_slot_to_branch(slot_path, workspace_name, commit_msg)

        # Remove the slot
        shutil.rmtree(slot_path)

        return sha

    def create_snapshot_copy_based(self, slot_path: Path, workspace_name: str, message: str) -> tuple[str, str]:
        """Create a snapshot for copy-based mounting.

        Syncs slot changes to branch, commits, and creates a snapshot tag.

        Args:
            slot_path: Path to the user workspace slot
            workspace_name: Name of the workspace
            message: Snapshot message

        Returns:
            Tuple of (snapshot_id, git_sha) where snapshot_id is snap-{sha}-{timestamp}
        """
        # Sync changes to branch and commit
        sha = self.sync_slot_to_branch(slot_path, workspace_name, message)

        # Create snapshot tag in the dev repo
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        snapshot_id = f"snap-{sha[:8]}-{timestamp}"

        self._run_git("tag", snapshot_id, sha, cwd=self.dev_repo)

        return snapshot_id, sha
