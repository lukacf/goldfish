"""High-level workspace operations.

Coordinates git_layer, audit, and state_md updates.
"""

import fcntl
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.errors import (
    GoldfishError,
    InvalidSlotError,
    SlotEmptyError,
    SlotNotEmptyError,
    SyncError,
    WorkspaceNotFoundError,
    validate_reason,
)
from goldfish.models import (
    CheckpointResponse,
    CreateWorkspaceResponse,
    DiffResponse,
    DirtyState,
    HibernateResponse,
    MountResponse,
    RollbackResponse,
    SlotInfo,
    SlotState,
    WorkspaceInfo,
)
from goldfish.workspace.git_layer import GitLayer


class WorkspaceManager:
    """Manages workspace slots and operations."""

    SOFT_LIMIT = 3  # Warn but don't block above this

    def __init__(
        self,
        config: GoldfishConfig,
        project_root: Path,
        db: Database,
        state_manager=None,  # Will be StateManager once implemented
    ):
        self.config = config
        self.project_root = project_root
        self.db = db
        self.state_manager = state_manager

        # Resolve dev repo path (relative to project parent, not project itself)
        dev_repo = (project_root.parent / config.dev_repo_path).resolve()
        self.workspaces_dir = project_root / config.workspaces_dir

        self.git = GitLayer(dev_repo, project_root, config.workspaces_dir)

    def _slot_path(self, slot: str) -> Path:
        """Get filesystem path for a slot."""
        return self.workspaces_dir / slot

    def _validate_slot(self, slot: str) -> None:
        """Validate slot name."""
        if slot not in self.config.slots:
            raise InvalidSlotError(
                f"Invalid slot: {slot}. Valid slots: {self.config.slots}"
            )

    @contextmanager
    def _acquire_slot_lock(self, slot: str):
        """Acquire an exclusive lock for a slot operation.

        Uses file-based locking to prevent concurrent operations on the same slot.
        This prevents TOCTOU race conditions and git lock conflicts.

        Args:
            slot: Slot name to lock

        Yields:
            None (lock is held during context)

        Raises:
            GoldfishError: If lock cannot be acquired within timeout
        """
        # Create locks directory if it doesn't exist
        locks_dir = self.project_root / ".goldfish-locks"
        locks_dir.mkdir(parents=True, exist_ok=True)

        lock_file_path = locks_dir / f"{slot}.lock"
        lock_file = None

        try:
            # Open/create lock file with O_NOFOLLOW to prevent symlink attacks
            fd = os.open(
                lock_file_path,
                os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW,
                0o644
            )
            lock_file = os.fdopen(fd, "w")

            # Try to acquire exclusive lock (non-blocking with timeout)
            import time
            timeout = 10  # seconds
            start_time = time.time()

            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break  # Lock acquired
                except (IOError, OSError):
                    # Lock not available - check timeout
                    if time.time() - start_time > timeout:
                        raise GoldfishError(
                            f"workspace is locked - another operation may be in progress"
                        )
                    time.sleep(0.01)  # Wait 10ms before retry

            # Lock acquired - yield control
            yield

        finally:
            # Release lock and close file
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                except (IOError, OSError):
                    pass  # Best effort cleanup

    def _get_slot_state(self, slot: str) -> SlotInfo:
        """Get current state of a slot."""
        slot_path = self._slot_path(slot)

        # Check if slot directory exists and has content
        if not slot_path.exists():
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        # Check if it's an empty directory
        try:
            if not any(slot_path.iterdir()):
                return SlotInfo(slot=slot, state=SlotState.EMPTY)
        except PermissionError:
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        # Find which branch is mounted here
        wt = self.git.get_worktree_for_slot(slot_path)
        if wt is None:
            # Directory exists but isn't a worktree - treat as empty
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        # Extract workspace name from branch
        branch = wt.get("branch", "")
        workspace = None
        if branch.startswith("refs/heads/experiment/"):
            workspace = branch.replace("refs/heads/experiment/", "")
        elif branch.startswith("experiment/"):
            workspace = branch.replace("experiment/", "")

        if workspace is None:
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        # Check dirty state
        dirty = DirtyState.DIRTY if self.git.is_dirty(slot_path) else DirtyState.CLEAN

        # Get last checkpoint
        last_checkpoint = self.git.get_latest_snapshot(slot_path)

        return SlotInfo(
            slot=slot,
            state=SlotState.MOUNTED,
            workspace=workspace,
            dirty=dirty,
            last_checkpoint=last_checkpoint,
        )

    def get_all_slots(self) -> list[SlotInfo]:
        """Get state of all slots."""
        return [self._get_slot_state(slot) for slot in self.config.slots]

    def count_active_slots(self) -> int:
        """Count how many slots have mounted workspaces."""
        return sum(1 for s in self.get_all_slots() if s.state == SlotState.MOUNTED)

    def get_workspace_path(self, workspace: str) -> Path:
        """Get filesystem path for a workspace.

        The workspace must be mounted to a slot to have a path.

        Args:
            workspace: Workspace name

        Returns:
            Path to the mounted workspace

        Raises:
            GoldfishError: If workspace is not currently mounted
        """
        # Find which slot this workspace is mounted to
        for slot_info in self.get_all_slots():
            if slot_info.workspace == workspace and slot_info.state == SlotState.MOUNTED:
                return self._slot_path(slot_info.slot)

        # Workspace not mounted
        raise GoldfishError(
            f"Workspace '{workspace}' is not currently mounted. "
            f"Mount it to a slot first using mount()."
        )

    def _regenerate_state_md(self) -> str:
        """Regenerate STATE.md and return content."""
        if self.state_manager:
            return self.state_manager.regenerate(
                slots=self.get_all_slots(),
                jobs=self.db.get_active_jobs(),
                source_count=len(self.db.list_sources()),
            )
        return "# Project\n\nSTATE.md not yet initialized"

    def mount(self, workspace: str, slot: str, reason: str) -> MountResponse:
        """Mount a workspace into a slot."""
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        # Check workspace exists
        if not self.git.branch_exists(workspace):
            raise WorkspaceNotFoundError(f"Workspace '{workspace}' does not exist")

        # Check soft limit
        warning = None
        active = self.count_active_slots()
        if active >= self.SOFT_LIMIT:
            warning = (
                f"You have {active} active workspaces. "
                f"Consider hibernating one to maintain focus."
            )

        # Use file-based locking to prevent concurrent mounts to the same slot
        # This prevents TOCTOU race conditions and git lock conflicts
        slot_path = self._slot_path(slot)
        with self._acquire_slot_lock(slot):
            # Check if slot is already mounted (under lock)
            slot_info = self._get_slot_state(slot)
            if slot_info.state == SlotState.MOUNTED:
                raise SlotNotEmptyError(
                    f"Slot {slot} already has workspace '{slot_info.workspace}'. "
                    f"Hibernate it first."
                )

            # Perform mount - now protected by lock
            try:
                self.git.add_worktree(workspace, slot_path)
            except GoldfishError as e:
                # Mount failed - re-check slot state to provide better error message
                slot_info = self._get_slot_state(slot)
                if slot_info.state == SlotState.MOUNTED:
                    raise SlotNotEmptyError(
                        f"Slot {slot} already has workspace '{slot_info.workspace}'. "
                        f"Hibernate it first."
                    )
                # Otherwise, re-raise original error
                raise

        # Log to audit
        self.db.log_audit(
            operation="mount",
            slot=slot,
            workspace=workspace,
            reason=reason,
            details={"warning": warning},
        )

        # Update STATE.md
        if self.state_manager:
            self.state_manager.add_action(f"Mounted '{workspace}' to {slot}")

        state_md = self._regenerate_state_md()

        # Get final state
        final_info = self._get_slot_state(slot)

        return MountResponse(
            success=True,
            slot=slot,
            workspace=workspace,
            state_md=state_md,
            dirty=final_info.dirty or DirtyState.CLEAN,
            last_checkpoint=final_info.last_checkpoint,
            warning=warning,
        )

    def hibernate(self, slot: str, reason: str) -> HibernateResponse:
        """Save and free a slot."""
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is already empty")

        workspace = slot_info.workspace
        slot_path = self._slot_path(slot)

        # Auto-checkpoint if dirty
        auto_checkpointed = False
        checkpoint_id = None
        if slot_info.dirty == DirtyState.DIRTY:
            checkpoint_id = self.git.create_snapshot(
                slot_path, f"Auto-checkpoint before hibernate: {reason}"
            )
            auto_checkpointed = True

        # Push to remote (best effort - log failures but don't block hibernate)
        pushed = False
        push_error = None
        if self.git.has_remote():
            try:
                self.git.push_branch(workspace)
                pushed = True
            except (GoldfishError, SyncError) as e:
                # Log the push failure but don't fail hibernate
                push_error = str(e)
                self.db.log_audit(
                    operation="push_failed",
                    slot=slot,
                    workspace=workspace,
                    reason=f"Push failed during hibernate: {push_error}",
                )

        # Remove worktree
        self.git.remove_worktree(slot_path)

        # Log to audit
        self.db.log_audit(
            operation="hibernate",
            slot=slot,
            workspace=workspace,
            reason=reason,
            details={
                "auto_checkpointed": auto_checkpointed,
                "checkpoint_id": checkpoint_id,
                "pushed": pushed,
            },
        )

        # Update STATE.md
        if self.state_manager:
            action = f"Hibernated '{workspace}' from {slot}"
            if auto_checkpointed:
                action += f" (auto-checkpoint: {checkpoint_id})"
            self.state_manager.add_action(action)

        state_md = self._regenerate_state_md()

        return HibernateResponse(
            success=True,
            slot=slot,
            workspace=workspace,
            state_md=state_md,
            auto_checkpointed=auto_checkpointed,
            checkpoint_id=checkpoint_id,
            pushed_to_remote=pushed,
        )

    def create_workspace(
        self, name: str, goal: str, reason: str, from_ref: str = "main"
    ) -> CreateWorkspaceResponse:
        """Create a new workspace from main (or another ref)."""
        validate_reason(reason, self.config.audit.min_reason_length)

        if self.git.branch_exists(name):
            raise GoldfishError(f"Workspace '{name}' already exists")

        # Create the branch
        self.git.create_branch(name, from_ref)

        # Log to audit
        self.db.log_audit(
            operation="create_workspace",
            workspace=name,
            reason=reason,
            details={"goal": goal, "from": from_ref},
        )

        # Update STATE.md
        if self.state_manager:
            self.state_manager.add_action(f"Created workspace '{name}': {goal}")

        state_md = self._regenerate_state_md()

        return CreateWorkspaceResponse(
            success=True,
            workspace=name,
            forked_from=from_ref,
            state_md=state_md,
        )

    def list_workspaces(self, limit: int = 50, offset: int = 0) -> list[WorkspaceInfo]:
        """List workspaces with pagination.

        Args:
            limit: Maximum number of workspaces to return (1-200, default 50)
            offset: Number of workspaces to skip (default 0)

        Returns:
            List of WorkspaceInfo objects

        Raises:
            GoldfishError: If limit or offset are out of bounds
        """
        # Validate bounds
        if limit < 1 or limit > 200:
            raise GoldfishError("limit must be between 1 and 200")
        if offset < 0:
            raise GoldfishError("offset must be >= 0")

        branches = self.git.list_branches()

        # Build map of mounted workspaces
        mounted_map: dict[str, str] = {}  # workspace -> slot
        for slot_info in self.get_all_slots():
            if slot_info.workspace:
                mounted_map[slot_info.workspace] = slot_info.slot

        workspaces = []
        for name in branches:
            info = self.git.get_branch_info(name)

            # Parse timestamps
            created_at = (
                datetime.fromisoformat(info["created_at"])
                if info["created_at"]
                else datetime.now(timezone.utc)
            )
            last_activity = (
                datetime.fromisoformat(info["last_activity"])
                if info["last_activity"]
                else created_at
            )

            workspaces.append(
                WorkspaceInfo(
                    name=name,
                    created_at=created_at,
                    goal="",  # Would need separate storage for goals
                    snapshot_count=info["snapshot_count"],
                    last_activity=last_activity,
                    is_mounted=name in mounted_map,
                    mounted_slot=mounted_map.get(name),
                )
            )

        # Sort by last_activity descending (most recent first)
        workspaces.sort(key=lambda w: w.last_activity, reverse=True)

        # Apply pagination
        return workspaces[offset : offset + limit]

    def checkpoint(self, slot: str, message: str) -> CheckpointResponse:
        """Create a snapshot of the current slot state."""
        self._validate_slot(slot)
        validate_reason(message, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty")

        slot_path = self._slot_path(slot)
        snapshot_id = self.git.create_snapshot(slot_path, message)

        # Log to audit
        self.db.log_audit(
            operation="checkpoint",
            slot=slot,
            workspace=slot_info.workspace,
            reason=message,
            details={"snapshot_id": snapshot_id},
        )

        # Update STATE.md
        if self.state_manager:
            self.state_manager.add_action(f"Checkpoint {snapshot_id}: {message[:40]}...")

        state_md = self._regenerate_state_md()

        return CheckpointResponse(
            success=True,
            slot=slot,
            snapshot_id=snapshot_id,
            message=message,
            state_md=state_md,
        )

    def get_slot_path(self, slot: str) -> Path:
        """Get the filesystem path for a slot (for job launcher)."""
        self._validate_slot(slot)
        return self._slot_path(slot)

    def get_slot_info(self, slot: str) -> SlotInfo:
        """Get info for a specific slot."""
        self._validate_slot(slot)
        return self._get_slot_state(slot)

    def get_workspace_for_slot(self, workspace_or_slot: str) -> Optional[str]:
        """Resolve workspace name from slot or workspace name.

        Args:
            workspace_or_slot: Either a slot name (e.g., "w1") or workspace name (e.g., "baseline")

        Returns:
            Workspace name if input is a slot with a mounted workspace, None otherwise
        """
        # Check if it's a valid slot
        if workspace_or_slot in self.config.slots:
            slot_info = self._get_slot_state(workspace_or_slot)
            return slot_info.workspace  # Will be None if slot is empty
        return None

    def diff(self, slot: str) -> DiffResponse:
        """Show changes in a slot since last checkpoint.

        Args:
            slot: Slot to diff

        Returns:
            DiffResponse with change summary and file list
        """
        self._validate_slot(slot)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty")

        slot_path = self._slot_path(slot)

        # Get diff output from git
        has_changes = self.git.is_dirty(slot_path)

        if not has_changes:
            return DiffResponse(
                slot=slot,
                has_changes=False,
                summary="No changes since last checkpoint",
                files_changed=[],
                diff_text="",
            )

        # Get list of changed files
        changed_files = self.git.get_changed_files(slot_path)

        # Get diff statistics
        diff_stats = self.git.get_diff_stats(slot_path)

        # Get full diff text
        diff_text = self.git.get_diff_text(slot_path)

        return DiffResponse(
            slot=slot,
            has_changes=True,
            summary=diff_stats,
            files_changed=changed_files,
            diff_text=diff_text,
        )

    def rollback(self, slot: str, snapshot_id: str, reason: str) -> RollbackResponse:
        """Rollback a slot to a previous snapshot.

        Discards all changes since the snapshot.

        Args:
            slot: Slot to rollback
            snapshot_id: Snapshot to rollback to
            reason: Why rolling back (min 15 chars)

        Returns:
            RollbackResponse with result
        """
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty")

        slot_path = self._slot_path(slot)

        # Perform the rollback
        files_reverted = self.git.checkout_snapshot(slot_path, snapshot_id)

        # Log to audit
        self.db.log_audit(
            operation="rollback",
            slot=slot,
            workspace=slot_info.workspace,
            reason=reason,
            details={
                "snapshot_id": snapshot_id,
                "files_reverted": files_reverted,
            },
        )

        # Update STATE.md
        if self.state_manager:
            self.state_manager.add_action(
                f"Rolled back {slot} to {snapshot_id} ({files_reverted} files)"
            )

        state_md = self._regenerate_state_md()

        return RollbackResponse(
            success=True,
            slot=slot,
            snapshot_id=snapshot_id,
            files_reverted=files_reverted,
            state_md=state_md,
        )

    def list_snapshots(
        self, workspace: str, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        """List snapshots for a workspace with pagination.

        Args:
            workspace: Workspace name
            limit: Maximum number of snapshots to return (1-200, default 50)
            offset: Number of snapshots to skip (default 0)

        Returns:
            List of dicts with snapshot_id, created_at, message

        Raises:
            GoldfishError: If limit or offset are out of bounds
        """
        from datetime import datetime

        # Validate bounds
        if limit < 1 or limit > 200:
            raise GoldfishError("limit must be between 1 and 200")
        if offset < 0:
            raise GoldfishError("offset must be >= 0")

        # Get snapshot IDs
        snapshot_ids = self.git.list_snapshots(workspace)

        # Get detailed info for each
        snapshots = []
        for snap_id in snapshot_ids:
            info = self.git.get_snapshot_info(snap_id)
            created_at = None
            if info.get("commit_date"):
                try:
                    created_at = datetime.fromisoformat(info["commit_date"])
                except ValueError:
                    pass

            snapshots.append({
                "snapshot_id": snap_id,
                "created_at": created_at,
                "message": info.get("message", ""),
            })

        # Sort by created_at descending (newest first)
        snapshots.sort(
            key=lambda x: x["created_at"] or datetime.min,
            reverse=True
        )

        # Apply pagination
        return snapshots[offset : offset + limit]
