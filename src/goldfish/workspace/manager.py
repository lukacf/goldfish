"""High-level workspace operations.

Coordinates git_layer, audit, and state_md updates.

Architecture: Copy-based workspaces with no git in user workspace.

MOUNT:  gf-dev/branch ──copy──▶ user/w1 (plain files, NO .git)
WORK:   Claude edits user/w1
RUN:    user/w1 ──sync──▶ gf-dev/branch ──commit──▶ execute with SHA

All git operations happen in the goldfish dev repo. The user's workspace
is just a directory of plain files with a `.goldfish-mount` metadata file.
"""

import fcntl
import json
import os
import warnings
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

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
    SaveVersionResponse,
    SlotInfo,
    SlotState,
    WorkflowInfo,
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
        self.dev_repo = config.get_dev_repo_path(project_root)
        self.workspaces_dir = project_root / config.workspaces_dir

        self.git = GitLayer(self.dev_repo, project_root, config.workspaces_dir)

    def _slot_path(self, slot: str) -> Path:
        """Get filesystem path for a slot."""
        return self.workspaces_dir / slot

    def _validate_slot(self, slot: str) -> None:
        """Validate slot name."""
        if slot not in self.config.slots:
            raise InvalidSlotError(f"Invalid slot: {slot}. Valid slots: {self.config.slots}")

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
        # Create locks directory in dev repo (Goldfish runtime artifact)
        locks_dir = self.dev_repo / ".goldfish" / "locks"
        locks_dir.mkdir(parents=True, exist_ok=True)

        lock_file_path = locks_dir / f"{slot}.lock"
        lock_file = None

        try:
            # Open/create lock file with O_NOFOLLOW to prevent symlink attacks
            fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o644)
            lock_file = os.fdopen(fd, "w")

            # Try to acquire exclusive lock (non-blocking with timeout)
            import time

            timeout = 10  # seconds
            start_time = time.time()

            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break  # Lock acquired
                except OSError as e:
                    # Lock not available - check timeout
                    if time.time() - start_time > timeout:
                        raise GoldfishError("workspace is locked - another operation may be in progress") from e
                    time.sleep(0.01)  # Wait 10ms before retry

            # Lock acquired - yield control
            yield

        finally:
            # Release lock and close file
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                except OSError:
                    pass  # Best effort cleanup

    def _get_slot_state(self, slot: str) -> SlotInfo:
        """Get current state of a slot.

        With copy-based mounting, we check for the .goldfish-mount metadata file
        instead of git worktrees. The slot is just a plain directory.
        """
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

        # Check for .goldfish-mount metadata file (copy-based mounting)
        metadata_file = slot_path / ".goldfish-mount"
        if not metadata_file.exists():
            # Directory exists but isn't a Goldfish workspace - treat as empty
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        try:
            metadata = json.loads(metadata_file.read_text())
            workspace = metadata.get("workspace_name")
        except (json.JSONDecodeError, OSError):
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        if workspace is None:
            return SlotInfo(slot=slot, state=SlotState.EMPTY)

        # Get last checkpoint from the workspace (use list_snapshots to get from branch)
        snapshots = self.git.list_snapshots(workspace)
        last_checkpoint = snapshots[0] if snapshots else None

        # Determine compare SHA for dirty check:
        # - If versions exist, compare against latest version
        # - Otherwise compare against mounted_sha (initial state)
        compare_sha = None
        latest_version = self.db.get_latest_version(workspace)
        if latest_version:
            compare_sha = latest_version["git_sha"]
        else:
            compare_sha = metadata.get("mounted_sha")

        # Check dirty state by comparing files against compare_sha
        dirty = DirtyState.DIRTY  # Default to dirty if comparison fails
        if compare_sha:
            try:
                is_dirty = self.git.is_slot_dirty(slot_path, workspace, compare_sha)
                dirty = DirtyState.DIRTY if is_dirty else DirtyState.CLEAN
            except Exception:
                # On error, assume dirty to be safe
                dirty = DirtyState.DIRTY

        # Get lineage information
        versions = self.db.list_versions(workspace)
        current_version = versions[0]["version"] if versions else None
        version_count = len(versions)

        # Get workspace lineage (parent info)
        lineage = self.db.get_workspace_lineage(workspace)
        parent_workspace = lineage.get("parent_workspace") if lineage else None
        parent_version = lineage.get("parent_version") if lineage else None

        # Get recent version history (last 5)
        version_history = (
            [
                {
                    "version": v["version"],
                    "git_sha": v["git_sha"][:8] if v["git_sha"] else None,
                    "created_by": v["created_by"],
                }
                for v in versions[:5]
            ]
            if versions
            else None
        )

        # Get branches (child workspaces)
        branches_raw = self.db.get_workspace_branches(workspace)
        branches = (
            [{"workspace": b["workspace_name"], "branched_at": b["parent_version"]} for b in branches_raw]
            if branches_raw
            else None
        )

        return SlotInfo(
            slot=slot,
            state=SlotState.MOUNTED,
            workspace=workspace,
            dirty=dirty,
            last_checkpoint=last_checkpoint,
            current_version=current_version,
            version_count=version_count,
            parent_workspace=parent_workspace,
            parent_version=parent_version,
            version_history=version_history,
            branches=branches,
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
            f"Workspace '{workspace}' is not currently mounted. Mount it to a slot first using mount()."
        )

    def _regenerate_state_md(self) -> str:
        """Regenerate STATE.md and return content."""
        if self.state_manager:
            jobs = self.db.get_active_jobs()
            # Get recent runs (last 5) for display in STATE.md
            recent_runs_rows = self.db.list_stage_runs_with_total(limit=5, offset=0)
            recent_runs = [dict(r) for r in recent_runs_rows] if recent_runs_rows else []
            result: str = self.state_manager.regenerate(
                slots=self.get_all_slots(),
                jobs=[dict(j) for j in jobs],  # Convert JobRow to dict
                source_count=len(self.db.list_sources()),
                recent_runs=recent_runs,
            )
            return result
        return "# Project\n\nSTATE.md not yet initialized"

    def _write_workspace_state_md(self, slot_path: Path, workspace: str, slot: str, event: str | None = None) -> None:
        """Write per-workspace STATE.md to slot directory.

        This provides workspace-specific context for Claude's compaction recovery.
        Each mounted workspace has its own STATE.md with:
        - Workspace name and goal
        - Current slot
        - Version history
        - Recent actions for this workspace
        """
        lines = [f"# Workspace: {workspace}", ""]

        # Goal
        goal = self.db.get_workspace_goal(workspace) or "Not set"
        lines.extend(["## Goal", goal, ""])

        # Status
        lines.append("## Status")
        lines.append(f"- Mounted to: {slot}")

        # Get version info
        versions = self.db.list_versions(workspace)
        if versions:
            latest = versions[0]
            lines.append(f"- Current version: {latest['version']}")
            lines.append(f"- Total versions: {len(versions)}")
        else:
            lines.append("- No versions yet (run checkpoint or execute to create)")
        lines.append("")

        # Lineage info
        lineage = self.db.get_workspace_lineage(workspace)
        if lineage and lineage.get("parent_workspace"):
            lines.append("## Lineage")
            parent_info = f"Branched from: {lineage['parent_workspace']}"
            if lineage.get("parent_version"):
                parent_info += f" @ {lineage['parent_version']}"
            lines.append(f"- {parent_info}")
            lines.append("")

        # Recent versions (up to 5)
        if versions:
            lines.append("## Version History")
            for v in versions[:5]:
                desc = v.get("description", "")
                created = v.get("created_at", "")
                if created and isinstance(created, str) and len(created) > 10:
                    created = created[:10]  # Just date
                line = f"- {v['version']}"
                if created:
                    line += f" ({created})"
                if desc:
                    line += f": {desc[:50]}"
                lines.append(line)
            if len(versions) > 5:
                lines.append(f"- ... and {len(versions) - 5} more")
            lines.append("")

        # Recent actions for this workspace (from audit log)
        recent_audits = self.db.get_recent_audit(limit=20)
        workspace_actions = [a for a in recent_audits if a.get("workspace") == workspace][:10]

        lines.append("## Recent Actions")
        if workspace_actions:
            for a in workspace_actions:
                timestamp = a.get("timestamp", "")
                if timestamp and isinstance(timestamp, str) and len(timestamp) > 16:
                    timestamp = timestamp[11:16]  # Just time HH:MM
                op = a.get("operation", "unknown")
                reason_text = a.get("reason", "")[:40]
                lines.append(f"- [{timestamp}] {op}: {reason_text}")
        else:
            lines.append("- No recent actions")

        if event:
            lines.append(f"- [now] {event}")
        lines.append("")

        # Configuration invariants
        if self.config.invariants:
            lines.append("## Invariants (DO NOT CHANGE)")
            for inv in self.config.invariants:
                lines.append(f"- {inv}")
            lines.append("")

        content = "\n".join(lines)

        # Write atomically
        state_path = slot_path / "STATE.md"
        state_path.write_text(content)

    def mount(self, workspace: str, slot: str, reason: str) -> MountResponse:
        """Mount a workspace into a slot.

        Copy-based mounting: Copies files from dev repo branch to slot directory.
        No .git in the slot - all versioning happens in the dev repo.
        """
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        # Check workspace exists
        if not self.git.branch_exists(workspace):
            raise WorkspaceNotFoundError(f"Workspace '{workspace}' does not exist")

        # Check if workspace is already mounted elsewhere
        existing_mount = self.db.get_mount_by_workspace(workspace)
        if existing_mount and existing_mount["status"] == "active":
            raise GoldfishError(
                f"Workspace '{workspace}' is already mounted in slot '{existing_mount['slot']}'. "
                f"Hibernate it first before mounting elsewhere."
            )

        # Check soft limit
        warning = None
        active = self.count_active_slots()
        if active >= self.SOFT_LIMIT:
            warning = f"You have {active} active workspaces. Consider hibernating one to maintain focus."

        # Use file-based locking to prevent concurrent mounts to the same slot
        # This prevents TOCTOU race conditions
        slot_path = self._slot_path(slot)
        with self._acquire_slot_lock(slot):
            # Check if slot is already mounted (under lock)
            slot_info = self._get_slot_state(slot)
            if slot_info.state == SlotState.MOUNTED:
                # Check if DB knows about this mount - if not, it's stale from a crash
                db_mount = self.db.get_mount(slot)
                if db_mount is not None:
                    # DB has record - it's really mounted
                    raise SlotNotEmptyError(
                        f"Slot {slot} already has workspace '{slot_info.workspace}'. Hibernate it first."
                    )
                # No DB record - stale mount from crash, we can overwrite
                # Clean up the stale directory first
                import shutil

                shutil.rmtree(slot_path)

            # Perform copy-based mount
            mount_info = self.git.copy_mount_workspace(workspace, slot_path)

            # Record mount in database
            self.db.record_mount(
                slot=slot,
                workspace_name=workspace,
                branch=mount_info["branch"],
                mounted_sha=mount_info["mounted_sha"],
            )

        # Log to audit
        self.db.log_audit(
            operation="mount",
            slot=slot,
            workspace=workspace,
            reason=reason,
            details={"warning": warning, "mounted_sha": mount_info["mounted_sha"]},
        )

        # Write per-workspace STATE.md to slot
        self._write_workspace_state_md(slot_path, workspace, slot, event=f"Mounted: {reason}")

        # Update global STATE.md
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
        """Save and free a slot.

        Copy-based hibernate: Syncs changes back to dev repo branch,
        commits them, then removes the slot directory.
        """
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY or slot_info.workspace is None:
            raise SlotEmptyError(f"Slot {slot} is already empty")

        workspace = slot_info.workspace
        slot_path = self._slot_path(slot)

        # Sync changes back to branch and commit (always, since we can't easily detect dirty)
        auto_checkpointed = False
        checkpoint_id = None
        try:
            commit_sha = self.git.copy_unmount_workspace(
                slot_path=slot_path,
                workspace_name=workspace,
                commit_msg=f"Hibernate: {reason}",
            )
            if commit_sha:
                auto_checkpointed = True
                # Create a snapshot-style ID for the checkpoint
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                checkpoint_id = f"snap-{commit_sha[:8]}-{timestamp}"
        except GoldfishError:
            # If sync fails, still try to clean up
            pass

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

        # Delete mount record from database
        self.db.delete_mount(slot)

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

    def create_workspace(self, name: str, goal: str, reason: str, from_ref: str = "main") -> CreateWorkspaceResponse:
        """Create a new workspace from main (or another ref)."""
        validate_reason(reason, self.config.audit.min_reason_length)

        if self.git.branch_exists(name):
            raise GoldfishError(f"Workspace '{name}' already exists")

        # Create the branch
        self.git.create_branch(name, from_ref)

        # Create workspace lineage record (tracks parent and history)
        self.db.create_workspace_lineage(
            workspace_name=name,
            parent_workspace=from_ref if from_ref != "main" else None,
            parent_version=None,  # No version when branching from ref
            description=goal,
        )

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

    def branch_workspace(
        self,
        from_workspace: str,
        from_version: str,
        new_workspace: str,
    ) -> None:
        """Create new workspace branched from specific version.

        Args:
            from_workspace: Source workspace name
            from_version: Version to branch from (e.g., "v3")
            new_workspace: Name for new workspace

        Raises:
            GoldfishError: If version not found or workspace already exists
        """
        # Get version info from database
        version = self.db.get_version(from_workspace, from_version)
        if not version:
            raise GoldfishError(f"Version '{from_version}' not found in workspace '{from_workspace}'")

        # Check new workspace doesn't already exist
        if self.git.branch_exists(new_workspace):
            raise GoldfishError(f"Workspace '{new_workspace}' already exists")

        # Create branch from the version's git SHA
        self.git.create_branch(new_workspace, version["git_sha"])

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
            created_at = datetime.fromisoformat(info["created_at"]) if info["created_at"] else datetime.now(UTC)
            last_activity = datetime.fromisoformat(info["last_activity"]) if info["last_activity"] else created_at

            # Get goal from database
            goal = self.db.get_workspace_goal(name) or ""

            workspaces.append(
                WorkspaceInfo(
                    name=name,
                    created_at=created_at,
                    goal=goal,
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

    def save_version(self, slot: str, message: str) -> SaveVersionResponse:
        """Create a version of the current slot state.

        Syncs slot changes to branch, commits, and creates a tagged version.
        The version (v1, v2, etc.) is the primary identifier for rollback
        and branching operations.

        Args:
            slot: Slot to save version from (w1, w2, or w3)
            message: Describe what this version represents (min 15 chars)

        Returns:
            SaveVersionResponse with version as primary identifier
        """
        self._validate_slot(slot)
        validate_reason(message, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty")

        slot_path = self._slot_path(slot)
        workspace = slot_info.workspace
        if workspace is None:
            raise SlotEmptyError(f"Slot {slot} has no workspace")
        git_tag, git_sha = self.git.create_snapshot_copy_based(slot_path, workspace, message)

        # Ensure workspace lineage exists in database
        if not self.db.workspace_exists(workspace):
            self.db.create_workspace_lineage(
                workspace_name=workspace,
                description="Auto-created for save_version",
            )

        # Register version in database (critical for branching/lineage)
        version = self.db.get_next_version_number(workspace)
        self.db.create_version(
            workspace_name=workspace,
            version=version,
            git_tag=git_tag,
            git_sha=git_sha,
            created_by="save_version",
            description=message,
        )

        # Log to audit
        self.db.log_audit(
            operation="save_version",
            slot=slot,
            workspace=slot_info.workspace,
            reason=message,
            details={"version": version, "git_tag": git_tag, "git_sha": git_sha},
        )

        # Update per-workspace STATE.md
        self._write_workspace_state_md(slot_path, workspace, slot, event=f"Version {version}: {message[:30]}")

        # Update global STATE.md
        if self.state_manager:
            self.state_manager.add_action(f"Version {version}: {message[:40]}...")

        state_md = self._regenerate_state_md()

        return SaveVersionResponse(
            success=True,
            slot=slot,
            version=version,
            git_tag=git_tag,
            git_sha=git_sha,
            message=message,
            state_md=state_md,
        )

    def checkpoint(self, slot: str, message: str) -> CheckpointResponse:
        """Create a snapshot of the current slot state.

        DEPRECATED: Use save_version() instead. checkpoint() will be removed
        in a future version.

        Copy-based checkpoint: Syncs slot changes to branch, commits, and tags.
        """
        warnings.warn(
            "checkpoint() is deprecated, use save_version() instead",
            DeprecationWarning,
            stacklevel=2,
        )

        # Use save_version internally
        result = self.save_version(slot, message)

        # Return old-style response for backwards compatibility
        return CheckpointResponse(
            success=result.success,
            slot=result.slot,
            snapshot_id=result.git_tag,  # Map git_tag to old snapshot_id field
            message=result.message,
            state_md=result.state_md,
        )

    def sync_and_version(self, slot: str, stage_name: str, reason: str | None = None) -> tuple[str, str]:
        """Sync slot changes to branch and create a version tag.

        This is the core provenance guard: ensures all code is committed
        before any run() execution. Every stage run has 100% provenance.

        Args:
            slot: Slot with mounted workspace
            stage_name: Name of stage being run (for version description)
            reason: Optional reason for version

        Returns:
            Tuple of (version_string, git_sha) - e.g., ("v1", "abc123...")

        Raises:
            SlotEmptyError: If slot is not mounted
        """
        self._validate_slot(slot)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY or slot_info.workspace is None:
            raise SlotEmptyError(f"Slot {slot} is empty - cannot version")

        workspace = slot_info.workspace
        slot_path = self._slot_path(slot)

        # Ensure workspace lineage exists in database
        if not self.db.workspace_exists(workspace):
            self.db.create_workspace_lineage(
                workspace_name=workspace,
                description=f"Auto-created for {stage_name}",
            )

        # 1. Sync slot changes to branch and commit
        commit_msg = reason or f"Auto-version for {stage_name}"
        git_sha = self.git.sync_slot_to_branch(slot_path, workspace, commit_msg)

        # 2. Get next version number and create tag
        next_version = self.db.get_next_version_number(workspace)
        git_tag = f"{workspace}-{next_version}"
        self.git.create_tag(workspace, git_tag, git_sha)

        # 3. Record version in database
        description = reason or f"Auto-version for {stage_name} run"
        self.db.create_version(
            workspace_name=workspace,
            version=next_version,
            git_tag=git_tag,
            git_sha=git_sha,
            created_by="run",
            description=description,
        )

        # 4. Update slot metadata to reflect synced state
        metadata_file = slot_path / ".goldfish-mount"
        if metadata_file.exists():
            metadata = json.loads(metadata_file.read_text())
            metadata["mounted_sha"] = git_sha
            metadata_file.write_text(json.dumps(metadata, indent=2))

        # 5. Record in audit log
        self.db.log_audit(
            operation="sync_and_version",
            slot=slot,
            workspace=workspace,
            reason=f"Version {next_version}: {commit_msg}",
            details={"version": next_version, "git_sha": git_sha, "stage": stage_name},
        )

        # 6. Update per-workspace STATE.md
        self._write_workspace_state_md(slot_path, workspace, slot, event=f"Version {next_version} for {stage_name}")

        return next_version, git_sha

    def get_slot_path(self, slot: str) -> Path:
        """Get the filesystem path for a slot (for job launcher)."""
        self._validate_slot(slot)
        return self._slot_path(slot)

    def get_slot_info(self, slot: str) -> SlotInfo:
        """Get info for a specific slot."""
        self._validate_slot(slot)
        return self._get_slot_state(slot)

    def get_workspace_for_slot(self, workspace_or_slot: str) -> str | None:
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

    def _parse_diff_target(self, target: str) -> dict:
        """Parse a diff target into its components.

        Args:
            target: Can be:
                - Slot: "w1", "w2", "w3"
                - Version: "v1", "v2" (requires workspace context)
                - Workspace@version: "baseline@v2", "experiment@v3"

        Returns:
            Dict with keys: type ("slot", "version"), and relevant fields
        """
        # Check if it's a slot
        if target in self.config.slots:
            slot_info = self._get_slot_state(target)
            if slot_info.state == SlotState.EMPTY:
                raise SlotEmptyError(f"Slot {target} is empty")
            return {
                "type": "slot",
                "slot": target,
                "workspace": slot_info.workspace,
                "path": self._slot_path(target),
            }

        # Check if it's workspace@version format
        if "@" in target:
            workspace, version = target.split("@", 1)
            version_info = self.db.get_version(workspace, version)
            if not version_info:
                raise GoldfishError(f"Version '{version}' not found for workspace '{workspace}'")
            return {
                "type": "version",
                "workspace": workspace,
                "version": version,
                "git_sha": version_info["git_sha"],
            }

        # Check if it's a bare version (v1, v2) - needs workspace context from caller
        if target.startswith("v") and target[1:].isdigit():
            return {
                "type": "bare_version",
                "version": target,
            }

        raise GoldfishError(
            f"Invalid diff target: '{target}'. " f"Use slot (w1), version (v1), or workspace@version (baseline@v2)"
        )

    def diff(self, target: str, against: str | None = None) -> DiffResponse:
        """Compare changes between targets.

        Single argument: Compare slot against its last saved version.
        Two arguments: Compare any two targets (slots, versions, workspace@version).

        Args:
            target: What to diff. Can be:
                - Slot: "w1" (compares against last version if alone)
                - Version: "v1" (needs workspace context or second arg)
                - Workspace@version: "baseline@v2"
            against: Optional second target to compare against.
                If omitted and target is a slot, compares against last version.

        Returns:
            DiffResponse with change summary, files, and comparison details

        Examples:
            diff("w1")                    # Slot vs last version
            diff("w1", "w2")              # Compare two slots
            diff("v1", "v5")              # Compare two versions (same workspace)
            diff("baseline@v1", "exp@v3") # Compare across workspaces
        """
        left = self._parse_diff_target(target)

        # Single argument: slot vs last version
        if against is None:
            if left["type"] != "slot":
                raise GoldfishError(
                    "Single-argument diff requires a slot (w1, w2, w3). "
                    "For versions, use diff('v1', 'v5') or diff('workspace@v1', 'workspace@v2')"
                )
            return self._diff_slot_against_last_version(left)

        # Two arguments: compare the two targets
        right = self._parse_diff_target(against)

        # Handle bare versions by inferring workspace from other target
        if left["type"] == "bare_version":
            workspace = right.get("workspace")
            if not workspace:
                raise GoldfishError("Cannot determine workspace for version. Use workspace@version format.")
            version_info = self.db.get_version(workspace, left["version"])
            if not version_info:
                raise GoldfishError(f"Version '{left['version']}' not found for workspace '{workspace}'")
            left = {
                "type": "version",
                "workspace": workspace,
                "version": left["version"],
                "git_sha": version_info["git_sha"],
            }

        if right["type"] == "bare_version":
            workspace = left.get("workspace")
            if not workspace:
                raise GoldfishError("Cannot determine workspace for version. Use workspace@version format.")
            version_info = self.db.get_version(workspace, right["version"])
            if not version_info:
                raise GoldfishError(f"Version '{right['version']}' not found for workspace '{workspace}'")
            right = {
                "type": "version",
                "workspace": workspace,
                "version": right["version"],
                "git_sha": version_info["git_sha"],
            }

        return self._diff_two_targets(left, right, target, against)

    def _diff_slot_against_last_version(self, slot_target: dict) -> DiffResponse:
        """Diff a slot against its workspace's last explicit version.

        Explicit versions are those created by save_version/checkpoint, NOT by run().
        This shows what changed since the user's last intentional save point.
        """
        slot = slot_target["slot"]
        workspace = slot_target["workspace"]
        slot_path = slot_target["path"]

        if not workspace:
            raise GoldfishError(f"Slot {slot} has no workspace mounted")

        # Get last EXPLICIT version (checkpoint/manual, not run)
        latest_version = self.db.get_latest_explicit_version(workspace)

        # Fallback to any version if no explicit versions exist
        if not latest_version:
            latest_version = self.db.get_latest_version(workspace)

        if not latest_version:
            # No versions yet - compare against branch head (initial state)
            metadata_file = slot_path / ".goldfish-mount"
            if not metadata_file.exists():
                raise GoldfishError(f"Slot {slot} is not a mounted workspace")
            metadata = json.loads(metadata_file.read_text())
            compare_sha = metadata.get("mounted_sha")
            right_label = "initial mount"
        else:
            compare_sha = latest_version["git_sha"]
            right_label = f"{workspace}@{latest_version['version']}"

        if not compare_sha:
            raise GoldfishError(f"No comparison base found for slot {slot}")

        # Perform the diff
        diff_result = self.git.diff_slot_against_sha(slot_path, workspace, compare_sha)

        return self._build_diff_response(
            diff_result=diff_result,
            left_label=slot,
            right_label=right_label,
            left_sha=None,  # Slot is live, no SHA
            right_sha=compare_sha[:8],
        )

    def _diff_two_targets(self, left: dict, right: dict, left_label: str, right_label: str) -> DiffResponse:
        """Diff two parsed targets against each other."""
        # Build labels for response
        if left["type"] == "version":
            left_display = f"{left['workspace']}@{left['version']}"
            left_sha = left["git_sha"]
        else:
            left_display = left_label
            left_sha = None

        if right["type"] == "version":
            right_display = f"{right['workspace']}@{right['version']}"
            right_sha = right["git_sha"]
        else:
            right_display = right_label
            right_sha = None

        # Case 1: Both are slots - diff directories
        if left["type"] == "slot" and right["type"] == "slot":
            diff_result = self._diff_directories(left["path"], right["path"])
            return self._build_diff_response(
                diff_result=diff_result,
                left_label=left_display,
                right_label=right_display,
                left_sha=None,
                right_sha=None,
            )

        # Case 2: Slot vs version
        if left["type"] == "slot" and right["type"] == "version":
            diff_result = self.git.diff_slot_against_sha(left["path"], left["workspace"], right["git_sha"])
            return self._build_diff_response(
                diff_result=diff_result,
                left_label=left_display,
                right_label=right_display,
                left_sha=None,
                right_sha=right_sha[:8] if right_sha else None,
            )

        # Case 3: Version vs slot
        if left["type"] == "version" and right["type"] == "slot":
            # Swap order for consistent diffing (slot against SHA)
            diff_result = self.git.diff_slot_against_sha(right["path"], right["workspace"], left["git_sha"])
            # Swap labels since we swapped the diff
            return self._build_diff_response(
                diff_result=diff_result,
                left_label=right_display,  # Swapped
                right_label=left_display,  # Swapped
                left_sha=None,
                right_sha=left_sha[:8] if left_sha else None,
            )

        # Case 4: Both are versions - diff git SHAs
        if left["type"] == "version" and right["type"] == "version":
            diff_result = self.git.diff_shas(left["git_sha"], right["git_sha"])
            return self._build_diff_response(
                diff_result=diff_result,
                left_label=left_display,
                right_label=right_display,
                left_sha=left_sha[:8] if left_sha else None,
                right_sha=right_sha[:8] if right_sha else None,
            )

        raise GoldfishError(f"Unsupported diff combination: {left['type']} vs {right['type']}")

    def _diff_directories(self, dir1: Path, dir2: Path) -> dict:
        """Diff two directories directly."""
        import subprocess

        exclude_patterns = [
            ".goldfish-mount",
            "STATE.md",
            ".git",
            "__pycache__",
            "*.pyc",
            ".pytest_cache",
        ]

        # Build diff command
        diff_cmd = ["diff", "-rq"]
        for pattern in exclude_patterns:
            diff_cmd.extend(["--exclude", pattern])
        diff_cmd.extend([str(dir1), str(dir2)])

        result = subprocess.run(diff_cmd, capture_output=True, text=True, timeout=30)

        # Parse diff output
        files_changed = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            if line.startswith("Files "):
                # Extract relative path
                parts = line.split(" and ")
                if len(parts) >= 2:
                    file_path = parts[1].replace(" differ", "").replace(str(dir2) + "/", "")
                    files_changed.append(file_path)
            elif line.startswith("Only in "):
                filename = line.split(": ")[-1]
                files_changed.append(filename)

        has_changes = len(files_changed) > 0

        # Get unified diff for details
        diff_text = ""
        if has_changes:
            diff_text_cmd = ["diff", "-u"]
            for pattern in exclude_patterns:
                diff_text_cmd.extend(["--exclude", pattern])
            diff_text_cmd.extend([str(dir1), str(dir2)])
            diff_result = subprocess.run(diff_text_cmd, capture_output=True, text=True, timeout=30)
            diff_text = diff_result.stdout

        return {
            "has_changes": has_changes,
            "summary": f"{len(files_changed)} file(s) differ" if has_changes else "No differences",
            "files_changed": files_changed,
            "diff_text": diff_text,
        }

    def _build_diff_response(
        self,
        diff_result: dict,
        left_label: str,
        right_label: str,
        left_sha: str | None,
        right_sha: str | None,
    ) -> DiffResponse:
        """Build a DiffResponse from diff results."""
        # Truncate diff text to avoid overwhelming output
        diff_text = diff_result.get("diff_text", "")
        max_diff_len = 5000
        if len(diff_text) > max_diff_len:
            diff_text = diff_text[:max_diff_len] + f"\n\n... [truncated, {len(diff_text)} chars total]"

        return DiffResponse(
            has_changes=diff_result["has_changes"],
            summary=diff_result.get("summary", "No changes") if diff_result["has_changes"] else "No changes",
            files_changed=diff_result.get("files_changed", []),
            diff_text=diff_text,
            left=left_label,
            right=right_label,
            left_sha=left_sha,
            right_sha=right_sha,
        )

    def rollback(self, slot: str, version: str, reason: str) -> RollbackResponse:
        """Rollback a slot to a previous version.

        Discards all changes since the version.

        Args:
            slot: Slot to rollback
            version: Version to rollback to (e.g., "v1", "v2")
            reason: Why rolling back (min 15 chars)

        Returns:
            RollbackResponse with result
        """
        self._validate_slot(slot)
        validate_reason(reason, self.config.audit.min_reason_length)

        slot_info = self._get_slot_state(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty")

        workspace = slot_info.workspace
        if workspace is None:
            raise SlotEmptyError(f"Slot {slot} has no workspace")

        # Look up version to get git_tag
        version_info = self.db.get_version(workspace, version)
        if version_info is None:
            raise GoldfishError(f"Version '{version}' not found for workspace '{workspace}'")

        git_tag = version_info["git_tag"]
        slot_path = self._slot_path(slot)

        # Perform the rollback using git_tag (copy-based)
        files_reverted = self.git.checkout_snapshot_copy_based(slot_path, git_tag)

        # Log to audit
        self.db.log_audit(
            operation="rollback",
            slot=slot,
            workspace=workspace,
            reason=reason,
            details={
                "version": version,
                "git_tag": git_tag,
                "files_reverted": files_reverted,
            },
        )

        # Update STATE.md
        if self.state_manager:
            self.state_manager.add_action(f"Rolled back {slot} to {version} ({files_reverted} files)")

        state_md = self._regenerate_state_md()

        return RollbackResponse(
            success=True,
            slot=slot,
            version=version,
            git_tag=git_tag,
            files_reverted=files_reverted,
            state_md=state_md,
        )

    def get_workspace(self, name: str) -> WorkspaceInfo:
        """Get detailed information about a specific workspace.

        Args:
            name: Workspace name

        Returns:
            WorkspaceInfo for the workspace

        Raises:
            WorkspaceNotFoundError: If workspace doesn't exist
        """
        # Check if workspace exists
        if not self.git.branch_exists(name):
            raise WorkspaceNotFoundError(f"Workspace '{name}' does not exist")

        # Get workspace info from git
        info = self.git.get_branch_info(name)

        # Build map of mounted workspaces
        mounted_map: dict[str, str] = {}  # workspace -> slot
        for slot_info in self.get_all_slots():
            if slot_info.workspace:
                mounted_map[slot_info.workspace] = slot_info.slot

        # Parse timestamps
        created_at = datetime.fromisoformat(info["created_at"]) if info["created_at"] else datetime.now(UTC)
        last_activity = datetime.fromisoformat(info["last_activity"]) if info["last_activity"] else created_at

        # Get goal from database
        goal = self.db.get_workspace_goal(name) or ""

        # Try to get pipeline/workflow info
        workflow = None
        try:
            workspace_path = self.get_workspace_path(name)
            pipeline_path = workspace_path / "pipeline.yaml"
            if pipeline_path.exists():
                import yaml

                with open(pipeline_path) as f:
                    pipeline_data = yaml.safe_load(f)
                if pipeline_data and "stages" in pipeline_data:
                    stage_names = [s.get("name", f"stage_{i}") for i, s in enumerate(pipeline_data["stages"])]
                    workflow = WorkflowInfo(stages=stage_names, has_pipeline=True)
        except Exception:
            # If we can't read pipeline, just skip it
            pass

        return WorkspaceInfo(
            name=name,
            created_at=created_at,
            goal=goal,
            snapshot_count=info["snapshot_count"],
            last_activity=last_activity,
            is_mounted=name in mounted_map,
            mounted_slot=mounted_map.get(name),
            workflow=workflow,
        )

    def list_snapshots(self, workspace: str, limit: int = 50, offset: int = 0) -> list[dict]:
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

            snapshots.append(
                {
                    "snapshot_id": snap_id,
                    "created_at": created_at,
                    "message": info.get("message", ""),
                }
            )

        # Sort by created_at descending (newest first)
        snapshots.sort(key=lambda x: x["created_at"] or datetime.min, reverse=True)

        # Apply pagination
        return snapshots[offset : offset + limit]
