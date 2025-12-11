"""Goldfish MCP tools - Workspace Tools

Extracted from server.py for better organization.
"""

import logging
import warnings
from datetime import datetime

from goldfish.errors import (
    GoldfishError,
    WorkspaceNotFoundError,
    validate_reason,
)
from goldfish.lineage.manager import LineageManager
from goldfish.models import (
    CheckpointResponse,
    CreateWorkspaceResponse,
    DeleteSnapshotResponse,
    DeleteWorkspaceResponse,
    DiffResponse,
    HibernateResponse,
    ListSnapshotsResponse,
    MountResponse,
    RollbackResponse,
    SnapshotInfo,
    UpdateWorkspaceGoalResponse,
    WorkspaceGoalResponse,
    WorkspaceInfo,
)
from goldfish.server import (
    _get_config,
    _get_db,
    _get_state_manager,
    _get_state_md,
    _get_workspace_manager,
    mcp,
)
from goldfish.validation import (
    validate_slot_name,
    validate_snapshot_id,
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def mount(workspace: str, slot: str, reason: str) -> MountResponse:
    """Load a workspace into a slot.

    Args:
        workspace: Name of the workspace to mount
        slot: Target slot (w1, w2, or w3)
        reason: Why you're mounting this workspace (min 15 chars)

    Returns workspace state and updated STATE.md.
    Warns (but doesn't block) if exceeding 3 active workspaces.
    """
    logger.info("mount() called", extra={"workspace": workspace, "slot": slot})

    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_workspace_name(workspace)
    validate_slot_name(slot, config.slots)

    try:
        result = workspace_manager.mount(workspace, slot, reason)
        logger.info("mount() succeeded", extra={"workspace": workspace, "slot": slot})
        return result
    except Exception as e:
        logger.error("mount() failed", extra={"workspace": workspace, "slot": slot, "error": str(e)})
        raise


@mcp.tool()
def hibernate(slot: str, reason: str) -> HibernateResponse:
    """Save current work and free a slot.

    Auto-checkpoints if there are unsaved changes.
    Pushes to remote for backup.

    Args:
        slot: Slot to hibernate (w1, w2, or w3)
        reason: Why you're hibernating (min 15 chars)
    """
    logger.info("hibernate() called", extra={"slot": slot})

    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)

    try:
        result = workspace_manager.hibernate(slot, reason)
        logger.info("hibernate() succeeded", extra={"slot": slot})
        return result
    except Exception as e:
        logger.error("hibernate() failed", extra={"slot": slot, "error": str(e)})
        raise


@mcp.tool()
def create_workspace(name: str, goal: str, reason: str) -> CreateWorkspaceResponse:
    """Create a new workspace from main.

    Args:
        name: Workspace name (use descriptive names like "fix-tbpe-labels")
        goal: What you're trying to achieve in this workspace
        reason: Why this workspace is needed (min 15 chars)

    The workspace is created but not mounted. Use mount() to work in it.
    """
    logger.info("create_workspace() called", extra={"workspace": name})

    workspace_manager = _get_workspace_manager()
    db = _get_db()

    # Validate inputs
    validate_workspace_name(name)

    try:
        result = workspace_manager.create_workspace(name, goal, reason)

        # Persist the goal in the database
        db.set_workspace_goal(name, goal)

        logger.info("create_workspace() succeeded", extra={"workspace": name})
        return result
    except Exception as e:
        logger.error("create_workspace() failed", extra={"workspace": name, "error": str(e)})
        raise


@mcp.tool()
def list_workspaces() -> list[WorkspaceInfo]:
    """List all workspaces (active and hibernated).

    Shows which workspaces are currently mounted and where.
    """
    workspace_manager = _get_workspace_manager()

    return workspace_manager.list_workspaces()


@mcp.tool()
def get_workspace(name: str) -> WorkspaceInfo:
    """Get detailed information about a specific workspace.

    Args:
        name: Name of the workspace to look up
    """
    workspace_manager = _get_workspace_manager()
    validate_workspace_name(name)

    return workspace_manager.get_workspace(name)


@mcp.tool()
def delete_workspace(workspace: str, reason: str) -> DeleteWorkspaceResponse:
    """Delete a workspace and all its snapshots.

    WARNING: This is irreversible. The workspace must not be mounted.

    Args:
        workspace: Name of the workspace to delete
        reason: Why you're deleting this workspace (min 15 chars)
    """
    logger.info("delete_workspace() called", extra={"workspace": workspace})

    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_reason(reason, config.audit.min_reason_length)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    # Check workspace is not mounted
    for slot_info in workspace_manager.get_all_slots():
        if slot_info.workspace == workspace:
            raise GoldfishError(
                f"Cannot delete workspace '{workspace}': it is mounted in slot {slot_info.slot}. Hibernate it first."
            )

    try:
        # Count snapshots to delete
        snapshots = workspace_manager.git.list_snapshots(workspace)
        snapshot_count = len(snapshots)

        # Delete all snapshots (tags)
        for snap_id in snapshots:
            workspace_manager.git.delete_snapshot(snap_id)

        # Delete the branch
        workspace_manager.git.delete_branch(workspace, force=True)

        # Delete workspace goal from database
        db.delete_workspace_goal(workspace)

        # Log to audit
        db.log_audit(
            operation="delete_workspace",
            workspace=workspace,
            reason=reason,
            details={"snapshots_deleted": snapshot_count},
        )

        # Update state
        state_manager.add_action(f"Deleted workspace '{workspace}' ({snapshot_count} snapshots)")

        logger.info(
            "delete_workspace() succeeded",
            extra={
                "workspace": workspace,
                "snapshots_deleted": snapshot_count,
            },
        )

        return DeleteWorkspaceResponse(
            success=True,
            workspace=workspace,
            snapshots_deleted=snapshot_count,
        )
    except Exception as e:
        logger.error("delete_workspace() failed", extra={"workspace": workspace, "error": str(e)})
        raise


@mcp.tool()
def checkpoint(slot: str, message: str) -> CheckpointResponse:
    """Create a snapshot of the current slot state.

    Args:
        slot: Slot to checkpoint (w1, w2, or w3)
        message: Describe what this checkpoint represents (min 15 chars)

    Creates an immutable snapshot that jobs can run against.
    """
    logger.info("checkpoint() called", extra={"slot": slot})

    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)

    try:
        result = workspace_manager.checkpoint(slot, message)
        logger.info("checkpoint() succeeded", extra={"slot": slot, "snapshot_id": result.snapshot_id})
        return result
    except Exception as e:
        logger.error("checkpoint() failed", extra={"slot": slot, "error": str(e)})
        raise


@mcp.tool()
def diff(slot: str) -> DiffResponse:
    """Show changes in a slot since last checkpoint.

    Args:
        slot: Slot to diff (w1, w2, or w3)

    Returns changes summary and list of modified files.
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)

    return workspace_manager.diff(slot)


@mcp.tool()
def rollback(slot: str, snapshot_id: str, reason: str) -> RollbackResponse:
    """Rollback a slot to a previous snapshot.

    Discards all changes since the snapshot. Use with caution.

    Args:
        slot: Slot to rollback (w1, w2, or w3)
        snapshot_id: Snapshot ID to rollback to (e.g., "snap-a1b2c3d-20251205-143000")
        reason: Why you're rolling back (min 15 chars)
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)
    validate_snapshot_id(snapshot_id)
    validate_reason(reason, config.audit.min_reason_length)

    return workspace_manager.rollback(slot, snapshot_id, reason)


@mcp.tool()
def list_snapshots(workspace: str, limit: int = 50, offset: int = 0) -> ListSnapshotsResponse:
    """[DEPRECATED] List snapshots for a workspace with pagination.

    Use get_workspace() instead, which includes version/snapshot history.

    Args:
        workspace: Workspace name to list snapshots for
        limit: Maximum number of snapshots to return (1-200, default 50)
        offset: Number of snapshots to skip for pagination (default 0)

    Returns:
        ListSnapshotsResponse with snapshots and pagination metadata
    """
    warnings.warn(
        "list_snapshots is deprecated, use get_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    workspace_manager = _get_workspace_manager()

    # Validate workspace name
    validate_workspace_name(workspace)

    # Validate pagination bounds
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    # Get all snapshots once and apply pagination in memory
    all_snapshots_data = workspace_manager.list_snapshots(workspace, limit=10000, offset=0)

    # Filter out snapshots without dates and convert to SnapshotInfo objects
    all_valid_snapshots = [
        SnapshotInfo(
            snapshot_id=s["snapshot_id"],
            created_at=s["created_at"],
            message=s["message"],
        )
        for s in all_snapshots_data
        if s["created_at"] is not None
    ]

    total_count = len(all_valid_snapshots)

    # Apply pagination in memory
    snapshots = all_valid_snapshots[offset : offset + limit]

    # Calculate has_more
    has_more = (offset + len(snapshots)) < total_count

    return ListSnapshotsResponse(
        workspace=workspace,
        snapshots=snapshots,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=has_more,
    )


@mcp.tool()
def get_snapshot(workspace: str, snapshot_id: str) -> SnapshotInfo:
    """[DEPRECATED] Get detailed information about a specific snapshot.

    Use get_workspace_lineage() instead for version/snapshot history.

    Args:
        workspace: Workspace name the snapshot belongs to
        snapshot_id: Snapshot ID (e.g., snap-abc1234-20251205-120000)

    Returns:
        SnapshotInfo with snapshot details

    Raises:
        GoldfishError: If workspace doesn't exist or snapshot not found in workspace
    """
    warnings.warn(
        "get_snapshot is deprecated, use get_workspace_lineage() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    workspace_manager = _get_workspace_manager()

    # Validate parameters
    validate_workspace_name(workspace)
    validate_snapshot_id(snapshot_id)

    # Check workspace exists by trying to get its snapshots
    try:
        snapshot_ids = workspace_manager.git.list_snapshots(workspace)
    except GoldfishError as e:
        raise GoldfishError(f"Workspace '{workspace}' not found or inaccessible: {e}") from e

    # Check if snapshot belongs to this workspace
    if snapshot_id not in snapshot_ids:
        raise GoldfishError(
            f"Snapshot '{snapshot_id}' not found in workspace '{workspace}'. "
            f"Use list_snapshots() to see available snapshots."
        )

    # Get snapshot info
    info = workspace_manager.git.get_snapshot_info(snapshot_id)
    created_at = None
    if info.get("commit_date"):
        try:
            created_at = datetime.fromisoformat(info["commit_date"])
        except ValueError as e:
            raise GoldfishError(f"Invalid date format for snapshot '{snapshot_id}': {info.get('commit_date')}") from e

    if created_at is None:
        raise GoldfishError(f"Snapshot '{snapshot_id}' has no valid creation date")

    return SnapshotInfo(
        snapshot_id=snapshot_id,
        created_at=created_at,
        message=info.get("message", ""),
    )


# ============== JOB TOOLS ==============


@mcp.tool()
def delete_snapshot(workspace: str, snapshot_id: str, reason: str) -> DeleteSnapshotResponse:
    """[DEPRECATED] Delete a specific snapshot from a workspace.

    This tool is deprecated. Snapshots are now managed as workspace versions.

    WARNING: This is irreversible. You cannot rollback to a deleted snapshot.

    Args:
        workspace: Workspace containing the snapshot
        snapshot_id: ID of the snapshot to delete
        reason: Why you're deleting this snapshot (min 15 chars)
    """
    warnings.warn(
        "delete_snapshot is deprecated, snapshots are now managed as workspace versions",
        DeprecationWarning,
        stacklevel=2,
    )
    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_snapshot_id(snapshot_id)
    validate_reason(reason, config.audit.min_reason_length)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    # Check snapshot exists in this workspace
    snapshots = workspace_manager.git.list_snapshots(workspace)
    if snapshot_id not in snapshots:
        raise GoldfishError(f"Snapshot '{snapshot_id}' not found in workspace '{workspace}'")

    # Delete the snapshot
    if not workspace_manager.git.delete_snapshot(snapshot_id):
        raise GoldfishError(f"Failed to delete snapshot '{snapshot_id}'")

    # Log to audit
    db.log_audit(
        operation="delete_snapshot",
        workspace=workspace,
        reason=reason,
        details={"snapshot_id": snapshot_id},
    )

    state_manager.add_action(f"Deleted snapshot '{snapshot_id}' from '{workspace}'")

    return DeleteSnapshotResponse(
        success=True,
        workspace=workspace,
        snapshot_id=snapshot_id,
    )


# ============== PIPELINE TOOLS ==============


@mcp.tool()
def get_workspace_goal(workspace: str) -> WorkspaceGoalResponse:
    """[DEPRECATED] Get the goal for a workspace.

    Use get_workspace() instead, which includes the goal in the response.

    Args:
        workspace: Workspace name to query
    """
    warnings.warn(
        "get_workspace_goal is deprecated, use get_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    db = _get_db()

    validate_workspace_name(workspace)

    goal = db.get_workspace_goal(workspace)

    return WorkspaceGoalResponse(
        workspace=workspace,
        goal=goal,
    )


@mcp.tool()
def update_workspace_goal(workspace: str, goal: str, reason: str) -> UpdateWorkspaceGoalResponse:
    """[DEPRECATED] Update the goal for a workspace.

    The goal is now set at workspace creation time. If you need to update it,
    consider creating a new workspace with the new goal.

    Args:
        workspace: Workspace name to update
        goal: New goal description
        reason: Why you're updating the goal (min 15 chars)
    """
    warnings.warn(
        "update_workspace_goal is deprecated, set goal at workspace creation time",
        DeprecationWarning,
        stacklevel=2,
    )
    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_reason(reason, config.audit.min_reason_length)

    # Update in database
    db.set_workspace_goal(workspace, goal)

    # Log audit
    db.log_audit(
        operation="update_workspace_goal",
        workspace=workspace,
        reason=reason,
        details={"goal": goal},
    )

    # Update state manager
    state_manager.set_goal(goal)
    state_manager.add_action(f"Updated workspace goal: {goal[:50]}...")

    state_md = _get_state_md()

    return UpdateWorkspaceGoalResponse(
        success=True,
        workspace=workspace,
        goal=goal,
        state_md=state_md,
    )


@mcp.tool()
def branch_workspace(from_workspace: str, from_version: str, new_workspace: str, reason: str) -> dict:
    """[DEPRECATED] Create new workspace branched from specific version.

    Use create_workspace() instead. Branching is now handled by workspace lineage
    which tracks parent relationships automatically.

    Args:
        from_workspace: Source workspace
        from_version: Version to branch from (e.g., "v3")
        new_workspace: Name for new workspace
        reason: Why branching (min 15 chars)

    Returns:
        Dict with:
        - workspace: New workspace name
        - parent: Parent workspace
        - parent_version: Version branched from
    """
    warnings.warn(
        "branch_workspace is deprecated, use create_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(from_workspace)
    validate_workspace_name(new_workspace)
    validate_reason(reason, config.audit.min_reason_length)

    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    lineage_mgr.branch_workspace(from_workspace, from_version, new_workspace, reason)

    return {"workspace": new_workspace, "parent": from_workspace, "parent_version": from_version}
