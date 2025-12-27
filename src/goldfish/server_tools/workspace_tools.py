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
    SaveVersionResponse,
    SnapshotInfo,
    UpdateWorkspaceGoalResponse,
    WorkspaceGoalResponse,
    WorkspaceInfo,
)
from goldfish.server_core import (
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
    validate_version,
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
def save_version(slot: str, message: str) -> SaveVersionResponse:
    """Create a version of the current slot state.

    Args:
        slot: Slot to save version from (w1, w2, or w3)
        message: Describe what this version represents (min 15 chars)

    Creates an immutable version that can be used for rollback and branching.
    The version (v1, v2, etc.) is the primary identifier.
    """
    logger.info("save_version() called", extra={"slot": slot})

    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)

    try:
        result = workspace_manager.save_version(slot, message)
        logger.info("save_version() succeeded", extra={"slot": slot, "version": result.version})
        return result
    except Exception as e:
        logger.error("save_version() failed", extra={"slot": slot, "error": str(e)})
        raise


@mcp.tool()
def checkpoint(slot: str, message: str) -> CheckpointResponse:
    """[DEPRECATED] Create a snapshot of the current slot state.

    Use save_version() instead. checkpoint() will be removed in a future version.

    Args:
        slot: Slot to checkpoint (w1, w2, or w3)
        message: Describe what this checkpoint represents (min 15 chars)

    Creates an immutable snapshot that jobs can run against.
    """
    warnings.warn(
        "checkpoint() is deprecated, use save_version() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    logger.info("checkpoint() called (deprecated)", extra={"slot": slot})

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
def diff(target: str, against: str | None = None) -> DiffResponse:
    """Compare changes between targets.

    Single argument: Compare slot against its last saved version (save_version/checkpoint).
    Two arguments: Compare any two targets.

    Args:
        target: What to diff. Can be:
            - Slot: "w1" (compares against last version if alone)
            - Workspace@version: "baseline@v2"
        against: Optional second target to compare against.

    Returns:
        DiffResponse with change summary, files changed, and what was compared.

    Examples:
        diff("w1")                       # Slot vs last version (most common)
        diff("w1", "w2")                 # Compare two slots
        diff("w1", "baseline@v3")        # Slot vs specific version
        diff("baseline@v1", "baseline@v5")  # Compare two versions
    """
    workspace_manager = _get_workspace_manager()
    return workspace_manager.diff(target, against)


@mcp.tool()
def rollback(slot: str, version: str, reason: str) -> RollbackResponse:
    """Rollback a slot to a previous version.

    Discards all changes since the version. Use with caution.

    Args:
        slot: Slot to rollback (w1, w2, or w3)
        version: Version to rollback to (e.g., "v1", "v2")
        reason: Why you're rolling back (min 15 chars)
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()

    # Validate inputs
    validate_slot_name(slot, config.slots)
    validate_version(version)  # Validates format: v1, v2, etc.
    validate_reason(reason, config.audit.min_reason_length)

    return workspace_manager.rollback(slot, version, reason)


# ============== VERSION TAG TOOLS ==============


@mcp.tool()
def tag_version(workspace: str, version: str, tag_name: str) -> dict:
    """Tag a version with a memorable name.

    Tags allow marking significant versions (e.g., "baseline-working", "best-model").
    Tags can be applied retroactively to any existing version.

    Args:
        workspace: Workspace name
        version: Version to tag (e.g., "v1", "v2")
        tag_name: Name for the tag (e.g., "baseline-working")

    Returns:
        Dict with tag info including workspace_name, version, tag_name, created_at
    """
    logger.info("tag_version() called", extra={"workspace": workspace, "version": version, "tag_name": tag_name})

    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_version(version)

    try:
        result = db.create_tag(workspace, version, tag_name)

        # Log to audit
        db.log_audit(
            operation="tag_version",
            workspace=workspace,
            reason=f"Tagged {version} as '{tag_name}'",
            details={"version": version, "tag_name": tag_name},
        )

        state_manager.add_action(f"Tagged {version} as '{tag_name}'")

        logger.info("tag_version() succeeded", extra={"workspace": workspace, "version": version, "tag_name": tag_name})
        return result
    except Exception as e:
        logger.error(
            "tag_version() failed",
            extra={"workspace": workspace, "version": version, "tag_name": tag_name, "error": str(e)},
        )
        raise


@mcp.tool()
def untag_version(workspace: str, tag_name: str) -> dict:
    """Remove a tag from a version.

    Args:
        workspace: Workspace name
        tag_name: Name of the tag to remove

    Returns:
        Dict with success status
    """
    logger.info("untag_version() called", extra={"workspace": workspace, "tag_name": tag_name})

    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)

    try:
        db.delete_tag(workspace, tag_name)

        # Log to audit
        db.log_audit(
            operation="untag_version",
            workspace=workspace,
            reason=f"Removed tag '{tag_name}'",
            details={"tag_name": tag_name},
        )

        state_manager.add_action(f"Removed tag '{tag_name}'")

        logger.info("untag_version() succeeded", extra={"workspace": workspace, "tag_name": tag_name})
        return {"success": True, "tag_name": tag_name}
    except Exception as e:
        logger.error("untag_version() failed", extra={"workspace": workspace, "tag_name": tag_name, "error": str(e)})
        raise


@mcp.tool()
def list_tags(workspace: str) -> list[dict]:
    """List all tags for a workspace.

    Args:
        workspace: Workspace name

    Returns:
        List of tag dicts with workspace_name, version, tag_name, created_at
    """
    db = _get_db()

    validate_workspace_name(workspace)

    return db.list_tags(workspace)


# ============== VERSION PRUNING TOOLS ==============


@mcp.tool()
def prune_version(workspace: str, version: str, reason: str) -> dict:
    """Prune a single version (soft delete).

    Pruned versions are hidden from list_versions() but can be restored.
    Tagged versions cannot be pruned (they are protected).

    Args:
        workspace: Workspace name
        version: Version to prune (e.g., "v2")
        reason: Why pruning this version (min 15 chars)

    Returns:
        Dict with pruned version info
    """
    logger.info("prune_version() called", extra={"workspace": workspace, "version": version})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_version(version)
    validate_reason(reason, config.audit.min_reason_length)

    try:
        result = db.prune_version(workspace, version, reason)

        # Log to audit
        db.log_audit(
            operation="prune_version",
            workspace=workspace,
            reason=reason,
            details={"version": version},
        )

        state_manager.add_action(f"Pruned version {version}")

        logger.info("prune_version() succeeded", extra={"workspace": workspace, "version": version})
        return result
    except Exception as e:
        logger.error("prune_version() failed", extra={"workspace": workspace, "version": version, "error": str(e)})
        raise


@mcp.tool()
def prune_versions(workspace: str, from_version: str, to_version: str, reason: str) -> dict:
    """Prune a range of versions (inclusive).

    Tagged versions within the range are skipped (not pruned).

    Args:
        workspace: Workspace name
        from_version: Start version (e.g., "v3")
        to_version: End version (e.g., "v7")
        reason: Why pruning this range (min 15 chars)

    Returns:
        Dict with pruned_count and skipped_tagged count
    """
    logger.info("prune_versions() called", extra={"workspace": workspace, "from": from_version, "to": to_version})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_version(from_version)
    validate_version(to_version)
    validate_reason(reason, config.audit.min_reason_length)

    try:
        result = db.prune_versions(workspace, from_version, to_version, reason)

        # Log to audit
        db.log_audit(
            operation="prune_versions",
            workspace=workspace,
            reason=reason,
            details={
                "from_version": from_version,
                "to_version": to_version,
                "pruned_count": result["pruned_count"],
            },
        )

        state_manager.add_action(f"Pruned {result['pruned_count']} versions ({from_version}-{to_version})")

        logger.info(
            "prune_versions() succeeded",
            extra={"workspace": workspace, "pruned_count": result["pruned_count"]},
        )
        return result
    except Exception as e:
        logger.error(
            "prune_versions() failed",
            extra={"workspace": workspace, "from": from_version, "to": to_version, "error": str(e)},
        )
        raise


@mcp.tool()
def prune_before_tag(workspace: str, tag_name: str, reason: str) -> dict:
    """Prune all versions before a tagged milestone.

    The tagged version itself is NOT pruned. Other tagged versions
    before the milestone are also protected (not pruned).

    Args:
        workspace: Workspace name
        tag_name: Tag marking the milestone (e.g., "first-working")
        reason: Why pruning versions before this milestone (min 15 chars)

    Returns:
        Dict with pruned_count
    """
    logger.info("prune_before_tag() called", extra={"workspace": workspace, "tag_name": tag_name})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_reason(reason, config.audit.min_reason_length)

    try:
        result = db.prune_before_tag(workspace, tag_name, reason)

        # Log to audit
        db.log_audit(
            operation="prune_before_tag",
            workspace=workspace,
            reason=reason,
            details={"tag_name": tag_name, "pruned_count": result["pruned_count"]},
        )

        state_manager.add_action(f"Pruned {result['pruned_count']} versions before '{tag_name}'")

        logger.info(
            "prune_before_tag() succeeded",
            extra={"workspace": workspace, "pruned_count": result["pruned_count"]},
        )
        return result
    except Exception as e:
        logger.error("prune_before_tag() failed", extra={"workspace": workspace, "tag_name": tag_name, "error": str(e)})
        raise


@mcp.tool()
def unprune_version(workspace: str, version: str) -> dict:
    """Restore a pruned version.

    Makes a previously pruned version visible again.

    Args:
        workspace: Workspace name
        version: Version to restore (e.g., "v3")

    Returns:
        Dict with restored version info
    """
    logger.info("unprune_version() called", extra={"workspace": workspace, "version": version})

    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_version(version)

    try:
        result = db.unprune_version(workspace, version)

        # Log to audit
        db.log_audit(
            operation="unprune_version",
            workspace=workspace,
            reason=f"Restored pruned version {version}",
            details={"version": version},
        )

        state_manager.add_action(f"Restored version {version}")

        logger.info("unprune_version() succeeded", extra={"workspace": workspace, "version": version})
        return result
    except Exception as e:
        logger.error("unprune_version() failed", extra={"workspace": workspace, "version": version, "error": str(e)})
        raise


@mcp.tool()
def unprune_versions(workspace: str, from_version: str, to_version: str) -> dict:
    """Restore a range of pruned versions.

    Args:
        workspace: Workspace name
        from_version: Start version (e.g., "v4")
        to_version: End version (e.g., "v6")

    Returns:
        Dict with unpruned_count
    """
    logger.info("unprune_versions() called", extra={"workspace": workspace, "from": from_version, "to": to_version})

    db = _get_db()
    state_manager = _get_state_manager()

    validate_workspace_name(workspace)
    validate_version(from_version)
    validate_version(to_version)

    try:
        result = db.unprune_versions(workspace, from_version, to_version)

        # Log to audit
        db.log_audit(
            operation="unprune_versions",
            workspace=workspace,
            reason=f"Restored pruned versions {from_version}-{to_version}",
            details={
                "from_version": from_version,
                "to_version": to_version,
                "unpruned_count": result["unpruned_count"],
            },
        )

        state_manager.add_action(f"Restored {result['unpruned_count']} versions ({from_version}-{to_version})")

        logger.info(
            "unprune_versions() succeeded",
            extra={"workspace": workspace, "unpruned_count": result["unpruned_count"]},
        )
        return result
    except Exception as e:
        logger.error(
            "unprune_versions() failed",
            extra={"workspace": workspace, "from": from_version, "to": to_version, "error": str(e)},
        )
        raise


@mcp.tool()
def get_pruned_count(workspace: str) -> dict:
    """Get the count of pruned versions in a workspace.

    Args:
        workspace: Workspace name

    Returns:
        Dict with workspace and pruned_count
    """
    db = _get_db()

    validate_workspace_name(workspace)

    count = db.get_pruned_count(workspace)
    return {"workspace": workspace, "pruned_count": count}


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
