"""Goldfish MCP tools - Workspace Tools

Extracted from server.py for better organization.
"""

import logging
from typing import Any, Literal

from goldfish.errors import (
    GoldfishError,
    WorkspaceNotFoundError,
    validate_reason,
)
from goldfish.lineage.manager import LineageManager
from goldfish.models import (
    CreateWorkspaceResponse,
    DeleteWorkspaceResponse,
    DiffResponse,
    HibernateResponse,
    MountResponse,
    RollbackResponse,
    SaveVersionResponse,
)
from goldfish.server_core import (
    _get_config,
    _get_db,
    _get_pipeline_manager,
    _get_state_manager,
    _get_state_md,
    _get_workspace_manager,
    mcp,
)
from goldfish.server_tools.backup_tools import trigger_backup
from goldfish.validation import (
    validate_from_ref,
    validate_slot_name,
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
def status() -> dict:
    """Get a global summary of the project state.

    Returns:
        - slots: Which workspaces are mounted where
        - active_jobs: Currently running stages/pipelines
        - source_count: Total registered data sources
        - recent_audit: Last 5 state-changing operations
        - state_md: The current content of STATE.md
    """
    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    slots = workspace_manager.get_all_slots()
    active_jobs_raw = db.get_active_jobs()
    from goldfish.jobs.conversion import job_dict_to_info

    active_jobs = [job_dict_to_info(j, db) for j in active_jobs_raw]
    source_count = db.count_sources()
    audit_entries = db.get_recent_audit(limit=5)
    state_md = _get_state_md()

    return {
        "project_name": config.project_name,
        "slots": slots,
        "active_jobs": active_jobs,
        "source_count": source_count,
        "recent_audit": [
            {"op": e["operation"], "reason": e["reason"], "ts": e["timestamp"][:19]} for e in audit_entries
        ],
        "state_md": state_md,
    }


@mcp.tool()
def inspect_workspace(
    name: str,
    version_limit: int = 10,
    version_offset: int = 0,
    include: list[str] | None = None,
) -> dict:
    """Get a comprehensive view of a workspace.

    Combines metadata, lineage (parent/branches), goal, and tags.
    Version history is paginated (newest first).

    By default returns essential info only. Use include=["pipeline"] to also get
    the full pipeline definition.

    Args:
        name: Workspace name or slot (e.g., "baseline" or "w1")
        version_limit: Max number of versions to return (default 10)
        version_offset: Skip this many versions (default 0)
        include: Optional list of additional data to include:
                 - "pipeline": Full pipeline YAML definition
    """
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    # Resolve slot
    workspace_name = workspace_manager.get_workspace_for_slot(name) or name
    validate_workspace_name(workspace_name)

    # 1. Basic Info & Goal
    ws_info = workspace_manager.get_workspace(workspace_name)
    goal = db.get_workspace_goal(workspace_name)

    # 2. Lineage (History and Branches)
    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    lineage = lineage_mgr.get_workspace_lineage(
        workspace_name,
        version_limit=version_limit,
        version_offset=version_offset,
    )

    # 3. Refresh STATE.md on disk if mounted
    if ws_info.is_mounted and ws_info.mounted_slot:
        try:
            slot_path = workspace_manager.get_slot_path(ws_info.mounted_slot)
            workspace_manager.refresh_workspace_state_md(slot_path, workspace_name, ws_info.mounted_slot)
        except Exception as e:
            logger.warning(f"Failed to refresh STATE.md for {workspace_name}: {e}")

    result: dict[str, Any] = {
        "name": workspace_name,
        "goal": goal,
        "is_mounted": ws_info.is_mounted,
        "slot": ws_info.mounted_slot,
        "lineage": lineage,
        "tags": db.list_tags(workspace_name),
    }

    # 4. Pipeline Info (only if explicitly requested)
    include_set = set(include) if include else set()
    if "pipeline" in include_set:
        pipeline_manager = _get_pipeline_manager()
        try:
            pipeline_def = pipeline_manager.get_pipeline(workspace_name)
        except Exception:
            pipeline_def = None
        result["pipeline"] = pipeline_def

    return result


@mcp.tool()
def manage_versions(
    workspace: str,
    action: Literal["list", "tag", "untag", "prune", "unprune", "prune_before_tag"],
    version: str | None = None,
    tag: str | None = None,
    reason: str | None = None,
    from_version: str | None = None,
    to_version: str | None = None,
    limit: int = 50,
    offset: int = 0,
    include_pruned: bool = False,
) -> dict:
    """Unified tool for version tagging, pruning, and listing.

    Args:
        workspace: Workspace name
        action: "list", "tag", "untag", "prune", "unprune"
        version: Target version (e.g., "v5")
        tag: Tag name for tag/untag actions
        reason: Why performing this action (for prune)
        from_version / to_version: Range for bulk pruning
        limit: Max versions to return for action='list' (default 50)
        offset: Skip versions for action='list' (default 0)
        include_pruned: For action='list' - include pruned versions (default False)
    """
    db = _get_db()
    validate_workspace_name(workspace)
    config = _get_config()

    if action == "list":
        versions = db.list_versions(workspace, include_pruned=include_pruned, limit=limit, offset=offset)
        return {"workspace": workspace, "versions": versions, "limit": limit, "offset": offset}

    elif action == "tag":
        if not version or not tag:
            raise GoldfishError("version and tag are required for action='tag'")
        tag_res = db.create_tag(workspace, version, tag)
        return {"success": True, "tag": tag_res}

    elif action == "untag":
        if not tag:
            raise GoldfishError("tag is required for action='untag'")
        db.delete_tag(workspace, tag)
        return {"success": True, "removed_tag": tag}

    elif action == "prune":
        if not reason:
            raise GoldfishError("reason is required for pruning")
        validate_reason(reason, config.audit.min_reason_length)
        prune_res: Any
        if from_version or to_version:
            if not (from_version and to_version):
                raise GoldfishError("Both from_version and to_version are required for range pruning")
            prune_res = db.prune_versions(workspace, from_version, to_version, reason)
        elif version:
            prune_res = db.prune_version(workspace, version, reason)
        else:
            raise GoldfishError("version or range (from/to) required for pruning")
        return {"success": True, "result": prune_res}

    elif action == "prune_before_tag":
        if not tag:
            raise GoldfishError("tag is required for action='prune_before_tag'")
        if not reason:
            raise GoldfishError("reason is required for pruning")
        validate_reason(reason, config.audit.min_reason_length)
        prune_before_res = db.prune_before_tag(workspace, tag, reason)
        return {"success": True, "result": prune_before_res}

    elif action == "unprune":
        unprune_res: Any
        if from_version or to_version:
            if not (from_version and to_version):
                raise GoldfishError("Both from_version and to_version are required for range unpruning")
            unprune_res = db.unprune_versions(workspace, from_version, to_version)
        elif version:
            unprune_res = db.unprune_version(workspace, version)
        return {"success": True, "result": unprune_res}

    raise GoldfishError(f"Unknown action: {action}")


@mcp.tool()
def create_workspace(name: str, goal: str, reason: str, from_ref: str | None = None) -> CreateWorkspaceResponse:
    """Create a new workspace, optionally branching from another workspace or ref.

    Args:
        name: Workspace name (use descriptive names like "fix-tbpe-labels")
        goal: What you're trying to achieve in this workspace
        reason: Why this workspace is needed (min 15 chars)
        from_ref: Branch from this ref instead of main. Can be another workspace name,
                  "main", "master", or "HEAD". Default: "main".

    The workspace is created but not mounted. Use mount() to work in it.
    """
    logger.info("create_workspace() called", extra={"workspace": name, "from_ref": from_ref})

    workspace_manager = _get_workspace_manager()
    db = _get_db()

    # Validate inputs
    validate_workspace_name(name)
    if from_ref is not None:
        validate_from_ref(from_ref)  # Rejects empty string, remote refs, etc.
    ref = from_ref if from_ref is not None else "main"

    try:
        result = workspace_manager.create_workspace(name, goal, reason, from_ref=ref)

        # Persist the goal in the database
        db.set_workspace_goal(name, goal)

        # Trigger automatic backup (high-value operation, bypasses rate limit)
        trigger_backup("create_workspace", {"workspace": name})

        logger.info("create_workspace() succeeded", extra={"workspace": name})
        return result
    except Exception as e:
        logger.error("create_workspace() failed", extra={"workspace": name, "error": str(e)})
        raise


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

        # Trigger automatic backup (high-value operation, bypasses rate limit)
        # Get workspace name from slot for backup details
        workspace_name = workspace_manager.get_workspace_for_slot(slot)
        trigger_backup(
            "save_version",
            {"slot": slot, "workspace": workspace_name, "version": result.version},
        )

        logger.info("save_version() succeeded", extra={"slot": slot, "version": result.version})
        return result
    except Exception as e:
        logger.error("save_version() failed", extra={"slot": slot, "error": str(e)})
        raise


@mcp.tool()
def diff(target: str, against: str | None = None) -> DiffResponse:
    """Compare changes between targets.

    Single argument: Compare slot against its last saved version (save_version).
    Two arguments: Compare any two targets.

    Args:
        target: What to diff. Can be:
            - Slot: "w1" (compares against last version if alone)
            - Workspace@version: "baseline@v2"
        against: Optional second target to compare against.

    Returns:
        DiffResponse with change summary, files changed, and what was compared.
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
