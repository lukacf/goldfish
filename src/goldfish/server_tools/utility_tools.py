"""Goldfish MCP tools - Utility Tools

Extracted from server.py for better organization.
"""

import logging
from datetime import datetime

from goldfish.errors import validate_reason
from goldfish.models import (
    LogThoughtResponse,
)
from goldfish.server_core import (
    _get_config,
    _get_db,
    _get_state_manager,
    _get_workspace_manager,
    mcp,
)

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def initialize_project(project_name: str, project_root: str, from_existing: str | None = None) -> dict:
    """Initialize a new Goldfish project in the specified directory.

    Creates the necessary directory structure, git repository, and configuration.
    This must be called before using other Goldfish tools in a new project.

    Args:
        project_name: Name for the project (used for config and dev repo naming)
        project_root: Root directory of the project (goldfish.yaml goes here)
        from_existing: Optional path to import existing code from

    Returns:
        dict with success status and project details
    """
    from pathlib import Path

    from goldfish.init import init_from_existing, init_project
    from goldfish.server import _init_server

    try:
        # Project root IS the project directory (don't create subdirectory)
        project_path = Path(project_root).resolve()

        if from_existing:
            source_path = Path(from_existing)
            config = init_from_existing(project_path, source_path)
            message = f"Initialized '{project_name}' with code from {from_existing}"
        else:
            config = init_project(project_name, project_path)
            message = f"Initialized '{project_name}'"

        # Initialize the server context now that project is set up
        try:
            _init_server(project_path)
            # Log to file for debugging
            try:
                with open("/tmp/goldfish_init_project.log", "a") as f:
                    f.write(f"✓ _init_server succeeded for {project_path}\n")
            except OSError:
                pass
            logger.info(f"Server initialized for project: {project_path}")
        except Exception as init_err:
            try:
                with open("/tmp/goldfish_init_project.log", "a") as f:
                    f.write(f"✗ _init_server FAILED for {project_path}: {init_err}\n")
            except OSError:
                pass
            logger.error(f"Failed to initialize server context: {init_err}")
            import traceback

            traceback.print_exc()
            # Re-raise to ensure the caller knows initialization failed
            raise

        # Dev repo path (relative to project parent)
        dev_repo_path = config.get_dev_repo_path(project_path)

        return {
            "success": True,
            "message": message,
            "project_path": str(project_path),
            "dev_repo": str(dev_repo_path),
            "config_file": str(project_path / "goldfish.yaml"),
            "state_file": str(dev_repo_path / config.state_md.path),
        }

    except Exception as e:
        logger.error(f"Failed to initialize project: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def reload_config() -> dict:
    """Reload configuration from goldfish.yaml.

    Call this after editing goldfish.yaml to pick up changes without
    restarting the MCP server.

    Returns:
        dict with success status and loaded configuration summary
    """
    from goldfish.server import _get_project_root, _init_server

    try:
        project_root = _get_project_root()
        _init_server(project_root)

        # Get the new config to show what was loaded
        config = _get_config()

        result = {
            "success": True,
            "message": "Configuration reloaded successfully",
            "project_name": config.project_name,
            "jobs_backend": config.jobs.backend,
            "gcs_configured": config.gcs is not None,
            "gce_configured": config.gce is not None,
        }

        if config.gce:
            result["gce_project"] = config.gce.effective_project_id
            result["gce_artifact_registry"] = config.gce.artifact_registry

        return result

    except Exception as e:
        logger.error(f"Failed to reload config: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def log_thought(thought: str, workspace: str | None = None, run_id: str | None = None) -> LogThoughtResponse:
    """Record reasoning for the audit trail.

    Use this to document why you're making decisions.
    Helps with context recovery after compaction.

    Args:
        thought: Your reasoning or decision rationale (min 15 chars)
        workspace: Optional workspace name or slot to associate this thought with.
        run_id: Optional stage run ID to associate this thought with.
    """
    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_reason(thought, config.audit.min_reason_length)

    # Associate with workspace if provided
    ws_name = None
    if workspace:
        # Resolve slot/workspace name
        ws_manager = _get_workspace_manager()
        ws_name = ws_manager.get_workspace_for_slot(workspace) or workspace

    db.log_audit(
        operation="thought",
        reason=thought,
        workspace=ws_name,
        details={"run_id": run_id} if run_id else None,
    )

    # Record in STATE.md (no truncation per user request)
    state_manager.add_action(f"Thought: {thought}")

    return LogThoughtResponse(
        logged=True,
        thought=thought,
        timestamp=datetime.now(),
        workspace=ws_name,
        run_id=run_id,
    )


@mcp.tool()
def get_workspace_thoughts(workspace: str, limit: int = 50, offset: int = 0) -> dict:
    """Get all thoughts logged for a workspace.

    Retrieves thoughts that were logged using log_thought() with this workspace.
    Useful for recovering context and understanding prior reasoning.

    Args:
        workspace: Workspace name or slot (e.g., "baseline" or "w1")
        limit: Maximum number of thoughts to return (default 50)
        offset: Number of thoughts to skip for pagination (default 0)

    Returns:
        dict with:
        - workspace: The resolved workspace name
        - thoughts: List of thought entries with timestamp, thought text, and optional run_id
        - total: Total count of thoughts for this workspace

    Related tools:
    - log_thought(): Record new thoughts
    - inspect_run(include=["thoughts"]): Get thoughts for a specific run
    - inspect_workspace(): Get workspace context including goal and history
    """
    import json

    db = _get_db()
    ws_manager = _get_workspace_manager()

    # Resolve slot to workspace name
    ws_name = ws_manager.get_workspace_for_slot(workspace) or workspace

    # Get thoughts from database
    thought_rows = db.get_workspace_thoughts(ws_name, limit=limit, offset=offset)
    total = db.count_workspace_thoughts(ws_name)

    # Format thoughts for response
    thoughts = []
    for row in thought_rows:
        thought_entry = {
            "timestamp": row["timestamp"],
            "thought": row["reason"],
        }
        # Extract run_id from details if present
        details_str = row.get("details")
        if details_str:
            try:
                details = json.loads(details_str)
                if details.get("run_id"):
                    thought_entry["run_id"] = details["run_id"]
            except (json.JSONDecodeError, TypeError):
                pass
        thoughts.append(thought_entry)

    return {
        "workspace": ws_name,
        "thoughts": thoughts,
        "total": total,
    }


@mcp.tool()
def dashboard() -> dict:
    """Get a quick overview of system state for situational awareness.

    Returns a summary designed for rapid understanding of:
    - What's broken (failed runs)
    - What's running (active runs)
    - Workspace status (mounted, dirty state)
    - Recent outcomes (success/bad_results trends)

    Unlike status(), this tool focuses on actionable information
    rather than audit trails.

    Returns:
        dict with:
        - failed_runs: Recent failed runs with error messages
        - active_runs: Currently running or pending stages
        - workspaces: Workspace summary with dirty status
        - source_count: Total registered data sources
        - recent_outcomes: Recent run outcomes for trend visibility

    Related tools:
    - status(): Global state including STATE.md and audit trail
    - list_all_runs(): Complete run history across workspaces
    - inspect_run(): Full details for a specific run
    """
    from goldfish.workspace.manager import DirtyState

    db = _get_db()
    ws_manager = _get_workspace_manager()

    # Get failed runs
    failed_rows = db.get_recent_failed_runs(limit=10)
    failed_runs = [
        {
            "run_id": row["id"],
            "workspace": row["workspace_name"],
            "stage": row["stage_name"],
            "error": row.get("error", "Unknown error"),
            "completed_at": row.get("completed_at"),
        }
        for row in failed_rows
    ]

    # Get active runs
    active_rows = db.get_active_runs()
    active_runs = [
        {
            "run_id": row["id"],
            "workspace": row["workspace_name"],
            "stage": row["stage_name"],
            "status": row["status"],
            "progress": row.get("progress"),
            "started_at": row.get("started_at"),
        }
        for row in active_rows
    ]

    # Get workspace summary with dirty status
    workspaces = []
    for ws_info in ws_manager.list_workspaces(limit=50):
        ws_entry = {
            "name": ws_info.name,
            "is_mounted": ws_info.is_mounted,
            "slot": ws_info.mounted_slot,
            "goal": ws_info.goal,
            "dirty": False,  # default
        }
        # Get dirty status if mounted
        if ws_info.is_mounted and ws_info.mounted_slot:
            try:
                slot_info = ws_manager.get_slot_info(ws_info.mounted_slot)
                ws_entry["dirty"] = slot_info.dirty == DirtyState.DIRTY
            except Exception:
                pass
        workspaces.append(ws_entry)

    # Get source count
    source_count = db.count_sources()

    # Get recent outcomes
    outcome_rows = db.get_recent_outcomes(limit=10)
    recent_outcomes = [
        {
            "workspace": row["workspace_name"],
            "stage": row["stage_name"],
            "outcome": row["outcome"],
            "completed_at": row.get("completed_at"),
        }
        for row in outcome_rows
    ]

    return {
        "failed_runs": failed_runs,
        "active_runs": active_runs,
        "workspaces": workspaces,
        "source_count": source_count,
        "recent_outcomes": recent_outcomes,
    }


@mcp.tool()
def validate_config(workspace: str | None = None) -> dict:
    """Validate configuration files for typos and errors.

    Validates goldfish.yaml and workspace pipeline/config files.
    Catches unknown fields, suggests corrections for typos, and checks YAML syntax.

    Args:
        workspace: Optional workspace name or slot to validate pipeline and stage configs.
                  If omitted, only validates goldfish.yaml.

    Returns:
        dict with:
        - valid: bool - True if all validations pass
        - errors: list - Critical issues that must be fixed
        - warnings: list - Non-critical issues (suggestions)
        - files_checked: list - Which files were validated
    """
    from goldfish.config_validation import validate_project_config
    from goldfish.server import _get_project_root

    project_root = _get_project_root()
    workspace_path = None
    workspace_name: str | None = workspace

    # Resolve workspace if specified
    if workspace:
        workspace_manager = _get_workspace_manager()

        # Resolve slot to workspace name
        resolved_name = workspace_manager.get_workspace_for_slot(workspace)
        if resolved_name:
            workspace_name = resolved_name

        # workspace_name is guaranteed to be str here (either original or resolved)
        assert workspace_name is not None
        try:
            workspace_path = workspace_manager.get_workspace_path(workspace_name)
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Workspace '{workspace}': {e}"],
                "warnings": [],
                "files_checked": [],
            }

    return validate_project_config(
        project_root=project_root,
        workspace_path=workspace_path,
        workspace_name=workspace_name,
    )


# ============== LINEAGE TOOLS ==============
