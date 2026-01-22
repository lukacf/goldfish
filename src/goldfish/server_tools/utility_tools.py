"""Goldfish MCP tools - Utility Tools

Extracted from server.py for better organization.
"""

import json
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


def _format_age(created_at: str | None) -> str:
    """Convert ISO timestamp to relative age like '2h ago'."""
    if not created_at:
        return "unknown"
    from datetime import UTC, datetime

    # Handle Z suffix for UTC
    timestamp = created_at.replace("Z", "+00:00")
    dt = datetime.fromisoformat(timestamp)

    # Ensure timezone-aware comparison
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    delta = now - dt

    if delta.days > 0:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    minutes = delta.seconds // 60
    return f"{minutes}m ago"


def _truncate_goal(goal: str | None, max_len: int = 80) -> str | None:
    """Truncate goal to max_len characters with ellipsis."""
    if not goal:
        return goal
    if len(goal) <= max_len:
        return goal
    return goal[:max_len] + "..."


def _parse_reason_from_json(reason_json: str | None) -> str | None:
    """Parse reason description from JSON."""
    if not reason_json:
        return None
    try:
        reason_data = json.loads(reason_json)
        desc = reason_data.get("description")
        return str(desc) if desc is not None else None
    except (json.JSONDecodeError, TypeError):
        return None


@mcp.tool()
def dashboard() -> dict:
    """Get a quick overview of system state for situational awareness.

    This is the essential tool for understanding what needs attention. Use it at the
    start of a session or whenever you need to understand current system state.

    Returns a structured summary with:
    - alerts: Failed runs and new SVS reviews requiring attention
    - active: Currently running stages
    - blocks: Pending finalizations grouped by workspace
    - workspaces: Mounted workspace status with dirty state
    - source_count: Total registered data sources

    SVS reviews are shown ONCE when new, then cleared from subsequent dashboard
    calls. Use inspect_run(include=["svs"]) to see all reviews for a run.

    Related tools:
    - status(): Global state including STATE.md and audit trail
    - list_history(): Experiment history for a workspace
    - inspect_run(): Full details for a specific run
    """
    from goldfish.workspace.manager import DirtyState

    db = _get_db()
    ws_manager = _get_workspace_manager()

    # Get failed runs with reason and age
    failed_rows = db.get_recent_failed_runs(limit=5)
    failed_recent = []
    for failed_row in failed_rows:
        reason = _parse_reason_from_json(failed_row.get("reason_json"))
        failed_recent.append(
            {
                "run_id": failed_row["id"],
                "workspace": failed_row["workspace_name"],
                "stage": failed_row["stage_name"],
                "reason": reason,
                "error": failed_row.get("error", "Unknown error"),
                "age": _format_age(failed_row.get("completed_at")),
            }
        )

    # Get active runs with elapsed time
    active_rows = db.get_active_runs()
    running = []
    for active_row in active_rows:
        reason = _parse_reason_from_json(active_row.get("reason_json"))
        running.append(
            {
                "run_id": active_row["id"],
                "workspace": active_row["workspace_name"],
                "stage": active_row["stage_name"],
                "reason": reason,
                "elapsed": _format_age(active_row.get("started_at")).replace(" ago", ""),
            }
        )

    # Get workspace summary - separate mounted and unmounted
    all_workspaces = list(ws_manager.list_workspaces(limit=50))
    mounted = []
    unmounted_count = 0

    for ws_info in all_workspaces:
        if ws_info.is_mounted and ws_info.mounted_slot:
            dirty = False
            try:
                slot_info = ws_manager.get_slot_info(ws_info.mounted_slot)
                dirty = slot_info.dirty == DirtyState.DIRTY
            except Exception:
                pass

            mounted.append(
                {
                    "slot": ws_info.mounted_slot,
                    "name": ws_info.name,
                    "goal": _truncate_goal(ws_info.goal),
                    "dirty": dirty,
                }
            )
        else:
            unmounted_count += 1

    # Get source count
    source_count = db.count_sources()

    # Get unnotified SVS reviews (new since last dashboard view)
    svs_rows = db.get_unnotified_svs_reviews(limit=50)
    svs_reviews = []
    review_ids_to_mark: list[int] = []
    for svs_row in svs_rows:
        # Parse findings count from JSON string
        findings_count = 0
        findings_json = svs_row.get("parsed_findings")
        if findings_json:
            try:
                findings = json.loads(findings_json)
                if isinstance(findings, list):
                    findings_count = len(findings)
            except (json.JSONDecodeError, TypeError):
                pass

        svs_reviews.append(
            {
                "run_id": svs_row["stage_run_id"],
                "workspace": svs_row.get("workspace_name"),
                "stage": svs_row.get("stage_name"),
                "review_type": svs_row["review_type"],
                "decision": svs_row["decision"],
                "findings_count": findings_count,
                "age": _format_age(svs_row.get("reviewed_at")),
            }
        )
        review_ids_to_mark.append(svs_row["id"])

    # Mark these reviews as notified so they don't appear again
    if review_ids_to_mark:
        db.mark_svs_reviews_notified(review_ids_to_mark)

    # Get pending finalizations grouped by workspace
    pending_by_workspace: dict[str, dict] = {}
    try:
        from goldfish.experiment_model.records import ExperimentRecordManager

        exp_manager = ExperimentRecordManager(db)
        # Get unique workspace names from mounted workspaces
        mounted_workspace_names: list[str] = [str(ws["name"]) for ws in mounted]
        for ws_name in mounted_workspace_names:
            unfinalized = exp_manager.list_unfinalized_runs(ws_name)
            if unfinalized:
                # Create example from first run with available info
                first_run = unfinalized[0]
                reason = first_run.get("reason", "")
                metric = first_run.get("primary_metric", {})
                if isinstance(metric, dict) and metric.get("value") is not None:
                    metric_str = f"{metric.get('name', 'metric')}={metric.get('value'):.1%}"
                else:
                    metric_str = "pending finalization"
                example = f"'{reason}' -> {metric_str}" if reason else metric_str

                pending_by_workspace[ws_name] = {
                    "count": len(unfinalized),
                    "example": example,
                }
    except Exception:
        # If experiment model fails, continue without it
        pass

    # Get recent outcomes (compact, max 5)
    outcome_rows = db.get_recent_outcomes(limit=5)
    recent_outcomes = [
        {
            "workspace": outcome_row["workspace_name"],
            "stage": outcome_row["stage_name"],
            "outcome": outcome_row["outcome"],
            "age": _format_age(outcome_row.get("completed_at")),
        }
        for outcome_row in outcome_rows
    ]

    return {
        "alerts": {
            "failed_recent": failed_recent,
            "svs_reviews": svs_reviews,
        },
        "active": {
            "running": running,
        },
        "blocks": {
            "pending_finalization": {
                "by_workspace": pending_by_workspace,
            },
        },
        "workspaces": {
            "mounted": mounted,
            "unmounted_count": unmounted_count,
        },
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
