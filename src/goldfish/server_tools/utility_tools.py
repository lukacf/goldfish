"""Goldfish MCP tools - Utility Tools

Extracted from server.py for better organization.
"""

import json
import logging
from datetime import datetime

from goldfish.errors import validate_reason
from goldfish.models import (
    AuditEntry,
    AuditLogResponse,
    LogThoughtResponse,
    StatusResponse,
)
from goldfish.server import (
    _get_config,
    _get_db,
    _get_state_manager,
    _get_state_md,
    _get_workspace_manager,
    mcp,
)
from goldfish.utils import parse_datetime
from goldfish.validation import (
    validate_workspace_name,
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
def status() -> StatusResponse:
    """Get current status: slots, jobs, sources, and STATE.md content.

    Returns complete context for orientation after context compaction.
    Call this first when resuming work.
    """
    from goldfish.context import has_context

    # Debug logging
    try:
        with open("/tmp/goldfish_status.log", "a") as f:
            f.write(f"status() called, has_context={has_context()}\n")
    except OSError:
        pass

    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    slots = workspace_manager.get_all_slots()
    active_jobs_raw = db.get_active_jobs()

    from goldfish.jobs.conversion import job_dict_to_info

    active_jobs = [job_dict_to_info(j, db) for j in active_jobs_raw]

    source_count = len(db.list_sources())
    state_md = _get_state_md()

    return StatusResponse(
        project_name=config.project_name,
        slots=slots,
        active_jobs=active_jobs,
        source_count=source_count,
        state_md=state_md,
    )


@mcp.tool()
def get_audit_log(
    limit: int = 20,
    workspace: str | None = None,
) -> AuditLogResponse:
    """Get recent audit trail entries.

    Shows what operations have been performed and why.

    Args:
        limit: Maximum number of entries to return (default 20)
        workspace: Optional filter by workspace name
    """
    db = _get_db()

    entries = db.get_recent_audit(limit=limit)

    # Filter by workspace if specified
    if workspace:
        validate_workspace_name(workspace)
        entries = [e for e in entries if e.get("workspace") == workspace]

    audit_entries = [
        AuditEntry(
            id=e["id"],
            timestamp=parse_datetime(e["timestamp"]),
            operation=e["operation"],
            slot=e.get("slot"),
            workspace=e.get("workspace"),
            reason=e["reason"],
            details=json.loads(e["details"]) if e["details"] else None,
        )
        for e in entries
    ]

    return AuditLogResponse(entries=audit_entries, count=len(audit_entries))


@mcp.tool()
def log_thought(thought: str) -> LogThoughtResponse:
    """Record reasoning for the audit trail.

    Use this to document why you're making decisions.
    Helps with context recovery after compaction.

    Args:
        thought: Your reasoning or decision rationale (min 15 chars)
    """
    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_reason(thought, config.audit.min_reason_length)

    db.log_audit(
        operation="thought",
        reason=thought,
    )

    # Truncate for STATE.md display
    truncated = thought[:50] + "..." if len(thought) > 50 else thought
    state_manager.add_action(f"Thought: {truncated}")

    return LogThoughtResponse(
        logged=True,
        thought=thought,
        timestamp=datetime.now(),
    )


# ============== LINEAGE TOOLS ==============
