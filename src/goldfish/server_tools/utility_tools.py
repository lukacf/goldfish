"""Goldfish MCP tools - Utility Tools

Extracted from server.py for better organization.
"""

from typing import Optional
import logging
from datetime import datetime, timezone

logger = logging.getLogger("goldfish.server")

# Import server context helpers
from goldfish.server import (
    mcp,
    _get_config,
    _get_db,
    _get_workspace_manager,
    _get_pipeline_manager,
    _get_state_manager,
    _get_job_launcher,
    _get_job_tracker,
    _get_dataset_registry,
    _get_state_md,
)

# Import models
from goldfish.models import *

# Import validation functions
from goldfish.validation import (
    validate_workspace_name,
    validate_slot_name,
    
    validate_snapshot_id,
    validate_job_id,
    validate_source_name,
    validate_output_name,
    validate_artifact_uri,
    
    validate_script_path,
)

# Import errors
from goldfish.errors import GoldfishError, validate_reason


@mcp.tool()
def status() -> StatusResponse:
    """Get current status: slots, jobs, sources, and STATE.md content.

    Returns complete context for orientation after context compaction.
    Call this first when resuming work.
    """
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
    workspace: Optional[str] = None,
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
            details=json.loads(e["details"]) if e.get("details") else None,
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
