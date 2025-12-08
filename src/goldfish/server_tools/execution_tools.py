"""Goldfish MCP tools - Execution Tools

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
    _get_stage_executor,
    _get_pipeline_executor,
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
from goldfish.errors import (
    GoldfishError,
    validate_reason,
    JobNotFoundError,
    SourceNotFoundError,
)


@mcp.tool()
def run_job(
    slot: str,
    script: str,
    reason: str,
    source_inputs: Optional[dict[str, str]] = None,
) -> RunJobResponse:
    """Launch a job on the current snapshot.

    Creates a snapshot first, then exports to experiment directory
    and launches the job. You can continue editing while the job runs.

    Args:
        slot: Slot containing the code to run
        script: Script to execute (e.g., "scripts/train.py")
        reason: What this job is testing (min 15 chars)
        source_inputs: Optional map of input names to source IDs
                       (e.g., {"raw_data": "eurusd_ticks"})
    """
    logger.info("run_job() called", extra={"slot": slot, "script": script})

    config = _get_config()
    job_launcher = _get_job_launcher()
    db = _get_db()

    # Validate inputs
    validate_slot_name(slot, config.slots)
    validate_script_path(script)
    validate_reason(reason, config.audit.min_reason_length)

    # Validate source_inputs if provided
    if source_inputs:
        for input_name, source_id in source_inputs.items():
            # Validate input name (alphanumeric + underscores)
            if not input_name or not input_name.replace("_", "").isalnum():
                raise GoldfishError(
                    f"Invalid input name '{input_name}': must be alphanumeric with underscores"
                )
            # Validate source exists
            validate_source_name(source_id)
            if not db.source_exists(source_id):
                raise SourceNotFoundError(f"Source not found: {source_id}")

    try:
        response = job_launcher.run_job(
            slot=slot,
            script=script,
            reason=reason,
            source_inputs=source_inputs,
        )

        logger.info("run_job() succeeded", extra={
            "slot": slot,
            "script": script,
            "job_id": response.job_id,
            "snapshot_id": response.snapshot_id,
        })

        # Add STATE.md to response
        state_md = _get_state_md()

        return RunJobResponse(
            success=response.success,
            job_id=response.job_id,
            snapshot_id=response.snapshot_id,
            experiment_dir=response.experiment_dir,
            artifact_uri=response.artifact_uri,
            state_md=state_md,
        )
    except Exception as e:
        logger.error("run_job() failed", extra={"slot": slot, "script": script, "error": str(e)})
        raise

@mcp.tool()
def job_status(job_id: str) -> JobInfo:
    """Get status and logs for a job.

    Args:
        job_id: The job ID returned by run_job()
    """
    db = _get_db()

    validate_job_id(job_id)

    job = db.get_job(job_id)
    if job is None:
        raise JobNotFoundError(f"Job not found: {job_id}")

    # Get input sources
    job_inputs = db.get_job_inputs(job_id)
    input_sources = [inp["source_name"] for inp in job_inputs]

    return JobInfo(
        job_id=job["id"],
        status=job["status"],
        workspace=job["workspace"],
        snapshot_id=job["snapshot_id"],
        script=job["script"],
        started_at=parse_datetime(job["started_at"]),
        completed_at=parse_optional_datetime(job.get("completed_at")),
        log_uri=job.get("log_uri"),
        artifact_uri=job.get("artifact_uri"),
        error=job.get("error"),
        input_sources=input_sources,
    )


# Maximum lines that can be requested from logs (prevents memory exhaustion)
_MAX_TAIL_LINES = 10000

@mcp.tool()
def get_job_logs(job_id: str, tail_lines: int = 100) -> JobLogsResponse:
    """Get logs from a running or completed job.

    Args:
        job_id: The job ID returned by run_job()
        tail_lines: Number of lines from end to return (default 100, max 10000)
    """
    db = _get_db()
    job_tracker = _get_job_tracker()

    validate_job_id(job_id)

    # Validate tail_lines bounds
    if tail_lines < 1:
        raise GoldfishError("tail_lines must be at least 1")
    if tail_lines > _MAX_TAIL_LINES:
        raise GoldfishError(f"tail_lines cannot exceed {_MAX_TAIL_LINES}")

    job = db.get_job(job_id)
    if job is None:
        raise JobNotFoundError(f"Job not found: {job_id}")

    # Get logs from tracker
    logs = job_tracker.get_job_logs(job_id)

    # Tail the logs
    if logs and tail_lines > 0:
        lines = logs.splitlines()
        if len(lines) > tail_lines:
            logs = "\n".join(lines[-tail_lines:])

    return JobLogsResponse(
        job_id=job_id,
        status=job["status"],
        logs=logs,
        log_uri=job.get("log_uri"),
        error="Logs not available" if logs is None else None,
    )

@mcp.tool()
def cancel_job(job_id: str, reason: str) -> CancelJobResponse:
    """Cancel a running or pending job.

    Args:
        job_id: The job ID to cancel
        reason: Why you're cancelling (min 15 chars)
    """
    logger.info("cancel_job() called", extra={"job_id": job_id})

    config = _get_config()
    job_tracker = _get_job_tracker()

    validate_job_id(job_id)
    validate_reason(reason, config.audit.min_reason_length)

    try:
        response = job_tracker.cancel_job(job_id, reason)

        logger.info("cancel_job() succeeded", extra={
            "job_id": job_id,
            "previous_status": response.previous_status,
        })

        # Add STATE.md to response
        state_md = _get_state_md()

        return CancelJobResponse(
            success=response.success,
            job_id=response.job_id,
            previous_status=response.previous_status,
            state_md=state_md,
        )
    except Exception as e:
        logger.error("cancel_job() failed", extra={"job_id": job_id, "error": str(e)})
        raise

@mcp.tool()
def list_jobs(
    status: Optional[str] = None,
    workspace: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> ListJobsResponse:
    """List jobs with optional filters and pagination.

    Args:
        status: Filter by status (pending, running, completed, failed, cancelled)
        workspace: Filter by workspace name
        limit: Maximum number of jobs to return (default 50, max 200)
        offset: Number of jobs to skip for pagination (default 0)
    """
    db = _get_db()

    # Validate limit bounds
    if limit < 1:
        raise GoldfishError("limit must be at least 1")
    if limit > 200:
        raise GoldfishError("limit cannot exceed 200")

    # Validate offset bounds
    if offset < 0:
        raise GoldfishError("offset must be non-negative")

    # Validate workspace if provided
    if workspace:
        validate_workspace_name(workspace)

    # Validate status if provided
    valid_statuses = {"pending", "running", "completed", "failed", "cancelled"}
    if status and status not in valid_statuses:
        raise GoldfishError(f"Invalid status. Must be one of: {', '.join(sorted(valid_statuses))}")

    # Get total count for pagination
    total_count = db.count_jobs(status=status, workspace=workspace)

    # Get jobs from database with pagination
    jobs_raw = db.list_jobs(status=status, workspace=workspace, limit=limit, offset=offset)

    from goldfish.jobs.conversion import job_dict_to_info

    jobs = [job_dict_to_info(j, db) for j in jobs_raw]

    filters = {}
    if status:
        filters["status"] = status
    if workspace:
        filters["workspace"] = workspace

    return ListJobsResponse(
        jobs=jobs,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=(offset + len(jobs)) < total_count,
        filters_applied=filters,
    )


# ============== SOURCE TOOLS ==============

@mcp.tool()
def run_stage(
    workspace: str,
    stage: str,
    config_override: Optional[dict] = None,
    inputs_override: Optional[dict] = None,
    reason: Optional[str] = None
) -> dict:
    """Run a single pipeline stage.

    Args:
        workspace: Workspace name (e.g., "baseline_lstm") or slot (e.g., "w1")
        stage: Stage name (e.g., "tokenize")
        config_override: Override config env vars (e.g., {"VOCAB_SIZE": "20000"})
        inputs_override: Override input sources for debugging
        reason: Why running this stage (min 15 chars)

    Returns:
        Dict with:
        - stage_run_id: Stage run identifier
        - workspace: Workspace name
        - version: Auto-created version (e.g., "v1")
        - stage: Stage name
        - status: Job status ("running", "pending")

    Auto-creates workspace version (git tag).
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()
    stage_executor = _get_stage_executor()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace (could be slot like "w1")
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    stage_run = stage_executor.run_stage(
        workspace=workspace_name,
        stage_name=stage,
        config_override=config_override or {},
        inputs_override=inputs_override or {},
        reason=reason or "Manual stage run"
    )

    return stage_run

@mcp.tool()
def run_pipeline(
    workspace: str,
    config_override: Optional[dict] = None,
    reason: Optional[str] = None
) -> dict:
    """Run full pipeline (all stages in sequence).

    Args:
        workspace: Workspace name or slot
        config_override: Dict of {stage_name: {var: value}}
        reason: Why running this pipeline (min 15 chars)

    Returns:
        Dict with:
        - runs: List of stage run info dicts

    Auto-creates workspace version (git tag).
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()
    pipeline_executor = _get_pipeline_executor()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    runs = pipeline_executor.run_pipeline(
        workspace=workspace_name,
        config_override=config_override or {},
        reason=reason or "Manual pipeline run"
    )

    return {"runs": runs}

@mcp.tool()
def run_partial_pipeline(
    workspace: str,
    from_stage: str,
    to_stage: str,
    config_override: Optional[dict] = None,
    reason: Optional[str] = None
) -> dict:
    """Run stages from_stage through to_stage (inclusive).

    Args:
        workspace: Workspace name or slot
        from_stage: First stage to run
        to_stage: Last stage to run (inclusive)
        config_override: Dict of {stage_name: {var: value}}
        reason: Why running these stages (min 15 chars)

    Returns:
        Dict with:
        - runs: List of stage run info dicts
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()
    pipeline_executor = _get_pipeline_executor()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    runs = pipeline_executor.run_partial_pipeline(
        workspace=workspace_name,
        from_stage=from_stage,
        to_stage=to_stage,
        config_override=config_override or {},
        reason=reason or "Manual partial pipeline run"
    )

    return {"runs": runs}


# ============== ENTRY POINT ==============
