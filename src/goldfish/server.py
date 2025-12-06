"""Goldfish MCP Server using FastMCP.

This is the main entry point that defines all MCP tools.
Uses ServerContext for dependency management instead of global variables.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP

logger = logging.getLogger("goldfish.server")

from goldfish.config import GoldfishConfig
from goldfish.context import ServerContext, set_context, get_context, has_context
from goldfish.db.database import Database
from goldfish.errors import (
    GoldfishError,
    JobNotFoundError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
    WorkspaceNotFoundError,
    validate_reason,
)
from goldfish.lineage.manager import LineageManager
from goldfish.validation import (
    validate_workspace_name,
    validate_source_name,
    validate_script_path,
    validate_slot_name,
    validate_snapshot_id,
    validate_output_name,
    validate_job_id,
    validate_artifact_uri,
)
from goldfish.models import (
    AuditEntry,
    AuditLogResponse,
    CancelJobResponse,
    CheckpointResponse,
    CreateWorkspaceResponse,
    DeleteSnapshotResponse,
    DeleteSourceResponse,
    DeleteWorkspaceResponse,
    DiffResponse,
    HibernateResponse,
    JobInfo,
    JobLogsResponse,
    PipelineResponse,
    ValidatePipelineResponse,
    UpdatePipelineResponse,
    ListJobsResponse,
    ListSnapshotsResponse,
    ListSourcesResponse,
    LogThoughtResponse,
    MountResponse,
    PromoteArtifactResponse,
    RegisterSourceResponse,
    RollbackResponse,
    RunJobResponse,
    SnapshotInfo,
    SourceInfo,
    SourceLineage,
    StatusResponse,
    UpdateWorkspaceGoalResponse,
    WorkspaceGoalResponse,
    WorkspaceInfo,
)
from goldfish.state.state_md import StateManager
from goldfish.utils import parse_datetime, parse_optional_datetime
from goldfish.workspace.manager import WorkspaceManager
from goldfish.jobs.launcher import JobLauncher
from goldfish.jobs.tracker import JobTracker
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.pipeline.manager import PipelineManager
from goldfish.pipeline.parser import PipelineNotFoundError, PipelineValidationError
from goldfish.datasets.registry import DatasetRegistry

# Initialize FastMCP server
mcp = FastMCP("goldfish")


# ============== CONTEXT ACCESSORS ==============
# These provide type-safe access to context components with clear error messages


def _get_config() -> GoldfishConfig:
    """Get config from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().config


def _get_db() -> Database:
    """Get database from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().db


def _get_workspace_manager() -> WorkspaceManager:
    """Get workspace manager from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().workspace_manager


def _get_pipeline_manager() -> PipelineManager:
    """Get pipeline manager from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().pipeline_manager


def _get_state_manager() -> StateManager:
    """Get state manager from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().state_manager


def _get_job_launcher() -> JobLauncher:
    """Get job launcher from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().job_launcher


def _get_job_tracker() -> JobTracker:
    """Get job tracker from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().job_tracker


def _get_pipeline_manager() -> PipelineManager:
    """Get pipeline manager from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().pipeline_manager


def _get_dataset_registry() -> DatasetRegistry:
    """Get dataset registry from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().dataset_registry


def configure_server(
    project_root: Path,
    config: GoldfishConfig,
    db: Database,
    workspace_manager: WorkspaceManager,
    state_manager: StateManager,
    job_launcher: JobLauncher,
    job_tracker: JobTracker,
    pipeline_manager: PipelineManager,
    dataset_registry: DatasetRegistry,
    stage_executor,
    pipeline_executor,
) -> None:
    """Configure server with custom dependencies.

    This is primarily for testing - allows injecting mocks.
    Uses ServerContext internally for proper dependency management.

    Args:
        project_root: Project root path
        config: Configuration object
        db: Database instance
        workspace_manager: Workspace manager instance
        state_manager: State manager instance
        job_launcher: Job launcher instance
        job_tracker: Job tracker instance
        pipeline_manager: Pipeline manager instance
        dataset_registry: Dataset registry instance
        stage_executor: Stage executor instance
        pipeline_executor: Pipeline executor instance
    """
    ctx = ServerContext(
        project_root=project_root,
        config=config,
        db=db,
        workspace_manager=workspace_manager,
        state_manager=state_manager,
        job_launcher=job_launcher,
        job_tracker=job_tracker,
        pipeline_manager=pipeline_manager,
        dataset_registry=dataset_registry,
        stage_executor=stage_executor,
        pipeline_executor=pipeline_executor,
    )
    set_context(ctx)


def reset_server() -> None:
    """Reset all server state.

    Primarily for testing - clears all global state between tests.
    """
    set_context(None)


def _init_server(project_root: Path) -> None:
    """Initialize server components."""
    project_root = project_root.resolve()
    config = GoldfishConfig.load(project_root)
    db = Database(project_root / config.db_path)

    # Initialize state manager
    state_manager = StateManager(project_root / config.state_md.path, config)

    # Initialize workspace manager with state manager
    workspace_manager = WorkspaceManager(config, project_root, db, state_manager)

    # Initialize job components
    job_launcher = JobLauncher(
        config, project_root, db, workspace_manager, state_manager
    )
    job_tracker = JobTracker(db, project_root)

    # Initialize dataset registry
    dataset_registry = DatasetRegistry(db, config)

    # Initialize pipeline manager with dataset registry for validation
    pipeline_manager = PipelineManager(db, workspace_manager, dataset_registry=dataset_registry)

    # Initialize execution components
    stage_executor = StageExecutor(
        db=db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        dataset_registry=dataset_registry
    )
    pipeline_executor = PipelineExecutor(
        stage_executor=stage_executor,
        pipeline_manager=pipeline_manager
    )

    # Create and set context
    ctx = ServerContext(
        project_root=project_root,
        config=config,
        db=db,
        workspace_manager=workspace_manager,
        state_manager=state_manager,
        job_launcher=job_launcher,
        job_tracker=job_tracker,
        pipeline_manager=pipeline_manager,
        dataset_registry=dataset_registry,
        stage_executor=stage_executor,
        pipeline_executor=pipeline_executor,
    )
    set_context(ctx)


def _get_state_md() -> str:
    """Regenerate and return STATE.md content."""
    if not has_context():
        return "# Project\n\nNot initialized"
    return get_context().get_state_md()


# ============== WORKSPACE TOOLS ==============


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
def get_workspace_goal(workspace: str) -> WorkspaceGoalResponse:
    """Get the goal for a workspace.

    Args:
        workspace: Workspace name to query
    """
    db = _get_db()

    validate_workspace_name(workspace)

    goal = db.get_workspace_goal(workspace)

    return WorkspaceGoalResponse(
        workspace=workspace,
        goal=goal,
    )


@mcp.tool()
def update_workspace_goal(workspace: str, goal: str, reason: str) -> UpdateWorkspaceGoalResponse:
    """Update the goal for a workspace.

    Args:
        workspace: Workspace name to update
        goal: New goal description
        reason: Why you're updating the goal (min 15 chars)
    """
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
def list_snapshots(
    workspace: str, limit: int = 50, offset: int = 0
) -> ListSnapshotsResponse:
    """List snapshots for a workspace with pagination.

    Use this before rollback to see available checkpoints.

    Args:
        workspace: Workspace name to list snapshots for
        limit: Maximum number of snapshots to return (1-200, default 50)
        offset: Number of snapshots to skip for pagination (default 0)

    Returns:
        ListSnapshotsResponse with snapshots and pagination metadata
    """
    workspace_manager = _get_workspace_manager()

    # Validate workspace name
    validate_workspace_name(workspace)

    # Validate pagination bounds
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    # Get all snapshots once and apply pagination in memory
    all_snapshots_data = workspace_manager.list_snapshots(
        workspace, limit=10000, offset=0
    )

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
    """Get detailed information about a specific snapshot.

    Use this to verify a snapshot exists and get its metadata before operations like rollback.

    Args:
        workspace: Workspace name the snapshot belongs to
        snapshot_id: Snapshot ID (e.g., snap-abc1234-20251205-120000)

    Returns:
        SnapshotInfo with snapshot details

    Raises:
        GoldfishError: If workspace doesn't exist or snapshot not found in workspace
    """
    from datetime import datetime

    workspace_manager = _get_workspace_manager()

    # Validate parameters
    validate_workspace_name(workspace)
    validate_snapshot_id(snapshot_id)

    # Check workspace exists by trying to get its snapshots
    try:
        snapshot_ids = workspace_manager.git.list_snapshots(workspace)
    except GoldfishError as e:
        raise GoldfishError(f"Workspace '{workspace}' not found or inaccessible: {e}")

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
        except ValueError:
            raise GoldfishError(
                f"Invalid date format for snapshot '{snapshot_id}': {info.get('commit_date')}"
            )

    if created_at is None:
        raise GoldfishError(f"Snapshot '{snapshot_id}' has no valid creation date")

    return SnapshotInfo(
        snapshot_id=snapshot_id,
        created_at=created_at,
        message=info.get("message", ""),
    )


# ============== JOB TOOLS ==============


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
def list_sources(
    status: Optional[str] = None,
    created_by: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> ListSourcesResponse:
    """List available data sources with pagination and filtering.

    Args:
        status: Filter by status (available, processing, error)
        created_by: Filter by creator (external, internal, etc.)
        limit: Maximum number of sources to return (1-200, default 50)
        offset: Number of sources to skip for pagination (default 0)

    Returns:
        ListSourcesResponse with sources and pagination metadata
    """
    db = _get_db()

    # Validate limit and offset
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    # Get total count matching filters
    total_count = db.count_sources(status=status, created_by=created_by)

    # Get page of sources
    sources = db.list_sources(
        status=status,
        created_by=created_by,
        limit=limit,
        offset=offset,
    )

    source_infos = [
        SourceInfo(
            name=s["name"],
            description=s.get("description"),
            created_at=parse_datetime(s["created_at"]),
            created_by=s["created_by"],
            gcs_location=s["gcs_location"],
            size_bytes=s.get("size_bytes"),
            status=s["status"],
        )
        for s in sources
    ]

    # Calculate has_more
    has_more = (offset + len(source_infos)) < total_count

    # Track which filters were applied
    filters_applied = {}
    if status:
        filters_applied["status"] = status
    if created_by:
        filters_applied["created_by"] = created_by

    return ListSourcesResponse(
        sources=source_infos,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=has_more,
        filters_applied=filters_applied,
    )


@mcp.tool()
def get_source(name: str) -> SourceInfo:
    """Get detailed information about a specific data source.

    Args:
        name: Name of the source to look up
    """
    db = _get_db()
    validate_source_name(name)

    source = db.get_source(name)
    if source is None:
        raise SourceNotFoundError(f"Source not found: {name}")

    return SourceInfo(
        name=source["name"],
        description=source.get("description"),
        created_at=parse_datetime(source["created_at"]),
        created_by=source["created_by"],
        gcs_location=source["gcs_location"],
        size_bytes=source.get("size_bytes"),
        status=source["status"],
    )


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
def register_source(
    name: str, gcs_path: str, description: str, reason: str
) -> RegisterSourceResponse:
    """Register an external data source.

    Args:
        name: Source name (e.g., "eurusd_real_ticks")
        gcs_path: GCS location (e.g., "gs://bucket/data/eurusd.csv")
        description: What this data contains
        reason: Why you're registering this source (min 15 chars)
    """
    logger.info("register_source() called", extra={"source": name, "gcs_path": gcs_path})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    # Validate inputs
    validate_source_name(name)
    validate_reason(reason, config.audit.min_reason_length)

    if db.source_exists(name):
        raise SourceAlreadyExistsError(f"Source '{name}' already exists")

    try:
        db.create_source(
            source_id=name,
            name=name,
            gcs_location=gcs_path,
            created_by="external",
            description=description,
        )

        db.log_audit(
            operation="register_source",
            reason=reason,
            details={"source": name, "gcs_path": gcs_path},
        )

        state_manager.add_action(f"Registered source '{name}'")

        logger.info("register_source() succeeded", extra={"source": name})

        source = SourceInfo(
            name=name,
            description=description,
            created_at=datetime.now(),
            created_by="external",
            gcs_location=gcs_path,
        )

        state_md = _get_state_md()

        return RegisterSourceResponse(success=True, source=source, state_md=state_md)
    except Exception as e:
        logger.error("register_source() failed", extra={"source": name, "error": str(e)})
        raise


@mcp.tool()
def promote_artifact(
    job_id: str, output_name: str, source_name: str, reason: str
) -> PromoteArtifactResponse:
    """Promote a job output to a reusable data source.

    This creates a registry entry pointing to the artifact location
    (no data copy - just a reference). Records lineage: the new source
    knows which job produced it and what input sources that job used.

    Args:
        job_id: ID of the completed job
        output_name: Name of the output in job config (e.g., "preprocessed")
        source_name: Name for the new source (e.g., "preprocessed_v1")
        reason: Why you're promoting this artifact (min 15 chars)
    """
    logger.info("promote_artifact() called", extra={
        "job_id": job_id,
        "output_name": output_name,
        "source_name": source_name,
    })

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    # Validate inputs
    validate_job_id(job_id)
    validate_source_name(source_name)
    validate_output_name(output_name)
    validate_reason(reason, config.audit.min_reason_length)

    # Check job exists and completed
    job = db.get_job(job_id)
    if job is None:
        raise JobNotFoundError(f"Job not found: {job_id}")

    if job["status"] != "completed":
        raise GoldfishError(f"Job {job_id} has not completed (status: {job['status']})")

    if db.source_exists(source_name):
        raise SourceAlreadyExistsError(f"Source '{source_name}' already exists")

    try:
        # Get artifact location (reference, no copy)
        artifact_uri = job.get("artifact_uri")
        if not artifact_uri:
            raise GoldfishError(f"Job {job_id} has no artifact URI")

        # Security: validate artifact URI (must be GCS, no path traversal)
        validate_artifact_uri(artifact_uri)

        # The artifact path for a specific output
        gcs_location = f"{artifact_uri.rstrip('/')}/{output_name}/"

        # Atomically create source and lineage (transaction prevents partial writes)
        with db.transaction() as conn:
            # Create source entry
            conn.execute(
                """
                INSERT INTO sources (id, name, description, created_at, created_by,
                                     gcs_location, size_bytes, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_name,
                    source_name,
                    f"Promoted from job {job_id} output '{output_name}'",
                    datetime.now(timezone.utc).isoformat(),
                    f"job:{job_id}",
                    gcs_location,
                    None,  # size_bytes
                    "available",
                    None,  # metadata
                ),
            )

            # Record lineage from job inputs
            job_inputs = db.get_job_inputs(job_id)
            timestamp = datetime.now(timezone.utc).isoformat()
            for inp in job_inputs:
                conn.execute(
                    """
                    INSERT INTO source_lineage (source_id, parent_source_id, job_id, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (source_name, inp["source_id"], job_id, timestamp),
                )

        db.log_audit(
            operation="promote_artifact",
            reason=reason,
            details={
                "job_id": job_id,
                "output_name": output_name,
                "source_name": source_name,
                "gcs_location": gcs_location,
            },
        )

        state_manager.add_action(f"Promoted '{output_name}' from job {job_id} → source '{source_name}'")

        logger.info("promote_artifact() succeeded", extra={
            "job_id": job_id,
            "source_name": source_name,
        })

        source = SourceInfo(
            name=source_name,
            description=f"Promoted from job {job_id} output '{output_name}'",
            created_at=datetime.now(),
            created_by=f"job:{job_id}",
            gcs_location=gcs_location,
        )

        lineage = SourceLineage(
            source_name=source_name,
            parent_sources=[inp["source_id"] for inp in job_inputs],
            job_id=job_id,
        )

        state_md = _get_state_md()

        return PromoteArtifactResponse(
            success=True, source=source, lineage=lineage, state_md=state_md
        )
    except Exception as e:
        logger.error("promote_artifact() failed", extra={
            "job_id": job_id,
            "source_name": source_name,
            "error": str(e),
        })
        raise


@mcp.tool()
def get_source_lineage(source_name: str) -> SourceLineage:
    """Get lineage information for a data source.

    Shows the parent sources and creating job for a source.
    Useful for understanding data provenance.

    Args:
        source_name: Name of the source to query
    """
    db = _get_db()

    # Validate source name
    validate_source_name(source_name)

    # Check source exists
    if not db.source_exists(source_name):
        raise SourceNotFoundError(f"Source not found: {source_name}")

    # Get lineage records
    lineage_records = db.get_lineage(source_name)

    parent_sources = []
    job_id = None

    for record in lineage_records:
        if record.get("parent_source_id"):
            parent_sources.append(record["parent_source_id"])
        if record.get("job_id") and job_id is None:
            job_id = record["job_id"]

    return SourceLineage(
        source_name=source_name,
        parent_sources=parent_sources,
        job_id=job_id,
    )


# ============== DELETE TOOLS ==============


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
                f"Cannot delete workspace '{workspace}': it is mounted in slot {slot_info.slot}. "
                f"Hibernate it first."
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

        logger.info("delete_workspace() succeeded", extra={
            "workspace": workspace,
            "snapshots_deleted": snapshot_count,
        })

        return DeleteWorkspaceResponse(
            success=True,
            workspace=workspace,
            snapshots_deleted=snapshot_count,
        )
    except Exception as e:
        logger.error("delete_workspace() failed", extra={"workspace": workspace, "error": str(e)})
        raise


@mcp.tool()
def delete_source(source_name: str, reason: str) -> DeleteSourceResponse:
    """Delete a data source from the registry.

    WARNING: This is irreversible. Jobs that used this source
    will have broken lineage references.

    Args:
        source_name: Name of the source to delete
        reason: Why you're deleting this source (min 15 chars)
    """
    logger.info("delete_source() called", extra={"source_name": source_name})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_source_name(source_name)
    validate_reason(reason, config.audit.min_reason_length)

    # Check source exists
    if not db.source_exists(source_name):
        raise SourceNotFoundError(f"Source not found: {source_name}")

    try:
        # Delete source (and lineage)
        db.delete_source(source_name)

        # Log to audit
        db.log_audit(
            operation="delete_source",
            reason=reason,
            details={"source_name": source_name},
        )

        state_manager.add_action(f"Deleted source '{source_name}'")

        logger.info("delete_source() succeeded", extra={"source_name": source_name})

        return DeleteSourceResponse(
            success=True,
            source_name=source_name,
        )
    except Exception as e:
        logger.error("delete_source() failed", extra={"source_name": source_name, "error": str(e)})
        raise


@mcp.tool()
def delete_snapshot(workspace: str, snapshot_id: str, reason: str) -> DeleteSnapshotResponse:
    """Delete a specific snapshot from a workspace.

    WARNING: This is irreversible. You cannot rollback to a deleted snapshot.

    Args:
        workspace: Workspace containing the snapshot
        snapshot_id: ID of the snapshot to delete
        reason: Why you're deleting this snapshot (min 15 chars)
    """
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
def get_pipeline(workspace: str) -> PipelineResponse:
    """Get pipeline definition for a workspace.

    Returns the pipeline.yaml content as a structured object.

    Args:
        workspace: Workspace name

    Returns:
        Pipeline definition with stages, inputs, outputs
    """
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        pipeline = pipeline_manager.get_pipeline(workspace)
        return PipelineResponse(workspace=workspace, pipeline=pipeline)
    except PipelineNotFoundError as e:
        raise GoldfishError(str(e)) from e
    except PipelineValidationError as e:
        raise GoldfishError(f"Pipeline is invalid: {e}") from e


@mcp.tool()
def validate_pipeline(workspace: str) -> ValidatePipelineResponse:
    """Validate pipeline definition for a workspace.

    Checks:
    - Stage files exist (modules/{stage}.py, configs/{stage}.yaml)
    - Signal types match between stages
    - No circular dependencies
    - Datasets exist (if referenced)

    Args:
        workspace: Workspace name

    Returns:
        Validation result with list of errors (empty if valid)
    """
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        errors = pipeline_manager.validate_pipeline(workspace)
        return ValidatePipelineResponse(
            workspace=workspace,
            valid=len(errors) == 0,
            errors=errors,
        )
    except PipelineNotFoundError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def update_pipeline(workspace: str, pipeline_yaml: str) -> UpdatePipelineResponse:
    """Update pipeline.yaml in workspace.

    Validates the pipeline before writing. Will reject invalid pipelines.

    Args:
        workspace: Workspace name
        pipeline_yaml: Complete pipeline.yaml content (YAML string)

    Returns:
        Updated pipeline definition
    """
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        pipeline = pipeline_manager.update_pipeline(workspace, pipeline_yaml)
        return UpdatePipelineResponse(
            success=True,
            workspace=workspace,
            pipeline=pipeline,
        )
    except PipelineValidationError as e:
        raise GoldfishError(f"Pipeline validation failed: {e}") from e


# ============== DATASET TOOLS ==============


@mcp.tool()
def register_dataset(
    name: str,
    source: str,
    description: str,
    format: str,
    metadata: Optional[dict] = None,
) -> dict:
    """Register a project-level dataset.

    Datasets are immutable data sources shared across all workspaces.
    For local files, Goldfish uploads them to GCS automatically.

    Args:
        name: Dataset identifier (e.g., "eurusd_raw_v3")
        source: Source location:
            - "local:/path/to/file.csv" - Local file (will be uploaded to GCS)
            - "gs://bucket/path" - GCS path (used directly)
        description: Human-readable description
        format: File format (csv, npy, directory, etc.)
        metadata: Optional metadata dict (e.g., {"rows": 1000, "columns": 5})

    Returns:
        Dataset info with GCS location

    Example:
        register_dataset(
            name="eurusd_raw_v3",
            source="local:/data/eurusd.csv",
            description="EUR/USD tick data, version 3",
            format="csv"
        )
    """
    from goldfish.validation import validate_source_name

    dataset_registry = _get_dataset_registry()
    validate_source_name(name)

    try:
        dataset = dataset_registry.register_dataset(
            name=name,
            source=source,
            description=description,
            format=format,
            metadata=metadata,
        )

        return {
            "success": True,
            "dataset": {
                "name": dataset.name,
                "gcs_location": dataset.gcs_location,
                "description": dataset.description,
                "format": format,
                "created_at": dataset.created_at.isoformat(),
                "size_bytes": dataset.size_bytes,
            },
        }
    except SourceAlreadyExistsError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def list_datasets(status: Optional[str] = None) -> dict:
    """List all registered datasets.

    Args:
        status: Optional status filter (available, pending, failed)

    Returns:
        List of datasets with their info
    """
    dataset_registry = _get_dataset_registry()

    datasets = dataset_registry.list_datasets(status=status)

    return {
        "datasets": [
            {
                "name": d.name,
                "gcs_location": d.gcs_location,
                "description": d.description,
                "created_at": d.created_at.isoformat(),
                "size_bytes": d.size_bytes,
                "status": d.status.value,
            }
            for d in datasets
        ],
        "count": len(datasets),
    }


@mcp.tool()
def get_dataset(name: str) -> dict:
    """Get dataset details.

    Args:
        name: Dataset name

    Returns:
        Dataset info

    Raises:
        SourceNotFoundError: If dataset not found
    """
    from goldfish.validation import validate_source_name

    dataset_registry = _get_dataset_registry()
    validate_source_name(name)

    try:
        dataset = dataset_registry.get_dataset(name)

        return {
            "name": dataset.name,
            "gcs_location": dataset.gcs_location,
            "description": dataset.description,
            "created_at": dataset.created_at.isoformat(),
            "created_by": dataset.created_by,
            "size_bytes": dataset.size_bytes,
            "status": dataset.status.value,
        }
    except SourceNotFoundError as e:
        raise GoldfishError(str(e)) from e


# ============== CONTEXT TOOLS ==============


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


@mcp.tool()
def get_workspace_lineage(workspace: str) -> dict:
    """Get full lineage for workspace.

    Shows workspace history, versions, and branches.

    Args:
        workspace: Workspace name (e.g., "baseline_lstm")

    Returns:
        Dict with:
        - name: Workspace name
        - created: Creation timestamp
        - parent: Parent workspace if branched
        - parent_version: Version branched from
        - description: Workspace description
        - versions: List of all versions with metadata
        - branches: Child workspaces branched from this one
    """
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    return lineage_mgr.get_workspace_lineage(workspace)


@mcp.tool()
def get_version_diff(workspace: str, from_version: str, to_version: str) -> dict:
    """Compare two versions of workspace.

    Shows what changed between versions (git diff, file changes).

    Args:
        workspace: Workspace name
        from_version: Starting version (e.g., "v1")
        to_version: Ending version (e.g., "v3")

    Returns:
        Dict with:
        - from_version: Starting version
        - to_version: Ending version
        - commits: List of git commits between versions
        - files: File changes between versions
    """
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    return lineage_mgr.get_version_diff(workspace, from_version, to_version)


@mcp.tool()
def get_run_provenance(stage_run_id: str) -> dict:
    """Get exact provenance of a stage run.

    Shows complete information about what produced a run:
    workspace, version, git SHA, config, inputs, outputs.

    Args:
        stage_run_id: Stage run identifier

    Returns:
        Dict with:
        - stage_run_id: Run identifier
        - workspace: Workspace name
        - version: Workspace version
        - git_sha: Exact git commit
        - stage: Stage name
        - config_override: Config overrides used
        - inputs: Input signals consumed
        - outputs: Output signals produced
    """
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    return lineage_mgr.get_run_provenance(stage_run_id)


@mcp.tool()
def branch_workspace(
    from_workspace: str,
    from_version: str,
    new_workspace: str,
    reason: str
) -> dict:
    """Create new workspace branched from specific version.

    Allows experimenting from a known-good version.

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
    config = _get_config()
    db = _get_db()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(from_workspace)
    validate_workspace_name(new_workspace)
    validate_reason(reason, config.audit.min_reason_length)

    lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
    lineage_mgr.branch_workspace(from_workspace, from_version, new_workspace, reason)

    return {
        "workspace": new_workspace,
        "parent": from_workspace,
        "parent_version": from_version
    }


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
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    pipeline_manager = _get_pipeline_manager()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace (could be slot like "w1")
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    stage_executor = StageExecutor(
        db=db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager
    )

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
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    pipeline_manager = _get_pipeline_manager()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    stage_executor = StageExecutor(
        db=db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager
    )

    pipeline_executor = PipelineExecutor(
        stage_executor=stage_executor,
        pipeline_manager=pipeline_manager
    )

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
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    pipeline_manager = _get_pipeline_manager()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

    # Resolve workspace
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    stage_executor = StageExecutor(
        db=db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager
    )

    pipeline_executor = PipelineExecutor(
        stage_executor=stage_executor,
        pipeline_manager=pipeline_manager
    )

    runs = pipeline_executor.run_partial_pipeline(
        workspace=workspace_name,
        from_stage=from_stage,
        to_stage=to_stage,
        config_override=config_override or {},
        reason=reason or "Manual partial pipeline run"
    )

    return {"runs": runs}


# ============== ENTRY POINT ==============


def run_server(project_root: Path) -> None:
    """Run the MCP server."""
    _init_server(project_root)
    mcp.run(transport="stdio")
