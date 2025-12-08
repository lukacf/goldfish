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

# Module-level variable to store project root (set when server starts)
_project_root: Optional[Path] = None


# ============== CONTEXT ACCESSORS ==============
# These provide type-safe access to context components with clear error messages


def _set_project_root(project_root: Path) -> None:
    """Set the project root directory."""
    global _project_root
    _project_root = project_root.resolve()


def _get_project_root() -> Path:
    """Get the project root directory."""
    if _project_root is None:
        raise GoldfishError("Server not initialized with project root")
    return _project_root


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


def _get_dataset_registry() -> DatasetRegistry:
    """Get dataset registry from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().dataset_registry


def _get_stage_executor():
    """Get stage executor from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().stage_executor


def _get_pipeline_executor():
    """Get pipeline executor from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().pipeline_executor


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

    # Update the module-level project root
    _set_project_root(project_root)

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
        project_root=project_root,
        dataset_registry=dataset_registry
    )
    pipeline_executor = PipelineExecutor(
        stage_executor=stage_executor,
        pipeline_manager=pipeline_manager,
        db=db
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




# ============== MCP TOOLS ==============
# Tools are organized in separate modules for maintainability

# Import all tool modules (this registers them with MCP and makes them available)
import goldfish.server_tools  # noqa: F401

# Re-export all tools for backward compatibility with existing code
from goldfish.server_tools.workspace_tools import (  # noqa: F401
    mount,
    hibernate,
    create_workspace,
    list_workspaces,
    get_workspace,
    delete_workspace,
    checkpoint,
    diff,
    rollback,
    list_snapshots,
    get_snapshot,
    delete_snapshot,
    get_workspace_goal,
    update_workspace_goal,
    branch_workspace
)
from goldfish.server_tools.execution_tools import (  # noqa: F401
    run_job,
    job_status,
    get_job_logs,
    cancel_job,
    list_jobs,
    run_stage,
    run_pipeline,
    run_partial_pipeline
)
from goldfish.server_tools.data_tools import (  # noqa: F401
    list_sources,
    get_source,
    register_source,
    delete_source,
    get_source_lineage,
    promote_artifact,
    register_dataset,
    list_datasets,
    get_dataset
)
from goldfish.server_tools.pipeline_tools import (  # noqa: F401
    get_pipeline,
    validate_pipeline,
    update_pipeline
)
from goldfish.server_tools.lineage_tools import (  # noqa: F401
    get_workspace_lineage,
    get_version_diff,
    get_run_provenance
)
from goldfish.server_tools.utility_tools import (  # noqa: F401
    initialize_project,
    status,
    get_audit_log,
    log_thought
)


def run_server(project_root: Path) -> None:
    """Run the MCP server."""
    from goldfish.errors import ProjectNotInitializedError

    # Debug logging
    try:
        with open("/tmp/goldfish_run_server.log", "a") as f:
            f.write(f"run_server called with project_root: {project_root}\n")
    except:
        pass

    # Store project root so it's available to tools even before initialization
    _set_project_root(project_root)

    try:
        _init_server(project_root)
        # Debug: Log successful initialization
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✓ Server initialized successfully for {project_root}\n")
        except:
            pass
    except ProjectNotInitializedError as e:
        # Server starts without initialization - user must call initialize_project() first
        logger.info(f"Starting uninitialized server in {project_root}. Call initialize_project() to set up.")
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✗ Server NOT initialized: {e}\n")
        except:
            pass
        pass
    except Exception as e:
        # Log any other initialization errors
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✗ Server initialization failed with unexpected error: {type(e).__name__}: {e}\n")
        except:
            pass
        raise

    mcp.run(transport="stdio")
