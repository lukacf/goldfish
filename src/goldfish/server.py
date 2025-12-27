"""Goldfish MCP Server using FastMCP.

This is the main entry point that defines all MCP tools.
Uses ServerContext for dependency management instead of global variables.
"""

import logging
from pathlib import Path

from goldfish.config import GoldfishConfig
from goldfish.context import ServerContext, set_context
from goldfish.datasets.registry import DatasetRegistry
from goldfish.db.database import Database
from goldfish.jobs.launcher import JobLauncher
from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.jobs.tracker import JobTracker
from goldfish.pipeline.manager import PipelineManager

# Import mcp and context accessors from server_core (avoids circular imports)
from goldfish.server_core import (
    _get_config,
    _get_dataset_registry,
    _get_db,
    _get_job_launcher,
    _get_job_tracker,
    _get_pipeline_executor,
    _get_pipeline_manager,
    _get_project_root,
    _get_stage_executor,
    _get_state_manager,
    _get_state_md,
    _get_workspace_manager,
    _set_project_root,
    mcp,
)
from goldfish.state.state_md import StateManager
from goldfish.workspace.manager import WorkspaceManager

logger = logging.getLogger("goldfish.server")

# Re-export for backward compatibility
__all__ = [
    "mcp",
    "_get_config",
    "_get_db",
    "_get_workspace_manager",
    "_get_pipeline_manager",
    "_get_state_manager",
    "_get_job_launcher",
    "_get_job_tracker",
    "_get_dataset_registry",
    "_get_stage_executor",
    "_get_pipeline_executor",
    "_get_project_root",
    "_get_state_md",
    "configure_server",
    "reset_server",
    "run_server",
]


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


def _ensure_worker_running(project_root: Path, dev_repo_path: Path) -> None:
    """Ensure the pipeline worker daemon is running.

    Spawns the worker as a detached subprocess if not already running.
    The worker continues running even after the MCP server exits.
    """
    import subprocess

    pid_file = dev_repo_path / ".goldfish" / "worker.pid"

    # Check if worker is already running
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            # Check if process is still alive
            import os

            os.kill(pid, 0)  # Doesn't kill, just checks
            logger.debug("Worker already running (pid=%d)", pid)
            return
        except (ValueError, ProcessLookupError, PermissionError):
            # PID file exists but process is dead - remove stale file
            pid_file.unlink(missing_ok=True)

    # Spawn worker as detached subprocess
    logger.info("Spawning pipeline worker daemon...")
    try:
        # Build command to run worker
        import sys

        cmd = [
            sys.executable,
            "-m",
            "goldfish",
            "worker",
            "--project",
            str(project_root),
            "--pid-file",
            str(pid_file),
        ]

        # Spawn detached subprocess that survives parent exit
        # Use start_new_session=True on Unix to detach from terminal
        kwargs: dict = {
            "start_new_session": True,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }

        subprocess.Popen(cmd, **kwargs)
        logger.info("Worker daemon spawned")
    except Exception as e:
        logger.warning("Failed to spawn worker daemon: %s", e)


def _init_server(project_root: Path) -> None:
    """Initialize server components."""
    project_root = project_root.resolve()

    # Update the module-level project root
    _set_project_root(project_root)

    config = GoldfishConfig.load(project_root)

    # Dev repo contains all Goldfish runtime artifacts (.goldfish/, workspaces/, STATE.md)
    dev_repo_path = config.get_dev_repo_path(project_root)

    # Database is in dev repo
    db = Database(dev_repo_path / config.db_path)

    # Initialize state manager (STATE.md is in dev repo)
    state_manager = StateManager(dev_repo_path / config.state_md.path, config)

    # Initialize workspace manager with state manager
    workspace_manager = WorkspaceManager(config, project_root, db, state_manager)

    # Initialize job components
    job_launcher = JobLauncher(config, project_root, db, workspace_manager, state_manager)
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
        dataset_registry=dataset_registry,
    )
    pipeline_executor = PipelineExecutor(stage_executor=stage_executor, pipeline_manager=pipeline_manager, db=db)

    # Ensure worker daemon is running (spawns if needed)
    _ensure_worker_running(project_root, dev_repo_path)

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


# ============== MCP TOOLS ==============
# Tools are organized in separate modules for maintainability
# These imports MUST be after mcp is defined since they use @mcp.tool() decorator
from goldfish.server_tools.data_tools import (  # noqa: E402, F401
    delete_source,
    get_source,
    get_source_lineage,
    list_sources,
    promote_artifact,
    register_dataset,
    register_source,
    update_source_metadata,
)
from goldfish.server_tools.execution_tools import (  # noqa: E402, F401
    cancel,
    get_outputs,
    get_run,
    list_runs,
    logs,
    run,
)
from goldfish.server_tools.lineage_tools import (  # noqa: E402, F401
    get_run_provenance,
    get_version_diff,
    get_workspace_lineage,
)
from goldfish.server_tools.logging_tools import (  # noqa: E402, F401
    get_logsql_guide,
    search_goldfish_logs,
)
from goldfish.server_tools.pipeline_tools import get_pipeline, update_pipeline, validate_pipeline  # noqa: E402, F401
from goldfish.server_tools.svs_tools import (  # noqa: E402, F401
    approve_pattern,
    get_failure_pattern,
    get_run_svs_findings,
    get_svs_reviews,
    list_failure_patterns,
    register_svs_tools,
    reject_pattern,
    review_pending_patterns,
    update_pattern,
)
from goldfish.server_tools.utility_tools import (  # noqa: E402, F401
    get_audit_log,
    initialize_project,
    log_thought,
    reload_config,
    status,
)

# Explicitly register SVS MCP tools (avoid import side effects)
register_svs_tools()

# Re-export all tools for backward compatibility with existing code
from goldfish.server_tools.workspace_tools import (  # noqa: E402, F401
    branch_workspace,
    checkpoint,
    create_workspace,
    delete_snapshot,
    delete_workspace,
    diff,
    get_pruned_count,
    get_snapshot,
    get_workspace,
    get_workspace_goal,
    hibernate,
    list_snapshots,
    list_tags,
    list_workspaces,
    mount,
    prune_before_tag,
    prune_version,
    prune_versions,
    rollback,
    save_version,
    tag_version,
    unprune_version,
    unprune_versions,
    untag_version,
    update_workspace_goal,
)


def run_server(project_root: Path) -> None:
    """Run the MCP server."""
    from goldfish.errors import ProjectNotInitializedError
    from goldfish.logging import setup_logging

    # Initialize centralized logging
    setup_logging(component="server")
    logger.info("Goldfish MCP server starting [project=%s]", project_root)

    # Debug logging
    try:
        with open("/tmp/goldfish_run_server.log", "a") as f:
            f.write(f"run_server called with project_root: {project_root}\n")
    except OSError:
        pass

    # Store project root so it's available to tools even before initialization
    _set_project_root(project_root)

    try:
        _init_server(project_root)
        # Debug: Log successful initialization
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✓ Server initialized successfully for {project_root}\n")
        except OSError:
            pass
    except ProjectNotInitializedError as e:
        # Server starts without initialization - user must call initialize_project() first
        logger.info(f"Starting uninitialized server in {project_root}. Call initialize_project() to set up.")
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✗ Server NOT initialized: {e}\n")
        except OSError:
            pass
        pass
    except Exception as e:
        # Log any other initialization errors
        try:
            with open("/tmp/goldfish_run_server.log", "a") as f:
                f.write(f"✗ Server initialization failed with unexpected error: {type(e).__name__}: {e}\n")
        except OSError:
            pass
        raise

    mcp.run(transport="stdio")
