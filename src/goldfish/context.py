"""Server context management for Goldfish.

Provides context management for the MCP server.
Uses a simple module-level variable since all MCP tool calls
run in the same process (ContextVar doesn't work well with
async frameworks where each handler runs in a different context).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.state.state_md import StateManager

if TYPE_CHECKING:
    from goldfish.datasets.registry import DatasetRegistry
    from goldfish.jobs.launcher import JobLauncher
    from goldfish.jobs.pipeline_executor import PipelineExecutor
    from goldfish.jobs.stage_executor import StageExecutor
    from goldfish.jobs.tracker import JobTracker
    from goldfish.pipeline.manager import PipelineManager
    from goldfish.workspace.manager import WorkspaceManager


@dataclass
class ServerContext:
    """Holds all server dependencies.

    This replaces the global variables in server.py with a proper
    context object that can be:
    - Easily mocked in tests
    - Type-checked by static analyzers
    """

    project_root: Path
    config: GoldfishConfig
    db: Database
    state_manager: StateManager
    workspace_manager: WorkspaceManager
    job_launcher: JobLauncher
    job_tracker: JobTracker
    pipeline_manager: PipelineManager
    dataset_registry: DatasetRegistry
    stage_executor: StageExecutor
    pipeline_executor: PipelineExecutor

    def get_state_md(self) -> str:
        """Regenerate and return STATE.md content."""
        jobs = self.db.get_active_jobs()
        return self.state_manager.regenerate(
            slots=self.workspace_manager.get_all_slots(),
            jobs=[dict(j) for j in jobs],  # Convert JobRow to dict
            source_count=len(self.db.list_sources()),
        )


# Module-level context storage (simple and reliable for single-process MCP)
_server_context: ServerContext | None = None


def get_context() -> ServerContext:
    """Get the current server context.

    Raises:
        RuntimeError: If server is not initialized
    """
    global _server_context
    if _server_context is None:
        raise RuntimeError("Server not initialized - call set_context() first")
    return _server_context


def set_context(ctx: ServerContext | None) -> None:
    """Set the server context.

    Args:
        ctx: Server context to set, or None to clear
    """
    global _server_context
    _server_context = ctx


def has_context() -> bool:
    """Check if server context is initialized."""
    global _server_context
    return _server_context is not None


class ServerContextManager:
    """Context manager for temporarily setting server context.

    Useful for testing - automatically restores previous context on exit.

    Example:
        with ServerContextManager(test_context):
            # test code that uses the context
        # original context is restored
    """

    def __init__(self, ctx: ServerContext):
        self.ctx = ctx
        self.previous: ServerContext | None = None

    def __enter__(self) -> ServerContext:
        global _server_context
        self.previous = _server_context
        _server_context = self.ctx
        return self.ctx

    def __exit__(self, *args) -> None:
        global _server_context
        _server_context = self.previous
