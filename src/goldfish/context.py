"""Server context management for Goldfish.

Provides thread-safe context management using contextvars,
eliminating global state in server.py.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
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
    - Thread-safe via contextvars
    - Type-checked by static analyzers
    """

    project_root: Path
    config: GoldfishConfig
    db: Database
    state_manager: StateManager
    # These are imported here to avoid circular imports
    workspace_manager: WorkspaceManager  # type: ignore
    job_launcher: JobLauncher  # type: ignore
    job_tracker: JobTracker  # type: ignore
    pipeline_manager: PipelineManager  # type: ignore
    dataset_registry: DatasetRegistry  # type: ignore
    stage_executor: StageExecutor  # type: ignore
    pipeline_executor: PipelineExecutor  # type: ignore

    def get_state_md(self) -> str:
        """Regenerate and return STATE.md content."""
        jobs = self.db.get_active_jobs()
        return self.state_manager.regenerate(
            slots=self.workspace_manager.get_all_slots(),
            jobs=[dict(j) for j in jobs],  # Convert JobRow to dict
            source_count=len(self.db.list_sources()),
        )


# Context variable for thread-safe access
_server_context: ContextVar[ServerContext | None] = ContextVar("server_context", default=None)


def get_context() -> ServerContext:
    """Get the current server context.

    Raises:
        RuntimeError: If server is not initialized
    """
    ctx = _server_context.get()
    if ctx is None:
        raise RuntimeError("Server not initialized - call set_context() first")
    return ctx


def set_context(ctx: ServerContext | None) -> None:
    """Set the server context.

    Args:
        ctx: Server context to set, or None to clear
    """
    _server_context.set(ctx)


def has_context() -> bool:
    """Check if server context is initialized."""
    return _server_context.get() is not None


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
        self.token: Token[ServerContext | None] | None = None

    def __enter__(self) -> ServerContext:
        self.token = _server_context.set(self.ctx)
        return self.ctx

    def __exit__(self, *args) -> None:
        if self.token is not None:
            _server_context.reset(self.token)
