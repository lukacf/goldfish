"""Goldfish MCP Server Core - FastMCP instance and context accessors.

This module contains the FastMCP instance and context accessor functions.
It's separated from server.py to avoid circular imports with tool modules.
"""

from pathlib import Path

from fastmcp import FastMCP

from goldfish.config import GoldfishConfig
from goldfish.context import get_context, has_context
from goldfish.datasets.registry import DatasetRegistry
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.infra.metadata.base import MetadataBus
from goldfish.jobs.launcher import JobLauncher
from goldfish.jobs.tracker import JobTracker
from goldfish.pipeline.manager import PipelineManager
from goldfish.state.state_md import StateManager
from goldfish.workspace.manager import WorkspaceManager

# Initialize FastMCP server
mcp: FastMCP = FastMCP("goldfish")

# Module-level variable to store project root (set when server starts)
_project_root: Path | None = None


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


def _get_metadata_bus() -> MetadataBus:
    """Get metadata bus from context or raise GoldfishError."""
    if not has_context():
        raise GoldfishError("Server not initialized")
    return get_context().metadata_bus


def _get_state_md() -> str:
    """Get the current STATE.md content."""
    return get_context().get_state_md()
