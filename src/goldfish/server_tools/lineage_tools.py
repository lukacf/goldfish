"""Goldfish MCP tools - Lineage Tools

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
