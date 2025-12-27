"""Goldfish MCP tools - Lineage Tools

Extracted from server.py for better organization.
"""

import logging

from goldfish.lineage.manager import LineageManager
from goldfish.server_core import (
    _get_db,
    _get_workspace_manager,
    mcp,
)
from goldfish.validation import (
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")


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
def get_stage_lineage(run_id: str, max_depth: int = 10) -> dict:
    """Get full upstream lineage tree for a stage run.

    Shows which version of each stage produced the inputs,
    recursively back to source datasets. This answers:
    "which preprocessing version is my model using?"

    Args:
        run_id: Stage run ID (e.g., "stage-abc123")
        max_depth: Maximum recursion depth (default 10)

    Returns:
        Nested lineage tree, e.g.:
        {
            "run_id": "stage-abc123",
            "stage": "training",
            "stage_version_num": 12,
            "git_sha": "a7b2c3d",
            "config_hash": "e4f5a6b...",
            "inputs": {
                "features": {
                    "source_type": "stage",
                    "source_stage": "tokenization",
                    "source_stage_version_num": 4,
                    "source_stage_run_id": "stage-def456",
                    "upstream": { ... recursive ... }
                }
            }
        }
    """
    db = _get_db()
    lineage = db.get_lineage_tree(run_id, max_depth=max_depth)

    if lineage is None:
        return {"success": False, "error": f"Stage run '{run_id}' not found"}

    return {"success": True, "lineage": lineage}


@mcp.tool()
def list_stage_versions(workspace: str, stage: str | None = None) -> dict:
    """List all versions of stages in a workspace.

    Stage versions track unique (code + config) combinations,
    independent of workspace versions. For example:
    - preprocessing-v5 (git_sha: abc123, config_hash: def456)
    - tokenization-v11 (git_sha: abc123, config_hash: 789012)

    Args:
        workspace: Workspace name
        stage: Optional stage name to filter (e.g., "preprocessing")

    Returns:
        {
            "workspace": "my-experiment",
            "versions": [
                {
                    "stage": "preprocessing",
                    "version_num": 5,
                    "git_sha": "abc123",
                    "config_hash": "def456...",
                    "created_at": "2024-12-10T15:30:00"
                },
                ...
            ]
        }
    """
    db = _get_db()
    validate_workspace_name(workspace)

    versions = db.list_stage_versions(workspace, stage=stage)

    return {
        "success": True,
        "workspace": workspace,
        "stage_filter": stage,
        "versions": [
            {
                "stage": v["stage_name"],
                "version_num": v["version_num"],
                "git_sha": v["git_sha"],
                "config_hash": v["config_hash"],
                "created_at": v["created_at"],
            }
            for v in versions
        ],
    }


@mcp.tool()
def find_runs_using_stage_version(
    workspace: str,
    stage: str,
    version: int,
) -> dict:
    """Find all runs that used a specific stage version as input.

    Useful for impact analysis: "if I change preprocessing-v5,
    which downstream runs would be affected?"

    Args:
        workspace: Workspace name
        stage: Stage name (e.g., "preprocessing")
        version: Stage version number (e.g., 5)

    Returns:
        {
            "stage_version": {"stage": "preprocessing", "version": 5},
            "downstream_runs": [
                {"run_id": "stage-abc123", "stage": "tokenization", "status": "completed"},
                {"run_id": "stage-def456", "stage": "training", "status": "running"},
            ]
        }
    """
    db = _get_db()
    validate_workspace_name(workspace)

    # Get the stage version ID
    stage_version = db.get_stage_version(workspace, stage, version)
    if stage_version is None:
        return {
            "success": False,
            "error": f"Stage version {stage}-v{version} not found in workspace '{workspace}'",
        }

    # Find downstream runs
    downstream = db.get_downstream_runs(stage_version["id"])

    return {
        "success": True,
        "stage_version": {
            "stage": stage,
            "version": version,
            "git_sha": stage_version["git_sha"],
            "config_hash": stage_version["config_hash"],
        },
        "downstream_runs": [
            {
                "run_id": run["id"],
                "stage": run["stage_name"],
                "status": run["status"],
                "started_at": run["started_at"],
            }
            for run in downstream
        ],
    }
