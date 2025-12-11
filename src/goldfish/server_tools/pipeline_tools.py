"""Goldfish MCP tools - Pipeline Tools

Extracted from server.py for better organization.

NOTE: These tools are DEPRECATED. Use get_workspace() instead, which now
includes pipeline information in the response.
"""

import logging
import warnings

from goldfish.errors import (
    GoldfishError,
    WorkspaceNotFoundError,
)
from goldfish.models import (
    PipelineResponse,
    UpdatePipelineResponse,
    ValidatePipelineResponse,
)
from goldfish.pipeline.parser import (
    PipelineNotFoundError,
    PipelineValidationError,
)
from goldfish.server import (
    _get_pipeline_manager,
    _get_workspace_manager,
    mcp,
)
from goldfish.validation import (
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def get_pipeline(workspace: str, pipeline: str | None = None) -> PipelineResponse:
    """[DEPRECATED] Get pipeline definition for a workspace.

    Use get_workspace() instead, which includes pipeline info.

    Returns the pipeline.yaml content as a structured object.

    Args:
        workspace: Workspace name

    Returns:
        Pipeline definition with stages, inputs, outputs
    """
    warnings.warn(
        "get_pipeline is deprecated, use get_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        pipeline_def = pipeline_manager.get_pipeline(workspace, pipeline)
        return PipelineResponse(workspace=workspace, pipeline=pipeline_def)
    except PipelineNotFoundError as e:
        raise GoldfishError(str(e)) from e
    except PipelineValidationError as e:
        raise GoldfishError(f"Pipeline is invalid: {e}") from e


@mcp.tool()
def validate_pipeline(workspace: str, pipeline: str | None = None) -> ValidatePipelineResponse:
    """[DEPRECATED] Validate pipeline definition for a workspace.

    Use get_workspace() instead, which validates and includes pipeline info.

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
    warnings.warn(
        "validate_pipeline is deprecated, use get_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        errors = pipeline_manager.validate_pipeline(workspace, pipeline)
        return ValidatePipelineResponse(
            workspace=workspace,
            valid=len(errors) == 0,
            errors=errors,
        )
    except PipelineNotFoundError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def update_pipeline(workspace: str, pipeline_yaml: str, pipeline: str | None = None) -> UpdatePipelineResponse:
    """[DEPRECATED] Update pipeline.yaml in workspace.

    Edit pipeline.yaml directly in the workspace instead.

    Validates the pipeline before writing. Will reject invalid pipelines.

    Args:
        workspace: Workspace name
        pipeline_yaml: Complete pipeline.yaml content (YAML string)

    Returns:
        Updated pipeline definition
    """
    warnings.warn(
        "update_pipeline is deprecated, edit pipeline.yaml directly",
        DeprecationWarning,
        stacklevel=2,
    )
    pipeline_manager = _get_pipeline_manager()
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    # Check workspace exists
    if not workspace_manager.git.branch_exists(workspace):
        raise WorkspaceNotFoundError(f"Workspace not found: {workspace}")

    try:
        pipeline_def = pipeline_manager.update_pipeline(workspace, pipeline_yaml, pipeline)
        return UpdatePipelineResponse(
            success=True,
            workspace=workspace,
            pipeline=pipeline_def,
        )
    except PipelineValidationError as e:
        raise GoldfishError(f"Pipeline validation failed: {e}") from e


@mcp.tool()
def list_pipelines(workspace: str) -> dict:
    """[DEPRECATED] List available pipeline files in a workspace.

    Use get_workspace() instead, which includes pipeline info.
    """
    warnings.warn(
        "list_pipelines is deprecated, use get_workspace() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    workspace_manager = _get_workspace_manager()

    validate_workspace_name(workspace)

    workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace
    workspace_path = workspace_manager.get_workspace_path(workspace_name)

    names = []
    default_path = workspace_path / "pipeline.yaml"
    if default_path.exists():
        names.append({"name": "pipeline", "path": str(default_path.relative_to(workspace_path))})

    pipelines_dir = workspace_path / "pipelines"
    if pipelines_dir.exists():
        for p in sorted(pipelines_dir.glob("*.yaml")):
            names.append({"name": p.stem, "path": str(p.relative_to(workspace_path))})

    return {"pipelines": names}


# ============== DATASET TOOLS ==============
