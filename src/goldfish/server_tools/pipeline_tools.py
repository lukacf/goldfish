"""Goldfish MCP tools - Pipeline Tools

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
from goldfish.errors import (
    GoldfishError,
    validate_reason,
    WorkspaceNotFoundError,
)
from goldfish.pipeline.parser import (
    PipelineNotFoundError,
    PipelineValidationError,
)


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
