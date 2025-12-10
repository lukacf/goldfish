"""Pipeline management for workspaces."""

from pathlib import Path
from typing import Optional

from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.models import PipelineDef
from goldfish.pipeline.parser import (
    PipelineNotFoundError,
    PipelineParser,
    PipelineValidationError,
)


class PipelineManager:
    """Manage pipeline definitions for workspaces."""

    def __init__(
        self,
        db: Database,
        workspace_manager,
        dataset_registry: Optional[object] = None,
    ):
        """Initialize pipeline manager.

        Args:
            db: Database instance
            workspace_manager: WorkspaceManager instance
            dataset_registry: Optional DatasetRegistry for validation
        """
        self.db = db
        self.workspace_manager = workspace_manager
        self.dataset_registry = dataset_registry
        self.parser = PipelineParser()

    def _pipeline_path(self, workspace: str, pipeline: Optional[str] = None) -> Path:
        workspace_path = self.workspace_manager.get_workspace_path(workspace)
        if pipeline:
            return workspace_path / "pipelines" / f"{pipeline}.yaml"
        return workspace_path / "pipeline.yaml"

    def get_pipeline(self, workspace: str, pipeline: Optional[str] = None) -> PipelineDef:
        """Load pipeline definition from workspace (supports named pipelines)."""
        pipeline_path = self._pipeline_path(workspace, pipeline)

        if not pipeline_path.exists():
            raise PipelineNotFoundError(
                f"No pipeline file found in workspace '{workspace}' at {pipeline_path}"
            )

        return self.parser.parse(pipeline_path)

    def validate_pipeline(self, workspace: str, pipeline: Optional[str] = None) -> list[str]:
        """Validate pipeline definition (default pipeline.yaml or pipelines/<name>.yaml)."""
        pipeline_def = self.get_pipeline(workspace, pipeline)
        workspace_path = self.workspace_manager.get_workspace_path(workspace)

        # Create dataset existence checker if registry available
        dataset_exists_fn = None
        if self.dataset_registry:
            dataset_exists_fn = lambda name: self.dataset_registry.dataset_exists(name)

        return self.parser.validate(pipeline_def, workspace_path, dataset_exists_fn)

    def update_pipeline(self, workspace: str, pipeline_yaml: str, pipeline: Optional[str] = None) -> PipelineDef:
        """Update pipeline.yaml in workspace.

        Validates before writing.

        Args:
            workspace: Workspace name
            pipeline_yaml: New pipeline YAML content

        Returns:
            Updated PipelineDef

        Raises:
            PipelineValidationError: If pipeline is invalid
            GoldfishError: If workspace not found or write fails
        """
        workspace_path = self.workspace_manager.get_workspace_path(workspace)
        pipeline_path = self._pipeline_path(workspace, pipeline)

        # Write to temp file and validate
        temp_path = pipeline_path.with_suffix(".yaml.tmp")

        try:
            temp_path.write_text(pipeline_yaml)

            # Parse to validate YAML syntax
            pipeline_obj = self.parser.parse(temp_path)

            # Validate structure
            dataset_exists_fn = None
            if self.dataset_registry:
                dataset_exists_fn = lambda name: self.dataset_registry.dataset_exists(name)

            errors = self.parser.validate(pipeline_obj, workspace_path, dataset_exists_fn)
            if errors:
                raise PipelineValidationError(
                    f"Pipeline validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
                )

            # Valid - move to actual location
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.rename(pipeline_path)

            return pipeline_obj

        except Exception:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            raise

    def pipeline_exists(self, workspace: str, pipeline: Optional[str] = None) -> bool:
        """Check if workspace has a pipeline file (default or named)."""
        try:
            pipeline_path = self._pipeline_path(workspace, pipeline)
            return pipeline_path.exists()
        except Exception:
            return False

    def create_default_pipeline(
        self,
        workspace: str,
        name: str,
        description: str = "",
    ) -> PipelineDef:
        """Create a minimal default pipeline.yaml for a new workspace.

        Args:
            workspace: Workspace name
            name: Pipeline name
            description: Pipeline description

        Returns:
            Created PipelineDef

        Raises:
            GoldfishError: If workspace not found or write fails
        """
        # Create minimal pipeline with no stages
        pipeline = PipelineDef(
            name=name,
            description=description,
            stages=[],
        )

        # Serialize and write
        pipeline_yaml = self.parser.serialize(pipeline)
        workspace_path = self.workspace_manager.get_workspace_path(workspace)
        pipeline_path = workspace_path / "pipeline.yaml"

        pipeline_path.write_text(pipeline_yaml)

        return pipeline
