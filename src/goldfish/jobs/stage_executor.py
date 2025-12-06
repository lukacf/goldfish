"""Stage execution engine for Goldfish."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.datasets.registry import DatasetRegistry
from goldfish.errors import GoldfishError
from goldfish.models import StageDef, StageRunInfo
from goldfish.pipeline.manager import PipelineManager
from goldfish.workspace.manager import WorkspaceManager


class StageExecutor:
    """Execute individual pipeline stages."""

    def __init__(
        self,
        db: Database,
        config: GoldfishConfig,
        workspace_manager: WorkspaceManager,
        pipeline_manager: PipelineManager,
        dataset_registry: DatasetRegistry,
    ):
        self.db = db
        self.config = config
        self.workspace_manager = workspace_manager
        self.pipeline_manager = pipeline_manager
        self.dataset_registry = dataset_registry

    def run_stage(
        self,
        workspace: str,
        stage_name: str,
        config_override: Optional[dict] = None,
        inputs_override: Optional[dict] = None,
        reason: Optional[str] = None,
    ) -> StageRunInfo:
        """Run a single pipeline stage.

        Args:
            workspace: Workspace name
            stage_name: Stage to run
            config_override: Override env vars from config
            inputs_override: Override input sources (for debugging)
            reason: Why this stage is being run

        Flow:
            1. Auto-version workspace (git tag)
            2. Load pipeline and stage definition
            3. Resolve input sources
            4. Build Docker image
            5. Generate entrypoint
            6. Launch container
            7. Monitor and track
        """
        # 1. Auto-version workspace
        version = self._auto_version(workspace, stage_name, reason)

        # 2. Load pipeline and stage
        pipeline = self.pipeline_manager.get_pipeline(workspace)
        stage = self._find_stage(pipeline, stage_name)

        # 3. Resolve inputs
        inputs = self._resolve_inputs(workspace, stage, inputs_override)

        # 4. Generate stage run ID
        stage_run_id = f"stage-{uuid4().hex[:8]}"

        # 5. Create stage run record
        self._create_stage_run_record(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=version,
            stage_name=stage_name,
            inputs=inputs,
            config_override=config_override,
            reason=reason,
        )

        # 6. Build Docker image (mocked for now - Phase 6)
        image_tag = self._build_docker_image(workspace, version)

        # 7. Launch container (mocked for now - Phase 6)
        self._launch_container(stage_run_id, workspace, stage_name, image_tag, inputs)

        return StageRunInfo(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=version,
            stage=stage_name,
            status="running",
            started_at=datetime.now(timezone.utc),
        )

    def _resolve_inputs(
        self,
        workspace: str,
        stage: StageDef,
        inputs_override: Optional[dict] = None,
    ) -> dict[str, str]:
        """Resolve input sources (dataset, signal, or override).

        Returns dict: {input_name: source_location}
        """
        inputs = {}

        for input_name, input_def in stage.inputs.items():
            # Check for override
            if inputs_override and input_name in inputs_override:
                inputs[input_name] = inputs_override[input_name]
                continue

            # Resolve based on type
            if input_def.type == "dataset":
                # Get dataset from registry
                dataset = self.dataset_registry.get_dataset(input_def.dataset)
                inputs[input_name] = dataset.gcs_location

            elif input_def.from_stage:
                # Find output from previous stage
                # Get most recent successful run of source stage
                stage_runs = self.db.list_stage_runs(
                    workspace_name=workspace, stage_name=input_def.from_stage
                )

                # Find completed run with the signal
                source_run_id = None
                for run in stage_runs:
                    if run["status"] == "completed":
                        source_run_id = run["id"]
                        break

                if not source_run_id:
                    raise GoldfishError(
                        f"No successful run found for stage '{input_def.from_stage}'"
                    )

                # Get signal from that run
                signals = self.db.list_signals(stage_run_id=source_run_id)
                signal_name = input_def.name  # Signal name from input definition

                signal = None
                for s in signals:
                    if s["signal_name"] == signal_name:
                        signal = s
                        break

                if not signal:
                    raise GoldfishError(
                        f"Signal '{signal_name}' not found in stage run {source_run_id}"
                    )

                inputs[input_name] = signal["storage_location"]

            else:
                raise GoldfishError(f"Cannot resolve input: {input_name}")

        return inputs

    def _auto_version(
        self, workspace: str, stage_name: str, reason: Optional[str]
    ) -> str:
        """Create automatic version for workspace.

        Returns version string (e.g., "v1", "v2")
        """
        # Ensure workspace lineage exists
        if not self.db.workspace_exists(workspace):
            self.db.create_workspace_lineage(
                workspace_name=workspace, description=f"Auto-created for {stage_name}"
            )

        # Get current git SHA
        git_sha = self.workspace_manager.git_layer.get_current_sha(workspace)

        # Get next version number
        next_version = self.db.get_next_version_number(workspace)

        # Create git tag
        git_tag = f"{workspace}-{next_version}"
        self.workspace_manager.git_layer.create_tag(workspace, git_tag, git_sha)

        # Create version record
        description = reason or f"Auto-version for {stage_name} run"
        self.db.create_version(
            workspace_name=workspace,
            version=next_version,
            git_tag=git_tag,
            git_sha=git_sha,
            created_by="run",
            description=description,
        )

        return next_version

    def _find_stage(self, pipeline, stage_name: str) -> StageDef:
        """Find stage definition in pipeline."""
        for stage in pipeline.stages:
            if stage.name == stage_name:
                return stage
        raise GoldfishError(f"Stage '{stage_name}' not found in pipeline")

    def _create_stage_run_record(
        self,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_name: str,
        inputs: dict,
        config_override: Optional[dict],
        reason: Optional[str],
    ):
        """Create stage run record in database."""
        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=version,
            stage_name=stage_name,
            config_override=config_override,
        )

        # Record input signals
        for input_name, storage_location in inputs.items():
            # Note: We don't have the source stage_run_id here for signals,
            # so we just record the location. Proper lineage tracking will
            # be added in Phase 5.
            pass

    def _build_docker_image(self, workspace: str, version: str) -> str:
        """Build Docker image for this run.

        Returns image tag.

        Note: Actual implementation in Phase 6.
        """
        image_tag = f"goldfish-{workspace}-{version}"
        # TODO: Implement Docker image building in Phase 6
        return image_tag

    def _launch_container(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        image_tag: str,
        inputs: dict,
    ):
        """Launch Docker container (local) or GCE instance.

        Note: Actual implementation in Phase 6.
        """
        # TODO: Implement container launching in Phase 6
        backend = self.config.jobs.backend

        if backend == "local":
            # Will launch local Docker container
            pass
        elif backend == "gce":
            # Will launch GCE instance
            pass
        else:
            raise ValueError(f"Unknown backend: {backend}")
