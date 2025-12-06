"""Pipeline execution engine for Goldfish."""

from typing import Optional

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunInfo
from goldfish.pipeline.manager import PipelineManager


class PipelineExecutor:
    """Execute full or partial pipelines."""

    def __init__(
        self,
        stage_executor: StageExecutor,
        pipeline_manager: PipelineManager,
        db: Database,
    ):
        self.stage_executor = stage_executor
        self.pipeline_manager = pipeline_manager
        self.db = db

    def run_pipeline(
        self,
        workspace: str,
        config_override: Optional[dict] = None,
        reason: Optional[str] = None,
    ) -> list[StageRunInfo]:
        """Run full pipeline (all stages in sequence).

        Args:
            workspace: Workspace name
            config_override: Dict of {stage_name: {var: value}}
            reason: Why running this pipeline

        Returns:
            List of stage runs
        """
        pipeline = self.pipeline_manager.get_pipeline(workspace)

        runs = []
        for stage in pipeline.stages:
            # Get stage-specific config override
            stage_config = None
            if config_override and stage.name in config_override:
                stage_config = config_override[stage.name]

            stage_run = self.stage_executor.run_stage(
                workspace=workspace,
                stage_name=stage.name,
                config_override=stage_config,
                reason=reason,
            )
            runs.append(stage_run)

            # Wait for completion before starting next stage
            self._wait_for_completion(stage_run.stage_run_id)

        return runs

    def _wait_for_completion(self, stage_run_id: str) -> None:
        """Wait for stage run to complete.

        Args:
            stage_run_id: Stage run identifier

        Raises:
            Exception: If stage fails
        """
        status = self.stage_executor.wait_for_completion(stage_run_id)
        if status == "failed":
            raise Exception(f"Stage run {stage_run_id} failed")

    def run_partial_pipeline(
        self,
        workspace: str,
        from_stage: str,
        to_stage: str,
        config_override: Optional[dict] = None,
        reason: Optional[str] = None,
    ) -> list[StageRunInfo]:
        """Run stages from_stage through to_stage.

        Args:
            workspace: Workspace name
            from_stage: First stage to run
            to_stage: Last stage to run (inclusive)
            config_override: Dict of {stage_name: {var: value}}
            reason: Why running these stages

        Returns:
            List of stage runs
        """
        pipeline = self.pipeline_manager.get_pipeline(workspace)

        # Find stage indices
        from_idx = None
        to_idx = None
        for i, stage in enumerate(pipeline.stages):
            if stage.name == from_stage:
                from_idx = i
            if stage.name == to_stage:
                to_idx = i

        if from_idx is None or to_idx is None:
            raise ValueError("Stage not found in pipeline")

        if from_idx > to_idx:
            raise ValueError("from_stage must come before to_stage")

        # Run stages in sequence
        runs = []
        for stage in pipeline.stages[from_idx : to_idx + 1]:
            # Get stage-specific config override
            stage_config = None
            if config_override and stage.name in config_override:
                stage_config = config_override[stage.name]

            stage_run = self.stage_executor.run_stage(
                workspace=workspace,
                stage_name=stage.name,
                config_override=stage_config,
                reason=reason,
            )
            runs.append(stage_run)

            # Wait for completion before starting next stage
            self._wait_for_completion(stage_run.stage_run_id)

        return runs

    def _wait_for_completion(self, stage_run_id: str) -> None:
        """Wait for stage run to complete.

        Args:
            stage_run_id: Stage run identifier

        Raises:
            Exception: If stage fails
        """
        status = self.stage_executor.wait_for_completion(stage_run_id)
        if status == "failed":
            raise Exception(f"Stage run {stage_run_id} failed")
