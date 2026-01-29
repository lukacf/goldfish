"""Stage execution orchestrator for Goldfish."""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from goldfish.config.logging import current_request_id, current_stage_run_id
from goldfish.config.settings import GoldfishSettings
from goldfish.jobs._stage_executor_impl import (
    StageExecutor as _StageExecutorImpl,
)
from goldfish.jobs._stage_executor_impl import (
    _extract_stage_run_id_from_path,
)
from goldfish.jobs.phases import build as phase_build
from goldfish.jobs.phases import finalize as phase_finalize
from goldfish.jobs.phases import launch as phase_launch
from goldfish.jobs.phases import monitor as phase_monitor
from goldfish.jobs.phases import resolve as phase_resolve
from goldfish.jobs.phases import review as phase_review
from goldfish.jobs.phases import sync as phase_sync
from goldfish.jobs.phases import validate as phase_validate
from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import PipelineDef, StageRunInfo
from goldfish.pipeline.validator import validate_pipeline_run
from goldfish.state_machine import EventContext as SMEventContext
from goldfish.state_machine import StageEvent, StageState
from goldfish.state_machine import transition as sm_transition
from goldfish.utils import parse_optional_datetime
from goldfish.utils.config_hash import compute_config_hash

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from goldfish.cloud.protocols import ImageBuilder, ObjectStorage, RunBackend, SignalBus
    from goldfish.config import GoldfishConfig
    from goldfish.datasets.registry import DatasetRegistry
    from goldfish.db.database import Database
    from goldfish.db.protocols import AuditStore, MetricsStore, StageRunStore, WorkspaceStore
    from goldfish.pipeline.manager import PipelineManager
    from goldfish.workspace.manager import WorkspaceManager


class StageExecutor(_StageExecutorImpl):
    """Execute individual pipeline stages (phase-aware orchestrator)."""

    settings: GoldfishSettings
    _initialized: bool = False

    def __init__(
        self,
        db: Database | None = None,
        config: GoldfishConfig | None = None,
        workspace_manager: WorkspaceManager | None = None,
        pipeline_manager: PipelineManager | None = None,
        project_root: Path | None = None,
        dataset_registry: DatasetRegistry | None = None,
        *,
        settings: GoldfishSettings | None = None,
        storage: ObjectStorage | None = None,
        run_backend: RunBackend | None = None,
        signal_bus: SignalBus | None = None,
        image_builder: ImageBuilder | None = None,
        workspace_store: WorkspaceStore | None = None,
        stage_run_store: StageRunStore | None = None,
        metrics_store: MetricsStore | None = None,
        audit_store: AuditStore | None = None,
    ) -> None:
        """Create a StageExecutor (supports settings-only injection)."""
        if (
            db is None
            and config is None
            and workspace_manager is None
            and pipeline_manager is None
            and project_root is None
            and dataset_registry is None
            and storage is None
            and run_backend is None
            and signal_bus is None
            and image_builder is None
        ):
            if settings is None:
                raise TypeError("StageExecutor requires either (db, config, ...) or settings")
            self.settings = settings
            self._initialized = False
            return

        if (
            db is None
            or config is None
            or workspace_manager is None
            or pipeline_manager is None
            or project_root is None
        ):
            raise TypeError("StageExecutor requires db, config, workspace_manager, pipeline_manager, project_root")

        super().__init__(
            db=db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=project_root,
            dataset_registry=dataset_registry,
            storage=storage,
            run_backend=run_backend,
            signal_bus=signal_bus,
            image_builder=image_builder,
        )
        self.settings = settings or self._derive_settings()
        self._initialized = True

    def _derive_settings(self) -> GoldfishSettings:
        dev_repo_path = Path(self.dev_repo)
        workspaces_path = self.project_root / self.config.workspaces_dir
        backend: Literal["local", "gce"] = "gce" if self.run_backend.capabilities.timeout_becomes_pending else "local"
        return GoldfishSettings(
            project_name=self.config.project_name,
            dev_repo_path=dev_repo_path,
            workspaces_path=workspaces_path,
            backend=backend,
            db_path=dev_repo_path / self.config.db_path,
            db_backend="sqlite",
            log_format="console",
            log_level="INFO",
            stage_timeout=int(os.getenv("GOLDFISH_STAGE_TIMEOUT", "3600")),
            gce_launch_timeout=int(os.getenv("GOLDFISH_GCE_LAUNCH_TIMEOUT", "1200")),
        )

    def _placeholder_context(self, stage_run_id: str) -> StageRunContext:
        return StageRunContext(
            stage_run_id=stage_run_id,
            workspace_name="",
            stage_name="",
            version="",
            pipeline=PipelineDef(name="placeholder", stages=[]),
            stage_config={},
            run_backend=self.run_backend,
            storage=self.storage,
            settings=self.settings,
        )

    def validate_pipeline_run(self, **kwargs: Any) -> dict:
        """Indirection seam for patching pipeline validation in tests."""
        return validate_pipeline_run(**kwargs)

    def run_stage(
        self,
        workspace: str,
        stage_name: str,
        pipeline_name: str | None = None,
        pipeline_run_id: str | None = None,
        config_override: dict | None = None,
        inputs_override: dict | None = None,
        reason: str | None = None,
        reason_structured: dict | None = None,
        wait: bool = False,
        stage_run_id: str | None = None,
        skip_review: bool = False,
        experiment_group: str | None = None,
        results_spec: dict | None = None,
    ) -> StageRunInfo:
        """Run a single pipeline stage (phase-orchestrated)."""
        if not self._initialized:
            raise RuntimeError("StageExecutor(settings=...) instance is not initialized for stage execution")

        # 0. Establish stage_run_id early for preflight tracking
        if stage_run_id is None:
            stage_run_id = f"stage-{uuid4().hex[:8]}"

        stage_run_id_token = current_stage_run_id.set(stage_run_id)
        request_id_token = current_request_id.set(pipeline_run_id) if pipeline_run_id is not None else None
        try:
            ctx = StageRunContext(
                stage_run_id=stage_run_id,
                workspace_name=workspace,
                stage_name=stage_name,
                version="",
                pipeline=PipelineDef(name=pipeline_name or "pipeline", stages=[]),
                stage_config={},
                run_backend=self.run_backend,
                storage=self.storage,
                settings=self.settings,
            )

            # 1. Sync + auto-version workspace (sets ctx.version)
            git_sha = phase_sync.sync_workspace(self, ctx, reason=reason)

            # 2. Load pipeline/stage + config (sets ctx.pipeline, phase_validate sets ctx.stage_config)
            stage = phase_validate.validate_stage(
                self, ctx, pipeline_name=pipeline_name, config_override=config_override
            )

            preflight_errors, preflight_warnings, blocked = phase_validate.svs_preflight(
                self,
                ctx,
                pipeline_name=pipeline_name,
                inputs_override=inputs_override,
                config_override=config_override,
                reason_structured=reason_structured,
                pipeline_run_id=pipeline_run_id,
            )
            if blocked is not None:
                return blocked

            # 2d. Compute stage version
            config_hash = compute_config_hash(ctx.stage_config)
            stage_version_id, stage_version_num, _ = self.db.get_or_create_stage_version(
                workspace=workspace,
                stage=stage_name,
                git_sha=git_sha,
                config_hash=config_hash,
            )

            # 2e. Create placeholder record IMMEDIATELY (to satisfy FKs for review/audit)
            record_id = self._create_stage_run_record(
                stage_run_id=stage_run_id,
                workspace=workspace,
                version=ctx.version,
                stage_name=stage_name,
                stage_version_id=stage_version_id,
                inputs={},  # Resolved later
                input_sources={},
                config_override=config_override,
                reason=reason,
                reason_structured=reason_structured,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
                profile=None,
                hints=None,
                config=ctx.stage_config,
                preflight_errors=preflight_errors,
                preflight_warnings=preflight_warnings,
                experiment_group=experiment_group,
                results_spec=results_spec,
            )

            # 3. Resolve inputs
            inputs, input_sources, input_context = phase_resolve.resolve_inputs(
                self,
                ctx,
                stage,
                inputs_override=inputs_override,
                pipeline_run_id=pipeline_run_id,
            )

            # 4. Pre-run review
            review = phase_review.pre_run_review(
                self,
                ctx,
                pipeline=ctx.pipeline,
                reason_structured=reason_structured,
                git_sha=git_sha,
                input_context=input_context,
                config_override=config_override,
                skip_review=skip_review,
            )
            blocked_info = phase_review.fail_if_review_blocking(
                self,
                ctx,
                review=review,
                record_id=record_id,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
            )
            if blocked_info is not None:
                return blocked_info

            # 5. Update record with resolved values
            self._update_queued_stage_run(
                stage_run_id=stage_run_id,
                workspace=workspace,
                version=ctx.version,
                stage_version_id=stage_version_id,
                inputs=inputs,
                input_sources=input_sources,
                config=ctx.stage_config,
                profile=ctx.stage_config.get("compute", {}).get("profile"),
                hints=ctx.stage_config.get("hints"),
                preflight_warnings=preflight_warnings,
                preflight_errors=preflight_errors,
                create_experiment_record=False,  # Already created in step 2e
                experiment_group=experiment_group,
            )

            try:
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.BUILD_START,
                    SMEventContext(timestamp=datetime.now(UTC), source="executor"),
                )

                profile_name = ctx.stage_config.get("compute", {}).get("profile")
                image_tag = phase_build.build_image(self, ctx, profile_name=profile_name)

                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.BUILD_OK,
                    SMEventContext(timestamp=datetime.now(UTC), source="executor"),
                )

                phase_launch.launch_container(
                    self,
                    ctx,
                    stage,
                    image_tag=image_tag,
                    inputs=inputs,
                    git_sha=git_sha,
                    run_reason=reason_structured,
                    config_override=config_override,
                    inputs_override=inputs_override,
                    pipeline_name=pipeline_name,
                    results_spec=results_spec,
                )
            except Exception as e:
                error_msg = str(e)
                ctx_sm = SMEventContext(timestamp=datetime.now(UTC), source="executor", error_message=error_msg)
                result = sm_transition(self.db, stage_run_id, StageEvent.BUILD_FAIL, ctx_sm)
                if not result.success:
                    sm_transition(self.db, stage_run_id, StageEvent.LAUNCH_FAIL, ctx_sm)

                self.db.update_stage_run_status(
                    stage_run_id=stage_run_id,
                    completed_at=datetime.now(UTC).isoformat(),
                    error=error_msg,
                )
                raise

            info = StageRunInfo(
                stage_run_id=stage_run_id,
                pipeline_run_id=pipeline_run_id,
                record_id=record_id,
                workspace=workspace,
                pipeline=pipeline_name,
                version=ctx.version,
                stage=stage_name,
                stage_version=stage_version_id,
                stage_version_num=stage_version_num,
                status=StageState.RUNNING,
                started_at=datetime.now(UTC),
                log_uri=str(self.dev_repo / ".goldfish" / "runs" / stage_run_id / "logs" / "output.log"),
                state=StageState.RUNNING.value,
                profile=ctx.stage_config.get("compute", {}).get("profile") if "compute" in ctx.stage_config else None,
                hints=ctx.stage_config.get("hints"),
                config=ctx.stage_config,
                inputs=inputs,
            )

            if wait:
                self.wait_for_completion(stage_run_id)
                refreshed = self.db.get_stage_run(stage_run_id)
                if refreshed:
                    base_fields = info.model_dump(
                        exclude={"status", "completed_at", "log_uri", "artifact_uri", "state", "outputs", "error"}
                    )
                    return StageRunInfo(
                        **base_fields,
                        status=refreshed.get("status", info.status),
                        completed_at=parse_optional_datetime(refreshed.get("completed_at")),
                        log_uri=refreshed.get("log_uri"),
                        artifact_uri=refreshed.get("artifact_uri"),
                        state=refreshed.get("state"),
                        outputs=json.loads(refreshed["outputs_json"]) if refreshed.get("outputs_json") else None,
                        error=refreshed.get("error"),
                    )

            return info
        finally:
            current_stage_run_id.reset(stage_run_id_token)
            if request_id_token is not None:
                current_request_id.reset(request_id_token)

    def wait_for_completion(self, stage_run_id: str, poll_interval: int = 5, timeout: int = 3600) -> str:
        ctx = self._placeholder_context(stage_run_id)
        return phase_monitor.monitor_status(
            super().wait_for_completion,
            ctx,
            poll_interval=poll_interval,
            timeout=timeout,
        )

    def _finalize_stage_run(self, stage_run_id: str, backend: str, status: str) -> None:
        ctx = self._placeholder_context(stage_run_id)
        phase_finalize.finalize_outputs(
            super()._finalize_stage_run,
            ctx,
            backend=backend,
            status=status,
        )


__all__ = ["StageExecutor", "_extract_stage_run_id_from_path"]
