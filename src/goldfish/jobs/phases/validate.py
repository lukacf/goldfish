"""Validate phase for stage execution."""

from __future__ import annotations

from typing import Any, Protocol

from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import PipelineDef, StageDef, StageRunInfo


class _ValidatorDeps(Protocol):
    pipeline_manager: Any

    def _find_stage(self, pipeline: PipelineDef, stage_name: str) -> StageDef: ...

    def _load_stage_config(self, workspace: str, stage_name: str) -> dict[str, Any] | None: ...


def validate_stage(
    deps: _ValidatorDeps,
    ctx: StageRunContext,
    *,
    pipeline_name: str | None,
    config_override: dict[str, Any] | None,
) -> StageDef:
    """Load pipeline + stage config and write them into ctx."""
    _ = ctx.settings
    pipeline = deps.pipeline_manager.get_pipeline(ctx.workspace_name, pipeline_name)
    stage = deps._find_stage(pipeline, ctx.stage_name)
    ctx.pipeline = pipeline

    stage_config = deps._load_stage_config(ctx.workspace_name, ctx.stage_name) or {}
    if config_override:
        stage_config.update(config_override)
    ctx.stage_config = stage_config

    return stage


class _PreflightDeps(Protocol):
    config: Any
    db: Any
    workspace_manager: Any

    def validate_pipeline_run(self, **kwargs: Any) -> dict[str, Any]: ...

    def _create_preflight_blocked_stage_run(
        self,
        *,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_name: str,
        errors: list[str],
        warnings: list[str],
        reason_structured: dict[str, Any] | None,
        pipeline_run_id: str | None,
        pipeline_name: str | None,
    ) -> StageRunInfo: ...


def svs_preflight(
    deps: _PreflightDeps,
    ctx: StageRunContext,
    *,
    pipeline_name: str | None,
    inputs_override: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
    reason_structured: dict[str, Any] | None,
    pipeline_run_id: str | None,
) -> tuple[list[str], list[str], StageRunInfo | None]:
    """Run SVS preflight validation and return (errors, warnings, blocked_info)."""
    _ = ctx.settings
    preflight_errors: list[str] = []
    preflight_warnings: list[str] = []

    if getattr(getattr(deps.config, "svs", None), "enabled", False):
        workspace_path = deps.workspace_manager.get_workspace_path(ctx.workspace_name)
        preflight = deps.validate_pipeline_run(
            workspace_name=ctx.workspace_name,
            workspace_path=workspace_path,
            db=deps.db,
            stages=[ctx.stage_name],
            pipeline_name=pipeline_name,
            inputs_override=inputs_override or {},
            config=deps.config,
            config_override=config_override,
        )
        preflight_errors = list(preflight.get("validation_errors", []))
        preflight_warnings = list(preflight.get("warnings", []))

        if preflight_errors:
            blocked = deps._create_preflight_blocked_stage_run(
                stage_run_id=ctx.stage_run_id,
                workspace=ctx.workspace_name,
                version=ctx.version,
                stage_name=ctx.stage_name,
                errors=preflight_errors,
                warnings=preflight_warnings,
                reason_structured=reason_structured,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
            )
            return preflight_errors, preflight_warnings, blocked

    return preflight_errors, preflight_warnings, None
