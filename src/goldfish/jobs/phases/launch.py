"""Launch container phase for stage execution."""

from __future__ import annotations

from typing import Any, Protocol

from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import StageDef
from goldfish.svs.contract import resolve_config_params


class _LaunchDeps(Protocol):
    def _launch_container(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        image_tag: str,
        inputs: dict[str, str],
        input_configs: dict[str, dict[str, Any]] | None = None,
        output_configs: dict[str, dict[str, Any]] | None = None,
        user_config: dict[str, Any] | None = None,
        git_sha: str | None = None,
        run_reason: dict[str, Any] | None = None,
        runtime: str = "python",
        entrypoint: str | None = None,
        config_override: dict[str, Any] | None = None,
        inputs_override: dict[str, Any] | None = None,
        pipeline_name: str | None = None,
        results_spec: dict[str, Any] | None = None,
    ) -> Any: ...


def launch_container(
    deps: _LaunchDeps,
    ctx: StageRunContext,
    stage: StageDef,
    *,
    image_tag: str,
    inputs: dict[str, str],
    git_sha: str | None,
    run_reason: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
    inputs_override: dict[str, Any] | None,
    pipeline_name: str | None,
    results_spec: dict[str, Any] | None,
) -> None:
    """Launch the stage container via the executor's run backend."""
    _ = ctx.settings
    input_configs: dict[str, dict[str, Any]] = {}
    for input_name, input_def in stage.inputs.items():
        input_configs[input_name] = {
            "location": inputs.get(input_name, ""),
            "format": input_def.format or input_def.type,
            "type": input_def.type,
            "schema": resolve_config_params(input_def.output_schema, ctx.stage_config)
            if input_def.output_schema is not None
            else None,
        }

    output_configs: dict[str, dict[str, Any]] = {}
    for output_name, output_def in stage.outputs.items():
        output_configs[output_name] = {
            "format": output_def.format or output_def.type,
            "type": output_def.type,
            "schema": resolve_config_params(output_def.output_schema, ctx.stage_config)
            if output_def.output_schema is not None
            else None,
        }

    deps._launch_container(
        stage_run_id=ctx.stage_run_id,
        workspace=ctx.workspace_name,
        stage_name=ctx.stage_name,
        image_tag=image_tag,
        inputs=inputs,
        input_configs=input_configs,
        output_configs=output_configs,
        user_config=ctx.stage_config,
        git_sha=git_sha,
        run_reason=run_reason,
        runtime=stage.runtime,
        entrypoint=stage.entrypoint,
        config_override=config_override,
        inputs_override=inputs_override,
        pipeline_name=pipeline_name,
        results_spec=results_spec,
    )
