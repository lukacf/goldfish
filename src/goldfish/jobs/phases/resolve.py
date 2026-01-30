"""Resolve inputs phase for stage execution."""

from __future__ import annotations

from typing import Any, Protocol

from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import StageDef


class _ResolveDeps(Protocol):
    def _resolve_inputs(
        self,
        workspace: str,
        stage: StageDef,
        inputs_override: dict[str, Any] | None = None,
        pipeline_run_id: str | None = None,
    ) -> tuple[dict[str, str], dict[str, dict[str, Any]], list[dict[str, Any]]]: ...


def resolve_inputs(
    deps: _ResolveDeps,
    ctx: StageRunContext,
    stage: StageDef,
    *,
    inputs_override: dict[str, Any] | None,
    pipeline_run_id: str | None,
) -> tuple[dict[str, str], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Resolve stage inputs and return (inputs, sources, input_context)."""
    _ = ctx.settings
    return deps._resolve_inputs(ctx.workspace_name, stage, inputs_override, pipeline_run_id=pipeline_run_id)
