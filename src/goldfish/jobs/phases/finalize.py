"""Finalize outputs phase for stage execution."""

from __future__ import annotations

from collections.abc import Callable

from goldfish.jobs.phases.context import StageRunContext


def finalize_outputs(
    finalize_fn: Callable[..., None],
    ctx: StageRunContext,
    *,
    backend: str,
    status: str,
) -> None:
    """Finalize a terminal stage run (persist logs/outputs/lineage)."""
    _ = ctx.settings
    finalize_fn(ctx.stage_run_id, backend, status)
