"""Monitor status phase for stage execution."""

from __future__ import annotations

from collections.abc import Callable

from goldfish.jobs.phases.context import StageRunContext


def monitor_status(
    wait_fn: Callable[..., str],
    ctx: StageRunContext,
    *,
    poll_interval: int,
    timeout: int,
) -> str:
    """Poll the backend until the stage reaches a terminal state."""
    _ = ctx.settings
    return wait_fn(ctx.stage_run_id, poll_interval=poll_interval, timeout=timeout)
