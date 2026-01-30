"""Pre-run review phase for stage execution."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from goldfish.jobs.phases.context import StageRunContext
from goldfish.models import PipelineDef, ReviewSeverity, StageRunInfo
from goldfish.state_machine import EventContext as SMEventContext
from goldfish.state_machine import StageEvent, StageState
from goldfish.state_machine import transition as sm_transition


class _ReviewDeps(Protocol):
    config: Any

    def _perform_pre_run_review(
        self,
        *,
        workspace: str,
        stage_name: str,
        pipeline: PipelineDef,
        reason_structured: dict[str, Any] | None,
        git_sha: str,
        input_context: list[dict[str, Any]],
        config_override: dict[str, Any] | None,
    ) -> Any: ...

    def _record_pre_run_review(self, stage_run_id: str, review: Any) -> None: ...


def pre_run_review(
    deps: _ReviewDeps,
    ctx: StageRunContext,
    *,
    pipeline: PipelineDef,
    reason_structured: dict[str, Any] | None,
    git_sha: str,
    input_context: list[dict[str, Any]],
    config_override: dict[str, Any] | None,
    skip_review: bool,
) -> Any | None:
    """Run pre-run review if enabled; return review, else None."""
    _ = ctx.settings
    enabled = bool(getattr(getattr(deps.config, "pre_run_review", None), "enabled", False))
    if skip_review or not enabled:
        return None

    review = deps._perform_pre_run_review(
        workspace=ctx.workspace_name,
        stage_name=ctx.stage_name,
        pipeline=pipeline,
        reason_structured=reason_structured,
        git_sha=git_sha,
        input_context=input_context,
        config_override=config_override,
    )
    if review:
        deps._record_pre_run_review(ctx.stage_run_id, review)
    return review


class _ReviewBlockDeps(Protocol):
    db: Any


def fail_if_review_blocking(
    deps: _ReviewBlockDeps,
    ctx: StageRunContext,
    *,
    review: Any | None,
    record_id: str,
    pipeline_run_id: str | None,
    pipeline_name: str | None,
) -> StageRunInfo | None:
    """If review blocks, mark run failed and return StageRunInfo; else None."""
    _ = ctx.settings
    if not review or not getattr(review, "has_blocking_issues", False):
        return None

    error_msg = f"Pre-run review blocked: {getattr(review, 'summary', '')}".rstrip()
    if getattr(review, "error_count", 0) > 0:
        error_details: list[str] = []
        for issue in getattr(review, "issues", []):
            if getattr(issue, "severity", None) == ReviewSeverity.ERROR:
                loc = (
                    f"{issue.file}:{issue.line}"
                    if getattr(issue, "file", None) and getattr(issue, "line", None)
                    else (getattr(issue, "file", "") or "")
                )
                msg = getattr(issue, "message", "")
                error_details.append(f"  - {loc}: {msg}" if loc else f"  - {msg}")
        if error_details:
            error_msg += "\n\nErrors:\n" + "\n".join(error_details[:5])

    now = datetime.now(UTC)
    sm_transition(
        deps.db,
        ctx.stage_run_id,
        StageEvent.SVS_BLOCK,
        SMEventContext(timestamp=now, source="executor", error_message=error_msg),
    )
    deps.db.update_stage_run_status(
        ctx.stage_run_id,
        completed_at=now.isoformat(),
        error=error_msg,
    )

    return StageRunInfo(
        stage_run_id=ctx.stage_run_id,
        pipeline_run_id=pipeline_run_id,
        record_id=record_id,
        workspace=ctx.workspace_name,
        pipeline=pipeline_name,
        version=ctx.version,
        stage=ctx.stage_name,
        status=StageState.FAILED,
        state=StageState.FAILED.value,
        started_at=now,
        completed_at=now,
        error=error_msg,
    )
