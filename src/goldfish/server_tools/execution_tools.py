"""Goldfish MCP tools - Execution Tools

Provides tools for running pipeline stages and monitoring execution.
"""

import json
import logging
import threading
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from goldfish.errors import (
    GoldfishError,
    validate_reason,
)
from goldfish.jobs.conversion import stage_run_dict_to_info
from goldfish.models import (
    ArtifactInfo,
    CancelRunResponse,
    GetOutputsResponse,
    GetRunMetricsResponse,
    GetRunResponse,
    MetricInfo,
    MetricSummary,
    RunReason,
    StageRunInfo,
    StageRunStatus,
)
from goldfish.server import (
    _get_config,
    _get_db,
    _get_pipeline_executor,
    _get_stage_executor,  # Used for logs, cancel, refresh
    _get_workspace_manager,
    mcp,
)
from goldfish.utils import parse_datetime
from goldfish.validation import (
    validate_metric_name,
    validate_stage_run_id,
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")

# Maximum lines that can be requested from logs (prevents memory exhaustion)
_MAX_TAIL_LINES = 10000

# Cursor tracking for follow mode - maps run_id to (cursor_position, last_access_time)
# Used to return only new logs since the last call
# Cursors are cleaned up after 1 hour of inactivity to prevent memory leaks
_log_cursors: dict[str, tuple[int, float]] = {}
_CURSOR_TTL_SECONDS = 3600  # 1 hour
_MAX_CURSORS = 1000  # Maximum number of cursors to keep
_log_cursor_lock = threading.Lock()


def _cleanup_stale_cursors() -> None:
    """Remove cursors that haven't been accessed in over TTL seconds."""
    import time

    with _log_cursor_lock:
        now = time.time()
        stale_keys = [
            run_id for run_id, (_, last_access) in _log_cursors.items() if now - last_access > _CURSOR_TTL_SECONDS
        ]
        for key in stale_keys:
            del _log_cursors[key]

        # If still over max, remove oldest entries
        if len(_log_cursors) > _MAX_CURSORS:
            sorted_by_access = sorted(_log_cursors.items(), key=lambda x: x[1][1])
            for key, _ in sorted_by_access[: len(_log_cursors) - _MAX_CURSORS]:
                del _log_cursors[key]


def _stage_run_row_to_info(row: dict) -> StageRunInfo:
    return stage_run_dict_to_info(row)


@mcp.tool()
def run(
    workspace: str,
    stages: list[str] | None = None,
    pipeline: str | None = None,
    config_override: dict | None = None,
    inputs_override: dict | None = None,
    reason: str | dict | None = None,
    wait: bool = False,
    dry_run: bool = False,
    skip_review: bool = False,
) -> dict:
    """Run pipeline stages.

    Args:
        workspace: Workspace name (e.g., "baseline_lstm") or slot (e.g., "w1")
        stages: Which stages to run:
            - None or empty: Run ALL stages in pipeline order
            - ["train"]: Run single stage
            - ["preprocess", "train"]: Run multiple specific stages in order
        pipeline: Pipeline file (default: pipeline.yaml, or pipelines/<name>.yaml)
        config_override: Override config vars. For single stage: {"VAR": "value"}.
                        For multiple stages: {"stage_name": {"VAR": "value"}}
        inputs_override: Override input sources for debugging
        reason: Why running - can be:
            - String (min 15 chars)
            - Dict with structured fields:
                {
                    "description": "What you're running",
                    "hypothesis": "What you expect to happen",
                    "approach": "How you're testing it",
                    "min_result": "Minimum bar for success",
                    "goal": "Best case outcome"
                }
        wait: False (default) returns immediately; True blocks until completion
        dry_run: If True, validate everything without launching. Returns what would
                 run and any validation errors found.
        skip_review: If True, skip the pre-run Claude review. Use when you've already
                    addressed review feedback and want to proceed immediately.

    Returns:
        Dict with:
        - runs: List of stage run info (run_id, workspace, version, stage, status)
        - pipeline_run_id: If running multiple stages
        If dry_run=True:
        - valid: Whether pipeline would run successfully
        - stages_to_run: List of stages that would execute
        - validation_errors: List of any issues found
        - warnings: Non-fatal issues

    Examples:
        run("w1")                           # Run all stages
        run("w1", stages=["train"])         # Run single stage
        run("w1", stages=["preprocess", "train", "evaluate"])  # Run specific stages
        run("w1", reason={"description": "Test new architecture", "hypothesis": "Will improve accuracy"})
        run("w1", dry_run=True)             # Validate without launching
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()
    pipeline_executor = _get_pipeline_executor()

    validate_workspace_name(workspace)

    # Parse reason into RunReason object
    run_reason: RunReason | None = None
    reason_str: str = "Run stages"

    if reason:
        if isinstance(reason, dict):
            # Structured reason
            try:
                run_reason = RunReason(**reason)
                reason_str = run_reason.to_summary()
                # Validate minimum length of description
                validate_reason(run_reason.description, config.audit.min_reason_length)
            except ValidationError as e:
                # Extract user-friendly error from Pydantic
                errors = e.errors()
                if errors:
                    first = errors[0]
                    field = ".".join(str(loc) for loc in first.get("loc", []))
                    msg = first.get("msg", "validation error")
                    raise GoldfishError(
                        f"Invalid reason: {field} - {msg}. "
                        f"Required: description (str, max 500). "
                        f"Optional: hypothesis, approach (max 1000), min_result, goal (max 500)."
                    ) from e
                raise GoldfishError(f"Invalid structured reason: {e}") from e
            except (ValueError, TypeError) as e:
                raise GoldfishError(f"Invalid structured reason: {e}") from e
        elif isinstance(reason, str):
            # Simple string reason (backward compatibility)
            validate_reason(reason, config.audit.min_reason_length)
            reason_str = reason
            run_reason = RunReason(description=reason)
        else:
            raise GoldfishError("reason must be a string or dict")

    # Resolve workspace (could be slot like "w1")
    workspace_name = workspace_manager.get_workspace_for_slot(workspace)
    if not workspace_name:
        workspace_name = workspace

    # Dry run mode: validate without launching
    if dry_run:
        from goldfish.pipeline.validator import validate_pipeline_run

        workspace_path = workspace_manager.get_workspace_path(workspace_name)
        db = _get_db()
        return validate_pipeline_run(
            workspace_name=workspace_name,
            workspace_path=workspace_path,
            db=db,
            stages=stages,
            pipeline_name=pipeline,
            inputs_override=inputs_override or {},
        )

    # Execute through run_stages
    result: dict[str, Any] = pipeline_executor.run_stages(
        workspace=workspace_name,
        stages=stages if stages else None,
        pipeline_name=pipeline,
        config_override=config_override or {},
        inputs_override=inputs_override or {},
        reason=reason_str,
        reason_structured=run_reason,
        async_mode=not wait,
        skip_review=skip_review,
    )

    return result


@mcp.tool()
def get_run(run_id: str) -> dict:
    """Get full details of a run: metadata, inputs, outputs, config, full error.

    Args:
        run_id: The run ID (e.g., "stage-abc123")

    Returns:
        Dict with stage_run info, inputs, outputs, config, and complete error message
    """
    db = _get_db()
    # Refresh status from backend
    _get_stage_executor().refresh_status_once(run_id)

    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    # Use truncate_error=False to get full error message
    result: dict[str, Any] = GetRunResponse(
        stage_run=stage_run_dict_to_info(row, truncate_error=False),
        inputs=json.loads(row["inputs_json"]) if row.get("inputs_json") else {},
        outputs=json.loads(row["outputs_json"]) if row.get("outputs_json") else [],
        config=json.loads(row["config_json"]) if row.get("config_json") else {},
    ).model_dump(mode="json")
    return result


@mcp.tool()
def logs(run_id: str, tail: int = 200, since: str | None = None, follow: bool = False) -> dict:
    """Get logs from a run.

    Args:
        run_id: The run ID (e.g., "stage-abc123")
        tail: Number of lines from end (default 200, max 10000)
        since: Only show logs after this ISO timestamp
        follow: If True, return only NEW logs since the last call (cursor-based)

    Returns:
        Dict with run_id, status, logs, log_uri
        If follow=True, also includes cursor_position and has_new_content
    """
    if tail < 1 or tail > _MAX_TAIL_LINES:
        raise GoldfishError(f"tail must be 1-{_MAX_TAIL_LINES}")

    db = _get_db()
    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    log_uri = row.get("log_uri")
    log_content: str | None = None

    def _parse_since(s: str | None) -> datetime | None:
        if not s:
            return None
        try:
            iso = s.replace("Z", "+00:00")
            dt = parse_datetime(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except Exception:
            return None

    def _line_ts(line: str) -> datetime | None:
        token = (line.split() or [""])[0].strip()
        if not token:
            return None
        try:
            ts = parse_datetime(token.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            return ts
        except Exception:
            return None

    def _filter_lines(lines_iter: list[str], since_ts: datetime | None) -> list[str]:
        if since_ts is None:
            return list(lines_iter)
        filtered = []
        for ln in lines_iter:
            ts = _line_ts(ln)
            if ts is None:
                filtered.append(ln)
                continue
            if ts >= since_ts:
                filtered.append(ln)
        return filtered

    since_ts = _parse_since(since)

    # Try persisted log file first
    if log_uri and log_uri.startswith("/"):
        try:
            from collections import deque

            with open(log_uri) as f:
                if tail:
                    lines_raw = deque(f, maxlen=tail)
                    lines = list(lines_raw)
                else:
                    lines = list(f)
            lines = _filter_lines(lines, since_ts)
            log_content = "".join(lines)
        except FileNotFoundError:
            log_content = None

    # Fallback to backend live logs
    if log_content is None:
        backend = row.get("backend_type") or "local"
        handle = row.get("backend_handle") or run_id
        try:
            if backend == "local":
                log_content = _get_stage_executor().local_executor.get_container_logs(
                    handle, tail_lines=tail, since=since
                )
            elif backend == "gce":
                log_content = _get_stage_executor().gce_launcher.get_instance_logs(handle, tail_lines=tail, since=since)
                if log_content is None:
                    log_content = "[GCE logs unavailable - not yet synced]"
            else:
                log_content = "Logs not available"
        except Exception as e:
            log_content = f"[Error fetching logs: {e}]"

    # Handle follow mode - return only new content since last cursor position
    if follow:
        import time

        # Periodically clean up stale cursors to prevent memory leaks
        _cleanup_stale_cursors()

        status = row.get("status")
        terminal_states = {
            StageRunStatus.COMPLETED,
            StageRunStatus.FAILED,
            StageRunStatus.CANCELED,
        }

        # Get current cursor position (0 if first call)
        with _log_cursor_lock:
            cursor_entry = _log_cursors.get(run_id)
            prev_cursor = cursor_entry[0] if cursor_entry else 0

        # Calculate new content
        full_content = log_content or ""
        content_len = len(full_content)

        if prev_cursor == 0:
            # First call: return last `tail` lines
            lines = full_content.split("\n")
            # Keep last `tail` lines (accounting for trailing newline)
            if lines and lines[-1] == "":
                lines = lines[:-1]
            if len(lines) > tail:
                lines = lines[-tail:]
            new_content = "\n".join(lines)
            if new_content and full_content.endswith("\n"):
                new_content += "\n"
        else:
            # Subsequent call: return only content after cursor
            new_content = full_content[prev_cursor:] if prev_cursor < content_len else ""

        has_new_content = len(new_content) > 0

        # Update cursor to end of current content with current timestamp
        with _log_cursor_lock:
            _log_cursors[run_id] = (content_len, time.time())

            # Clean up cursor when run reaches terminal state
            if status in terminal_states:
                _log_cursors.pop(run_id, None)

        return {
            "run_id": run_id,
            "status": status,
            "logs": new_content,
            "log_uri": log_uri,
            "cursor_position": content_len,
            "has_new_content": has_new_content,
        }

    return {
        "run_id": run_id,
        "status": row.get("status"),
        "logs": log_content,
        "log_uri": log_uri,
    }


@mcp.tool()
def cancel(run_id: str, reason: str) -> dict:
    """Cancel a running stage.

    Args:
        run_id: The run ID to cancel
        reason: Why cancelling (min 15 chars)

    Returns:
        Dict with success status and previous_status
    """
    config = _get_config()
    validate_reason(reason, config.audit.min_reason_length)

    db = _get_db()
    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    backend = row.get("backend_type") or "local"
    handle = row.get("backend_handle") or run_id

    # Attempt state change atomically: only if still running
    # Clear progress field to avoid stale "canceled:running" display
    updated = 0
    with db._conn() as conn:
        updated = conn.execute(
            "UPDATE stage_runs SET status=?, progress=NULL, completed_at=?, error=? WHERE id=? AND status=?",
            (
                StageRunStatus.CANCELED,
                datetime.now(UTC).isoformat(),
                f"Canceled: {reason}",
                run_id,
                StageRunStatus.RUNNING,
            ),
        ).rowcount

    if updated == 0:
        fail_result: dict[str, Any] = CancelRunResponse(
            success=False,
            error="Run is not running (already completed/failed/canceled)",
            previous_status=row.get("status"),
        ).model_dump(mode="json")
        return fail_result

    # Best-effort backend cleanup
    # For GCE: use delete_instance() to fully terminate (stop just pauses)
    # For local: stop_container() is correct since containers auto-remove (--rm)
    try:
        if backend == "local":
            _get_stage_executor().local_executor.stop_container(handle)
        elif backend == "gce":
            _get_stage_executor().gce_launcher.delete_instance(handle)
    except Exception as e:
        # Log error but don't fail the cancel - DB state is already updated
        logger.warning(f"Failed to cleanup backend for {run_id} ({backend}:{handle}): {e}")

    success_result: dict[str, Any] = CancelRunResponse(success=True, previous_status=row.get("status")).model_dump(
        mode="json"
    )
    return success_result


@mcp.tool()
def list_runs(
    workspace: str | None = None,
    stage: str | None = None,
    status: str | None = None,
    pipeline_run_id: str | None = None,
    limit: int = 5,
    offset: int = 0,
) -> dict:
    """List runs (newest first) - compact view.

    Returns a compact summary of recent runs. Use get_run(run_id) for full details.
    When filtering by pipeline_run_id, also shows queued stages that haven't started yet.

    Args:
        workspace: Filter by workspace name
        stage: Filter by stage name
        status: Filter by status (pending, running, completed, failed, canceled, queued)
        pipeline_run_id: Filter by pipeline run
        limit: Max results (default 5)
        offset: Pagination offset

    Returns:
        Dict with compact runs list showing: run_id, stage, status, progress, started_at
    """
    db = _get_db()
    compact_runs = []

    # When filtering by pipeline_run_id, first show queued stages that haven't started
    queued_stages = []
    if pipeline_run_id and offset == 0:
        queued_stages = db.get_queued_stages_for_pipeline(pipeline_run_id)
        for q in queued_stages:
            compact_runs.append(
                {
                    "run_id": None,  # No run_id yet - still queued
                    "stage": q["stage_name"],
                    "status": "queued",
                    "started": None,
                    "error": None,
                }
            )

    rows = db.list_stage_runs_with_total(
        workspace_name=workspace,
        stage_name=stage,
        status=status,
        pipeline_run_id=pipeline_run_id,
        limit=limit,
        offset=offset,
    )
    total = rows[0]["total_count"] if rows else 0

    # Compact view: just essential fields, one line per run
    for r in rows:
        # Build compact status string
        status_str = r["status"]
        if r.get("progress"):
            status_str = f"{r['status']}:{r['progress']}"

        # Truncate error to ~50 chars for compact view
        error_snippet = None
        if r.get("error"):
            err = r["error"].split("\n")[0][:50]
            error_snippet = err + "..." if len(r["error"]) > 50 else err

        # Include attempt and outcome for context
        attempt_info = f"#{r['attempt_num']}" if r.get("attempt_num") else None
        outcome = r.get("outcome")  # success, bad_results, or None

        compact_runs.append(
            {
                "run_id": r.get("id") or r.get("stage_run_id"),
                "stage": r["stage_name"],
                "attempt": attempt_info,
                "status": status_str,
                "outcome": outcome,
                "started": r.get("started_at", "")[:19] if r.get("started_at") else None,  # Trim to datetime
                "error": error_snippet,
            }
        )

    # Adjust total to include queued stages
    total_with_queued = total + len(queued_stages)

    result: dict = {
        "runs": compact_runs,
        "total_count": total_with_queued,
        "has_more": offset + len(rows) < total,
        "hint": "Use get_run(run_id) for full details including logs and complete error messages",
    }

    # Add attempt summary when filtering by workspace (provides context without noise)
    if workspace and offset == 0:
        attempts = db.list_attempts(workspace, stage_name=stage, limit=10)
        if attempts:
            result["attempt_summary"] = attempts

    return result


@mcp.tool()
def get_outputs(run_id: str) -> dict:
    """Get outputs from a completed run.

    Args:
        run_id: The run ID

    Returns:
        Dict with run_id and outputs list
    """
    db = _get_db()
    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")
    outputs = json.loads(row["outputs_json"]) if row.get("outputs_json") else []
    result: dict[str, Any] = GetOutputsResponse(stage_run_id=run_id, outputs=outputs).model_dump(mode="json")
    return result


@mcp.tool()
def get_run_metrics(
    run_id: str,
    metric_name: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> dict:
    """Get metrics and artifacts from a stage run.

    Returns metrics logged during stage execution, summary statistics,
    and artifacts. Useful for analyzing run performance and results.
    Supports filtering and pagination for large metric sets.

    Args:
        run_id: The stage run ID (e.g., "stage-abc123")
        metric_name: Optional filter by metric name (e.g., "loss")
        limit: Optional limit on number of metrics returned (1-10000)
        offset: Optional offset for pagination (default 0)

    Returns:
        Dict with:
        - metrics: List of individual metric data points
        - summary: Summary statistics (min, max, last, count) per metric
        - artifacts: List of artifacts logged during execution
        - total_metrics: Total count of metrics (before limit/offset)

    Examples:
        # Get all metrics from a training run
        metrics = get_run_metrics("stage-abc123")
        print(f"Total: {metrics['total_metrics']}")

        # Filter by metric name
        loss_metrics = get_run_metrics("stage-abc123", metric_name="loss")

        # Paginate large metric sets
        page1 = get_run_metrics("stage-abc123", limit=1000, offset=0)
        page2 = get_run_metrics("stage-abc123", limit=1000, offset=1000)
    """
    db = _get_db()

    # Validate inputs (security: reject invalid IDs before any DB access)
    validate_stage_run_id(run_id)
    if metric_name is not None:
        validate_metric_name(metric_name)

    # Validate parameters
    if limit is not None and (limit < 1 or limit > 10000):
        raise GoldfishError("limit must be 1-10000")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    # Verify run exists
    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    # Get total count first (for pagination info) - separate query
    total_metrics = db.count_run_metrics(run_id, metric_name=metric_name)

    # Get metrics with SQL-level pagination (critical for performance)
    # This avoids loading all metrics into memory then slicing
    metric_rows = db.get_run_metrics(
        run_id,
        metric_name=metric_name,
        limit=limit,
        offset=offset,
    )

    metrics = [
        MetricInfo(
            name=m["name"],
            value=m["value"],
            step=m["step"],
            timestamp=m["timestamp"],
        )
        for m in metric_rows
    ]

    # Get summary (filter pushed to SQL for efficiency)
    summary_rows = db.get_metrics_summary(run_id, metric_name=metric_name)

    summaries = [
        MetricSummary(
            name=s["name"],
            min_value=s["min_value"],
            max_value=s["max_value"],
            last_value=s["last_value"],
            count=s["count"],
        )
        for s in summary_rows
    ]

    # Get artifacts
    artifact_rows = db.get_run_artifacts(run_id)
    artifacts = [
        ArtifactInfo(
            name=a["name"],
            path=a["path"],
            backend_url=a["backend_url"],
            created_at=a["created_at"],
        )
        for a in artifact_rows
    ]

    result: dict[str, Any] = GetRunMetricsResponse(
        stage_run_id=run_id,
        metrics=metrics,
        summary=summaries,
        artifacts=artifacts,
        total_metrics=total_metrics,
    ).model_dump(mode="json")

    return result


@mcp.tool()
def get_pipeline_status(pipeline_run_id: str) -> dict:
    """Get detailed status of a pipeline run including queue state.

    Use this to debug why a pipeline isn't progressing.

    Args:
        pipeline_run_id: The pipeline run ID (e.g., "prun-abc123")

    Returns:
        Dict with pipeline run info, queue entries, and their statuses
    """
    db = _get_db()
    result = db.get_pipeline_run_status(pipeline_run_id)
    if not result:
        raise GoldfishError(f"Pipeline run not found: {pipeline_run_id}")
    return result


@mcp.tool()
def mark_outcome(
    run_id: str,
    outcome: str,
) -> dict:
    """Mark the outcome of a completed run.

    Use this to indicate whether a run produced good results or not.
    Marking 'success' closes the current attempt and starts a new one
    for subsequent runs.

    Args:
        run_id: Stage run ID (e.g., "stage-abc123")
        outcome: 'success' (good results) or 'bad_results' (ran but produced garbage)

    Returns:
        Dict with success status and updated run info
    """
    db = _get_db()

    if outcome not in ("success", "bad_results"):
        return {
            "success": False,
            "error": f"Invalid outcome '{outcome}'. Must be 'success' or 'bad_results'",
        }

    # Check run exists and is completed
    run = db.get_stage_run(run_id)
    if not run:
        return {"success": False, "error": f"Run not found: {run_id}"}

    if run["status"] != StageRunStatus.COMPLETED:
        return {
            "success": False,
            "error": f"Can only mark outcome for completed runs. Current status: {run['status']}",
        }

    updated = db.update_run_outcome(run_id, outcome)
    if not updated:
        return {"success": False, "error": "Failed to update outcome"}

    return {
        "success": True,
        "run_id": run_id,
        "outcome": outcome,
        "message": "Attempt closed - next run will start a new attempt"
        if outcome == "success"
        else "Run marked as bad_results - still in current attempt",
    }
