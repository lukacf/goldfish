"""Goldfish MCP tools - Execution Tools

Provides tools for running pipeline stages and monitoring execution.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from goldfish.errors import (
    GoldfishError,
    validate_reason,
)
from goldfish.jobs.conversion import stage_run_dict_to_info
from goldfish.models import (
    CancelRunResponse,
    GetOutputsResponse,
    GetRunResponse,
    StageRunInfo,
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
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")

# Maximum lines that can be requested from logs (prevents memory exhaustion)
_MAX_TAIL_LINES = 10000


def _stage_run_row_to_info(row: dict) -> StageRunInfo:
    return stage_run_dict_to_info(row)


@mcp.tool()
def run(
    workspace: str,
    stages: list[str] | None = None,
    pipeline: str | None = None,
    config_override: dict | None = None,
    inputs_override: dict | None = None,
    reason: str | None = None,
    wait: bool = False,
    dry_run: bool = False,
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
        reason: Why running (min 15 chars)
        wait: False (default) returns immediately; True blocks until completion
        dry_run: If True, validate everything without launching. Returns what would
                 run and any validation errors found.

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
        run("w1", dry_run=True)             # Validate without launching
    """
    config = _get_config()
    workspace_manager = _get_workspace_manager()
    pipeline_executor = _get_pipeline_executor()

    validate_workspace_name(workspace)
    if reason:
        validate_reason(reason, config.audit.min_reason_length)

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

    # Unified execution through run_stages
    result: dict[str, Any] = pipeline_executor.run_stages(
        workspace=workspace_name,
        stages=stages if stages else None,
        pipeline_name=pipeline,
        config_override=config_override or {},
        inputs_override=inputs_override or {},
        reason=reason or "Run stages",
        async_mode=not wait,
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
def logs(run_id: str, tail: int = 200, since: str | None = None) -> dict:
    """Get logs from a run.

    Args:
        run_id: The run ID (e.g., "stage-abc123")
        tail: Number of lines from end (default 200, max 10000)
        since: Only show logs after this ISO timestamp

    Returns:
        Dict with run_id, status, logs, log_uri
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
            "UPDATE stage_runs SET status='canceled', progress=NULL, completed_at=?, error=? WHERE id=? AND status='running'",
            (
                datetime.now(UTC).isoformat(),
                f"Canceled: {reason}",
                run_id,
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

    if run["status"] != "completed":
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
