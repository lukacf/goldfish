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
    ListRunsResponse,
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

    Returns:
        Dict with:
        - runs: List of stage run info (run_id, workspace, version, stage, status)
        - pipeline_run_id: If running multiple stages

    Examples:
        run("w1")                           # Run all stages
        run("w1", stages=["train"])         # Run single stage
        run("w1", stages=["preprocess", "train", "evaluate"])  # Run specific stages
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
    """Get full details of a run: metadata, inputs, outputs, config.

    Args:
        run_id: The run ID (e.g., "stage-abc123")

    Returns:
        Dict with stage_run info, inputs, outputs, and config
    """
    db = _get_db()
    # Refresh status from backend
    _get_stage_executor().refresh_status_once(run_id)

    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    result: dict[str, Any] = GetRunResponse(
        stage_run=_stage_run_row_to_info(row),
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
    updated = 0
    with db._conn() as conn:
        updated = conn.execute(
            "UPDATE stage_runs SET status='canceled', completed_at=?, error=? WHERE id=? AND status='running'",
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

    # Best-effort backend stop
    try:
        if backend == "local":
            _get_stage_executor().local_executor.stop_container(handle)
        elif backend == "gce":
            _get_stage_executor().gce_launcher.stop_instance(handle)
    except Exception:
        pass

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
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List runs (newest first).

    Args:
        workspace: Filter by workspace name
        stage: Filter by stage name
        status: Filter by status (pending, running, completed, failed, canceled)
        pipeline_run_id: Filter by pipeline run
        limit: Max results (default 50)
        offset: Pagination offset

    Returns:
        Dict with runs list, total_count, has_more
    """
    db = _get_db()
    rows = db.list_stage_runs_with_total(
        workspace_name=workspace,
        stage_name=stage,
        status=status,
        pipeline_run_id=pipeline_run_id,
        limit=limit,
        offset=offset,
    )
    total = rows[0]["total_count"] if rows else 0
    result: dict[str, Any] = ListRunsResponse(
        runs=[_stage_run_row_to_info(r) for r in rows],
        total_count=total,
        has_more=offset + len(rows) < total,
    ).model_dump(mode="json")
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
