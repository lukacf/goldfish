"""Goldfish MCP tools - Execution Tools

Provides tools for running pipeline stages and monitoring execution.
"""

import json
import logging
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any, cast

from pydantic import ValidationError

from goldfish.errors import (
    GoldfishError,
    validate_reason,
)
from goldfish.infra.metadata.base import MetadataSignal
from goldfish.jobs.conversion import stage_run_dict_to_info
from goldfish.models import (
    CancelRunResponse,
    RunReason,
    StageRunInfo,
    StageRunProgress,
    StageRunStatus,
)
from goldfish.server_core import (
    _get_config,
    _get_db,
    _get_metadata_bus,
    _get_pipeline_executor,
    _get_stage_executor,  # Used for logs, cancel, refresh
    _get_workspace_manager,
    mcp,
)
from goldfish.server_tools.backup_tools import trigger_backup
from goldfish.utils import parse_datetime
from goldfish.validation import (
    validate_stage_run_id,
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.server")

# Maximum lines that can be requested from logs (prevents memory exhaustion)
_MAX_TAIL_LINES = 10000
DEFAULT_METRICS_LIMIT = 1000
DEFAULT_ARTIFACT_LIMIT = 1000
UNBOUNDED_METRICS_WARNING = 10000
UNBOUNDED_ARTIFACTS_WARNING = 5000


def _read_max_metrics_offset() -> int:
    value = os.environ.get("GOLDFISH_METRICS_MAX_OFFSET")
    if not value:
        return 1_000_000
    try:
        parsed = int(value)
    except ValueError:
        return 1_000_000
    return max(1, min(100_000_000, parsed))


MAX_METRICS_OFFSET = _read_max_metrics_offset()

# Cursor tracking for follow mode - maps run_id to (cursor_position, last_access_time)
# Used to return only new logs since the last call
# Cursors are cleaned up after 1 hour of inactivity to prevent memory leaks
_log_cursors: dict[str, tuple[int, float]] = {}
_CURSOR_TTL_SECONDS = 3600  # 1 hour
_MAX_CURSORS = 1000  # Maximum number of cursors to keep
_log_cursor_lock = threading.Lock()


def _read_overdrive_ack_timeout() -> float:
    value = os.environ.get("GOLDFISH_OVERDRIVE_ACK_TIMEOUT")
    if not value:
        return 0.0
    try:
        parsed = float(value)
    except ValueError:
        return 0.0
    return max(0.5, min(10.0, parsed))


def _overdrive_ack_timeout(row: dict) -> float:
    override = _read_overdrive_ack_timeout()
    if override:
        return override
    backend = row.get("backend_type") or "local"
    progress = row.get("progress")
    if backend == "local":
        return 1.0
    if backend == "gce" and progress == StageRunProgress.RUNNING:
        return 4.0
    return 2.0


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
    results_spec: dict | None = None,
    experiment_group: str | None = None,
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
        results_spec: Expected results specification (recommended). Dict with:
            Required: primary_metric, direction, min_value, goal_value,
                     dataset_split, tolerance, context (verbose description)
            Optional: secondary_metrics, baseline_run, failure_threshold, known_caveats
        experiment_group: Optional grouping for filtering and summary
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

    Note:
        This tool enforces a strict finalization gate: if there are terminal runs
        (completed, preempted, crashed, canceled) with unfinalized results in the
        workspace, the run will be blocked. Use finalize_run() to finalize pending
        results before starting new runs.

    Examples:
        run("w1")                           # Run all stages
        run("w1", stages=["train"])         # Run single stage
        run("w1", stages=["preprocess", "train", "evaluate"])  # Run specific stages
        run("w1", reason={"description": "Test new architecture", "hypothesis": "Will improve accuracy"})
        run("w1", dry_run=True)             # Validate without launching
        run("w1", results_spec={"primary_metric": "accuracy", "direction": "maximize", ...})
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

    # Check finalization gate - block if there are unfinalized terminal runs
    from goldfish.experiment_model.records import ExperimentRecordManager
    from goldfish.experiment_model.schemas import (
        InvalidResultsSpecError,
        validate_results_spec,
    )

    db = _get_db()
    exp_manager = ExperimentRecordManager(db)
    gate_result = exp_manager.check_finalization_gate(workspace_name)

    if gate_result["blocked"]:
        unfinalized = gate_result.get("unfinalized", [])
        run_ids = [r.get("stage_run_id", r.get("record_id")) for r in unfinalized[:3]]
        raise GoldfishError(
            f"Finalization gate blocked: {len(unfinalized)} unfinalized terminal run(s) "
            f"in workspace '{workspace_name}'. Use finalize_run() to finalize results "
            f"before starting new runs. Pending runs: {', '.join(run_ids)}" + ("..." if len(unfinalized) > 3 else "")
        )

    # Validate results_spec - required for actual runs, optional for dry_run
    if results_spec is None and not dry_run:
        raise GoldfishError(
            "results_spec is required for runs. Provide a dict with: "
            "primary_metric, direction, min_value, goal_value, dataset_split, tolerance, context"
        )
    if results_spec is not None:
        try:
            validate_results_spec(results_spec)
        except InvalidResultsSpecError as e:
            raise GoldfishError(f"Invalid results_spec: {e.message}") from e

    # Dry run mode: validate without launching
    if dry_run:
        from goldfish.pipeline.validator import validate_pipeline_run

        workspace_path = workspace_manager.get_workspace_path(workspace_name)
        db = _get_db()
        config = _get_config()
        return validate_pipeline_run(
            workspace_name=workspace_name,
            workspace_path=workspace_path,
            db=db,
            stages=stages,
            pipeline_name=pipeline,
            inputs_override=inputs_override or {},
            config=config,
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
        results_spec=results_spec,
        experiment_group=experiment_group,
    )

    # Store results_spec for each run if provided
    # For sync runs, result contains "stage_runs" list from StageRunInfo.model_dump()
    # For async runs, the spec will be stored when the worker creates the stage run
    runs_list = result.get("stage_runs") or result.get("runs") or []
    if results_spec is not None and runs_list:
        for run_info in runs_list:
            stage_run_id = run_info.get("stage_run_id")
            record_id = run_info.get("record_id")
            if stage_run_id and record_id:
                try:
                    exp_manager.save_results_spec(stage_run_id, record_id, results_spec)
                except Exception as e:
                    logger.warning("Failed to save results_spec for %s: %s", stage_run_id, e)

    # Trigger automatic backup after starting run
    trigger_backup(
        "run",
        {"workspace": workspace_name, "stages": stages or ["all"]},
    )

    return result


@mcp.tool()
def inspect_run(run_id: str, include: list[str] | None = None) -> dict:
    """Get a comprehensive, synthesized view of a run.

    This is the master tool for understanding run progress, results, and health.
    It combines metadata, dashboard (progress/trends), manifest (config/io),
    and provenance into a single response.

    Args:
        run_id: The stage run ID (e.g., "stage-abc123")
        include: List of data to include. Defaults to ["dashboard", "metadata", "thoughts"].
                Options: ["dashboard", "metadata", "manifest", "provenance", "svs", "thoughts", "attempt"]

    Returns:
        Dict with synthesized run data.

    Related tools:
    - list_history(): Find experiment records for a workspace
    - inspect_record(include=["comparison"]): Compare a record to previous/best
    - logs(): Get execution logs for a run
    """
    db = _get_db()
    workspace_manager = _get_workspace_manager()
    validate_stage_run_id(run_id)

    # 1. Trigger low-latency sync if running
    row = db.get_stage_run(run_id)
    if not row:
        raise GoldfishError(f"Run not found: {run_id}")

    sync_status = "not_running"
    if row["status"] == StageRunStatus.RUNNING:
        # Refresh status once for async runs to avoid stale launch/finalize states.
        try:
            _get_stage_executor().refresh_status_once(run_id)
            refreshed = db.get_stage_run(run_id)
            if refreshed:
                row = refreshed
        except Exception:
            pass

        progress = row.get("progress")
        backend_type = row.get("backend_type")
        if backend_type == "gce" and progress in {StageRunProgress.BUILD, StageRunProgress.LAUNCH}:
            sync_status = "starting"
        elif backend_type == "gce" and progress == StageRunProgress.FINALIZING:
            sync_status = "finalizing"
        else:
            try:
                bus = _get_metadata_bus()
                import uuid

                req_id = str(uuid.uuid4())[:8]
                sig = MetadataSignal(command="sync", request_id=req_id, payload={"run_id": run_id})
                target = row.get("backend_handle")

                # If GCE, resolve zone to ensure correct targeting
                if row.get("backend_type") == "gce" and target:
                    try:
                        stage_exec = _get_stage_executor()
                        zone = stage_exec.gce_launcher._find_instance_zone(target)
                        if zone:
                            target = f"zones/{zone}/instances/{target}"
                    except Exception as e:
                        logger.warning(f"Failed to resolve zone for {target}: {e}")

                bus.set_signal("goldfish", sig, target=target)

                # Wait for ACK (dynamic timeout)
                # ACK means "signal received, uploads starting" (not "uploads complete")
                ack_timeout = _overdrive_ack_timeout(row)
                start_time = time.time()
                sync_status = "timeout"
                while time.time() - start_time < ack_timeout:
                    ack = bus.get_ack("goldfish", target=target)
                    if ack == req_id:
                        sync_status = "synced"
                        # Wait a bit for uploads to complete after ACK
                        # VM sets ACK first, then uploads metrics (~2s)
                        time.sleep(2.0)
                        break
                    time.sleep(0.1)

                if sync_status == "timeout" and row.get("backend_type") == "gce":
                    sync_status = "pending"
            except Exception as e:
                logger.debug(f"Failed to trigger sync signal: {e}")
                sync_status = f"error: {e}"

        # Ingest metrics/SVS if sync was triggered (even without ACK confirmation).
        # The ACK may fail if the instance lacks IAM permissions to set metadata,
        # but the instance still uploads files to GCS. Pull them anyway.
        # NOTE: For local backend, the LocalMetadataSyncer also ingests metrics,
        # but doing it here makes `inspect_run` self-contained and reliable.
        if sync_status in ("synced", "pending"):
            try:
                stage_exec = _get_stage_executor()
                stage_exec.sync_metrics_if_running(run_id)
                stage_exec.sync_svs_if_running(run_id)
            except Exception as e:
                logger.debug(f"Failed to sync metrics/SVS for {run_id}: {e}")

            # Re-fetch row after sync to get updated timestamps (last_metrics_sync_at)
            refreshed = db.get_stage_run(run_id)
            if refreshed:
                row = refreshed

    # Set default includes if None
    if include is None:
        include = ["dashboard", "metadata", "thoughts"]

    result: dict[str, Any] = {"run_id": run_id}

    if "metadata" in include:
        result.update(
            {
                "workspace": row["workspace_name"],
                "stage": row["stage_name"],
                "status": row["status"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "error": row.get("error"),
            }
        )

    # 2. Synthesize Dashboard (Trends + Progress)
    if "dashboard" in include:
        progress = row.get("progress")
        dashboard_metrics = ["loss", "accuracy", "val_loss", "val_accuracy", "ppl"]
        run_config: dict[str, Any] = {}
        try:
            run_config = json.loads(row["config_json"]) if row.get("config_json") else {}
            if "dashboard_metrics" in run_config:
                dashboard_metrics = run_config["dashboard_metrics"]
        except Exception:
            pass

        summary_rows = db.get_metrics_summary(run_id)
        summaries = {s["name"]: s for s in summary_rows}

        # If no configured metrics are present but we have metrics, fall back to
        # showing the most frequent ones (helps non-train stages).
        if summaries:
            has_any_requested = any(name in summaries for name in dashboard_metrics)
            if not has_any_requested and "dashboard_metrics" not in (run_config or {}):
                dashboard_metrics = [
                    s["name"]
                    for s in sorted(
                        summary_rows,
                        key=lambda r: (r.get("count") or 0),
                        reverse=True,
                    )[:8]
                ]

        metric_trends = db.get_metrics_trends(run_id, dashboard_metrics)

        synthesized_metrics = {}
        for name in dashboard_metrics:
            if name in summaries:
                s = summaries[name]
                trend_vals = metric_trends.get(name, [])
                trend = "stable"
                if len(trend_vals) >= 2:
                    prev, last = trend_vals[0], trend_vals[1]
                    if last < prev:
                        trend = "downward"
                    elif last > prev:
                        trend = "upward"

                synthesized_metrics[name] = {
                    "value": s["last_value"],
                    "min": s["min_value"],
                    "max": s["max_value"],
                    "count": s["count"],
                    "trend": trend,
                }

        health = {}
        for name, s in summaries.items():
            if any(k in name.lower() for k in ["gpu", "vram", "memory"]):
                health[name] = s["last_value"]

        # Calculate actual latest metric timestamp from the data itself
        # This shows when the most recent metric was RECORDED (on the VM)
        latest_metric_at: str | None = None
        for s in summary_rows:
            ts = s.get("last_timestamp")
            if ts and isinstance(ts, str):
                if latest_metric_at is None or ts > latest_metric_at:
                    latest_metric_at = ts

        # Calculate data staleness
        data_age_seconds: float | None = None
        if latest_metric_at:
            try:
                from goldfish.utils import parse_datetime

                latest_dt = parse_datetime(latest_metric_at.replace("Z", "+00:00"))
                if latest_dt.tzinfo is None:
                    latest_dt = latest_dt.replace(tzinfo=UTC)
                data_age_seconds = (datetime.now(UTC) - latest_dt).total_seconds()
            except Exception:
                pass

        # Determine sync method based on current overdrive result
        # "overdrive" = ACK received, data pulled via on-demand sync
        # "polling" = ACK timeout, data came from periodic 30s sync
        # "none" = no metrics data yet
        sync_method = "none"
        if latest_metric_at:
            if sync_status == "synced":
                sync_method = "overdrive"
            else:
                sync_method = "polling"

        # Get new (unnotified) SVS reviews for this run
        svs_rows = db.get_unnotified_svs_reviews(limit=50)
        new_svs_reviews = []
        review_ids_to_mark: list[int] = []
        for svs_row in svs_rows:
            # Only include reviews for this specific run
            if svs_row["stage_run_id"] != run_id:
                continue

            # Parse findings to include actual content
            findings = []
            findings_json = svs_row.get("parsed_findings")
            if findings_json:
                try:
                    parsed = json.loads(findings_json)
                    if isinstance(parsed, list):
                        findings = parsed
                except (json.JSONDecodeError, TypeError):
                    pass

            new_svs_reviews.append(
                {
                    "review_type": svs_row["review_type"],
                    "decision": svs_row["decision"],
                    "findings": findings,
                    "full_text": svs_row.get("response_text"),
                    "reviewed_at": svs_row["reviewed_at"],
                }
            )
            review_ids_to_mark.append(svs_row["id"])

        # Mark these reviews as notified
        if review_ids_to_mark:
            db.mark_svs_reviews_notified(review_ids_to_mark)

        result["dashboard"] = {
            "progress": progress,
            "metrics": synthesized_metrics,
            "health": health,
            "sync_status": sync_status,
            "sync_method": sync_method,  # "overdrive" | "polling" | "none"
            "latest_metric_at": latest_metric_at,  # When newest metric was recorded on VM
            "data_age_seconds": round(data_age_seconds, 1) if data_age_seconds is not None else None,
            "new_svs_reviews": new_svs_reviews,
        }

    if "manifest" in include:
        result["manifest"] = {
            "config": json.loads(row["config_json"]) if row.get("config_json") else {},
            "inputs": json.loads(row["inputs_json"]) if row.get("inputs_json") else {},
            "outputs": json.loads(row["outputs_json"]) if row.get("outputs_json") else [],
            "reason": json.loads(row["reason_json"]) if row.get("reason_json") else None,
        }

    if "provenance" in include:
        from goldfish.lineage.manager import LineageManager

        lineage_mgr = LineageManager(db=db, workspace_manager=workspace_manager)
        result["provenance"] = lineage_mgr.get_run_provenance(run_id)

    if "svs" in include:
        svs_data = None
        preflight_errors = json.loads(row["preflight_errors_json"]) if row.get("preflight_errors_json") else []
        preflight_warnings = json.loads(row["preflight_warnings_json"]) if row.get("preflight_warnings_json") else []

        # Get pre-run reviews from svs_reviews table
        pre_run_reviews = db.get_svs_reviews(stage_run_id=run_id, review_type="pre_run")
        formatted_pre_run = []
        for r in pre_run_reviews:
            parsed = None
            if r.get("parsed_findings"):
                try:
                    parsed = json.loads(str(r["parsed_findings"]))
                except (json.JSONDecodeError, TypeError):
                    pass

            formatted_pre_run.append(
                {
                    "decision": r["decision"],
                    "model": r["model_used"],
                    "findings": parsed,
                    "full_text": r.get("response_text"),
                }
            )

        if preflight_errors or preflight_warnings or row.get("svs_findings_json") or formatted_pre_run:
            svs_data = {
                "preflight": {"errors": preflight_errors, "warnings": preflight_warnings},
                "pre_run": formatted_pre_run,
                "during_run": None,
                "post_run": None,
            }
            if row.get("svs_findings_json"):
                try:
                    findings = json.loads(row["svs_findings_json"])
                    if isinstance(findings, dict):
                        svs_data["during_run"] = findings.get("during_run")
                        svs_data["post_run"] = findings.get("ai_review")
                except Exception:
                    pass

        result["svs"] = svs_data

    if "thoughts" in include:
        thought_rows = db.get_run_thoughts(run_id)
        result["thoughts"] = [
            {
                "timestamp": r["timestamp"],
                "thought": r["reason"],
            }
            for r in thought_rows
        ]

    # Attempt context - shows "You are on attempt #3, 2 failures so far"
    if "attempt" in include:
        attempt_num = row.get("attempt_num")
        if attempt_num:
            attempt_context = db.get_attempt_context(
                workspace_name=row["workspace_name"],
                stage_name=row["stage_name"],
                attempt_num=attempt_num,
            )
            if attempt_context:
                result["attempt_context"] = attempt_context

    return cast(dict, result)


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
            # If backend lookup fails, and run failed, use error field
            if row.get("status") == StageRunStatus.FAILED and row.get("error"):
                log_content = f"Execution failed before logs were available.\n\nError:\n{row['error']}"
            else:
                log_content = f"[Error fetching logs: {e}]"

    # If run failed and we still have no logs, use error message from DB
    if not log_content and row.get("status") == StageRunStatus.FAILED and row.get("error"):
        log_content = f"No execution logs found.\n\nError from system:\n{row['error']}"

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
