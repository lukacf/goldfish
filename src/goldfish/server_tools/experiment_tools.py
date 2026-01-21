"""Goldfish MCP tools - Experiment Tools

Provides tools for experiment record management, finalization, and history.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Literal

from goldfish.errors import GoldfishError
from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import (
    InvalidFinalizeResultsError,
    InvalidResultsSpecError,
)
from goldfish.server_core import (
    _get_db,
    _get_workspace_manager,
    mcp,
)
from goldfish.validation import validate_workspace_name

logger = logging.getLogger("goldfish.server")


def _get_experiment_manager() -> ExperimentRecordManager:
    """Get or create the experiment record manager."""
    db = _get_db()
    return ExperimentRecordManager(db)


@mcp.tool()
def finalize_run(
    record_or_run_id: str,
    results: dict,
) -> dict:
    """Finalize ML results for a run - authoritative outcome recording.

    This is the canonical way to record ML experiment outcomes. The finalized
    results become the source of truth for comparison and history.

    Args:
        record_or_run_id: Can be:
            - record_id (ULID): "01HXYZ..."
            - stage_run_id: "stage-abc123"
        results: Final results dict with:
            - primary_metric: Metric name (e.g., "dir_acc_binary")
            - direction: "maximize" or "minimize"
            - value: Final metric value (e.g., 0.631)
            - dataset_split: "train", "val", "test", or "other"
            - ml_outcome: "success", "partial", "miss", or "unknown"
            - notes: Verbose rationale and interpretation (min 15 chars)
            Optional:
            - unit: "fraction", "percent", "loss", or null
            - step: Training step number
            - epoch: Training epoch number
            - secondary: Dict of secondary metric values
            - termination: {"infra_outcome": "completed|preempted|crashed|canceled"}

    Returns:
        Dict with:
        - record_id: The experiment record ID
        - stage_run_id: The stage run ID
        - results_status: "finalized"
        - comparison: Computed comparison block (vs_previous, vs_best, config_diff)

    Raises:
        GoldfishError: If results validation fails or record not found

    Example:
        finalize_run("stage-abc123", {
            "primary_metric": "dir_acc_binary",
            "direction": "maximize",
            "value": 0.631,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Achieved target with stable convergence. Loss settled at 2.28."
        })
    """
    manager = _get_experiment_manager()

    try:
        result = manager.finalize_run(record_or_run_id, results)
        return result
    except InvalidFinalizeResultsError as e:
        raise GoldfishError(f"Invalid results: {e.message}") from e
    except ValueError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def list_history(
    workspace: str,
    record_type: Literal["run", "checkpoint"] | None = None,
    stage: str | None = None,
    tagged: bool | str | None = None,
    metric: str | None = None,
    min_value: float | None = None,
    experiment_group: str | None = None,
    sort_by: Literal["created", "metric"] = "created",
    desc: bool = True,
    include_pruned: bool = False,
    include_internal_ids: bool = False,
    finalized_only: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """Browse experiment history for a workspace - the essential tool for understanding what has been run.

    This is the primary tool for reviewing experiment progress. Use it to see recent runs,
    compare outcomes, and track what has been tried. Returns records newest-first by default.

    Args:
        workspace: Workspace name or slot (e.g., "w1", "baseline_lstm")
        record_type: Filter by type - "run" or "checkpoint", or None for all
        stage: Filter by stage name (runs only)
        tagged: Filter by tags:
            - None: No tag filtering
            - True: Only tagged records
            - "tag-name": Records with specific tag
        metric: Filter by metric name
        min_value: Minimum metric value filter
        experiment_group: Filter by experiment group
        sort_by: Sort order - "created" (default) or "metric"
        desc: Sort descending (newest/highest first) if True
        include_pruned: Include records from pruned versions (default False)
        include_internal_ids: Include internal stage_run_id in results (default False)
        finalized_only: Only show runs with finalized results (default False)
        limit: Max records to return (default 20)
        offset: Skip first N records (for pagination)

    Returns:
        Dict with workspace context and records list containing:
        - record_id, version, stage, reason, primary_metric, ml_outcome, age
        - tags (only if non-empty)

    Example:
        list_history("w1")  # Recent runs
        list_history("w1", finalized_only=True)  # Only finalized runs
        list_history("w1", tagged="best")  # Records with "best" tag
    """
    workspace_manager = _get_workspace_manager()
    manager = _get_experiment_manager()

    validate_workspace_name(workspace)

    # Resolve workspace (could be slot like "w1") - fallback to original if not a slot
    workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace

    records = manager.list_history(
        workspace_name=workspace_name,
        record_type=record_type,
        stage=stage,
        tagged=tagged,
        metric=metric,
        min_value=min_value,
        experiment_group=experiment_group,
        sort_by=sort_by,
        desc=desc,
        include_pruned=include_pruned,
        include_internal_ids=include_internal_ids,
        finalized_only=finalized_only,
        limit=limit,
        offset=offset,
    )

    # Post-process records to clean up output
    cleaned_records = []
    for record in records.get("records", []):
        cleaned = dict(record)
        # Remove workspace_name from each record (caller already knows it)
        cleaned.pop("workspace_name", None)
        # Remove empty tags
        if "tags" in cleaned and not cleaned["tags"]:
            del cleaned["tags"]
        # Remove null experiment_group
        if cleaned.get("experiment_group") is None:
            cleaned.pop("experiment_group", None)
        # Remove redundant type field (almost all records are "run")
        cleaned.pop("type", None)
        cleaned_records.append(cleaned)

    return {
        "workspace": workspace_name,
        "records": cleaned_records,
        "total": records.get("total", 0),
        "has_more": records.get("has_more", False),
    }


@mcp.tool()
def inspect_record(
    ref: str,
    include: list[str] | None = None,
    workspace: str | None = None,
) -> dict:
    """Inspect an experiment record in detail.

    Args:
        ref: Record reference - can be:
            - record_id (ULID): "01HXYZ..."
            - tag: "@best-v1" (with @ prefix, requires workspace parameter)
            - stage_run_id: "stage-abc123"
        include: Optional list of additional data to include:
            - "results": Auto + final results
            - "comparison": vs_previous, vs_best, config_diff
            - "tags": All tags on the record
        workspace: Workspace name or slot - REQUIRED when ref is a @tag

    Returns:
        Dict with record details:
        - record_id, type, workspace, version, stage
        - created_at
        - results_status, ml_outcome (if run)
        - Plus any requested include data

    Example:
        inspect_record("01HXYZ...")  # By record ID
        inspect_record("@best-v1", workspace="w1")  # By tag (requires workspace)
        inspect_record("stage-abc123", include=["results", "comparison"])
    """
    manager = _get_experiment_manager()
    workspace_manager = _get_workspace_manager()

    # Resolve workspace if provided (could be slot like "w1")
    workspace_name: str | None = None
    if workspace:
        workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace

    # Validate that @tag references have workspace
    if ref.startswith("@") and workspace_name is None:
        raise GoldfishError("Tag reference requires workspace parameter (e.g., workspace='w1')")

    # Default includes
    if include is None:
        include = ["results", "tags"]

    result = manager.inspect_record(ref, include=include, workspace_name=workspace_name)

    if result is None:
        raise GoldfishError(f"Record not found: {ref}")

    return result


@mcp.tool()
def tag_record(
    ref: str,
    tag: str,
) -> dict:
    """Tag an experiment record.

    Tags are unique per workspace. For run records, creates both a run_tag
    AND a version_tag with the same name. For checkpoints, creates only
    a version_tag.

    Args:
        ref: Record reference - can be:
            - record_id (ULID): "01HXYZ..."
            - stage_run_id: "stage-abc123"
        tag: Tag name (e.g., "best-v1", "baseline")

    Returns:
        Dict with:
        - record_id: The tagged record
        - tag: The tag name
        - record_type: "run" or "checkpoint"

    Raises:
        GoldfishError: If record not found or tag already exists

    Example:
        tag_record("01HXYZ...", "best-v1")
        tag_record("stage-abc123", "baseline")
    """
    manager = _get_experiment_manager()

    try:
        result = manager.tag_record(ref, tag)
        return result
    except ValueError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def list_unfinalized_runs(
    workspace: str,
) -> dict:
    """List runs that need finalization.

    Returns terminal infra runs (completed, preempted, crashed, canceled)
    that don't have finalized results yet.

    Args:
        workspace: Workspace name or slot

    Returns:
        Dict with:
        - runs: List of unfinalized runs with record_id, stage_run_id,
                stage_name, infra_outcome, results_status

    Example:
        list_unfinalized_runs("w1")
    """
    workspace_manager = _get_workspace_manager()
    manager = _get_experiment_manager()

    validate_workspace_name(workspace)
    workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace

    runs = manager.list_unfinalized_runs(workspace_name)

    return {"runs": runs}


@mcp.tool()
def get_debug_info(
    ref: str,
    workspace: str | None = None,
) -> dict:
    """Get debug info for a run (internal IDs, GCS paths, logs).

    Args:
        ref: Record reference (record_id, stage_run_id, or @tag)
        workspace: Workspace name or slot - REQUIRED when ref is a @tag

    Returns:
        Dict with debug information:
        - record_id: Experiment record ID
        - stage_run_id: Internal run ID
        - backend_type: "local" or "gce"
        - backend_handle: Container ID or instance name
        - log_uri: Log file location
        - artifact_uri: Artifact storage location

    Example:
        get_debug_info("01HXYZ...")
        get_debug_info("@best-v1", workspace="w1")
    """
    manager = _get_experiment_manager()
    workspace_manager = _get_workspace_manager()
    db = _get_db()

    # Resolve workspace if provided (could be slot like "w1")
    workspace_name: str | None = None
    if workspace:
        workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace

    # Resolve ref to record
    record = None
    if ref.startswith("@"):
        # Tag reference - need workspace context
        if workspace_name is None:
            raise GoldfishError("Tag reference requires workspace parameter (e.g., workspace='w1')")
        record = manager.get_record_by_tag(workspace_name, ref[1:])
    elif ref.startswith("stage-"):
        record = manager.get_record_by_stage_run(ref)
    else:
        record = manager.get_record(ref)

    if record is None:
        raise GoldfishError(f"Record not found: {ref}")

    stage_run_id = record.get("stage_run_id")
    if stage_run_id is None:
        raise GoldfishError("Debug info only available for run records")

    # Get stage run details
    with db._conn() as conn:
        row = conn.execute(
            """
            SELECT id, backend_type, backend_handle, log_uri, artifact_uri
            FROM stage_runs WHERE id = ?
            """,
            (stage_run_id,),
        ).fetchone()

    if row is None:
        raise GoldfishError(f"Stage run not found: {stage_run_id}")

    return {
        "record_id": record["record_id"],
        "stage_run_id": stage_run_id,
        "backend_type": row["backend_type"],
        "backend_handle": row["backend_handle"],
        "log_uri": row["log_uri"],
        "artifact_uri": row["artifact_uri"],
    }


@mcp.tool()
def get_experiment_context(
    workspace: str,
) -> dict:
    """Get experiment context for a workspace.

    Returns current best, pending finalizations, recent trend, and regression alerts.
    This is the same context returned by mount() for experiment-aware workflows.

    Args:
        workspace: Workspace name or slot

    Returns:
        Dict with:
        - current_best: Best tagged record info (tag, record_id, metric, value)
        - awaiting_finalization: List of record IDs needing finalization
        - recent_trend: Last N finalized values for primary metric
        - regression_alerts: Any detected regressions

    Example:
        get_experiment_context("w1")
    """
    workspace_manager = _get_workspace_manager()
    manager = _get_experiment_manager()

    validate_workspace_name(workspace)
    workspace_name = workspace_manager.get_workspace_for_slot(workspace) or workspace

    context = manager.get_experiment_context(workspace_name)
    return context


@mcp.tool()
def save_results_spec(
    stage_run_id: str,
    record_id: str,
    spec: dict,
) -> dict:
    """Save a results_spec for a run.

    The results_spec defines the expected metrics, comparison baseline, and
    success criteria for a run. This should be called when starting a run.

    Args:
        stage_run_id: Stage run ID
        record_id: Experiment record ID
        spec: Results spec dict with:
            Required:
            - primary_metric: Metric name
            - direction: "maximize" or "minimize"
            - min_value: Minimum acceptable value
            - goal_value: Target value
            - dataset_split: "train", "val", "test", or "other"
            - tolerance: Acceptable variance from goal
            - context: Verbose description (min 15 chars)
            Optional:
            - secondary_metrics: List of additional metrics to track
            - baseline_run: Reference for comparison ("stage-xxx", "@tag", or record_id)
            - failure_threshold: Value below which run is considered failed
            - known_caveats: List of known issues or limitations

    Returns:
        Dict with success confirmation

    Raises:
        GoldfishError: If spec validation fails
    """
    manager = _get_experiment_manager()

    try:
        manager.save_results_spec(stage_run_id, record_id, spec)
        return {"success": True, "stage_run_id": stage_run_id}
    except InvalidResultsSpecError as e:
        raise GoldfishError(f"Invalid results_spec: {e.message}") from e


def _format_age(created_at: str) -> str:
    """Convert ISO timestamp to relative age like '2h ago'.

    Args:
        created_at: ISO format timestamp string

    Returns:
        Human-readable relative time (e.g., "5m ago", "2h ago", "3d ago")
    """
    # Handle Z suffix for UTC
    timestamp = created_at.replace("Z", "+00:00")
    dt = datetime.fromisoformat(timestamp)

    # Ensure timezone-aware comparison
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    delta = now - dt

    if delta.days > 0:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    minutes = delta.seconds // 60
    return f"{minutes}m ago"


def _get_lineage_impl(
    run_id: str,
    direction: Literal["upstream", "downstream"] = "downstream",
) -> dict:
    """Internal implementation for get_lineage tool.

    Args:
        run_id: Stage run ID (e.g., "stage-abc123")
        direction: "downstream" = who consumed my outputs
                   "upstream" = where did my inputs come from

    Returns:
        Dict with connected runs including stage, reason, signal name, outcome, and age.
    """
    db = _get_db()

    # Verify run exists
    run = db.get_stage_run(run_id)
    if run is None:
        raise GoldfishError(f"Stage run not found: {run_id}")

    if direction == "downstream":
        # Find runs that consumed outputs from this run
        downstream_signals = db.list_signals(source_stage_run_id=run_id)

        # Deduplicate by consumer run
        seen_consumers: set[str] = set()
        consumers = []

        for signal in downstream_signals:
            consumer_id = signal.get("stage_run_id")
            if not consumer_id or consumer_id in seen_consumers:
                continue
            seen_consumers.add(consumer_id)

            # Get consumer run details
            consumer_run = db.get_stage_run(consumer_id)
            if consumer_run is None:
                continue

            # Parse reason from JSON
            reason = None
            reason_json = consumer_run.get("reason_json")
            if reason_json:
                try:
                    reason_data = json.loads(reason_json)
                    reason = reason_data.get("description")
                except (json.JSONDecodeError, TypeError):
                    pass

            # Format age
            started_at = consumer_run.get("started_at")
            age = _format_age(started_at) if started_at else "unknown"

            consumers.append(
                {
                    "run_id": consumer_id,
                    "stage": consumer_run.get("stage_name"),
                    "reason": reason,
                    "signal_consumed": signal.get("signal_name"),
                    "outcome": consumer_run.get("state"),
                    "age": age,
                }
            )

        return {
            "run_id": run_id,
            "direction": "downstream",
            "consumers": consumers,
        }
    else:
        # Find runs that produced inputs for this run
        input_signals = db.list_signals(stage_run_id=run_id, signal_type="input")

        # Group by producer run
        seen_producers: set[str] = set()
        producers = []

        for signal in input_signals:
            producer_id = signal.get("source_stage_run_id")
            if not producer_id or producer_id in seen_producers:
                continue
            seen_producers.add(producer_id)

            # Get producer run details
            producer_run = db.get_stage_run(producer_id)
            if producer_run is None:
                continue

            # Parse reason from JSON
            reason = None
            reason_json = producer_run.get("reason_json")
            if reason_json:
                try:
                    reason_data = json.loads(reason_json)
                    reason = reason_data.get("description")
                except (json.JSONDecodeError, TypeError):
                    pass

            # Format age
            started_at = producer_run.get("started_at")
            age = _format_age(started_at) if started_at else "unknown"

            producers.append(
                {
                    "run_id": producer_id,
                    "stage": producer_run.get("stage_name"),
                    "reason": reason,
                    "signal_produced": signal.get("signal_name"),
                    "outcome": producer_run.get("state"),
                    "age": age,
                }
            )

        return {
            "run_id": run_id,
            "direction": "upstream",
            "producers": producers,
        }


@mcp.tool()
def get_lineage(
    run_id: str,
    direction: Literal["upstream", "downstream"] = "downstream",
) -> dict:
    """Track which runs consumed this run's outputs (downstream) or produced its inputs (upstream).

    This is the essential tool for understanding experiment dependencies. Use it to:
    - Find all runs that depend on a completed run's outputs (downstream)
    - Trace where a run's inputs came from (upstream)
    - Debug data flow issues in multi-stage pipelines

    Args:
        run_id: Stage run ID (e.g., "stage-abc123")
        direction: "downstream" (default) = who consumed my outputs
                   "upstream" = where did my inputs come from

    Returns:
        Dict with connected runs including stage, reason, signal name, outcome, and age.

    Example:
        get_lineage("stage-abc123")  # What runs used my outputs?
        get_lineage("stage-abc123", direction="upstream")  # Where did my data come from?
    """
    return _get_lineage_impl(run_id, direction)
