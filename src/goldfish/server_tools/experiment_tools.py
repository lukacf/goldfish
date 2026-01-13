"""Goldfish MCP tools - Experiment Tools

Provides tools for experiment record management, finalization, and history.
"""

import logging
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
    sort_by: Literal["created", "metric"] = "created",
    desc: bool = True,
    include_pruned: bool = False,
    include_internal_ids: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List experiment records (runs + checkpoints), newest first.

    Args:
        workspace: Workspace name or slot (e.g., "w1", "baseline_lstm")
        record_type: Filter by type - "run" or "checkpoint", or None for all
        stage: Filter by stage name (runs only)
        tagged: Filter by tags:
            - None: No tag filtering
            - True: Only tagged records
            - "tag-name": Records with specific tag
        metric: Filter by metric name (reserved for future use)
        min_value: Minimum metric value filter (reserved for future use)
        sort_by: Sort order - "created" (default) or "metric"
        desc: Sort descending (newest/highest first) if True
        include_pruned: Include records from pruned versions (default False)
        include_internal_ids: Include internal stage_run_id in results (default False)
        limit: Max records to return (default 50)
        offset: Skip first N records (for pagination)

    Returns:
        Dict with:
        - records: List of experiment records with:
            - record_id, type, workspace, version, stage (if run)
            - results_status, ml_outcome (if run)
            - tags (merged from run_tags + version_tags)
            - created_at
        - total: Total count (before limit/offset)
        - has_more: Whether more records exist

    Example:
        list_history("w1")  # All records
        list_history("w1", record_type="run", stage="train")  # Train runs only
        list_history("w1", tagged=True)  # Only tagged records
        list_history("w1", tagged="best-v1")  # Records with specific tag
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
        sort_by=sort_by,
        desc=desc,
        include_pruned=include_pruned,
        include_internal_ids=include_internal_ids,
        limit=limit,
        offset=offset,
    )

    # The manager returns a dict with 'records' key
    records_list = records.get("records", [])

    # Determine if there are more records
    total_count = len(records_list)
    has_more = total_count == limit  # Approximate - if we got limit, there might be more

    return {
        "records": records_list,
        "total": total_count,
        "has_more": has_more,
    }


@mcp.tool()
def inspect_record(
    ref: str,
    include: list[str] | None = None,
) -> dict:
    """Inspect an experiment record in detail.

    Args:
        ref: Record reference - can be:
            - record_id (ULID): "01HXYZ..."
            - tag: "@best-v1" (with @ prefix)
            - stage_run_id: "stage-abc123"
        include: Optional list of additional data to include:
            - "results": Auto + final results
            - "comparison": vs_previous, vs_best, config_diff
            - "tags": All tags on the record

    Returns:
        Dict with record details:
        - record_id, type, workspace, version, stage
        - created_at
        - results_status, ml_outcome (if run)
        - Plus any requested include data

    Example:
        inspect_record("01HXYZ...")  # By record ID
        inspect_record("@best-v1")  # By tag
        inspect_record("stage-abc123", include=["results", "comparison"])
    """
    manager = _get_experiment_manager()

    # Default includes
    if include is None:
        include = ["results", "tags"]

    result = manager.inspect_record(ref, include=include)

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
) -> dict:
    """Get debug info for a run (internal IDs, GCS paths, logs).

    Args:
        ref: Record reference (record_id, stage_run_id, or @tag)

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
        get_debug_info("@best-v1")
    """
    manager = _get_experiment_manager()
    db = _get_db()

    # Resolve ref to record
    record = None
    if ref.startswith("@"):
        # Tag reference - need workspace context
        raise GoldfishError("Tag reference requires workspace context. Use record_id or stage_run_id.")
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
