"""TypedDict definitions for database row types.

These provide type safety for dict objects returned from database queries.
"""

from typing_extensions import TypedDict


class AuditRow(TypedDict):
    """Row from the audit table."""

    id: int
    timestamp: str
    operation: str
    slot: str | None
    workspace: str | None
    reason: str
    details: str | None  # JSON string


class SourceRow(TypedDict):
    """Row from the sources table."""

    id: str
    name: str
    description: str | None
    created_at: str
    created_by: str
    gcs_location: str
    size_bytes: int | None
    status: str
    metadata: str | None  # JSON string


class LineageRow(TypedDict):
    """Row from the source_lineage table."""

    id: int
    source_id: str
    parent_source_id: str | None
    job_id: str | None
    created_at: str


class JobRow(TypedDict):
    """Row from the jobs table."""

    id: str
    workspace: str
    snapshot_id: str
    script: str
    experiment_dir: str | None
    status: str
    started_at: str
    completed_at: str | None
    log_uri: str | None
    artifact_uri: str | None
    error: str | None
    metadata: str | None  # JSON string


class JobInputRow(TypedDict):
    """Row from the job_inputs table."""

    job_id: str
    source_id: str
    input_name: str


class JobInputWithSource(TypedDict):
    """Job input row joined with source info."""

    job_id: str
    source_id: str
    input_name: str
    source_name: str
    gcs_location: str


class WorkspaceGoalRow(TypedDict):
    """Row from the workspace_goals table."""

    workspace: str
    goal: str
    created_at: str
    updated_at: str


class WorkspaceRow(TypedDict):
    """Row representing a workspace for store protocols.

    This is a minimal, protocol-facing view that can be backed by one or more
    tables (e.g., workspace_lineage + workspace_goals) in different DB backends.
    """

    workspace_name: str
    goal: str
    created_at: str
    updated_at: str


class StageVersionRow(TypedDict):
    """Row from the stage_versions table.

    Tracks unique (code + config) combinations per stage,
    enabling "preprocessing-v5", "tokenization-v11" independent
    of workspace versions.
    """

    id: int
    workspace_name: str
    stage_name: str
    version_num: int
    git_sha: str
    config_hash: str
    created_at: str


class StageRunRow(TypedDict):
    """Row from the stage_runs table (minimal protocol-facing view)."""

    id: str
    workspace_name: str
    version: str
    stage_name: str
    status: str
    started_at: str
    completed_at: str | None


class MetricRow(TypedDict):
    """Row from the run_metrics table."""

    id: int
    stage_run_id: str
    name: str
    value: float
    step: int | None
    timestamp: str


class MetricsSummaryRow(TypedDict):
    """Row from the run_metrics_summary table."""

    stage_run_id: str
    name: str
    min_value: float | None
    max_value: float | None
    last_value: float | None
    last_timestamp: str | None
    count: int


class ArtifactRow(TypedDict):
    """Row from the run_artifacts table."""

    id: int
    stage_run_id: str
    name: str
    path: str
    backend_url: str | None
    created_at: str


class SVSReviewRow(TypedDict):
    """Row from the svs_reviews table.

    Stores AI review results for pre-run, during-run, and post-run validation.
    """

    id: int
    stage_run_id: str
    signal_name: str | None
    review_type: str  # 'pre_run' | 'during_run' | 'post_run'
    model_used: str
    prompt_hash: str
    stats_json: str | None  # JSON string
    response_text: str | None
    parsed_findings: str | None  # JSON string
    decision: str  # 'approved' | 'blocked' | 'warned'
    policy_overrides: str | None  # JSON string
    reviewed_at: str
    duration_ms: int | None
    notified: int  # 0 = not yet shown in dashboard, 1 = already shown


class FailurePatternRow(TypedDict):
    """Row from the failure_patterns table.

    Tracks self-learning failure detection heuristics with approval workflow.
    """

    id: str  # UUID
    symptom: str
    root_cause: str
    detection_heuristic: str
    prevention: str
    severity: str | None  # CRITICAL, HIGH, MEDIUM, LOW
    stage_type: str | None
    source_run_id: str | None
    source_workspace: str | None
    created_at: str
    last_seen_at: str | None
    occurrence_count: int
    status: str  # pending, approved, rejected, archived
    confidence: str | None  # HIGH, MEDIUM, LOW
    approved_at: str | None
    approved_by: str | None
    rejection_reason: str | None
    manually_edited: bool
    enabled: bool


class VersionTagRow(TypedDict):
    """Row from the workspace_version_tags table."""

    workspace_name: str
    version: str
    tag_name: str
    created_at: str


class PrunedVersionRow(TypedDict):
    """Pruned version info (from workspace_versions)."""

    workspace_name: str
    version: str
    pruned_at: str
    prune_reason: str


class DockerBuildRow(TypedDict):
    """Row from the docker_builds table.

    Tracks Docker image builds (local and Cloud Build).
    """

    id: str  # "build-{uuid8}"
    image_type: str  # "cpu" or "gpu"
    target: str  # "base", "project", or "workspace"
    backend: str  # "local" or "cloud"
    cloud_build_id: str | None  # GCP Cloud Build operation ID (if backend=cloud)
    status: str  # "pending", "building", "completed", "failed", "cancelled"
    image_tag: str | None  # Local tag (e.g., "goldfish-base-gpu:v4")
    registry_tag: str | None  # Full registry tag
    started_at: str  # ISO timestamp
    completed_at: str | None  # ISO timestamp
    error: str | None  # Error message if failed
    logs_uri: str | None  # GCS path to logs (Cloud Build only)
    workspace_name: str | None  # Workspace name (for workspace builds only)
    version: str | None  # Workspace version (for workspace builds only)
    content_hash: str | None  # SHA256 of build context (for cache hit detection)
    created_at: str


class BackupRow(TypedDict):
    """Row from the backup_history table.

    Tracks database backups with tiered retention (GFS).
    """

    id: int
    backup_id: str  # "backup-{uuid8}"
    tier: str  # "event", "daily", "weekly", "monthly"
    trigger: str  # "run", "save_version", "create_workspace", "manual", etc.
    trigger_details_json: str | None  # JSON: workspace, version, run_id, etc.
    gcs_path: str  # GCS path to backup file
    size_bytes: int | None  # Compressed size
    created_at: str  # When backup was created
    expires_at: str  # When backup should be cleaned up
    deleted_at: str | None  # When backup was deleted (NULL = still exists)


# =============================================================================
# Experiment Model Types
# =============================================================================


class ExperimentRecordRow(TypedDict):
    """Row from the experiment_records table.

    User-facing entity representing either a run or checkpoint.
    Makes experiment memory first-class.
    """

    record_id: str  # ULID for lexicographic ordering
    workspace_name: str
    type: str  # "run" | "checkpoint"
    stage_run_id: str | None  # FK stage_runs (NULL for checkpoints)
    version: str  # FK workspace_versions
    experiment_group: str | None  # Optional grouping for filtering
    created_at: str


class RunResultsRow(TypedDict):
    """Row from the run_results table.

    Auto + final results with ML/infra outcome separation.
    """

    stage_run_id: str  # FK stage_runs (PK)
    record_id: str  # FK experiment_records
    results_status: str  # "missing" | "auto" | "finalized"
    infra_outcome: str  # "completed" | "preempted" | "crashed" | "canceled" | "unknown"
    ml_outcome: str  # "success" | "partial" | "miss" | "unknown"
    results_auto: str | None  # JSON (immutable)
    results_final: str | None  # JSON (authoritative)
    comparison: str | None  # JSON (computed at finalize)
    finalized_by: str | None
    finalized_at: str | None


class RunResultsSpecRow(TypedDict):
    """Row from the run_results_spec table.

    Required at run time for structured + verbose results spec.
    """

    stage_run_id: str  # FK stage_runs (PK)
    record_id: str  # FK experiment_records
    spec_json: str  # JSON results spec
    created_at: str


class RunTagRow(TypedDict):
    """Row from the run_tags table.

    User-defined names for significant runs (e.g., @best-25m-63pct).
    """

    workspace_name: str
    record_id: str  # FK experiment_records
    tag_name: str
    created_at: str


# =============================================================================
# State Machine Types
# =============================================================================


class StageStateTransitionRow(TypedDict):
    """Row from the stage_state_transitions table.

    Audit trail for all state transitions in stage runs.
    Records from_state → to_state with the triggering event and normalized context.
    """

    id: int
    stage_run_id: str  # FK stage_runs
    from_state: str
    to_state: str
    event: str
    phase: str | None
    termination_cause: str | None
    exit_code: int | None
    exit_code_exists: int | None
    error_message: str | None
    source: str
    created_at: str
