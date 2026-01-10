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
    target: str  # "base" or "project"
    backend: str  # "local" or "cloud"
    cloud_build_id: str | None  # GCP Cloud Build operation ID (if backend=cloud)
    status: str  # "pending", "building", "completed", "failed", "cancelled"
    image_tag: str | None  # Local tag (e.g., "goldfish-base-gpu:v4")
    registry_tag: str | None  # Full registry tag
    started_at: str  # ISO timestamp
    completed_at: str | None  # ISO timestamp
    error: str | None  # Error message if failed
    logs_uri: str | None  # GCS path to logs (Cloud Build only)
    created_at: str
