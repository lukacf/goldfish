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
    count: int


class ArtifactRow(TypedDict):
    """Row from the run_artifacts table."""

    id: int
    stage_run_id: str
    name: str
    path: str
    backend_url: str | None
    created_at: str
