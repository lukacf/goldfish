"""Pydantic models for Goldfish responses."""

from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, Field


class SlotState(str, Enum):
    EMPTY = "empty"
    MOUNTED = "mounted"


class DirtyState(str, Enum):
    CLEAN = "clean"
    DIRTY = "dirty"


class JobStatus(str, Enum):
    """Legacy job status (run_job). Prefer StageRunStatus for new code."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"  # Note: legacy spelling with double 'l'


class StageRunStatus(str, Enum):
    """Status values for stage_runs table.

    Note: Progress is tracked separately in the 'progress' column.
    Display format combines them as "status:progress" (e.g., "running:build").
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"  # American spelling (single 'l')


class StageRunProgress(str, Enum):
    """Progress values for stage_runs.progress column."""

    BUILD = "build"  # Building Docker image
    LAUNCH = "launch"  # Launching container/instance
    RUNNING = "running"  # Executing stage code
    FINALIZING = "finalizing"  # Recording outputs, fetching logs


class PipelineStatus(str, Enum):
    """Status for pipeline_runs and pipeline_stage_queue tables."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    SKIPPED = "skipped"  # Only for pipeline_stage_queue


class SourceStatus(str, Enum):
    AVAILABLE = "available"
    PENDING = "pending"
    FAILED = "failed"


# --- Slot/Workspace Models ---


class SlotInfo(BaseModel):
    """Information about a workspace slot."""

    slot: str  # e.g., "w1"
    state: SlotState
    workspace: str | None = None  # Workspace name if mounted
    dirty: DirtyState | None = None
    last_checkpoint: str | None = None
    context: str | None = None  # One-line description

    # Lineage information (Phase 7)
    current_version: str | None = None  # Current version (e.g., "v3")
    version_count: int | None = None  # Total number of versions
    parent_workspace: str | None = None  # Parent workspace if branched
    parent_version: str | None = None  # Version branched from
    version_history: list[dict] | None = None  # Recent versions
    branches: list[dict] | None = None  # Child workspaces


class WorkflowInfo(BaseModel):
    """Pipeline/workflow information for a workspace."""

    stages: list[str]  # List of stage names in pipeline
    has_pipeline: bool = True


class WorkspaceInfo(BaseModel):
    """Information about a workspace."""

    name: str
    created_at: datetime
    goal: str
    snapshot_count: int
    last_activity: datetime
    is_mounted: bool
    mounted_slot: str | None = None
    workflow: WorkflowInfo | None = None  # Pipeline info (replaces deprecated get_pipeline)


# --- Response Models ---


class StatusResponse(BaseModel):
    """Response from status() tool."""

    project_name: str
    slots: list[SlotInfo]
    active_jobs: list["JobInfo"]
    source_count: int
    state_md: str


class MountResponse(BaseModel):
    """Response from mount() tool."""

    success: bool
    slot: str
    workspace: str
    state_md: str
    dirty: DirtyState
    last_checkpoint: str | None = None
    warning: str | None = None  # For soft limit warnings


class HibernateResponse(BaseModel):
    """Response from hibernate() tool."""

    success: bool
    slot: str
    workspace: str
    state_md: str
    auto_checkpointed: bool
    checkpoint_id: str | None = None
    pushed_to_remote: bool


class CreateWorkspaceResponse(BaseModel):
    """Response from create_workspace() tool."""

    success: bool
    workspace: str
    forked_from: str  # "main" or another branch
    state_md: str


class CheckpointResponse(BaseModel):
    """Response from checkpoint() tool.

    DEPRECATED: Use SaveVersionResponse from save_version() instead.
    """

    success: bool
    slot: str
    snapshot_id: str  # e.g., "snap-a1b2c3d4"
    message: str
    state_md: str


class SaveVersionResponse(BaseModel):
    """Response from save_version() tool.

    Returns version as primary identifier, with git_tag as internal detail.
    """

    success: bool
    slot: str
    version: str  # Primary identifier, e.g., "v1", "v2"
    git_tag: str  # Internal git tag, e.g., "snap-a1b2c3d4-20251210-143000"
    git_sha: str  # Full git SHA for provenance
    message: str
    state_md: str


# --- Job Models ---


class JobInfo(BaseModel):
    """Information about a job."""

    job_id: str
    status: JobStatus
    workspace: str
    snapshot_id: str
    script: str
    started_at: datetime
    completed_at: datetime | None = None
    log_uri: str | None = None
    artifact_uri: str | None = None
    error: str | None = None
    input_sources: list[str] = Field(default_factory=list)

    @property
    def elapsed_seconds(self) -> float | None:
        """Elapsed time in seconds (uses completed_at if done, else current time)."""

        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        elif self.status in (JobStatus.RUNNING, JobStatus.PENDING):
            now = datetime.now(UTC)
            # Ensure started_at has timezone info
            start = self.started_at
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            return (now - start).total_seconds()
        return None

    @property
    def is_terminal(self) -> bool:
        """Whether the job has reached a terminal state."""
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


class RunJobResponse(BaseModel):
    """Response from run_job() tool."""

    success: bool
    job_id: str
    snapshot_id: str
    experiment_dir: str
    artifact_uri: str | None = None
    log_uri: str | None = None
    initial_status: str = JobStatus.PENDING
    state_md: str | None = None


# --- Source Models ---


class SourceInfo(BaseModel):
    """Information about a data source."""

    name: str
    description: str | None = None
    created_at: datetime
    created_by: str  # "job:{job_id}" or "external"
    gcs_location: str
    size_bytes: int | None = None
    status: SourceStatus = SourceStatus.AVAILABLE


class SourceLineage(BaseModel):
    """Lineage information for a source."""

    source_name: str
    parent_sources: list[str] = Field(default_factory=list)
    job_id: str | None = None


class RegisterSourceResponse(BaseModel):
    """Response from register_source() tool."""

    success: bool
    source: SourceInfo
    state_md: str


class PromoteArtifactResponse(BaseModel):
    """Response from promote_artifact() tool."""

    success: bool
    source: SourceInfo
    lineage: SourceLineage
    state_md: str


class ListSourcesResponse(BaseModel):
    """Response from list_sources() tool."""

    sources: list[SourceInfo]
    total_count: int  # Total number of sources matching filters
    offset: int  # Offset used for this request
    limit: int  # Limit used for this request
    has_more: bool  # True if there are more results beyond this page
    filters_applied: dict = {}  # Filters that were applied (status, created_by)


class RegisterDatasetResponse(BaseModel):
    """Response from register_dataset() tool."""

    success: bool
    dataset: SourceInfo


# --- Thought Logging ---


class LogThoughtResponse(BaseModel):
    """Response from log_thought() tool."""

    logged: bool
    thought: str
    timestamp: datetime


# --- Diff Response ---


class DiffResponse(BaseModel):
    """Response from diff() tool."""

    has_changes: bool  # Whether there are differences
    summary: str  # Human-readable summary (e.g., "2 files changed, 10 insertions(+)")
    files_changed: list[str]  # List of changed file paths
    diff_text: str = ""  # Optional full diff output
    # What was compared
    left: str  # Left side of comparison (e.g., "w1", "baseline@v2")
    right: str  # Right side of comparison (e.g., "v3", "experiment@v1")
    left_sha: str | None = None  # Git SHA of left side (if applicable)
    right_sha: str | None = None  # Git SHA of right side (if applicable)


class RollbackResponse(BaseModel):
    """Response from rollback() tool."""

    success: bool
    slot: str
    version: str  # Version that was rolled back to (e.g., "v1")
    git_tag: str = ""  # Internal git tag (e.g., "snap-xxx")
    files_reverted: int  # Number of files changed
    state_md: str = ""  # Updated STATE.md content


class CancelJobResponse(BaseModel):
    """Response from cancel_job() tool."""

    success: bool
    job_id: str
    previous_status: str  # Status before cancellation
    state_md: str = ""  # Updated STATE.md content


class JobLogsResponse(BaseModel):
    """Response from get_job_logs() tool."""

    job_id: str
    status: str  # Current job status
    logs: str | None = None  # Log content if available
    log_uri: str | None = None  # URI where logs are stored
    error: str | None = None  # Error message if logs unavailable


class SnapshotInfo(BaseModel):
    """Information about a workspace snapshot."""

    snapshot_id: str  # e.g., snap-abc1234-20251205-120000
    created_at: datetime  # When the snapshot was created
    message: str  # Commit message for this snapshot


class ListSnapshotsResponse(BaseModel):
    """Response from list_snapshots() tool."""

    workspace: str
    snapshots: list[SnapshotInfo]
    total_count: int  # Total number of snapshots
    offset: int  # Offset used for this request
    limit: int  # Limit used for this request
    has_more: bool  # True if there are more snapshots beyond this page


class WorkspaceGoalResponse(BaseModel):
    """Response from get_workspace_goal() tool."""

    workspace: str
    goal: str | None = None  # None if not set


class UpdateWorkspaceGoalResponse(BaseModel):
    """Response from update_workspace_goal() tool."""

    success: bool
    workspace: str
    goal: str
    state_md: str = ""


class ListJobsResponse(BaseModel):
    """Response from list_jobs() tool with pagination support."""

    jobs: list["JobInfo"]
    total_count: int
    offset: int = 0
    limit: int = 50
    has_more: bool = False
    filters_applied: dict = {}


class DeleteWorkspaceResponse(BaseModel):
    """Response from delete_workspace() tool."""

    success: bool
    workspace: str
    snapshots_deleted: int = 0


class DeleteSourceResponse(BaseModel):
    """Response from delete_source() tool."""

    success: bool
    source_name: str


class DeleteSnapshotResponse(BaseModel):
    """Response from delete_snapshot() tool."""

    success: bool
    workspace: str
    snapshot_id: str


class AuditEntry(BaseModel):
    """A single audit trail entry."""

    id: int
    timestamp: datetime
    operation: str
    slot: str | None = None
    workspace: str | None = None
    reason: str
    details: dict | None = None


class AuditLogResponse(BaseModel):
    """Response from get_audit_log() tool."""

    entries: list[AuditEntry]
    count: int


# --- Pipeline Models ---


class SignalDef(BaseModel):
    """Definition of an input or output signal."""

    name: str
    type: str  # dataset, npy, csv, directory, file
    from_stage: str | None = None  # For inputs: which stage produces this
    signal: str | None = None  # For from_stage: which output signal to use (defaults to input name)
    dataset: str | None = None  # For dataset type: dataset name
    storage: str | None = None  # gcs, hyperdisk, local (hint)
    format: str | None = None  # Override format detection
    artifact: bool | None = False  # Mark output as artifact for auto-registration


class StageDef(BaseModel):
    """Definition of a pipeline stage."""

    name: str
    inputs: dict[str, SignalDef] = Field(default_factory=dict)
    outputs: dict[str, SignalDef] = Field(default_factory=dict)


class PipelineDef(BaseModel):
    """Complete pipeline definition."""

    name: str
    description: str = ""
    stages: list[StageDef]


class PipelineResponse(BaseModel):
    """Response from get_pipeline() tool."""

    workspace: str
    pipeline: PipelineDef


class ValidatePipelineResponse(BaseModel):
    """Response from validate_pipeline() tool."""

    workspace: str
    valid: bool
    errors: list[str] = Field(default_factory=list)


class UpdatePipelineResponse(BaseModel):
    """Response from update_pipeline() tool."""

    success: bool
    workspace: str
    pipeline: PipelineDef


# --- Stage Execution ---


class StageRunInfo(BaseModel):
    """Information about a stage run."""

    stage_run_id: str
    pipeline_run_id: str | None = None
    workspace: str
    pipeline: str | None = None
    version: str
    stage: str
    stage_version: int | None = None  # Stage version ID (FK to stage_versions)
    stage_version_num: int | None = None  # Human-readable version number (1, 2, 3...)
    profile: str | None = None
    hints: dict | None = None
    status: str  # pending, running, completed, failed
    started_at: datetime | None = None
    completed_at: datetime | None = None
    progress: str | None = None
    log_uri: str | None = None
    artifact_uri: str | None = None
    outputs: list | None = None
    config: dict | None = None
    inputs: dict | None = None
    error: str | None = None


class RunStageResponse(BaseModel):
    """Response from run_stage() tool."""

    stage_run: StageRunInfo
    message: str = ""


class RunPipelineResponse(BaseModel):
    """Response from run_pipeline() tool."""

    stage_runs: list[StageRunInfo]
    message: str = ""


# --- Stage Run Observability Responses ---


class ListRunsResponse(BaseModel):
    runs: list[StageRunInfo]
    total_count: int
    has_more: bool


class StageLogsResponse(BaseModel):
    stage_run_id: str
    status: str | None = None
    logs: str | None = None
    log_uri: str | None = None


class GetOutputsResponse(BaseModel):
    stage_run_id: str
    outputs: list


class GetRunResponse(BaseModel):
    stage_run: StageRunInfo
    inputs: dict
    outputs: list
    config: dict


class CancelRunResponse(BaseModel):
    success: bool
    previous_status: str | None = None
    error: str | None = None


# --- Run Reason Structure ---


class RunReason(BaseModel):
    """Structured reason for running stages with experiment hypothesis and goals.

    All fields have max_length constraints to prevent DoS attacks.

    Fields:
        description: What you're running (required, max 500 chars)
        hypothesis: What you expect to happen (max 1000 chars)
        approach: How you're testing it (max 1000 chars)
        min_result: Minimum bar for success (max 500 chars)
        goal: Best case outcome (max 500 chars)
    """

    description: str = Field(max_length=500)
    hypothesis: str | None = Field(default=None, max_length=1000)
    approach: str | None = Field(default=None, max_length=1000)
    min_result: str | None = Field(default=None, max_length=500)
    goal: str | None = Field(default=None, max_length=500)

    def to_summary(self) -> str:
        """Convert to a single-line summary for display."""
        parts = [self.description]
        if self.hypothesis:
            parts.append(f"H: {self.hypothesis}")
        return " | ".join(parts)

    def to_markdown(self) -> str:
        """Convert to markdown format for STATE.md."""
        lines = [f"**Description:** {self.description}"]
        if self.hypothesis:
            lines.append(f"**Hypothesis:** {self.hypothesis}")
        if self.approach:
            lines.append(f"**Approach:** {self.approach}")
        if self.min_result:
            lines.append(f"**Min Result:** {self.min_result}")
        if self.goal:
            lines.append(f"**Goal:** {self.goal}")
        return "\n".join(lines)


# --- Pre-Run Review Models ---


class ReviewSeverity(str, Enum):
    """Severity levels for review issues."""

    ERROR = "error"  # Blocking issue - will fail
    WARNING = "warning"  # Potential problem - may fail
    NOTE = "note"  # Suggestion - won't fail


class ReviewIssue(BaseModel):
    """A single issue found during pre-run review."""

    severity: ReviewSeverity
    stage: str  # Which stage this applies to
    message: str
    file: str | None = None
    line: int | None = None


class RunReview(BaseModel):
    """Result of pre-run review by Claude.

    When has_blocking_issues is True, the run should be blocked
    and recorded as failed with the review text in the error field.
    """

    approved: bool  # True if no blocking issues
    issues: list[ReviewIssue] = Field(default_factory=list)
    summary: str  # Brief summary for error field
    full_review: str  # Complete review text from Claude
    reviewed_stages: list[str] = Field(default_factory=list)
    review_time_ms: int = 0

    @property
    def has_blocking_issues(self) -> bool:
        """True if any issue has ERROR severity."""
        return any(i.severity == ReviewSeverity.ERROR for i in self.issues)

    @property
    def error_count(self) -> int:
        """Count of ERROR severity issues."""
        return sum(1 for i in self.issues if i.severity == ReviewSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        """Count of WARNING severity issues."""
        return sum(1 for i in self.issues if i.severity == ReviewSeverity.WARNING)


# =============================================================================
# Metrics API Models
# =============================================================================


class MetricInfo(BaseModel):
    """Individual metric data point (timestamp is ISO 8601 UTC string)."""

    name: str
    value: float
    step: int | None = None
    timestamp: str


class MetricSummary(BaseModel):
    """Summary statistics for a metric."""

    name: str
    min_value: float | None = None
    max_value: float | None = None
    last_value: float | None = None
    count: int


class ArtifactInfo(BaseModel):
    """Artifact logged during stage execution."""

    name: str
    path: str
    backend_url: str | None = None
    created_at: str


class GetRunMetricsResponse(BaseModel):
    """Response from get_run_metrics tool.

    Returns metrics, summary statistics, and artifacts for a stage run.
    """

    stage_run_id: str
    metrics: list[MetricInfo] = Field(default_factory=list)
    summary: list[MetricSummary] = Field(default_factory=list)
    artifacts: list[ArtifactInfo] = Field(default_factory=list)
    total_metrics: int = 0
    total_artifacts: int = 0
    warnings: list[str] = Field(default_factory=list)
