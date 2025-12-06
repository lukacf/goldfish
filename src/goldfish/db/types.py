"""TypedDict definitions for database row types.

These provide type safety for dict objects returned from database queries.
"""

from typing import Optional
from typing_extensions import TypedDict


class AuditRow(TypedDict):
    """Row from the audit table."""

    id: int
    timestamp: str
    operation: str
    slot: Optional[str]
    workspace: Optional[str]
    reason: str
    details: Optional[str]  # JSON string


class SourceRow(TypedDict):
    """Row from the sources table."""

    id: str
    name: str
    description: Optional[str]
    created_at: str
    created_by: str
    gcs_location: str
    size_bytes: Optional[int]
    status: str
    metadata: Optional[str]  # JSON string


class LineageRow(TypedDict):
    """Row from the source_lineage table."""

    id: int
    source_id: str
    parent_source_id: Optional[str]
    job_id: Optional[str]
    created_at: str


class JobRow(TypedDict):
    """Row from the jobs table."""

    id: str
    workspace: str
    snapshot_id: str
    script: str
    experiment_dir: Optional[str]
    status: str
    started_at: str
    completed_at: Optional[str]
    log_uri: Optional[str]
    artifact_uri: Optional[str]
    error: Optional[str]
    metadata: Optional[str]  # JSON string


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
