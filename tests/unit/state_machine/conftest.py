"""Test fixtures for state machine unit tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine import (
    EventContext,
    ProgressPhase,
    SourceType,
    TerminationCause,
)


@pytest.fixture
def now() -> datetime:
    """Provide a consistent timestamp for tests."""
    return datetime.now(UTC)


@pytest.fixture
def base_context(now: datetime) -> EventContext:
    """Create a minimal EventContext for testing."""
    return EventContext(timestamp=now, source="executor")


@pytest.fixture
def exit_success_context(now: datetime) -> EventContext:
    """Context for EXIT_SUCCESS event (exit code 0)."""
    return EventContext(
        timestamp=now,
        source="daemon",
        exit_code=0,
        exit_code_exists=True,
    )


@pytest.fixture
def exit_failure_context(now: datetime) -> EventContext:
    """Context for EXIT_FAILURE event (non-zero exit code)."""
    return EventContext(
        timestamp=now,
        source="daemon",
        exit_code=1,
        exit_code_exists=True,
    )


@pytest.fixture
def exit_missing_confirmed_context(now: datetime) -> EventContext:
    """Context for EXIT_MISSING with confirmed dead instance."""
    return EventContext(
        timestamp=now,
        source="daemon",
        exit_code_exists=False,
        instance_confirmed_dead=True,
        termination_cause=TerminationCause.CRASHED,
    )


@pytest.fixture
def exit_missing_unconfirmed_context(now: datetime) -> EventContext:
    """Context for EXIT_MISSING without confirmed dead instance."""
    return EventContext(
        timestamp=now,
        source="daemon",
        exit_code_exists=False,
        instance_confirmed_dead=False,
    )


@pytest.fixture
def instance_lost_context(now: datetime) -> EventContext:
    """Context for INSTANCE_LOST event (preemption)."""
    return EventContext(
        timestamp=now,
        source="daemon",
        termination_cause=TerminationCause.PREEMPTED,
        instance_confirmed_dead=True,
    )


@pytest.fixture
def timeout_context(now: datetime) -> EventContext:
    """Context for TIMEOUT event."""
    return EventContext(
        timestamp=now,
        source="daemon",
        termination_cause=TerminationCause.TIMEOUT,
    )


@pytest.fixture
def user_cancel_context(now: datetime) -> EventContext:
    """Context for USER_CANCEL event."""
    return EventContext(
        timestamp=now,
        source="mcp_tool",
    )


@pytest.fixture
def finalize_ok_context(now: datetime) -> EventContext:
    """Context for FINALIZE_OK event."""
    return EventContext(
        timestamp=now,
        source="executor",
        phase=ProgressPhase.CLEANUP,
    )


@pytest.fixture
def finalize_fail_critical_context(now: datetime) -> EventContext:
    """Context for FINALIZE_FAIL with critical=True."""
    return EventContext(
        timestamp=now,
        source="executor",
        critical=True,
        error_message="Failed to save outputs to GCS",
    )


@pytest.fixture
def finalize_fail_noncritical_context(now: datetime) -> EventContext:
    """Context for FINALIZE_FAIL with critical=False."""
    return EventContext(
        timestamp=now,
        source="executor",
        critical=False,
        error_message="Failed to collect optional metrics",
    )


@pytest.fixture
def timeout_finalizing_done_context(now: datetime) -> EventContext:
    """Context for TIMEOUT in FINALIZING with critical phases done."""
    return EventContext(
        timestamp=now,
        source="daemon",
        termination_cause=TerminationCause.TIMEOUT,
        critical_phases_done=True,
    )


@pytest.fixture
def timeout_finalizing_not_done_context(now: datetime) -> EventContext:
    """Context for TIMEOUT in FINALIZING with critical phases NOT done."""
    return EventContext(
        timestamp=now,
        source="daemon",
        termination_cause=TerminationCause.TIMEOUT,
        critical_phases_done=False,
    )


@pytest.fixture
def svs_block_context(now: datetime) -> EventContext:
    """Context for SVS_BLOCK event."""
    return EventContext(
        timestamp=now,
        source="executor",
        svs_finding_id="finding-abc123",
        error_message="SVS blocked: potential data leak detected",
    )


# Helper for creating runs in specific states
def create_run_in_state(
    db: Database,
    state: str,
    run_id: str = "stage-test123",
    workspace_name: str = "test-workspace",
    version: str = "v1",
    stage_name: str = "train",
    backend_type: str | None = None,
    backend_handle: str | None = None,
) -> str:
    """Create a stage run in the specified state for testing.

    Args:
        db: Database instance.
        state: The state to set the run to.
        run_id: Stage run ID (default: stage-test123).
        workspace_name: Workspace name (default: test-workspace).
        version: Version string (default: v1).
        stage_name: Stage name (default: train).
        backend_type: Optional backend type ("local" or "gce").
        backend_handle: Optional backend handle (container ID or instance name).

    Returns:
        The run_id of the created run.
    """
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        # Create workspace and version first to satisfy foreign key constraints
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at) VALUES (?, ?)""",
            (workspace_name, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace_name, version, f"{workspace_name}-{version}", "abc123", now, "test"),
        )
        conn.execute(
            """
            INSERT INTO stage_runs (
                id, workspace_name, version, stage_name, state, status,
                backend_type, backend_handle, started_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (run_id, workspace_name, version, stage_name, state, "running", backend_type, backend_handle),
        )
    return run_id


# Helpers for creating contexts with specific states
def make_context(
    source: SourceType = "executor",
    exit_code: int | None = None,
    exit_code_exists: bool = False,
    instance_confirmed_dead: bool = False,
    critical: bool | None = None,
    critical_phases_done: bool | None = None,
    termination_cause: TerminationCause | None = None,
    phase: ProgressPhase | None = None,
    error_message: str | None = None,
    svs_finding_id: str | None = None,
    gcs_error: bool = False,
    gcs_outage_started: datetime | None = None,
) -> EventContext:
    """Factory function to create EventContext with specific values."""
    return EventContext(
        timestamp=datetime.now(UTC),
        source=source,
        exit_code=exit_code,
        exit_code_exists=exit_code_exists,
        instance_confirmed_dead=instance_confirmed_dead,
        critical=critical,
        critical_phases_done=critical_phases_done,
        termination_cause=termination_cause,
        phase=phase,
        error_message=error_message,
        svs_finding_id=svs_finding_id,
        gcs_error=gcs_error,
        gcs_outage_started=gcs_outage_started,
    )


@pytest.fixture
def sample_run(test_db: Database) -> str:
    """Create a sample run in PREPARING state for testing.

    Returns:
        The run_id of the created run.
    """
    return create_run_in_state(
        db=test_db,
        state="preparing",
        run_id="stage-abc123456",
        workspace_name="test-workspace",
        version="v1",
        stage_name="train",
    )
