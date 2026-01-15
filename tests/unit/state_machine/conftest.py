"""Test fixtures for state machine unit tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

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


@pytest.fixture
def force_terminate_context(now: datetime) -> EventContext:
    """Context for FORCE_TERMINATE admin event."""
    return EventContext(
        timestamp=now,
        source="admin",
        termination_cause=TerminationCause.MANUAL,
    )


@pytest.fixture
def force_complete_context(now: datetime) -> EventContext:
    """Context for FORCE_COMPLETE admin event."""
    return EventContext(
        timestamp=now,
        source="admin",
    )


@pytest.fixture
def force_fail_context(now: datetime) -> EventContext:
    """Context for FORCE_FAIL admin event."""
    return EventContext(
        timestamp=now,
        source="admin",
        error_message="Admin marked as failed",
    )


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
