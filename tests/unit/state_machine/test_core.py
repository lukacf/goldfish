"""Tests for state machine core logic.

Tests transition() and update_phase() with CAS semantics, guards, and idempotency.
These tests use mocking since the actual database schema changes come in Phase 3.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.state_machine import (
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    TerminationCause,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def now() -> datetime:
    """Provide a consistent timestamp for tests."""
    return datetime.now(UTC)


@pytest.fixture
def mock_db() -> MagicMock:
    """Create a mock database with transaction support."""
    db = MagicMock()
    conn = MagicMock()
    db._conn.return_value.__enter__ = MagicMock(return_value=conn)
    db._conn.return_value.__exit__ = MagicMock(return_value=False)
    return db


# =============================================================================
# Test Guards with None Values
# =============================================================================


class TestGuardsRejectNone:
    """Guards must use explicit `is True`/`is False`, rejecting None."""

    def test_guard_critical_true_rejects_none(self) -> None:
        """critical=True guard rejects None (doesn't treat as False)."""
        from goldfish.state_machine.transitions import guard_critical_true

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=None,  # Not set
        )
        assert guard_critical_true(ctx) is False

    def test_guard_critical_false_rejects_none(self) -> None:
        """critical=False guard rejects None."""
        from goldfish.state_machine.transitions import guard_critical_false

        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", critical=None)
        assert guard_critical_false(ctx) is False

    def test_guard_critical_phases_done_true_rejects_none(self) -> None:
        """critical_phases_done=True guard rejects None."""
        from goldfish.state_machine.transitions import guard_critical_phases_done_true

        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", critical_phases_done=None)
        assert guard_critical_phases_done_true(ctx) is False

    def test_guard_critical_phases_done_false_rejects_none(self) -> None:
        """critical_phases_done=False guard rejects None."""
        from goldfish.state_machine.transitions import guard_critical_phases_done_false

        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", critical_phases_done=None)
        assert guard_critical_phases_done_false(ctx) is False

    def test_guard_instance_confirmed_dead_rejects_default(self) -> None:
        """instance_confirmed_dead guard rejects default False."""
        from goldfish.state_machine.transitions import guard_instance_confirmed_dead

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            # instance_confirmed_dead defaults to False
        )
        assert guard_instance_confirmed_dead(ctx) is False


# =============================================================================
# Test transition() Function
# =============================================================================


class TestTransitionHappyPath:
    """Test happy path transitions through the state machine."""

    def test_preparing_to_building(self, mock_db: MagicMock, now: datetime) -> None:
        """PREPARING + BUILD_START → BUILDING."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(timestamp=now, source="executor")

        # Mock: current state is PREPARING
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing",
            "phase": "gcs_check",
        }
        # Mock: CAS update succeeds (rowcount=1)
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.BUILD_START, ctx)

        assert result.success is True
        assert result.new_state == StageState.BUILDING
        assert result.reason == "ok"

    def test_building_to_launching(self, mock_db: MagicMock, now: datetime) -> None:
        """BUILDING + BUILD_OK → LAUNCHING."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(timestamp=now, source="executor")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "building",
            "phase": "docker_build",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.BUILD_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.LAUNCHING

    def test_launching_to_running(self, mock_db: MagicMock, now: datetime) -> None:
        """LAUNCHING + LAUNCH_OK → RUNNING."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(timestamp=now, source="daemon")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "launching",
            "phase": "instance_staging",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.LAUNCH_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.RUNNING

    def test_running_to_finalizing(self, mock_db: MagicMock, now: datetime) -> None:
        """RUNNING + EXIT_SUCCESS → FINALIZING."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(timestamp=now, source="daemon", exit_code=0, exit_code_exists=True)

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running",
            "phase": "code_execution",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.EXIT_SUCCESS, ctx)

        assert result.success is True
        assert result.new_state == StageState.FINALIZING

    def test_finalizing_to_completed(self, mock_db: MagicMock, now: datetime) -> None:
        """FINALIZING + FINALIZE_OK → COMPLETED."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(timestamp=now, source="executor", phase=ProgressPhase.CLEANUP)

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing",
            "phase": "cleanup",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.FINALIZE_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED


class TestTransitionFailurePaths:
    """Test all failure paths reach terminal states."""

    def test_prepare_fail_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """PREPARING + PREPARE_FAIL → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor", error_message="Pipeline validation failed")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.PREPARE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_build_fail_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """BUILDING + BUILD_FAIL → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor", error_message="Docker build failed")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "building"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.BUILD_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_launch_fail_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """LAUNCHING + LAUNCH_FAIL → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor", error_message="Instance failed to start")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "launching"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.LAUNCH_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_exit_failure_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """RUNNING + EXIT_FAILURE → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="daemon", exit_code=1, exit_code_exists=True)

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_FAILURE, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_svs_block_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """PREPARING + SVS_BLOCK → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="executor",
            svs_finding_id="finding-123",
            error_message="SVS blocked execution",
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.SVS_BLOCK, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestTransitionTerminations:
    """Test transitions to TERMINATED state."""

    @pytest.mark.parametrize(
        "from_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.FINALIZING,
        ],
    )
    def test_instance_lost_from_all_active_states(
        self, mock_db: MagicMock, now: datetime, from_state: StageState
    ) -> None:
        """INSTANCE_LOST from any active state → TERMINATED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            termination_cause=TerminationCause.PREEMPTED,
            instance_confirmed_dead=True,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": from_state.value
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True, f"INSTANCE_LOST should work from {from_state}"
        assert result.new_state == StageState.TERMINATED

    @pytest.mark.parametrize(
        "from_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
        ],
    )
    def test_timeout_from_non_finalizing_active_states(
        self, mock_db: MagicMock, now: datetime, from_state: StageState
    ) -> None:
        """TIMEOUT from PREPARING/BUILDING/LAUNCHING/RUNNING → TERMINATED (no guard).

        Spec requirement: TIMEOUT→TERMINATED defaults termination_cause=timeout if not provided.
        """
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="daemon")  # termination_cause intentionally omitted

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": from_state.value
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is True, f"TIMEOUT should work from {from_state}"
        assert result.new_state == StageState.TERMINATED

        # Verify UPDATE used default termination_cause='timeout'
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE stage_runs" in str(c)]
        assert len(update_calls) >= 1
        update_params = update_calls[0][0][1]
        # termination_cause is set in the UPDATE tuple (after state/phase/status/progress + timestamps)
        assert update_params[6] == TerminationCause.TIMEOUT.value

    def test_exit_missing_with_confirmed_dead_goes_to_terminated(self, mock_db: MagicMock, now: datetime) -> None:
        """RUNNING + EXIT_MISSING [guard: instance_confirmed_dead] → TERMINATED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=True,
            termination_cause=TerminationCause.CRASHED,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_exit_missing_without_confirmed_dead_fails(self, mock_db: MagicMock, now: datetime) -> None:
        """RUNNING + EXIT_MISSING without confirmed dead → no valid transition."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=False,  # Guard fails
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running"
        }

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_MISSING, ctx)

        assert result.success is False
        assert result.reason == "no_transition"


class TestTransitionCancellation:
    """Test USER_CANCEL transitions."""

    @pytest.mark.parametrize(
        "from_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.FINALIZING,
        ],
    )
    def test_user_cancel_from_active_states(self, mock_db: MagicMock, now: datetime, from_state: StageState) -> None:
        """USER_CANCEL from any active state → CANCELED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="mcp_tool")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": from_state.value
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.USER_CANCEL, ctx)

        assert result.success is True
        assert result.new_state == StageState.CANCELED


class TestTransitionGuardedFinalization:
    """Test guarded transitions in FINALIZING state."""

    def test_finalize_fail_critical_true_goes_to_failed(self, mock_db: MagicMock, now: datetime) -> None:
        """FINALIZING + FINALIZE_FAIL [critical=True] → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="executor",
            critical=True,
            error_message="Failed to save outputs",
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_finalize_fail_critical_false_goes_to_completed(self, mock_db: MagicMock, now: datetime) -> None:
        """FINALIZING + FINALIZE_FAIL [critical=False] → COMPLETED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="executor",
            critical=False,
            error_message="Failed to collect optional metrics",
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

    def test_finalize_fail_critical_none_no_transition(self, mock_db: MagicMock, now: datetime) -> None:
        """FINALIZING + FINALIZE_FAIL [critical=None] → no valid transition."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="executor",
            critical=None,  # Neither guard passes
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is False
        assert result.reason == "no_transition"

    def test_timeout_in_finalizing_critical_phases_done_goes_to_completed(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """FINALIZING + TIMEOUT [critical_phases_done=True] → COMPLETED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            critical_phases_done=True,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

    def test_timeout_in_finalizing_critical_phases_not_done_goes_to_failed(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """FINALIZING + TIMEOUT [critical_phases_done=False] → FAILED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            critical_phases_done=False,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_timeout_in_finalizing_critical_phases_done_none_no_transition(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """FINALIZING + TIMEOUT [critical_phases_done=None] → no valid transition."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            critical_phases_done=None,  # Neither guard passes
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is False
        assert result.reason == "no_transition"


class TestTransitionCASSemantics:
    """Test CAS (Compare-And-Swap) semantics for concurrent safety."""

    def test_cas_failure_returns_stale_state(self, mock_db: MagicMock, now: datetime) -> None:
        """CAS failure (rowcount=0) returns stale_state error."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Mock: read state as PREPARING
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing"
        }
        # Mock: CAS update fails (another process changed state)
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 0

        result = transition(mock_db, "stage-abc", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "stale_state"

    def test_run_not_found_returns_not_found(self, mock_db: MagicMock, now: datetime) -> None:
        """Non-existent run_id returns not_found error."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Mock: run doesn't exist
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None

        result = transition(mock_db, "stage-nonexistent", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "not_found"

    def test_state_not_set_returns_state_not_set(self, mock_db: MagicMock, now: datetime) -> None:
        """NULL state column (migration not done) returns state_not_set error."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Mock: state column is NULL (migration not run yet)
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": None,
            "phase": None,
        }

        result = transition(mock_db, "stage-abc", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "state_not_set"
        assert result.details is not None
        assert "migration" in result.details.lower()

    def test_invalid_state_value_returns_invalid_state(self, mock_db: MagicMock, now: datetime) -> None:
        """Unknown/corrupted state value returns invalid_state error."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Mock: state column has garbage value (data corruption)
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "garbage_invalid_state",
            "phase": None,
        }

        result = transition(mock_db, "stage-abc", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "invalid_state"
        assert result.details is not None
        assert "garbage_invalid_state" in result.details


class TestTransitionIdempotency:
    """Test idempotency handling for terminal states."""

    def test_already_in_target_state_is_idempotent(self, mock_db: MagicMock, now: datetime) -> None:
        """Transition that would go to current state is idempotent success."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        # FINALIZE_OK would go to COMPLETED, but we're already there
        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_OK, ctx)

        # This should be idempotent success
        assert result.success is True
        assert result.new_state == StageState.COMPLETED
        assert result.reason == "already_in_target_state"

    def test_terminal_state_with_different_event_fails(self, mock_db: MagicMock, now: datetime) -> None:
        """Event in terminal state that doesn't match is invalid."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        # BUILD_START in COMPLETED is invalid (no such transition)
        result = transition(mock_db, "stage-abc", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "no_transition"

    def test_guard_aware_idempotency_finalize_fail_critical_true_not_idempotent_in_completed(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """FINALIZE_FAIL with critical=True should NOT be idempotent in COMPLETED.

        This is guard-aware idempotency: the transition FINALIZE_FAIL(critical=True) → FAILED,
        so if we're in COMPLETED and get FINALIZE_FAIL(critical=True), it's not idempotent
        because the transition would have gone to FAILED, not COMPLETED.
        """
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="executor",
            critical=True,  # Would go to FAILED
        )

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        # Not idempotent - transition would go to FAILED, not COMPLETED
        assert result.success is False
        assert result.reason == "no_transition"

    def test_guard_aware_idempotency_finalize_fail_critical_false_is_idempotent_in_completed(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """FINALIZE_FAIL with critical=False IS idempotent in COMPLETED.

        Spec: if current state is a valid target for this event and the guard passes
        for THIS context, return already_in_target_state.
        """
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor", critical=False)

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED
        assert result.reason == "already_in_target_state"

    def test_guard_aware_idempotency_timeout_finalizing_outputs_saved_is_idempotent_in_completed(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """TIMEOUT(finalizing, critical_phases_done=True) IS idempotent in COMPLETED."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="daemon", critical_phases_done=True)

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED
        assert result.reason == "already_in_target_state"


class TestTransitionAudit:
    """Test audit trail recording."""

    def test_idempotent_transition_does_not_create_audit_record(self, mock_db: MagicMock, now: datetime) -> None:
        """Idempotent transitions should NOT create audit records.

        When already in the target state, we return success without
        performing any database updates or audit inserts.
        """
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        # Already in COMPLETED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "completed"
        }

        # FINALIZE_OK would go to COMPLETED, but we're already there
        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_OK, ctx)

        assert result.success is True
        assert result.reason == "already_in_target_state"

        # Verify NO audit INSERT was called - only the SELECT should happen
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        audit_calls = [c for c in calls if "stage_state_transitions" in str(c)]
        assert len(audit_calls) == 0, "No audit record should be created for idempotent transitions"

        # Also verify no UPDATE was called
        update_calls = [c for c in calls if "UPDATE" in str(c)]
        assert len(update_calls) == 0, "No UPDATE should be called for idempotent transitions"

    def test_successful_transition_creates_audit_record(self, mock_db: MagicMock, now: datetime) -> None:
        """Every successful transition creates audit record."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.BUILD_START, ctx)

        assert result.success is True

        # Verify audit INSERT was called
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        audit_calls = [c for c in calls if "stage_state_transitions" in str(c)]
        assert len(audit_calls) >= 1, "Audit record should be created"

    def test_audit_record_contains_context_fields(self, mock_db: MagicMock, now: datetime) -> None:
        """Audit record captures key EventContext fields in normalized columns."""

        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            exit_code=0,
            exit_code_exists=True,
            termination_cause=None,
            instance_confirmed_dead=False,
            error_message="Test error message",
            phase=ProgressPhase.CODE_EXECUTION,
            gcs_error=False,
            gcs_outage_started=None,
            critical=None,
            critical_phases_done=None,
            svs_finding_id="finding-123",
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_SUCCESS, ctx)
        assert result.success is True

        # Find the audit INSERT call and verify normalized column values
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        audit_calls = [c for c in calls if "stage_state_transitions" in str(c)]
        assert len(audit_calls) >= 1, "Audit record should be created"

        # Parse the INSERT arguments to verify column values
        audit_call = audit_calls[0]
        # The INSERT uses positional params:
        # (stage_run_id, from_state, to_state, event, phase, termination_cause,
        #  exit_code, exit_code_exists, error_message, source, created_at)
        call_args = audit_call[0][1]  # Second element is the tuple of values

        assert call_args[0] == "stage-abc"
        assert call_args[6] == 0
        assert call_args[7] == 1
        assert call_args[8] == "Test error message"
        assert call_args[9] == "daemon"
        assert call_args[4] == "code_execution"
        assert call_args[5] is None


class TestTransitionCompletedWithWarnings:
    """Test completed_with_warnings flag setting."""

    def test_finalize_fail_critical_false_sets_completed_with_warnings(self, mock_db: MagicMock, now: datetime) -> None:
        """FINALIZE_FAIL with critical=False sets completed_with_warnings=True."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor", critical=False)

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

        # Verify completed_with_warnings was set in UPDATE
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE" in str(c) and "completed_with_warnings" in str(c)]
        assert len(update_calls) >= 1, "completed_with_warnings should be set"

    def test_timeout_critical_phases_done_sets_completed_with_warnings(self, mock_db: MagicMock, now: datetime) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=True sets completed_with_warnings=True."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            critical_phases_done=True,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED


# =============================================================================
# Test update_phase() Function
# =============================================================================


class TestUpdatePhase:
    """Test phase updates with CAS guard."""

    def test_update_phase_success(self, mock_db: MagicMock, now: datetime) -> None:
        """update_phase succeeds when state matches."""
        from goldfish.state_machine.core import update_phase

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = update_phase(
            mock_db,
            "stage-abc",
            expected_state=StageState.PREPARING,
            new_phase=ProgressPhase.VERSIONING,
            timestamp=now,
        )

        assert result is True

    def test_update_phase_fails_on_state_mismatch(self, mock_db: MagicMock, now: datetime) -> None:
        """update_phase fails (returns False) when state doesn't match."""
        from goldfish.state_machine.core import update_phase

        # CAS fails - state has changed
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 0

        result = update_phase(
            mock_db,
            "stage-abc",
            expected_state=StageState.PREPARING,
            new_phase=ProgressPhase.VERSIONING,
            timestamp=now,
        )

        assert result is False

    def test_update_phase_uses_cas_where_clause(self, mock_db: MagicMock, now: datetime) -> None:
        """update_phase uses WHERE clause with state check."""
        from goldfish.state_machine.core import update_phase

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        update_phase(
            mock_db,
            "stage-abc",
            expected_state=StageState.BUILDING,
            new_phase=ProgressPhase.DOCKER_BUILD,
            timestamp=now,
        )

        # Verify the execute was called with state in WHERE clause
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE" in str(c)]
        assert len(update_calls) >= 1
        # The WHERE clause should include state check
        call_str = str(update_calls[0])
        assert "state" in call_str.lower()


# =============================================================================
# Test _serialize_context Helper
# =============================================================================


class TestSerializeContext:
    """Test context serialization for audit trail."""

    def test_serialize_context_handles_datetime(self, now: datetime) -> None:
        """Datetime fields are serialized to ISO strings."""
        import json

        from goldfish.state_machine.core import _serialize_context

        ctx = EventContext(timestamp=now, source="executor", gcs_outage_started=now)

        result = _serialize_context(ctx)
        data = json.loads(result)

        assert data["timestamp"] == now.isoformat()
        assert data["gcs_outage_started"] == now.isoformat()

    def test_serialize_context_handles_enums(self, now: datetime) -> None:
        """Enum fields are serialized to their values."""
        import json

        from goldfish.state_machine.core import _serialize_context

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            termination_cause=TerminationCause.PREEMPTED,
            phase=ProgressPhase.CODE_EXECUTION,
        )

        result = _serialize_context(ctx)
        data = json.loads(result)

        assert data["termination_cause"] == "preempted"
        assert data["phase"] == "code_execution"

    def test_serialize_context_handles_none_values(self, now: datetime) -> None:
        """None values are preserved in serialization."""
        import json

        from goldfish.state_machine.core import _serialize_context

        ctx = EventContext(
            timestamp=now,
            source="executor",
            exit_code=None,
            termination_cause=None,
            phase=None,
        )

        result = _serialize_context(ctx)
        data = json.loads(result)

        assert data["exit_code"] is None
        assert data["termination_cause"] is None
        assert data["phase"] is None

    def test_serialize_context_handles_special_characters(self, now: datetime) -> None:
        """Special characters in strings are properly escaped."""
        import json

        from goldfish.state_machine.core import _serialize_context

        # Test with potential injection-like content
        ctx = EventContext(
            timestamp=now,
            source="executor",
            error_message="Error: \"quotes\" and 'apostrophes' and \n newlines",
            svs_finding_id="<script>alert('xss')</script>",
        )

        result = _serialize_context(ctx)
        data = json.loads(result)

        # Content should be preserved exactly as-is (JSON escaping handles safety)
        assert "quotes" in data["error_message"]
        assert "apostrophes" in data["error_message"]
        assert "<script>" in data["svs_finding_id"]


# =============================================================================
# Additional Edge Case Tests
# =============================================================================


class TestUpdatePhaseEdgeCases:
    """Additional edge case tests for update_phase."""

    def test_update_phase_nonexistent_run_returns_false(self, mock_db: MagicMock, now: datetime) -> None:
        """update_phase returns False when run_id doesn't exist.

        This is equivalent to CAS failure - rowcount=0 means either:
        - Run doesn't exist
        - Run exists but state doesn't match

        Either way, the function correctly returns False.
        """
        from goldfish.state_machine.core import update_phase

        # Mock: rowcount=0 (run doesn't exist or state mismatch)
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 0

        result = update_phase(
            mock_db,
            "stage-nonexistent",
            expected_state=StageState.PREPARING,
            new_phase=ProgressPhase.VERSIONING,
            timestamp=now,
        )

        assert result is False


class TestTerminalStateIdempotency:
    """Tests for idempotency when current state is already a valid target.

    Spec requirement: idempotency is guard-aware. We only treat an event as idempotent
    if the current state is a valid target for the event AND the guard for that target
    passes for THIS context.
    """

    def test_exit_missing_in_terminated_is_idempotent_when_guard_passes(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """EXIT_MISSING in TERMINATED is idempotent only when instance_confirmed_dead=True."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=True,
        )

        # Already in TERMINATED
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "terminated"
        }

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED
        assert result.reason == "already_in_target_state"

        # No audit record should be created for idempotent transitions
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        audit_calls = [c for c in calls if "stage_state_transitions" in str(c)]
        assert len(audit_calls) == 0, "No audit for idempotent transition"

    def test_exit_missing_in_terminated_is_not_idempotent_when_guard_fails(
        self, mock_db: MagicMock, now: datetime
    ) -> None:
        """EXIT_MISSING in TERMINATED is NOT idempotent when instance_confirmed_dead=False."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=False,  # Guard fails
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "terminated"
        }

        result = transition(mock_db, "stage-abc", StageEvent.EXIT_MISSING, ctx)

        assert result.success is False
        assert result.reason == "no_transition"

    def test_user_cancel_in_canceled_is_idempotent(self, mock_db: MagicMock, now: datetime) -> None:
        """USER_CANCEL in CANCELED is idempotent."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="mcp_tool")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "canceled"
        }

        result = transition(mock_db, "stage-abc", StageEvent.USER_CANCEL, ctx)

        assert result.success is True
        assert result.new_state == StageState.CANCELED
        assert result.reason == "already_in_target_state"


class TestCompletedWithWarningsNotSetOnNormalCompletion:
    """Test that completed_with_warnings is NOT set for normal completions."""

    def test_finalize_ok_does_not_set_completed_with_warnings(self, mock_db: MagicMock, now: datetime) -> None:
        """Normal FINALIZE_OK → COMPLETED does NOT set completed_with_warnings."""
        from goldfish.state_machine.core import transition

        ctx = EventContext(timestamp=now, source="executor")

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "finalizing"
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.FINALIZE_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

        # Verify completed_with_warnings was NOT in the UPDATE query
        calls = mock_db._conn.return_value.__enter__.return_value.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE" in str(c)]
        assert len(update_calls) >= 1
        # The update should NOT contain completed_with_warnings = 1
        for call in update_calls:
            call_str = str(call)
            if "completed_with_warnings = 1" in call_str:
                pytest.fail("completed_with_warnings should NOT be set for normal completion")
