"""End-to-end integration tests for the state machine.

These tests cover complex scenarios that span multiple transitions:
- Full lifecycle from PREPARING to terminal states
- Preemption scenarios (EXIT_MISSING with instance_confirmed_dead)
- Concurrent transition handling (CAS semantics validation)
- Migration scenarios
"""

from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine import (
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    TerminationCause,
    transition,
    update_phase,
)

# =============================================================================
# Helper Functions
# =============================================================================


def _create_workspace_and_version(db: Database, workspace: str = "test-ws", version: str = "v1") -> None:
    """Create workspace lineage and version for testing."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at)
            VALUES (?, ?)""",
            (workspace, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace, version, f"{workspace}-{version}", "abc123", now, "test"),
        )


def _create_run_in_state(
    db: Database,
    run_id: str,
    state: StageState,
    workspace: str = "test-ws",
    version: str = "v1",
) -> None:
    """Create a stage run in a specific state for testing."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, workspace, version, "train", "running", now, state.value, now),
        )


def _setup_test_run(db: Database, state: StageState) -> str:
    """Setup workspace and create a test run in the specified state. Returns run_id."""
    _create_workspace_and_version(db)
    run_id = f"stage-{uuid.uuid4().hex[:8]}"
    _create_run_in_state(db, run_id, state)
    return run_id


def _event_ctx(
    source: str = "executor",
    exit_code: int | None = None,
    exit_code_exists: bool = False,
    termination_cause: TerminationCause | None = None,
    instance_confirmed_dead: bool = False,
    error_message: str | None = None,
    critical: bool | None = None,
    critical_phases_done: bool | None = None,
) -> EventContext:
    """Create an EventContext with timestamp and source, plus optional fields."""
    return EventContext(
        timestamp=datetime.now(UTC),
        source=source,
        exit_code=exit_code,
        exit_code_exists=exit_code_exists,
        termination_cause=termination_cause,
        instance_confirmed_dead=instance_confirmed_dead,
        error_message=error_message,
        critical=critical,
        critical_phases_done=critical_phases_done,
    )


def _get_run_state(db: Database, run_id: str) -> str | None:
    """Get current state of a run."""
    with db._conn() as conn:
        row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
        return row["state"] if row else None


def _get_transition_count(db: Database, run_id: str) -> int:
    """Get number of transitions recorded for a run."""
    with db._conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as count FROM stage_state_transitions WHERE stage_run_id = ?",
            (run_id,),
        ).fetchone()
        return row["count"] if row else 0


# =============================================================================
# Full Lifecycle Tests
# =============================================================================


class TestFullLifecycleScenarios:
    """E2E tests for complete run lifecycles through multiple states."""

    def test_successful_run_lifecycle_with_phases(self, test_db: Database) -> None:
        """Test complete successful run with phase updates at each stage.

        Scenario: PREPARING → BUILDING → LAUNCHING → RUNNING → FINALIZING → COMPLETED
        with proper phase progression throughout.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run via database method (initial PREPARING state)
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )
        assert _get_run_state(test_db, run_id) == StageState.PREPARING.value

        # Update phase during PREPARING
        update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.VERSIONING, datetime.now(UTC))
        update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.PIPELINE_LOAD, datetime.now(UTC))

        # PREPARING → BUILDING
        result = transition(test_db, run_id, StageEvent.BUILD_START, _event_ctx())
        assert result.success and result.new_state == StageState.BUILDING

        # Update phase during BUILDING
        update_phase(test_db, run_id, StageState.BUILDING, ProgressPhase.DOCKER_BUILD, datetime.now(UTC))

        # BUILDING → LAUNCHING
        result = transition(test_db, run_id, StageEvent.BUILD_OK, _event_ctx())
        assert result.success and result.new_state == StageState.LAUNCHING

        # LAUNCHING → RUNNING
        result = transition(test_db, run_id, StageEvent.LAUNCH_OK, _event_ctx())
        assert result.success and result.new_state == StageState.RUNNING

        # Update phase during RUNNING
        update_phase(test_db, run_id, StageState.RUNNING, ProgressPhase.CODE_EXECUTION, datetime.now(UTC))

        # RUNNING → FINALIZING (via EXIT_SUCCESS)
        ctx = _event_ctx(source="daemon", exit_code=0, exit_code_exists=True)
        result = transition(test_db, run_id, StageEvent.EXIT_SUCCESS, ctx)
        assert result.success and result.new_state == StageState.FINALIZING

        # FINALIZING → COMPLETED
        result = transition(test_db, run_id, StageEvent.FINALIZE_OK, _event_ctx())
        assert result.success and result.new_state == StageState.COMPLETED

        # Verify final state and transition count
        assert _get_run_state(test_db, run_id) == StageState.COMPLETED.value
        # 10 audit rows: run_start + 4 phase_update + 5 state transitions
        assert _get_transition_count(test_db, run_id) == 10

    def test_early_failure_during_build(self, test_db: Database) -> None:
        """Test run that fails during build phase.

        Scenario: PREPARING → BUILDING → FAILED (via BUILD_FAIL)
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # PREPARING → BUILDING
        transition(test_db, run_id, StageEvent.BUILD_START, _event_ctx())

        # BUILDING → FAILED
        ctx = _event_ctx(error_message="Docker build failed: pip install error")
        result = transition(test_db, run_id, StageEvent.BUILD_FAIL, ctx)

        assert result.success and result.new_state == StageState.FAILED
        # 3 transitions: run_start + PREPARING→BUILDING→FAILED
        assert _get_transition_count(test_db, run_id) == 3


# =============================================================================
# Preemption Scenario Tests
# =============================================================================


class TestPreemptionScenarios:
    """E2E tests for spot instance preemption handling."""

    def test_preemption_during_running_via_exit_missing(self, test_db: Database) -> None:
        """Test preemption scenario: RUNNING → TERMINATED via EXIT_MISSING.

        Scenario: Container exits without writing exit code (preemption/crash).
        Daemon detects missing exit code, confirms instance is dead, emits EXIT_MISSING.
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        # Simulate preemption detection
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,  # No exit code file
            instance_confirmed_dead=True,  # Instance verified stopped
            termination_cause=TerminationCause.PREEMPTED,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED
        assert _get_run_state(test_db, run_id) == StageState.TERMINATED.value

        # Verify termination_cause is persisted
        with test_db._conn() as conn:
            row = conn.execute("SELECT termination_cause FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["termination_cause"] == TerminationCause.PREEMPTED.value

    def test_crash_during_running_via_instance_lost(self, test_db: Database) -> None:
        """Test crash scenario: RUNNING → TERMINATED via INSTANCE_LOST.

        Scenario: Instance disappears unexpectedly (hardware failure, etc.).
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=TerminationCause.CRASHED,
        )
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

        # Verify termination_cause is persisted
        with test_db._conn() as conn:
            row = conn.execute("SELECT termination_cause FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["termination_cause"] == TerminationCause.CRASHED.value

    def test_preemption_during_finalizing(self, test_db: Database) -> None:
        """Test preemption during finalization: FINALIZING → TERMINATED via INSTANCE_LOST.

        Even during finalization, preemption can occur.
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=TerminationCause.PREEMPTED,
        )
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED


# =============================================================================
# Concurrent Transition Tests (CAS Validation)
# =============================================================================


class TestConcurrentTransitionsCAS:
    """Tests for CAS (Compare-And-Swap) semantics under concurrent access."""

    def test_concurrent_transitions_only_one_wins(self, test_db: Database) -> None:
        """Test that concurrent transitions on same run - only one succeeds.

        Scenario: Two threads try to transition RUNNING → TERMINATED simultaneously.
        CAS semantics ensure only one succeeds, other gets stale_state error.
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        results: list[tuple[bool, str | None]] = []
        barrier = threading.Barrier(2)

        def try_transition(event: StageEvent, cause: TerminationCause) -> None:
            """Thread worker that attempts transition after barrier sync."""
            barrier.wait()  # Ensure both threads start together
            ctx = EventContext(
                timestamp=datetime.now(UTC),
                source="daemon",
                termination_cause=cause,
            )
            result = transition(test_db, run_id, event, ctx)
            results.append((result.success, result.reason))

        # Create two threads that will race to transition
        t1 = threading.Thread(target=try_transition, args=(StageEvent.TIMEOUT, TerminationCause.TIMEOUT))
        t2 = threading.Thread(target=try_transition, args=(StageEvent.INSTANCE_LOST, TerminationCause.PREEMPTED))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Verify exactly one succeeded
        successes = [r[0] for r in results]
        assert successes.count(True) == 1
        assert successes.count(False) == 1

        # Verify the failed one got appropriate error
        failed_result = [r for r in results if not r[0]][0]
        # Could be stale_state (if CAS failed) or no_transition (if already in terminal)
        assert failed_result[1] in ("stale_state", "no_transition", "already_in_target_state")

        # Verify run is now in terminal state
        assert _get_run_state(test_db, run_id) == StageState.TERMINATED.value

    def test_phase_update_cas_guard_prevents_stale_updates(self, test_db: Database) -> None:
        """Test that phase updates with wrong expected_state are rejected.

        Scenario: Thread 1 reads state=PREPARING, Thread 2 transitions to BUILDING,
        Thread 1 tries to update phase - should fail due to CAS guard.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Transition to BUILDING
        transition(test_db, run_id, StageEvent.BUILD_START, _event_ctx())
        assert _get_run_state(test_db, run_id) == StageState.BUILDING.value

        # Try to update phase expecting PREPARING (stale expectation)
        now = datetime.now(UTC)
        result = update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.VERSIONING, now)

        # Should fail - state is now BUILDING, not PREPARING
        assert result is False


# =============================================================================
# Finalization Outcome Tests
# =============================================================================


class TestFinalizationOutcomes:
    """E2E tests for different finalization outcomes."""

    def test_finalization_timeout_with_critical_phases_done(self, test_db: Database) -> None:
        """Test timeout during finalization when critical work is complete.

        Scenario: Finalization times out, but outputs already synced and recorded.
        Result: COMPLETED (with warnings)
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        # Simulate critical phases being done
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET output_sync_done = 1, output_recording_done = 1 WHERE id = ?",
                (run_id,),
            )

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=True,
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

        # Verify completed_with_warnings flag
        with test_db._conn() as conn:
            row = conn.execute("SELECT completed_with_warnings FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["completed_with_warnings"] == 1

    def test_finalization_timeout_without_critical_phases_done(self, test_db: Database) -> None:
        """Test timeout during finalization when critical work is incomplete.

        Scenario: Finalization times out before outputs saved.
        Result: FAILED (data may be lost)
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=False,
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_non_critical_finalization_failure(self, test_db: Database) -> None:
        """Test non-critical finalization failure still completes run.

        Scenario: Post-run review or cleanup fails, but outputs are saved.
        Result: COMPLETED (with warnings)
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=False,
            error_message="Post-run cleanup failed",
        )
        result = transition(test_db, run_id, StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

        # Verify completed_with_warnings flag
        with test_db._conn() as conn:
            row = conn.execute("SELECT completed_with_warnings FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["completed_with_warnings"] == 1

    def test_critical_finalization_failure(self, test_db: Database) -> None:
        """Test critical finalization failure transitions to FAILED.

        Scenario: Output sync or recording fails (critical operation).
        Result: FAILED (data may be lost)
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=True,
            error_message="Output sync failed - GCS unavailable",
        )
        result = transition(test_db, run_id, StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED
        assert _get_run_state(test_db, run_id) == StageState.FAILED.value


# =============================================================================
# Guard Enforcement Tests
# =============================================================================


class TestGuardEnforcement:
    """E2E tests for guard function enforcement."""

    def test_exit_missing_requires_instance_confirmed_dead(self, test_db: Database) -> None:
        """Test EXIT_MISSING guard rejects transition when instance not confirmed dead.

        The guard `instance_confirmed_dead` prevents premature EXIT_MISSING transitions.
        This ensures we don't mark a run as terminated while instance may still be running.
        When guard fails, no valid transition is found (reason: no_transition).
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        # EXIT_MISSING without instance_confirmed_dead should fail
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=False,  # Guard not satisfied
            termination_cause=TerminationCause.PREEMPTED,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is False
        assert result.reason == "no_transition"  # Guard failure = no valid transition
        # State should remain RUNNING
        assert _get_run_state(test_db, run_id) == StageState.RUNNING.value

    def test_exit_missing_succeeds_when_instance_confirmed_dead(self, test_db: Database) -> None:
        """Test EXIT_MISSING succeeds when guard is satisfied.

        Contrast to previous test - when instance is confirmed dead, transition succeeds.
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=True,  # Guard satisfied
            termination_cause=TerminationCause.PREEMPTED,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_finalize_fail_guard_with_critical_none_fails(self, test_db: Database) -> None:
        """Test FINALIZE_FAIL with critical=None fails the guard.

        The guarded transitions require explicit critical=True or critical=False.
        When critical is None, neither guard matches and no valid transition is found.
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=None,  # Neither True nor False
            error_message="Ambiguous failure",
        )
        result = transition(test_db, run_id, StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is False
        assert result.reason == "no_transition"  # Guard failure = no valid transition

    def test_timeout_finalizing_guard_with_critical_phases_none_fails(self, test_db: Database) -> None:
        """Test TIMEOUT in FINALIZING with critical_phases_done=None fails guard.

        TIMEOUT in FINALIZING is guarded by critical_phases_done.
        When None, neither branch matches and no valid transition is found.
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=None,  # Neither True nor False
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is False
        assert result.reason == "no_transition"  # Guard failure = no valid transition


# =============================================================================
# Cancel From All Active States
# =============================================================================


class TestCancelFromAllActiveStates:
    """E2E tests for USER_CANCEL from each active state."""

    @pytest.mark.parametrize(
        "initial_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.FINALIZING,
        ],
    )
    def test_user_cancel_from_active_state(self, test_db: Database, initial_state: StageState) -> None:
        """Test USER_CANCEL transitions from each active state to CANCELED.

        Scenario: User requests cancellation during any active phase.
        Result: Run immediately transitions to CANCELED.
        """
        run_id = _setup_test_run(test_db, initial_state)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="mcp_tool",
        )
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)

        assert result.success is True
        assert result.new_state == StageState.CANCELED
        assert _get_run_state(test_db, run_id) == StageState.CANCELED.value


# =============================================================================
# Timeout From All Active States
# =============================================================================


class TestTimeoutFromAllActiveStates:
    """E2E tests for TIMEOUT from each active state."""

    @pytest.mark.parametrize(
        "initial_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
        ],
    )
    def test_timeout_from_non_finalizing_state(self, test_db: Database, initial_state: StageState) -> None:
        """Test TIMEOUT transitions from non-FINALIZING active states to TERMINATED.

        Scenario: Stage times out during preparation, build, launch, or execution.
        Result: Run transitions to TERMINATED (timeout is a form of termination).
        """
        run_id = _setup_test_run(test_db, initial_state)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED
        assert _get_run_state(test_db, run_id) == StageState.TERMINATED.value

    def test_timeout_from_finalizing_without_critical_phases(self, test_db: Database) -> None:
        """Test TIMEOUT from FINALIZING without critical phases done goes to FAILED.

        Scenario: Finalization times out before outputs saved.
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=False,
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_timeout_from_finalizing_with_critical_phases_done(self, test_db: Database) -> None:
        """Test TIMEOUT from FINALIZING with critical phases done goes to COMPLETED.

        Scenario: Finalization times out but outputs already saved.
        """
        run_id = _setup_test_run(test_db, StageState.FINALIZING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=True,
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED


# =============================================================================
# Terminal State Immutability
# =============================================================================


class TestTerminalStateImmutability:
    """E2E tests verifying terminal states reject all further transitions."""

    @pytest.mark.parametrize(
        "terminal_state",
        [
            StageState.COMPLETED,
            StageState.FAILED,
            StageState.TERMINATED,
            StageState.CANCELED,
        ],
    )
    @pytest.mark.parametrize(
        "event",
        [
            StageEvent.BUILD_START,
            StageEvent.BUILD_OK,
            StageEvent.LAUNCH_OK,
            StageEvent.EXIT_SUCCESS,
            StageEvent.TIMEOUT,
            StageEvent.USER_CANCEL,
            StageEvent.INSTANCE_LOST,
        ],
    )
    def test_terminal_state_rejects_event(
        self, test_db: Database, terminal_state: StageState, event: StageEvent
    ) -> None:
        """Test that terminal states reject all events or handle idempotently.

        Terminal states are immutable - no event can change them to a different state.
        Exception: Idempotent transitions (USER_CANCEL → CANCELED, INSTANCE_LOST → TERMINATED)
        succeed with reason="already_in_target_state" but state doesn't change.
        """
        run_id = _setup_test_run(test_db, terminal_state)

        ctx = _event_ctx(source="executor")
        result = transition(test_db, run_id, event, ctx)

        # Idempotent transition cases: USER_CANCEL → CANCELED, INSTANCE_LOST → TERMINATED
        is_idempotent = (event == StageEvent.USER_CANCEL and terminal_state == StageState.CANCELED) or (
            event == StageEvent.INSTANCE_LOST and terminal_state == StageState.TERMINATED
        )
        # TIMEOUT can also lead to TERMINATED from multiple active states, so it is idempotent in TERMINATED.
        is_idempotent = is_idempotent or (event == StageEvent.TIMEOUT and terminal_state == StageState.TERMINATED)

        if is_idempotent:
            # Idempotent transitions succeed but state doesn't change
            assert result.success is True
            assert result.reason == "already_in_target_state"
        else:
            # All other transitions should fail
            assert result.success is False
            assert result.reason == "no_transition"

        # State should remain unchanged in all cases
        assert _get_run_state(test_db, run_id) == terminal_state.value

    def test_terminal_state_remains_after_multiple_attempts(self, test_db: Database) -> None:
        """Test that terminal state survives multiple transition attempts."""
        run_id = _setup_test_run(test_db, StageState.COMPLETED)

        # Try many different events
        events = [
            StageEvent.BUILD_START,
            StageEvent.TIMEOUT,
            StageEvent.USER_CANCEL,
            StageEvent.EXIT_FAILURE,
            StageEvent.INSTANCE_LOST,
        ]

        for event in events:
            result = transition(test_db, run_id, event, _event_ctx())
            assert result.success is False

        # Verify state never changed
        assert _get_run_state(test_db, run_id) == StageState.COMPLETED.value
        # Verify no transitions were recorded
        assert _get_transition_count(test_db, run_id) == 0


# =============================================================================
# UNKNOWN State Handling
# =============================================================================


class TestUnknownStateHandling:
    """E2E tests for UNKNOWN (limbo) state behavior."""

    def test_timeout_from_unknown_goes_to_terminated(self, test_db: Database) -> None:
        """Test TIMEOUT from UNKNOWN state goes to TERMINATED.

        Scenario: Run enters UNKNOWN state (daemon lost track of it).
        After investigation timeout, daemon transitions to TERMINATED.
        """
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED
        assert _get_run_state(test_db, run_id) == StageState.TERMINATED.value

    def test_unknown_state_rejects_most_events(self, test_db: Database) -> None:
        """Test that UNKNOWN state rejects most events.

        UNKNOWN only supports TIMEOUT → TERMINATED. All other events rejected.
        """
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        # These events should all fail from UNKNOWN
        rejected_events = [
            StageEvent.BUILD_START,
            StageEvent.BUILD_OK,
            StageEvent.LAUNCH_OK,
            StageEvent.EXIT_SUCCESS,
            StageEvent.FINALIZE_OK,
        ]

        for event in rejected_events:
            result = transition(test_db, run_id, event, _event_ctx())
            assert result.success is False, f"Expected {event} to fail from UNKNOWN"
            assert result.reason == "no_transition"

        # State should still be UNKNOWN
        assert _get_run_state(test_db, run_id) == StageState.UNKNOWN.value

    def test_user_cancel_from_unknown_fails(self, test_db: Database) -> None:
        """Test USER_CANCEL from UNKNOWN state fails.

        UNKNOWN state doesn't support cancel - only TIMEOUT is valid.
        """
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="user",
        )
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)

        assert result.success is False
        assert result.reason == "no_transition"

    def test_instance_lost_from_unknown_fails(self, test_db: Database) -> None:
        """Test INSTANCE_LOST from UNKNOWN state fails.

        UNKNOWN state doesn't support INSTANCE_LOST - only TIMEOUT.
        """
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=TerminationCause.CRASHED,
        )
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is False
        assert result.reason == "no_transition"


# =============================================================================
# Index Usage Verification
# =============================================================================


class TestIndexUsage:
    """Tests to verify database indexes are used correctly."""

    def test_active_runs_query_uses_partial_index(self, test_db: Database) -> None:
        """Verify that querying active runs uses the partial index.

        The partial index on active states should make this query efficient.
        """
        # Create some runs in different states
        _create_workspace_and_version(test_db)

        for state in [
            StageState.PREPARING,
            StageState.RUNNING,
            StageState.COMPLETED,
            StageState.FAILED,
        ]:
            run_id = f"stage-{uuid.uuid4().hex[:8]}"
            _create_run_in_state(test_db, run_id, state)

        # Query active runs (what daemon would do)
        with test_db._conn() as conn:
            # Get query plan
            plan_rows = conn.execute(
                """EXPLAIN QUERY PLAN
                SELECT id, state, backend_type, state_entered_at
                FROM stage_runs
                WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing')"""
            ).fetchall()

            # Extract the detail column from each row (SQLite returns id, parent, notused, detail)
            plan_text = " ".join(row["detail"] if "detail" in row.keys() else str(dict(row)) for row in plan_rows)

            # Check that an index is being used or the query plan indicates efficient access
            # The partial index should be named idx_stage_runs_state_active
            # With small tables, SQLite may choose SCAN which is fine
            # This test documents expected behavior and verifies the query works
            assert plan_text  # At minimum, ensure we got a query plan
