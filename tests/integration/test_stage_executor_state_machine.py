"""Integration tests for state machine database layer.

Tests that verify the state machine properly integrates with the database layer:
- Initial state is correctly set when creating stage runs
- State transitions work correctly via the transition() function
- Phase updates work correctly via the update_phase() function
- Audit trail is recorded in stage_state_transitions table

These tests use the state machine primitives directly (transition, update_phase)
to verify database integration. Full stage executor integration tests are in
the e2e test suite.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

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


def _create_svs_review(
    db: Database,
    stage_run_id: str,
    decision: str = "blocked",
    review_type: str = "pre_run",
) -> str:
    """Create an SVS review record for testing. Returns the review ID."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        cursor = conn.execute(
            """INSERT INTO svs_reviews
            (stage_run_id, review_type, model_used, prompt_hash, decision, reviewed_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (stage_run_id, review_type, "test-model", "test-hash", decision, now),
        )
        return str(cursor.lastrowid)


def _get_state_transitions(db: Database, run_id: str) -> list[dict]:
    """Get all state transitions for a run (normalized columns)."""
    with db._conn() as conn:
        rows = conn.execute(
            """SELECT from_state, to_state, event, created_at, phase, termination_cause,
                      exit_code, exit_code_exists, error_message, source
            FROM stage_state_transitions
            WHERE stage_run_id = ?
            ORDER BY id""",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def _get_run_state(db: Database, run_id: str) -> dict:
    """Get current state and phase of a run."""
    with db._conn() as conn:
        row = conn.execute(
            "SELECT state, phase FROM stage_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        return dict(row) if row else {}


def _get_run_extras(db: Database, run_id: str) -> dict:
    """Get additional columns from stage_runs for verification."""
    with db._conn() as conn:
        row = conn.execute(
            """SELECT completed_with_warnings, termination_cause
            FROM stage_runs WHERE id = ?""",
            (run_id,),
        ).fetchone()
        return dict(row) if row else {}


def _get_run_timestamps(db: Database, run_id: str) -> dict:
    """Get timestamp columns from stage_runs for verification."""
    with db._conn() as conn:
        row = conn.execute(
            """SELECT started_at, state_entered_at, phase_updated_at, completed_at
            FROM stage_runs WHERE id = ?""",
            (run_id,),
        ).fetchone()
        return dict(row) if row else {}


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


def _event_ctx(source: str = "executor", **kwargs: object) -> EventContext:
    """Create an EventContext with timestamp and source, plus any extra fields."""
    return EventContext(timestamp=datetime.now(UTC), source=source, **kwargs)  # type: ignore[arg-type]


class TestCreateStageRunInitialState:
    """Tests that verify database layer sets correct initial state for new runs.

    When create_stage_run() is called, the new run should start in PREPARING
    state with GCS_CHECK phase, matching the state machine specification.
    """

    def test_new_run_starts_in_preparing_state(self, test_db: Database) -> None:
        """New stage runs should start in PREPARING state."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create a stage run using the database method
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        state_info = _get_run_state(test_db, run_id)
        assert state_info["state"] == StageState.PREPARING.value

    def test_new_run_starts_with_gcs_check_phase(self, test_db: Database) -> None:
        """New stage runs should start in GCS_CHECK phase."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        state_info = _get_run_state(test_db, run_id)
        assert state_info["phase"] == ProgressPhase.GCS_CHECK.value

    def test_new_run_records_run_start_transition(self, test_db: Database) -> None:
        """create_stage_run() records a run_start pseudo-event for audit."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        transitions = _get_state_transitions(test_db, run_id)
        assert len(transitions) == 1
        t0 = transitions[0]
        assert t0["from_state"] == "none"
        assert t0["to_state"] == StageState.PREPARING.value
        assert t0["event"] == "run_start"
        assert t0["phase"] == ProgressPhase.GCS_CHECK.value
        assert t0["termination_cause"] is None
        assert t0["exit_code"] is None
        assert t0["exit_code_exists"] in (0, None)
        assert t0["error_message"] is None
        assert t0["source"] == "executor"

    def test_new_run_sets_phase_updated_at(self, test_db: Database) -> None:
        """create_stage_run() should set phase_updated_at at creation."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        ts = _get_run_timestamps(test_db, run_id)
        assert ts["phase_updated_at"] is not None
        assert ts["phase_updated_at"] == ts["state_entered_at"]


class TestBuildEventEmission:
    """Tests for BUILD_START, BUILD_OK, BUILD_FAIL transitions.

    These transitions occur during the Docker image build phase.
    BUILD_START moves from PREPARING to BUILDING.
    BUILD_OK moves from BUILDING to LAUNCHING.
    BUILD_FAIL moves from BUILDING to FAILED (terminal).
    """

    def test_build_start_transitions_to_building(self, test_db: Database) -> None:
        """BUILD_START event should transition from PREPARING to BUILDING."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in PREPARING state
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Emit BUILD_START
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.BUILD_START, ctx)

        assert result.success is True
        assert result.new_state == StageState.BUILDING

        state_info = _get_run_state(test_db, run_id)
        assert state_info["state"] == StageState.BUILDING.value

    def test_build_ok_transitions_to_launching(self, test_db: Database) -> None:
        """BUILD_OK event should transition from BUILDING to LAUNCHING."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in BUILDING state
        _create_run_in_state(test_db, run_id, StageState.BUILDING)

        # Emit BUILD_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.BUILD_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.LAUNCHING

    def test_build_fail_transitions_to_failed(self, test_db: Database) -> None:
        """BUILD_FAIL event should transition from BUILDING to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in BUILDING state
        _create_run_in_state(test_db, run_id, StageState.BUILDING)

        # Emit BUILD_FAIL
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", error_message="Docker build failed")
        result = transition(test_db, run_id, StageEvent.BUILD_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestLaunchEventEmission:
    """Tests for LAUNCH_OK, LAUNCH_FAIL transitions.

    These transitions occur when starting the container/instance.
    LAUNCH_OK moves from LAUNCHING to RUNNING.
    LAUNCH_FAIL moves from LAUNCHING to FAILED (terminal).
    """

    def test_launch_ok_transitions_to_running(self, test_db: Database) -> None:
        """LAUNCH_OK event should transition from LAUNCHING to RUNNING."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in LAUNCHING state
        _create_run_in_state(test_db, run_id, StageState.LAUNCHING)

        # Emit LAUNCH_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.LAUNCH_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.RUNNING

    def test_launch_fail_transitions_to_failed(self, test_db: Database) -> None:
        """LAUNCH_FAIL event should transition from LAUNCHING to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in LAUNCHING state
        _create_run_in_state(test_db, run_id, StageState.LAUNCHING)

        # Emit LAUNCH_FAIL
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", error_message="Container failed to start")
        result = transition(test_db, run_id, StageEvent.LAUNCH_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestPhaseUpdates:
    """Tests for phase updates within a state.

    Phases track fine-grained progress within a state (e.g., GCS_CHECK,
    VERSIONING, PIPELINE_LOAD during PREPARING). Phase updates do not
    trigger state transitions.
    """

    def test_phase_can_be_updated_within_state(self, test_db: Database) -> None:
        """Phase updates should work without changing state."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in PREPARING state
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Update phase to VERSIONING (requires expected_state and timestamp)
        now = datetime.now(UTC)
        result = update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.VERSIONING, now)

        assert result is True
        state_info = _get_run_state(test_db, run_id)
        assert state_info["state"] == StageState.PREPARING.value
        assert state_info["phase"] == ProgressPhase.VERSIONING.value

        transitions = _get_state_transitions(test_db, run_id)
        assert len(transitions) == 2
        assert transitions[-1]["event"] == "phase_update"
        assert transitions[-1]["from_state"] == StageState.PREPARING.value
        assert transitions[-1]["to_state"] == StageState.PREPARING.value
        assert transitions[-1]["phase"] == ProgressPhase.VERSIONING.value
        assert transitions[-1]["source"] == "executor"

    def test_phase_updates_to_pipeline_load(self, test_db: Database) -> None:
        """Phase should update to PIPELINE_LOAD during preparation."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        now = datetime.now(UTC)
        update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.PIPELINE_LOAD, now)

        state_info = _get_run_state(test_db, run_id)
        assert state_info["phase"] == ProgressPhase.PIPELINE_LOAD.value

        transitions = _get_state_transitions(test_db, run_id)
        assert len(transitions) == 2
        assert transitions[-1]["event"] == "phase_update"
        assert transitions[-1]["phase"] == ProgressPhase.PIPELINE_LOAD.value

    def test_phase_update_rejects_older_timestamp(self, test_db: Database) -> None:
        """update_phase() rejects out-of-order timestamps (prevents phase regression)."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        newer = datetime.now(UTC)
        assert update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.VERSIONING, newer) is True

        older = newer - timedelta(microseconds=1)
        assert update_phase(test_db, run_id, StageState.PREPARING, ProgressPhase.GCS_CHECK, older) is False

        # Phase should remain at VERSIONING
        state_info = _get_run_state(test_db, run_id)
        assert state_info["phase"] == ProgressPhase.VERSIONING.value

        # Only run_start + the successful phase_update should be recorded
        transitions = _get_state_transitions(test_db, run_id)
        assert [t["event"] for t in transitions] == ["run_start", "phase_update"]


class TestPrepareFailEventEmission:
    """Tests for PREPARE_FAIL and SVS_BLOCK transitions.

    These transitions handle failures during the preparation phase.
    PREPARE_FAIL handles validation/preflight failures.
    SVS_BLOCK handles pre-run review rejections.
    """

    def test_prepare_fail_transitions_to_failed(self, test_db: Database) -> None:
        """PREPARE_FAIL event should transition from PREPARING to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Emit PREPARE_FAIL
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor", error_message="Preflight validation failed")
        result = transition(test_db, run_id, StageEvent.PREPARE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_svs_block_transitions_to_failed(self, test_db: Database) -> None:
        """SVS_BLOCK event should transition from PREPARING to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Create an SVS review record for the run
        svs_review_id = _create_svs_review(test_db, run_id, decision="blocked")

        # Emit SVS_BLOCK
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            svs_review_id=svs_review_id,
            error_message="SVS pre-run review blocked execution",
        )
        result = transition(test_db, run_id, StageEvent.SVS_BLOCK, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestPostRunEventEmission:
    """Tests for POST_RUN_OK and POST_RUN_FAIL transitions (v1.2).

    These transitions handle the post-run phase after execution.
    POST_RUN_OK moves from POST_RUN to AWAITING_USER_FINALIZATION.
    POST_RUN_FAIL outcome depends on the critical flag:
    - critical=True → FAILED
    - critical=False → AWAITING_USER_FINALIZATION (non-critical failures still need user finalization)
    """

    def test_post_run_ok_transitions_to_awaiting_finalization(self, test_db: Database) -> None:
        """POST_RUN_OK event should transition from POST_RUN to AWAITING_USER_FINALIZATION."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in POST_RUN state
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        # Emit POST_RUN_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.POST_RUN_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

    def test_post_run_fail_critical_transitions_to_failed(self, test_db: Database) -> None:
        """POST_RUN_FAIL with critical=True should transition to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in POST_RUN state
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        # Emit POST_RUN_FAIL with critical=True
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=True,
            error_message="Output recording failed",
        )
        result = transition(test_db, run_id, StageEvent.POST_RUN_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_post_run_fail_non_critical_transitions_to_awaiting_finalization(self, test_db: Database) -> None:
        """POST_RUN_FAIL with critical=False should transition to AWAITING_USER_FINALIZATION."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in POST_RUN state
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        # Emit POST_RUN_FAIL with critical=False
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=False,
            error_message="Post-run review failed (non-critical)",
        )
        result = transition(test_db, run_id, StageEvent.POST_RUN_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION


class TestFullLifecycle:
    """End-to-end lifecycle tests.

    These tests verify that a complete run lifecycle works correctly,
    from PREPARING through all states to a terminal state.
    """

    def test_successful_run_lifecycle(self, test_db: Database) -> None:
        """Test complete successful run (v1.2).

        PREPARING→BUILDING→LAUNCHING→RUNNING→POST_RUN→AWAITING_USER_FINALIZATION→COMPLETED
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Start in PREPARING
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )
        assert _get_run_state(test_db, run_id)["state"] == StageState.PREPARING.value

        # PREPARING → BUILDING
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.BUILD_START, ctx)
        assert result.new_state == StageState.BUILDING

        # BUILDING → LAUNCHING
        result = transition(test_db, run_id, StageEvent.BUILD_OK, ctx)
        assert result.new_state == StageState.LAUNCHING

        # LAUNCHING → RUNNING
        result = transition(test_db, run_id, StageEvent.LAUNCH_OK, ctx)
        assert result.new_state == StageState.RUNNING

        # RUNNING → POST_RUN (via EXIT_SUCCESS)
        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon", exit_code=0, exit_code_exists=True)
        result = transition(test_db, run_id, StageEvent.EXIT_SUCCESS, ctx)
        assert result.new_state == StageState.POST_RUN

        # POST_RUN → AWAITING_USER_FINALIZATION
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.POST_RUN_OK, ctx)
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

        # AWAITING_USER_FINALIZATION → COMPLETED (via USER_FINALIZE)
        ctx = EventContext(timestamp=datetime.now(UTC), source="mcp_tool")
        result = transition(test_db, run_id, StageEvent.USER_FINALIZE, ctx)
        assert result.new_state == StageState.COMPLETED

        # Verify final state
        assert _get_run_state(test_db, run_id)["state"] == StageState.COMPLETED.value

        # Verify audit trail - comprehensive check of all columns
        transitions = _get_state_transitions(test_db, run_id)
        # run_start + 6 state transitions (v1.2 adds AWAITING_USER_FINALIZATION)
        assert len(transitions) == 7

        # Verify run_start pseudo-event
        assert transitions[0]["from_state"] == "none"
        assert transitions[0]["to_state"] == StageState.PREPARING.value
        assert transitions[0]["event"] == "run_start"
        assert transitions[0]["created_at"] is not None

        # Verify first state transition (PREPARING → BUILDING via BUILD_START)
        assert transitions[1]["from_state"] == StageState.PREPARING.value
        assert transitions[1]["to_state"] == StageState.BUILDING.value
        assert transitions[1]["event"] == StageEvent.BUILD_START.value
        assert transitions[1]["created_at"] is not None

        # Verify last transition (AWAITING_USER_FINALIZATION → COMPLETED via USER_FINALIZE)
        assert transitions[-1]["from_state"] == StageState.AWAITING_USER_FINALIZATION.value
        assert transitions[-1]["to_state"] == StageState.COMPLETED.value
        assert transitions[-1]["event"] == StageEvent.USER_FINALIZE.value
        assert transitions[-1]["created_at"] is not None

        # Verify middle transitions have correct from_state chain
        assert transitions[2]["from_state"] == StageState.BUILDING.value
        assert transitions[3]["from_state"] == StageState.LAUNCHING.value
        assert transitions[4]["from_state"] == StageState.RUNNING.value
        assert transitions[5]["from_state"] == StageState.POST_RUN.value


class TestTransitionErrorCases:
    """Tests for transition() error handling.

    These tests verify the database layer correctly handles error cases:
    - Non-existent runs
    - Invalid events for current state
    """

    def test_transition_on_nonexistent_run_returns_not_found(self, test_db: Database) -> None:
        """transition() returns not_found for non-existent run."""
        _create_workspace_and_version(test_db)

        ctx = EventContext(timestamp=datetime.now(UTC), source="test")
        result = transition(test_db, "stage-nonexistent", StageEvent.BUILD_START, ctx)

        assert result.success is False
        assert result.reason == "not_found"

    def test_transition_with_invalid_event_returns_no_transition(self, test_db: Database) -> None:
        """transition() returns no_transition for invalid event in current state."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in PREPARING state
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Try to emit BUILD_OK from PREPARING (invalid - should be BUILD_START first)
        ctx = EventContext(timestamp=datetime.now(UTC), source="test")
        result = transition(test_db, run_id, StageEvent.BUILD_OK, ctx)

        assert result.success is False
        assert result.reason == "no_transition"


class TestUpdatePhaseErrorCases:
    """Tests for update_phase() error handling.

    These tests verify the database layer correctly handles CAS guard failures.
    """

    def test_update_phase_with_wrong_expected_state_returns_false(self, test_db: Database) -> None:
        """update_phase() returns False when expected_state doesn't match actual state."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in PREPARING state
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Try to update phase expecting BUILDING state (run is in PREPARING)
        now = datetime.now(UTC)
        result = update_phase(test_db, run_id, StageState.BUILDING, ProgressPhase.DOCKER_BUILD, now)

        # Should return False due to CAS guard failure
        assert result is False

        # Phase should remain unchanged
        state_info = _get_run_state(test_db, run_id)
        assert state_info["phase"] == ProgressPhase.GCS_CHECK.value

    def test_update_phase_on_nonexistent_run_returns_false(self, test_db: Database) -> None:
        """update_phase() returns False for non-existent run (UPDATE affects 0 rows)."""
        _create_workspace_and_version(test_db)

        # Try to update phase on a run that doesn't exist
        now = datetime.now(UTC)
        result = update_phase(test_db, "stage-nonexistent", StageState.PREPARING, ProgressPhase.VERSIONING, now)

        # Should return False since no row was updated
        assert result is False


class TestOtherEventTypes:
    """Tests for additional event types to ensure database layer handles them.

    These tests verify EXIT_FAILURE, USER_CANCEL, TIMEOUT, and INSTANCE_LOST
    events work correctly with the real database.
    """

    def test_exit_failure_transitions_to_failed(self, test_db: Database) -> None:
        """EXIT_FAILURE event should transition from RUNNING to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code=1,
            exit_code_exists=True,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_FAILURE, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_user_cancel_transitions_to_canceled(self, test_db: Database) -> None:
        """USER_CANCEL event should transition from RUNNING to CANCELED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="mcp_tool",
            error_message="User requested cancellation",
        )
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)

        assert result.success is True
        assert result.new_state == StageState.CANCELED

    def test_timeout_transitions_to_terminated(self, test_db: Database) -> None:
        """TIMEOUT event should transition from RUNNING to TERMINATED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon")
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_instance_lost_transitions_to_terminated(self, test_db: Database) -> None:
        """INSTANCE_LOST event should transition from RUNNING to TERMINATED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon")
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED


class TestExitMissingGuardedTransition:
    """Tests for EXIT_MISSING event with instance_confirmed_dead guard."""

    def test_exit_missing_with_instance_confirmed_dead_transitions_to_terminated(self, test_db: Database) -> None:
        """EXIT_MISSING with instance_confirmed_dead=True should transition to TERMINATED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=True,
            termination_cause=TerminationCause.CRASHED,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_exit_missing_without_instance_confirmed_dead_fails_guard(self, test_db: Database) -> None:
        """EXIT_MISSING without instance_confirmed_dead=True should fail guard."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=False,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is False
        assert result.reason == "no_transition"


class TestTimeoutInPostRunGuardedTransitions:
    """Tests for TIMEOUT event in POST_RUN state with critical_phases_done guards (v1.2)."""

    def test_timeout_in_post_run_with_critical_phases_done_true_transitions_to_awaiting(
        self, test_db: Database
    ) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=True goes to AWAITING_USER_FINALIZATION."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon", critical_phases_done=True)
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

    def test_timeout_in_post_run_with_critical_phases_done_false_transitions_to_failed(self, test_db: Database) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=False should transition to FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon", critical_phases_done=False)
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestCompletedWithWarningsFlag:
    """Tests for completed_with_warnings flag persistence."""

    def test_post_run_fail_non_critical_sets_completed_with_warnings(self, test_db: Database) -> None:
        """POST_RUN_FAIL(critical=False) should set completed_with_warnings=1 in database.

        In v1.2, the transition goes to AWAITING_USER_FINALIZATION, not COMPLETED.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        # Emit POST_RUN_FAIL with critical=False
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=False,
            error_message="Non-critical finalization error",
        )
        result = transition(test_db, run_id, StageEvent.POST_RUN_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

        # Verify completed_with_warnings flag is set in database
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT completed_with_warnings FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert row["completed_with_warnings"] == 1

    def test_timeout_in_post_run_with_critical_phases_done_sets_completed_with_warnings(
        self, test_db: Database
    ) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=True sets completed_with_warnings=1.

        In v1.2, the transition goes to AWAITING_USER_FINALIZATION, not COMPLETED.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.POST_RUN)

        # Emit TIMEOUT with critical_phases_done=True
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=True,
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

        # Verify completed_with_warnings flag is set in database
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT completed_with_warnings FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert row["completed_with_warnings"] == 1


class TestTerminationCausePersistence:
    """Tests for termination_cause column persistence."""

    def test_exit_missing_persists_termination_cause(self, test_db: Database) -> None:
        """EXIT_MISSING should persist termination_cause to database."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        # Emit EXIT_MISSING with termination_cause=CRASHED
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code_exists=False,
            instance_confirmed_dead=True,
            termination_cause=TerminationCause.CRASHED,
        )
        result = transition(test_db, run_id, StageEvent.EXIT_MISSING, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

        # Verify termination_cause is persisted
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT termination_cause FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert row["termination_cause"] == TerminationCause.CRASHED.value

    def test_instance_lost_persists_termination_cause(self, test_db: Database) -> None:
        """INSTANCE_LOST should persist termination_cause to database."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run_in_state(test_db, run_id, StageState.RUNNING)

        # Emit INSTANCE_LOST with termination_cause=PREEMPTED
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=TerminationCause.PREEMPTED,
        )
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

        # Verify termination_cause is persisted
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT termination_cause FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert row["termination_cause"] == TerminationCause.PREEMPTED.value


# =============================================================================
# Missing Transition Coverage Tests
# =============================================================================


class TestTimeoutFromAllStates:
    """Tests for TIMEOUT event from all active states (except POST_RUN which has guarded transitions)."""

    @pytest.mark.parametrize(
        "from_state",
        [StageState.PREPARING, StageState.BUILDING, StageState.LAUNCHING, StageState.RUNNING],
    )
    def test_timeout_from_active_state_transitions_to_terminated(
        self, test_db: Database, from_state: StageState
    ) -> None:
        """TIMEOUT from PREPARING/BUILDING/LAUNCHING/RUNNING should transition to TERMINATED."""
        run_id = _setup_test_run(test_db, from_state)

        result = transition(test_db, run_id, StageEvent.TIMEOUT, _event_ctx(source="daemon"))

        assert result.success is True
        assert result.new_state == StageState.TERMINATED


class TestUserCancelFromAllStates:
    """Tests for USER_CANCEL event from all active states."""

    @pytest.mark.parametrize(
        "from_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.POST_RUN,
        ],
    )
    def test_user_cancel_from_active_state_transitions_to_canceled(
        self, test_db: Database, from_state: StageState
    ) -> None:
        """USER_CANCEL from any active state should transition to CANCELED."""
        run_id = _setup_test_run(test_db, from_state)

        ctx = _event_ctx(source="mcp_tool", error_message="User requested cancellation")
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)

        assert result.success is True
        assert result.new_state == StageState.CANCELED


class TestInstanceLostFromAllStates:
    """Tests for INSTANCE_LOST event from all active states."""

    @pytest.mark.parametrize(
        "from_state",
        [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.POST_RUN,
        ],
    )
    def test_instance_lost_from_active_state_transitions_to_terminated(
        self, test_db: Database, from_state: StageState
    ) -> None:
        """INSTANCE_LOST from any active state should transition to TERMINATED."""
        run_id = _setup_test_run(test_db, from_state)

        ctx = _event_ctx(source="daemon", termination_cause=TerminationCause.PREEMPTED)
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED


class TestUnknownStateTransitions:
    """Tests for transitions from UNKNOWN state.

    UNKNOWN is a limbo state - only TIMEOUT is valid, other events are rejected.
    """

    def test_timeout_from_unknown_transitions_to_terminated(self, test_db: Database) -> None:
        """TIMEOUT from UNKNOWN state should transition to TERMINATED."""
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        result = transition(test_db, run_id, StageEvent.TIMEOUT, _event_ctx(source="daemon"))

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_user_cancel_from_unknown_returns_no_transition(self, test_db: Database) -> None:
        """USER_CANCEL from UNKNOWN should fail - only TIMEOUT is valid."""
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        ctx = _event_ctx(source="mcp_tool", error_message="User cancel attempt")
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)

        assert result.success is False
        assert result.reason == "no_transition"
        # Verify state hasn't changed
        assert _get_run_state(test_db, run_id)["state"] == StageState.UNKNOWN.value

    def test_instance_lost_from_unknown_returns_no_transition(self, test_db: Database) -> None:
        """INSTANCE_LOST from UNKNOWN should fail - only TIMEOUT is valid."""
        run_id = _setup_test_run(test_db, StageState.UNKNOWN)

        ctx = _event_ctx(source="daemon", termination_cause=TerminationCause.PREEMPTED)
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        assert result.success is False
        assert result.reason == "no_transition"
        # Verify state hasn't changed
        assert _get_run_state(test_db, run_id)["state"] == StageState.UNKNOWN.value


class TestTerminalStateImmutability:
    """Tests verifying terminal states cannot be exited.

    Some (event, terminal_state) pairs are idempotent - the event's target state
    matches the current terminal state, so it returns success with 'already_in_target_state'.
    These are tested separately.
    """

    # Idempotent pairs: event has a transition that targets the same state we're already in
    # Note: TIMEOUT→TERMINATED is NOT idempotent because there's no TIMEOUT transition FROM TERMINATED
    IDEMPOTENT_PAIRS = {
        (StageEvent.POST_RUN_OK, StageState.COMPLETED),  # targets COMPLETED
        (StageEvent.USER_CANCEL, StageState.CANCELED),  # targets CANCELED
        (StageEvent.EXIT_FAILURE, StageState.FAILED),  # targets FAILED
        (StageEvent.INSTANCE_LOST, StageState.TERMINATED),  # targets TERMINATED
    }

    @pytest.mark.parametrize(
        "terminal_state",
        [StageState.COMPLETED, StageState.FAILED, StageState.TERMINATED, StageState.CANCELED],
    )
    @pytest.mark.parametrize(
        "event",
        [
            StageEvent.BUILD_START,
            StageEvent.BUILD_OK,
            StageEvent.LAUNCH_OK,
            StageEvent.EXIT_SUCCESS,
        ],
    )
    def test_terminal_states_reject_non_idempotent_events(
        self, test_db: Database, terminal_state: StageState, event: StageEvent
    ) -> None:
        """Events that would change state should be rejected from terminal states."""
        run_id = _setup_test_run(test_db, terminal_state)

        result = transition(test_db, run_id, event, _event_ctx(source="daemon"))

        assert result.success is False
        assert result.reason == "no_transition"
        # Verify state hasn't changed
        assert _get_run_state(test_db, run_id)["state"] == terminal_state.value

    @pytest.mark.parametrize(
        ("event", "terminal_state"),
        [
            (StageEvent.USER_FINALIZE, StageState.COMPLETED),  # v1.2: USER_FINALIZE → COMPLETED
            (StageEvent.USER_CANCEL, StageState.CANCELED),
            (StageEvent.EXIT_FAILURE, StageState.FAILED),
            (StageEvent.INSTANCE_LOST, StageState.TERMINATED),
            # TIMEOUT is NOT idempotent from TERMINATED - no transition defined from TERMINATED
            # POST_RUN_OK is NOT idempotent from COMPLETED (v1.2) - goes to AWAITING_USER_FINALIZATION
        ],
    )
    def test_idempotent_transitions_return_already_in_target_state(
        self, test_db: Database, event: StageEvent, terminal_state: StageState
    ) -> None:
        """Idempotent transitions return success with already_in_target_state."""
        run_id = _setup_test_run(test_db, terminal_state)

        result = transition(test_db, run_id, event, _event_ctx(source="daemon"))

        # Idempotent: success=True but reason indicates no actual change
        assert result.success is True
        assert result.reason == "already_in_target_state"
        assert result.new_state == terminal_state


class TestAuditTrailNormalizedColumns:
    """Tests verifying audit trail records normalized columns (no context_json)."""

    def test_transition_records_source_and_error_message(self, test_db: Database) -> None:
        """Transition should record source and error_message in dedicated columns."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            error_message="Test error",
        )
        transition(test_db, run_id, StageEvent.BUILD_START, ctx)

        transitions = _get_state_transitions(test_db, run_id)
        # run_start + build_start
        assert len(transitions) == 2

        assert transitions[1]["source"] == "executor"
        assert transitions[1]["error_message"] == "Test error"
        assert transitions[1]["created_at"] is not None

    def test_exit_failure_records_exit_code_fields(self, test_db: Database) -> None:
        """EXIT_FAILURE should record exit_code + exit_code_exists in dedicated columns."""
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code=42,
            exit_code_exists=True,
        )
        transition(test_db, run_id, StageEvent.EXIT_FAILURE, ctx)

        transitions = _get_state_transitions(test_db, run_id)
        assert transitions[0]["exit_code"] == 42
        assert transitions[0]["exit_code_exists"] == 1

    def test_terminated_records_termination_cause(self, test_db: Database) -> None:
        """INSTANCE_LOST should record termination_cause in dedicated column."""
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=TerminationCause.PREEMPTED,
        )
        transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)

        transitions = _get_state_transitions(test_db, run_id)
        assert transitions[0]["termination_cause"] == "preempted"


class TestNegativeGuardCases:
    """Tests for guard failures with edge case values."""

    def test_post_run_fail_with_critical_none_fails_guard(self, test_db: Database) -> None:
        """POST_RUN_FAIL with critical=None should fail guard (no matching transition)."""
        run_id = _setup_test_run(test_db, StageState.POST_RUN)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=None,  # Neither True nor False
        )
        result = transition(test_db, run_id, StageEvent.POST_RUN_FAIL, ctx)

        assert result.success is False
        assert result.reason == "no_transition"

    def test_timeout_post_run_with_critical_phases_done_none_fails_guard(self, test_db: Database) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=None should fail guard."""
        run_id = _setup_test_run(test_db, StageState.POST_RUN)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=None,  # Neither True nor False
        )
        result = transition(test_db, run_id, StageEvent.TIMEOUT, ctx)

        # Should not match the guarded POST_RUN→TIMEOUT transitions,
        # but there's no unguarded POST_RUN→TIMEOUT→TERMINATED
        assert result.success is False
        assert result.reason == "no_transition"

    def test_exit_failure_without_exit_code_exists_succeeds(self, test_db: Database) -> None:
        """EXIT_FAILURE succeeds without exit_code_exists guard - emitter responsibility.

        Note: EXIT_SUCCESS/EXIT_FAILURE don't have guards on exit_code_exists.
        The daemon/emitter is responsible for only emitting these events when
        the exit code file exists. The state machine trusts the event emitter.
        This test documents that EXIT_FAILURE is unguarded (like EXIT_SUCCESS).
        """
        run_id = _setup_test_run(test_db, StageState.RUNNING)

        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            exit_code=1,
            exit_code_exists=False,  # Not guarded - emitter responsibility
        )
        result = transition(test_db, run_id, StageEvent.EXIT_FAILURE, ctx)

        # Transition succeeds - no guard on exit_code_exists
        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestCompletedWithWarningsNormal:
    """Tests verifying completed_with_warnings is NOT set for normal completion."""

    def test_post_run_ok_does_not_set_completed_with_warnings(self, test_db: Database) -> None:
        """POST_RUN_OK should not set completed_with_warnings flag.

        In v1.2, POST_RUN_OK goes to AWAITING_USER_FINALIZATION, not COMPLETED.
        """
        run_id = _setup_test_run(test_db, StageState.POST_RUN)

        result = transition(test_db, run_id, StageEvent.POST_RUN_OK, _event_ctx())

        assert result.success is True
        assert result.new_state == StageState.AWAITING_USER_FINALIZATION

        extras = _get_run_extras(test_db, run_id)
        # Should be NULL (falsy) or 0, not 1
        assert not extras.get("completed_with_warnings")


class TestFailedAndTerminatedLifecycles:
    """Tests for non-success lifecycle paths."""

    def test_failed_lifecycle_via_build_fail(self, test_db: Database) -> None:
        """Test PREPARING→BUILDING→FAILED via BUILD_FAIL."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # PREPARING → BUILDING
        result = transition(test_db, run_id, StageEvent.BUILD_START, _event_ctx())
        assert result.new_state == StageState.BUILDING

        # BUILDING → FAILED
        ctx = _event_ctx(error_message="Docker build error")
        result = transition(test_db, run_id, StageEvent.BUILD_FAIL, ctx)
        assert result.new_state == StageState.FAILED

        # Verify audit trail
        transitions = _get_state_transitions(test_db, run_id)
        # run_start + build_start + build_fail
        assert len(transitions) == 3
        assert transitions[2]["to_state"] == StageState.FAILED.value

    def test_terminated_lifecycle_via_instance_lost(self, test_db: Database) -> None:
        """Test PREPARING→BUILDING→LAUNCHING→RUNNING→TERMINATED via INSTANCE_LOST."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # PREPARING → BUILDING → LAUNCHING → RUNNING
        transition(test_db, run_id, StageEvent.BUILD_START, _event_ctx())
        transition(test_db, run_id, StageEvent.BUILD_OK, _event_ctx())
        transition(test_db, run_id, StageEvent.LAUNCH_OK, _event_ctx())
        assert _get_run_state(test_db, run_id)["state"] == StageState.RUNNING.value

        # RUNNING → TERMINATED
        ctx = _event_ctx(source="daemon", termination_cause=TerminationCause.PREEMPTED)
        result = transition(test_db, run_id, StageEvent.INSTANCE_LOST, ctx)
        assert result.new_state == StageState.TERMINATED

        # Verify audit trail
        transitions = _get_state_transitions(test_db, run_id)
        # run_start + build_start + build_ok + launch_ok + instance_lost
        assert len(transitions) == 5
        assert transitions[4]["to_state"] == StageState.TERMINATED.value

    def test_canceled_lifecycle_via_user_cancel(self, test_db: Database) -> None:
        """Test mid-flight cancellation: PREPARING→BUILDING→CANCELED."""
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
        assert _get_run_state(test_db, run_id)["state"] == StageState.BUILDING.value

        # BUILDING → CANCELED
        ctx = _event_ctx(source="mcp_tool", error_message="User canceled during build")
        result = transition(test_db, run_id, StageEvent.USER_CANCEL, ctx)
        assert result.new_state == StageState.CANCELED

        # Verify audit trail
        transitions = _get_state_transitions(test_db, run_id)
        # run_start + build_start + user_cancel
        assert len(transitions) == 3
        assert transitions[2]["to_state"] == StageState.CANCELED.value
