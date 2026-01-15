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
from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.state_machine import (
    ProgressPhase,
    StageEvent,
    StageState,
)


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


def _get_state_transitions(db: Database, run_id: str) -> list[dict]:
    """Get all state transitions for a run."""
    with db._conn() as conn:
        rows = conn.execute(
            """SELECT from_state, to_state, event, timestamp
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

    def test_new_run_does_not_auto_record_transition(self, test_db: Database) -> None:
        """create_stage_run() does not record an initial transition.

        The initial state is set directly in the INSERT statement, not via
        the transition() function. Transition audit trail starts with the
        first explicit transition (e.g., BUILD_START).
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        transitions = _get_state_transitions(test_db, run_id)
        # No transitions recorded by create_stage_run itself
        assert len(transitions) == 0


class TestBuildEventEmission:
    """Tests for BUILD_START, BUILD_OK, BUILD_FAIL transitions.

    These transitions occur during the Docker image build phase.
    BUILD_START moves from PREPARING to BUILDING.
    BUILD_OK moves from BUILDING to LAUNCHING.
    BUILD_FAIL moves from BUILDING to FAILED (terminal).
    """

    def test_build_start_transitions_to_building(self, test_db: Database) -> None:
        """BUILD_START event should transition from PREPARING to BUILDING."""
        from goldfish.state_machine import EventContext, transition

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
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in BUILDING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.BUILDING.value, now),
            )

        # Emit BUILD_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.BUILD_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.LAUNCHING

    def test_build_fail_transitions_to_failed(self, test_db: Database) -> None:
        """BUILD_FAIL event should transition from BUILDING to FAILED."""
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in BUILDING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.BUILDING.value, now),
            )

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
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in LAUNCHING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.LAUNCHING.value, now),
            )

        # Emit LAUNCH_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.LAUNCH_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.RUNNING

    def test_launch_fail_transitions_to_failed(self, test_db: Database) -> None:
        """LAUNCH_FAIL event should transition from LAUNCHING to FAILED."""
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in LAUNCHING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.LAUNCHING.value, now),
            )

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
        from goldfish.state_machine import update_phase

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

    def test_phase_updates_to_pipeline_load(self, test_db: Database) -> None:
        """Phase should update to PIPELINE_LOAD during preparation."""
        from goldfish.state_machine import update_phase

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


class TestPrepareFailEventEmission:
    """Tests for PREPARE_FAIL and SVS_BLOCK transitions.

    These transitions handle failures during the preparation phase.
    PREPARE_FAIL handles validation/preflight failures.
    SVS_BLOCK handles pre-run review rejections.
    """

    def test_prepare_fail_transitions_to_failed(self, test_db: Database) -> None:
        """PREPARE_FAIL event should transition from PREPARING to FAILED."""
        from goldfish.state_machine import EventContext, transition

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
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )

        # Emit SVS_BLOCK
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            svs_finding_id="svs-123",
            error_message="SVS pre-run review blocked execution",
        )
        result = transition(test_db, run_id, StageEvent.SVS_BLOCK, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestFinalizeEventEmission:
    """Tests for FINALIZE_OK and FINALIZE_FAIL transitions.

    These transitions handle the finalization phase after execution.
    FINALIZE_OK moves from FINALIZING to COMPLETED.
    FINALIZE_FAIL outcome depends on the critical flag:
    - critical=True → FAILED
    - critical=False → COMPLETED (non-critical failures don't fail the run)
    """

    def test_finalize_ok_transitions_to_completed(self, test_db: Database) -> None:
        """FINALIZE_OK event should transition from FINALIZING to COMPLETED."""
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in FINALIZING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.FINALIZING.value, now),
            )

        # Emit FINALIZE_OK
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.FINALIZE_OK, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

    def test_finalize_fail_critical_transitions_to_failed(self, test_db: Database) -> None:
        """FINALIZE_FAIL with critical=True should transition to FAILED."""
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in FINALIZING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.FINALIZING.value, now),
            )

        # Emit FINALIZE_FAIL with critical=True
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=True,
            error_message="Output recording failed",
        )
        result = transition(test_db, run_id, StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

    def test_finalize_fail_non_critical_transitions_to_completed(self, test_db: Database) -> None:
        """FINALIZE_FAIL with critical=False should transition to COMPLETED."""
        from goldfish.state_machine import EventContext, transition

        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in FINALIZING state
        with test_db._conn() as conn:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                """INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "test-ws", "v1", "train", "running", now, StageState.FINALIZING.value, now),
            )

        # Emit FINALIZE_FAIL with critical=False
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            critical=False,
            error_message="Post-run review failed (non-critical)",
        )
        result = transition(test_db, run_id, StageEvent.FINALIZE_FAIL, ctx)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED


class TestFullLifecycle:
    """End-to-end lifecycle tests.

    These tests verify that a complete run lifecycle works correctly,
    from PREPARING through all states to a terminal state.
    """

    def test_successful_run_lifecycle(self, test_db: Database) -> None:
        """Test complete successful run: PREPARING→BUILDING→LAUNCHING→RUNNING→FINALIZING→COMPLETED."""
        from goldfish.state_machine import EventContext, transition

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

        # RUNNING → FINALIZING (via EXIT_SUCCESS)
        ctx = EventContext(timestamp=datetime.now(UTC), source="daemon", exit_code=0, exit_code_exists=True)
        result = transition(test_db, run_id, StageEvent.EXIT_SUCCESS, ctx)
        assert result.new_state == StageState.FINALIZING

        # FINALIZING → COMPLETED
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        result = transition(test_db, run_id, StageEvent.FINALIZE_OK, ctx)
        assert result.new_state == StageState.COMPLETED

        # Verify final state
        assert _get_run_state(test_db, run_id)["state"] == StageState.COMPLETED.value

        # Verify audit trail - comprehensive check of all columns
        transitions = _get_state_transitions(test_db, run_id)
        assert len(transitions) == 5  # 5 transitions total

        # Verify first transition (PREPARING → BUILDING via BUILD_START)
        assert transitions[0]["from_state"] == StageState.PREPARING.value
        assert transitions[0]["to_state"] == StageState.BUILDING.value
        assert transitions[0]["event"] == StageEvent.BUILD_START.value
        assert transitions[0]["timestamp"] is not None

        # Verify last transition (FINALIZING → COMPLETED via FINALIZE_OK)
        assert transitions[-1]["from_state"] == StageState.FINALIZING.value
        assert transitions[-1]["to_state"] == StageState.COMPLETED.value
        assert transitions[-1]["event"] == StageEvent.FINALIZE_OK.value
        assert transitions[-1]["timestamp"] is not None

        # Verify middle transitions have correct from_state chain
        assert transitions[1]["from_state"] == StageState.BUILDING.value
        assert transitions[2]["from_state"] == StageState.LAUNCHING.value
        assert transitions[3]["from_state"] == StageState.RUNNING.value
