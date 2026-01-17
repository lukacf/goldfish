"""Tests for state machine type definitions.

Verifies enums, dataclasses, and type aliases match the specification.
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.state_machine import (
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    TerminationCause,
    TransitionDef,
    TransitionResult,
)


class TestStageState:
    """Tests for StageState enum."""

    def test_state_count(self) -> None:
        """Verify we have exactly 11 states as specified (v1.2 spec)."""
        assert len(StageState) == 11

    def test_active_states_exist(self) -> None:
        """Verify all 6 active states exist (v1.2: added AWAITING_USER_FINALIZATION)."""
        assert StageState.PREPARING.value == "preparing"
        assert StageState.BUILDING.value == "building"
        assert StageState.LAUNCHING.value == "launching"
        assert StageState.RUNNING.value == "running"
        assert StageState.POST_RUN.value == "post_run"
        assert StageState.AWAITING_USER_FINALIZATION.value == "awaiting_user_finalization"

    def test_terminal_states_exist(self) -> None:
        """Verify all 5 terminal states exist."""
        assert StageState.COMPLETED.value == "completed"
        assert StageState.FAILED.value == "failed"
        assert StageState.TERMINATED.value == "terminated"
        assert StageState.CANCELED.value == "canceled"
        assert StageState.UNKNOWN.value == "unknown"

    def test_string_enum_behavior(self) -> None:
        """StageState inherits from str, allowing string operations."""
        # Verify value access
        assert StageState.RUNNING.value == "running"
        # Verify str enum allows assignment to str variable
        running: str = StageState.RUNNING
        assert running == "running"

    def test_finalizing_renamed_to_post_run(self) -> None:
        """Verify FINALIZING was renamed to POST_RUN (v1.2 spec)."""
        # FINALIZING should not exist
        assert not hasattr(StageState, "FINALIZING")
        # POST_RUN should exist
        assert StageState.POST_RUN.value == "post_run"


class TestStageEvent:
    """Tests for StageEvent enum."""

    def test_event_count(self) -> None:
        """Verify we have exactly 17 events (v1.2: added USER_FINALIZE)."""
        assert len(StageEvent) == 17

    def test_build_events(self) -> None:
        """Verify build-related events."""
        assert StageEvent.BUILD_START.value == "build_start"
        assert StageEvent.BUILD_OK.value == "build_ok"
        assert StageEvent.BUILD_FAIL.value == "build_fail"

    def test_launch_events(self) -> None:
        """Verify launch-related events."""
        assert StageEvent.LAUNCH_OK.value == "launch_ok"
        assert StageEvent.LAUNCH_FAIL.value == "launch_fail"

    def test_exit_events(self) -> None:
        """Verify exit-related events."""
        assert StageEvent.EXIT_SUCCESS.value == "exit_success"
        assert StageEvent.EXIT_FAILURE.value == "exit_failure"
        assert StageEvent.EXIT_MISSING.value == "exit_missing"

    def test_post_run_events(self) -> None:
        """Verify post-run events (v1.2: renamed from finalize)."""
        assert StageEvent.POST_RUN_OK.value == "post_run_ok"
        assert StageEvent.POST_RUN_FAIL.value == "post_run_fail"

    def test_infrastructure_events(self) -> None:
        """Verify infrastructure events."""
        assert StageEvent.INSTANCE_LOST.value == "instance_lost"
        assert StageEvent.TIMEOUT.value == "timeout"

    def test_user_events(self) -> None:
        """Verify user events (v1.2: added USER_FINALIZE)."""
        assert StageEvent.USER_CANCEL.value == "user_cancel"
        assert StageEvent.USER_FINALIZE.value == "user_finalize"

    def test_preparation_events(self) -> None:
        """Verify preparation events."""
        assert StageEvent.PREPARE_FAIL.value == "prepare_fail"
        assert StageEvent.SVS_BLOCK.value == "svs_block"

    def test_finalize_events_renamed(self) -> None:
        """Verify FINALIZE_* events were renamed to POST_RUN_* (v1.2 spec)."""
        # Old FINALIZE_* events should not exist
        assert not hasattr(StageEvent, "FINALIZE_OK")
        assert not hasattr(StageEvent, "FINALIZE_FAIL")
        # New POST_RUN_* events should exist
        assert StageEvent.POST_RUN_OK.value == "post_run_ok"
        assert StageEvent.POST_RUN_FAIL.value == "post_run_fail"


class TestTerminationCause:
    """Tests for TerminationCause enum."""

    def test_cause_count(self) -> None:
        """Verify we have exactly 6 causes as specified."""
        assert len(TerminationCause) == 6

    def test_causes_exist(self) -> None:
        """Verify all termination causes."""
        assert TerminationCause.PREEMPTED.value == "preempted"
        assert TerminationCause.CRASHED.value == "crashed"
        assert TerminationCause.ORPHANED.value == "orphaned"
        assert TerminationCause.TIMEOUT.value == "timeout"
        assert TerminationCause.AI_STOPPED.value == "ai_stopped"
        assert TerminationCause.MANUAL.value == "manual"


class TestProgressPhase:
    """Tests for ProgressPhase enum."""

    def test_phase_count(self) -> None:
        """Verify we have exactly 20 phases as specified."""
        assert len(ProgressPhase) == 20

    def test_preparing_phases(self) -> None:
        """Verify PREPARING phases."""
        preparing_phases = [
            ProgressPhase.GCS_CHECK,
            ProgressPhase.VERSIONING,
            ProgressPhase.PIPELINE_LOAD,
            ProgressPhase.SVS_PREFLIGHT,
            ProgressPhase.CONFIG_LOAD,
            ProgressPhase.INPUT_RESOLVE,
            ProgressPhase.PRE_RUN_REVIEW,
        ]
        assert len(preparing_phases) == 7
        for phase in preparing_phases:
            assert phase in ProgressPhase

    def test_building_phases(self) -> None:
        """Verify BUILDING phases."""
        assert ProgressPhase.IMAGE_CHECK.value == "image_check"
        assert ProgressPhase.DOCKER_BUILD.value == "docker_build"

    def test_launching_phases(self) -> None:
        """Verify LAUNCHING phases."""
        assert ProgressPhase.INSTANCE_CREATE.value == "instance_create"
        assert ProgressPhase.INSTANCE_PROVISIONING.value == "instance_provisioning"
        assert ProgressPhase.INSTANCE_STAGING.value == "instance_staging"

    def test_running_phases(self) -> None:
        """Verify RUNNING phases."""
        assert ProgressPhase.CONTAINER_INIT.value == "container_init"
        assert ProgressPhase.CODE_EXECUTION.value == "code_execution"

    def test_finalizing_phases(self) -> None:
        """Verify FINALIZING phases."""
        finalizing_phases = [
            ProgressPhase.OUTPUT_SYNC,
            ProgressPhase.OUTPUT_RECORDING,
            ProgressPhase.LOG_FETCH,
            ProgressPhase.METRICS_COLLECTION,
            ProgressPhase.POST_RUN_REVIEW,
            ProgressPhase.CLEANUP,
        ]
        assert len(finalizing_phases) == 6
        for phase in finalizing_phases:
            assert phase in ProgressPhase


class TestEventContext:
    """Tests for EventContext dataclass."""

    def test_required_fields(self) -> None:
        """EventContext requires timestamp and source."""
        now = datetime.now(UTC)
        ctx = EventContext(timestamp=now, source="executor")
        assert ctx.timestamp == now
        assert ctx.source == "executor"

    def test_default_values(self) -> None:
        """All optional fields should have proper defaults."""
        now = datetime.now(UTC)
        ctx = EventContext(timestamp=now, source="executor")

        # Exit context defaults
        assert ctx.exit_code is None
        assert ctx.exit_code_exists is False

        # Termination context defaults
        assert ctx.termination_cause is None
        assert ctx.instance_confirmed_dead is False

        # Error context defaults
        assert ctx.error_message is None

        # Progress context defaults
        assert ctx.phase is None

        # GCS context defaults
        assert ctx.gcs_error is False
        assert ctx.gcs_outage_started is None

        # Finalization context defaults
        assert ctx.critical is None
        assert ctx.critical_phases_done is None

        # SVS context defaults
        assert ctx.svs_review_id is None

    def test_exit_code_handling(self) -> None:
        """exit_code and exit_code_exists should work together."""
        now = datetime.now(UTC)

        # Exit code exists and is 0
        ctx = EventContext(timestamp=now, source="daemon", exit_code=0, exit_code_exists=True)
        assert ctx.exit_code == 0
        assert ctx.exit_code_exists is True

        # Exit code exists and is non-zero
        ctx = EventContext(timestamp=now, source="daemon", exit_code=1, exit_code_exists=True)
        assert ctx.exit_code == 1
        assert ctx.exit_code_exists is True

        # Exit code missing (crash/preemption)
        ctx = EventContext(timestamp=now, source="daemon", exit_code_exists=False)
        assert ctx.exit_code is None
        assert ctx.exit_code_exists is False


class TestTransitionResult:
    """Tests for TransitionResult dataclass."""

    def test_success_result(self) -> None:
        """Successful transition result."""
        result = TransitionResult(
            success=True,
            new_state=StageState.RUNNING,
            reason="ok",
        )
        assert result.success is True
        assert result.new_state == StageState.RUNNING
        assert result.reason == "ok"

    def test_failure_result(self) -> None:
        """Failed transition result."""
        result = TransitionResult(
            success=False,
            reason="not_found",
            details="No transition from COMPLETED on BUILD_START",
        )
        assert result.success is False
        assert result.new_state is None
        assert result.reason == "not_found"
        assert result.details is not None


class TestTransitionDef:
    """Tests for TransitionDef dataclass."""

    def test_basic_transition(self) -> None:
        """Basic transition without guard."""
        t = TransitionDef(
            from_state=StageState.PREPARING,
            event=StageEvent.BUILD_START,
            to_state=StageState.BUILDING,
        )
        assert t.from_state == StageState.PREPARING
        assert t.event == StageEvent.BUILD_START
        assert t.to_state == StageState.BUILDING
        assert t.guard is None
        assert t.guard_name is None

    def test_guarded_transition(self) -> None:
        """Transition with guard function (v1.2: uses POST_RUN and POST_RUN_FAIL)."""

        def my_guard(ctx: EventContext) -> bool:
            return ctx.critical is True

        t = TransitionDef(
            from_state=StageState.POST_RUN,
            event=StageEvent.POST_RUN_FAIL,
            to_state=StageState.FAILED,
            guard=my_guard,
            guard_name="critical=True",
        )
        assert t.guard == my_guard
        assert t.guard_name == "critical=True"

    def test_guard_name_auto_set(self) -> None:
        """Guard name should be auto-set from function name if not provided."""

        def check_critical(ctx: EventContext) -> bool:
            return ctx.critical is True

        t = TransitionDef(
            from_state=StageState.POST_RUN,
            event=StageEvent.POST_RUN_FAIL,
            to_state=StageState.FAILED,
            guard=check_critical,
        )
        assert t.guard_name == "check_critical"
