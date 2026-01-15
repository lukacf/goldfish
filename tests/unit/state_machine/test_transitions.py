"""Tests for state machine transitions.

Tests the transition table and find_transition() function.
"""

from __future__ import annotations

from goldfish.state_machine import (
    ACTIVE_STATES,
    LIMBO_STATES,
    STATE_ENTRY_PHASES,
    TERMINAL_STATES,
    TRANSITIONS,
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    find_transition,
    get_transitions_for_event,
    get_transitions_from_state,
)
from tests.unit.state_machine.conftest import make_context


class TestTransitionTable:
    """Tests for the TRANSITIONS constant."""

    def test_transition_count(self) -> None:
        """Verify we have exactly 39 transitions as specified."""
        assert len(TRANSITIONS) == 39

    def test_all_active_states_have_transitions(self) -> None:
        """Every active state should have at least one outgoing transition."""
        states_with_transitions = {t.from_state for t in TRANSITIONS}
        for state in ACTIVE_STATES:
            assert state in states_with_transitions, f"{state} has no transitions"

    def test_terminal_states_have_no_outgoing_transitions(self) -> None:
        """Terminal states should not have any outgoing transitions."""
        for t in TRANSITIONS:
            assert t.from_state not in TERMINAL_STATES, f"Terminal state {t.from_state} has transition on {t.event}"

    def test_unknown_state_has_transitions(self) -> None:
        """UNKNOWN state should have escape transitions."""
        unknown_transitions = [t for t in TRANSITIONS if t.from_state == StageState.UNKNOWN]
        assert len(unknown_transitions) == 4
        # Should have TIMEOUT, FORCE_TERMINATE, FORCE_COMPLETE, FORCE_FAIL
        events = {t.event for t in unknown_transitions}
        assert events == {
            StageEvent.TIMEOUT,
            StageEvent.FORCE_TERMINATE,
            StageEvent.FORCE_COMPLETE,
            StageEvent.FORCE_FAIL,
        }

    def test_all_transitions_have_valid_states(self) -> None:
        """All transitions should reference valid StageState values."""
        all_states = set(StageState)
        for t in TRANSITIONS:
            assert t.from_state in all_states
            assert t.to_state in all_states

    def test_all_transitions_have_valid_events(self) -> None:
        """All transitions should reference valid StageEvent values."""
        all_events = set(StageEvent)
        for t in TRANSITIONS:
            assert t.event in all_events


class TestStateCategories:
    """Tests for state category constants."""

    def test_terminal_states(self) -> None:
        """Verify terminal states match spec."""
        assert TERMINAL_STATES == frozenset(
            {
                StageState.COMPLETED,
                StageState.FAILED,
                StageState.TERMINATED,
                StageState.CANCELED,
            }
        )

    def test_active_states(self) -> None:
        """Verify active states match spec."""
        assert ACTIVE_STATES == frozenset(
            {
                StageState.PREPARING,
                StageState.BUILDING,
                StageState.LAUNCHING,
                StageState.RUNNING,
                StageState.FINALIZING,
            }
        )

    def test_no_overlap_between_categories(self) -> None:
        """Terminal and active states should not overlap."""
        assert TERMINAL_STATES.isdisjoint(ACTIVE_STATES)


class TestFindTransition:
    """Tests for find_transition() function."""

    def test_simple_transition_found(self, base_context: EventContext) -> None:
        """find_transition returns TransitionDef for valid transition."""
        t = find_transition(StageState.PREPARING, StageEvent.BUILD_START, base_context)
        assert t is not None
        assert t.from_state == StageState.PREPARING
        assert t.event == StageEvent.BUILD_START
        assert t.to_state == StageState.BUILDING

    def test_invalid_transition_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for invalid (state, event) pair."""
        # BUILD_OK is not valid in PREPARING state
        t = find_transition(StageState.PREPARING, StageEvent.BUILD_OK, base_context)
        assert t is None

    def test_terminal_state_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for terminal states."""
        t = find_transition(StageState.COMPLETED, StageEvent.BUILD_START, base_context)
        assert t is None

    def test_string_state_converted(self, base_context: EventContext) -> None:
        """find_transition accepts string state values."""
        t = find_transition("preparing", StageEvent.BUILD_START, base_context)
        assert t is not None
        assert t.to_state == StageState.BUILDING

    def test_string_event_converted(self, base_context: EventContext) -> None:
        """find_transition accepts string event values."""
        t = find_transition(StageState.PREPARING, "build_start", base_context)
        assert t is not None
        assert t.to_state == StageState.BUILDING

    def test_invalid_string_state_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for invalid state string."""
        t = find_transition("invalid_state", StageEvent.BUILD_START, base_context)
        assert t is None

    def test_invalid_string_event_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for invalid event string."""
        t = find_transition(StageState.PREPARING, "invalid_event", base_context)
        assert t is None


class TestGuardedTransitions:
    """Tests for transitions with guard conditions."""

    def test_exit_missing_with_confirmed_dead(self) -> None:
        """EXIT_MISSING with instance_confirmed_dead=True → TERMINATED."""
        ctx = make_context(instance_confirmed_dead=True)
        t = find_transition(StageState.RUNNING, StageEvent.EXIT_MISSING, ctx)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_exit_missing_without_confirmed_dead(self) -> None:
        """EXIT_MISSING without instance_confirmed_dead → no transition."""
        ctx = make_context(instance_confirmed_dead=False)
        t = find_transition(StageState.RUNNING, StageEvent.EXIT_MISSING, ctx)
        assert t is None

    def test_finalize_fail_critical_true(self) -> None:
        """FINALIZE_FAIL with critical=True → FAILED."""
        ctx = make_context(critical=True)
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_finalize_fail_critical_false(self) -> None:
        """FINALIZE_FAIL with critical=False → COMPLETED."""
        ctx = make_context(critical=False)
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_finalize_fail_critical_none(self) -> None:
        """FINALIZE_FAIL with critical=None → no transition (guard fails)."""
        ctx = make_context(critical=None)
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_FAIL, ctx)
        assert t is None

    def test_timeout_finalizing_critical_phases_done(self) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=True → COMPLETED."""
        ctx = make_context(critical_phases_done=True)
        t = find_transition(StageState.FINALIZING, StageEvent.TIMEOUT, ctx)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_timeout_finalizing_critical_phases_not_done(self) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=False → FAILED."""
        ctx = make_context(critical_phases_done=False)
        t = find_transition(StageState.FINALIZING, StageEvent.TIMEOUT, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_timeout_finalizing_critical_phases_none(self) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=None → no transition."""
        ctx = make_context(critical_phases_done=None)
        t = find_transition(StageState.FINALIZING, StageEvent.TIMEOUT, ctx)
        assert t is None


class TestPreparingTransitions:
    """Tests for PREPARING state transitions."""

    def test_build_start(self, base_context: EventContext) -> None:
        """BUILD_START → BUILDING."""
        t = find_transition(StageState.PREPARING, StageEvent.BUILD_START, base_context)
        assert t is not None
        assert t.to_state == StageState.BUILDING

    def test_prepare_fail(self, base_context: EventContext) -> None:
        """PREPARE_FAIL → FAILED."""
        t = find_transition(StageState.PREPARING, StageEvent.PREPARE_FAIL, base_context)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_svs_block(self, svs_block_context: EventContext) -> None:
        """SVS_BLOCK → FAILED."""
        t = find_transition(StageState.PREPARING, StageEvent.SVS_BLOCK, svs_block_context)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.PREPARING, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.PREPARING, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.PREPARING, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.PREPARING, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestBuildingTransitions:
    """Tests for BUILDING state transitions."""

    def test_build_ok(self, base_context: EventContext) -> None:
        """BUILD_OK → LAUNCHING."""
        t = find_transition(StageState.BUILDING, StageEvent.BUILD_OK, base_context)
        assert t is not None
        assert t.to_state == StageState.LAUNCHING

    def test_build_fail(self, base_context: EventContext) -> None:
        """BUILD_FAIL → FAILED."""
        t = find_transition(StageState.BUILDING, StageEvent.BUILD_FAIL, base_context)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.BUILDING, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED


class TestLaunchingTransitions:
    """Tests for LAUNCHING state transitions."""

    def test_launch_ok(self, base_context: EventContext) -> None:
        """LAUNCH_OK → RUNNING."""
        t = find_transition(StageState.LAUNCHING, StageEvent.LAUNCH_OK, base_context)
        assert t is not None
        assert t.to_state == StageState.RUNNING

    def test_launch_fail(self, base_context: EventContext) -> None:
        """LAUNCH_FAIL → FAILED."""
        t = find_transition(StageState.LAUNCHING, StageEvent.LAUNCH_FAIL, base_context)
        assert t is not None
        assert t.to_state == StageState.FAILED


class TestRunningTransitions:
    """Tests for RUNNING state transitions."""

    def test_exit_success(self, exit_success_context: EventContext) -> None:
        """EXIT_SUCCESS → FINALIZING."""
        t = find_transition(StageState.RUNNING, StageEvent.EXIT_SUCCESS, exit_success_context)
        assert t is not None
        assert t.to_state == StageState.FINALIZING

    def test_exit_failure(self, exit_failure_context: EventContext) -> None:
        """EXIT_FAILURE → FAILED."""
        t = find_transition(StageState.RUNNING, StageEvent.EXIT_FAILURE, exit_failure_context)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.RUNNING, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestFinalizingTransitions:
    """Tests for FINALIZING state transitions."""

    def test_finalize_ok(self, finalize_ok_context: EventContext) -> None:
        """FINALIZE_OK → COMPLETED."""
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_OK, finalize_ok_context)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.FINALIZING, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.FINALIZING, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED

    def test_force_complete(self, force_complete_context: EventContext) -> None:
        """FORCE_COMPLETE → COMPLETED."""
        t = find_transition(StageState.FINALIZING, StageEvent.FORCE_COMPLETE, force_complete_context)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.FINALIZING, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestUnknownTransitions:
    """Tests for UNKNOWN state transitions."""

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.UNKNOWN, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.UNKNOWN, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_force_complete(self, force_complete_context: EventContext) -> None:
        """FORCE_COMPLETE → COMPLETED."""
        t = find_transition(StageState.UNKNOWN, StageEvent.FORCE_COMPLETE, force_complete_context)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_force_fail(self, force_fail_context: EventContext) -> None:
        """FORCE_FAIL → FAILED."""
        t = find_transition(StageState.UNKNOWN, StageEvent.FORCE_FAIL, force_fail_context)
        assert t is not None
        assert t.to_state == StageState.FAILED


class TestBuildingTransitionsComplete:
    """Complete tests for BUILDING state transitions (all 6)."""

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.BUILDING, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.BUILDING, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.BUILDING, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestLaunchingTransitionsComplete:
    """Complete tests for LAUNCHING state transitions (all 6)."""

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.LAUNCHING, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.LAUNCHING, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.LAUNCHING, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.LAUNCHING, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestRunningTransitionsComplete:
    """Complete tests for RUNNING state transitions (all 7)."""

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.RUNNING, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.RUNNING, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED

    def test_force_terminate(self, force_terminate_context: EventContext) -> None:
        """FORCE_TERMINATE → TERMINATED."""
        t = find_transition(StageState.RUNNING, StageEvent.FORCE_TERMINATE, force_terminate_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


class TestLimboStates:
    """Tests for LIMBO_STATES constant."""

    def test_limbo_states_contains_unknown(self) -> None:
        """LIMBO_STATES should contain only UNKNOWN."""
        assert LIMBO_STATES == frozenset({StageState.UNKNOWN})

    def test_unknown_not_in_terminal_or_active(self) -> None:
        """UNKNOWN should not be in TERMINAL_STATES or ACTIVE_STATES."""
        assert StageState.UNKNOWN not in TERMINAL_STATES
        assert StageState.UNKNOWN not in ACTIVE_STATES

    def test_all_categories_cover_all_states(self) -> None:
        """TERMINAL + ACTIVE + LIMBO should equal all StageState values."""
        all_categorized = TERMINAL_STATES | ACTIVE_STATES | LIMBO_STATES
        assert all_categorized == set(StageState)


class TestStateEntryPhases:
    """Tests for STATE_ENTRY_PHASES constant."""

    def test_all_states_have_entry_phase(self) -> None:
        """Every state should have an entry in STATE_ENTRY_PHASES."""
        for state in StageState:
            assert state in STATE_ENTRY_PHASES, f"{state} missing from STATE_ENTRY_PHASES"

    def test_active_states_have_phases(self) -> None:
        """Active states should have non-None entry phases."""
        for state in ACTIVE_STATES:
            assert STATE_ENTRY_PHASES[state] is not None, f"{state} should have a phase"
            assert isinstance(STATE_ENTRY_PHASES[state], ProgressPhase)

    def test_terminal_states_have_none_phases(self) -> None:
        """Terminal states should have None entry phases."""
        for state in TERMINAL_STATES:
            assert STATE_ENTRY_PHASES[state] is None, f"{state} should have None phase"

    def test_specific_entry_phases(self) -> None:
        """Verify specific entry phases match spec."""
        assert STATE_ENTRY_PHASES[StageState.PREPARING] == ProgressPhase.GCS_CHECK
        assert STATE_ENTRY_PHASES[StageState.BUILDING] == ProgressPhase.IMAGE_CHECK
        assert STATE_ENTRY_PHASES[StageState.LAUNCHING] == ProgressPhase.INSTANCE_CREATE
        assert STATE_ENTRY_PHASES[StageState.RUNNING] == ProgressPhase.CONTAINER_INIT
        assert STATE_ENTRY_PHASES[StageState.FINALIZING] == ProgressPhase.OUTPUT_SYNC


class TestUtilityFunctions:
    """Tests for get_transitions_from_state() and get_transitions_for_event()."""

    def test_get_transitions_from_preparing(self) -> None:
        """get_transitions_from_state returns correct transitions for PREPARING."""
        transitions = get_transitions_from_state(StageState.PREPARING)
        assert len(transitions) == 7
        assert all(t.from_state == StageState.PREPARING for t in transitions)

    def test_get_transitions_from_finalizing(self) -> None:
        """get_transitions_from_state returns correct transitions for FINALIZING."""
        transitions = get_transitions_from_state(StageState.FINALIZING)
        assert len(transitions) == 9
        assert all(t.from_state == StageState.FINALIZING for t in transitions)

    def test_get_transitions_from_terminal_state(self) -> None:
        """get_transitions_from_state returns empty list for terminal states."""
        transitions = get_transitions_from_state(StageState.COMPLETED)
        assert transitions == []

    def test_get_transitions_for_user_cancel(self) -> None:
        """get_transitions_for_event returns all USER_CANCEL transitions."""
        transitions = get_transitions_for_event(StageEvent.USER_CANCEL)
        # USER_CANCEL is valid in all 5 active states (including FINALIZING)
        assert len(transitions) == 5
        assert all(t.event == StageEvent.USER_CANCEL for t in transitions)
        assert all(t.to_state == StageState.CANCELED for t in transitions)

    def test_get_transitions_for_timeout(self) -> None:
        """get_transitions_for_event returns all TIMEOUT transitions."""
        transitions = get_transitions_for_event(StageEvent.TIMEOUT)
        # TIMEOUT: 4 active (PREPARING, BUILDING, LAUNCHING, RUNNING) + 2 FINALIZING guards + 1 UNKNOWN = 7
        assert len(transitions) == 7
        assert all(t.event == StageEvent.TIMEOUT for t in transitions)

    def test_get_transitions_for_finalize_fail(self) -> None:
        """get_transitions_for_event returns both guarded FINALIZE_FAIL transitions."""
        transitions = get_transitions_for_event(StageEvent.FINALIZE_FAIL)
        assert len(transitions) == 2
        # One goes to FAILED (critical=True), one to COMPLETED (critical=False)
        to_states = {t.to_state for t in transitions}
        assert to_states == {StageState.FAILED, StageState.COMPLETED}


class TestGuardEvaluationOrder:
    """Tests verifying guard evaluation order."""

    def test_first_matching_guard_wins(self) -> None:
        """When first guard passes, subsequent guards are not checked."""
        # For FINALIZE_FAIL, critical=True comes first in table
        ctx = make_context(critical=True)
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED
        assert t.guard_name == "critical=True"

    def test_second_guard_checked_when_first_fails(self) -> None:
        """When first guard fails, second guard is checked."""
        # For FINALIZE_FAIL, critical=False is second in table
        ctx = make_context(critical=False)
        t = find_transition(StageState.FINALIZING, StageEvent.FINALIZE_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.COMPLETED
        assert t.guard_name == "critical=False"


class TestEmptyStringEdgeCases:
    """Tests for empty string edge cases."""

    def test_empty_string_state_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for empty string state."""
        t = find_transition("", StageEvent.BUILD_START, base_context)
        assert t is None

    def test_empty_string_event_returns_none(self, base_context: EventContext) -> None:
        """find_transition returns None for empty string event."""
        t = find_transition(StageState.PREPARING, "", base_context)
        assert t is None
