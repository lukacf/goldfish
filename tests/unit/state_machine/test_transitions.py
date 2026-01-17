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
        """Verify we have exactly 34 transitions (v1.2: added AWAITING_USER_FINALIZATION)."""
        assert len(TRANSITIONS) == 34

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
        """UNKNOWN state should have TIMEOUT escape transition."""
        unknown_transitions = [t for t in TRANSITIONS if t.from_state == StageState.UNKNOWN]
        assert len(unknown_transitions) == 1
        # Should only have TIMEOUT (admin FORCE_* events removed)
        events = {t.event for t in unknown_transitions}
        assert events == {StageEvent.TIMEOUT}

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
        """Verify active states match spec (v1.2: renamed FINALIZING→POST_RUN, added AWAITING_USER_FINALIZATION)."""
        assert ACTIVE_STATES == frozenset(
            {
                StageState.PREPARING,
                StageState.BUILDING,
                StageState.LAUNCHING,
                StageState.RUNNING,
                StageState.POST_RUN,
                StageState.AWAITING_USER_FINALIZATION,
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

    def test_post_run_fail_critical_true(self) -> None:
        """POST_RUN_FAIL with critical=True → FAILED (v1.2: renamed from FINALIZE_FAIL)."""
        ctx = make_context(critical=True)
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_post_run_fail_critical_false(self) -> None:
        """POST_RUN_FAIL with critical=False → AWAITING_USER_FINALIZATION (v1.2)."""
        ctx = make_context(critical=False)
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.AWAITING_USER_FINALIZATION

    def test_post_run_fail_critical_none(self) -> None:
        """POST_RUN_FAIL with critical=None → no transition (guard fails)."""
        ctx = make_context(critical=None)
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        assert t is None

    def test_timeout_post_run_critical_phases_done(self) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=True → AWAITING_USER_FINALIZATION (v1.2)."""
        ctx = make_context(critical_phases_done=True)
        t = find_transition(StageState.POST_RUN, StageEvent.TIMEOUT, ctx)
        assert t is not None
        assert t.to_state == StageState.AWAITING_USER_FINALIZATION

    def test_timeout_post_run_critical_phases_not_done(self) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=False → FAILED."""
        ctx = make_context(critical_phases_done=False)
        t = find_transition(StageState.POST_RUN, StageEvent.TIMEOUT, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED

    def test_timeout_post_run_critical_phases_none(self) -> None:
        """TIMEOUT in POST_RUN with critical_phases_done=None → no transition."""
        ctx = make_context(critical_phases_done=None)
        t = find_transition(StageState.POST_RUN, StageEvent.TIMEOUT, ctx)
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
        """EXIT_SUCCESS → POST_RUN (v1.2: renamed from FINALIZING)."""
        t = find_transition(StageState.RUNNING, StageEvent.EXIT_SUCCESS, exit_success_context)
        assert t is not None
        assert t.to_state == StageState.POST_RUN

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


class TestPostRunTransitions:
    """Tests for POST_RUN state transitions (v1.2: renamed from FINALIZING)."""

    def test_post_run_ok(self, post_run_ok_context: EventContext) -> None:
        """POST_RUN_OK → AWAITING_USER_FINALIZATION (v1.2)."""
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_OK, post_run_ok_context)
        assert t is not None
        assert t.to_state == StageState.AWAITING_USER_FINALIZATION

    def test_instance_lost(self, instance_lost_context: EventContext) -> None:
        """INSTANCE_LOST → TERMINATED."""
        t = find_transition(StageState.POST_RUN, StageEvent.INSTANCE_LOST, instance_lost_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.POST_RUN, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED


class TestAwaitingUserFinalizationTransitions:
    """Tests for AWAITING_USER_FINALIZATION state transitions (v1.2: new state)."""

    def test_user_finalize(self, base_context: EventContext) -> None:
        """USER_FINALIZE → COMPLETED."""
        t = find_transition(StageState.AWAITING_USER_FINALIZATION, StageEvent.USER_FINALIZE, base_context)
        assert t is not None
        assert t.to_state == StageState.COMPLETED

    def test_user_cancel(self, user_cancel_context: EventContext) -> None:
        """USER_CANCEL → CANCELED."""
        t = find_transition(StageState.AWAITING_USER_FINALIZATION, StageEvent.USER_CANCEL, user_cancel_context)
        assert t is not None
        assert t.to_state == StageState.CANCELED


class TestUnknownTransitions:
    """Tests for UNKNOWN state transitions."""

    def test_timeout(self, timeout_context: EventContext) -> None:
        """TIMEOUT → TERMINATED."""
        t = find_transition(StageState.UNKNOWN, StageEvent.TIMEOUT, timeout_context)
        assert t is not None
        assert t.to_state == StageState.TERMINATED


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
        """Active states should have non-None entry phases (except AWAITING_USER_FINALIZATION).

        v1.2: AWAITING_USER_FINALIZATION is an active state (non-terminal) but has no
        progress phases because it's simply waiting for user action, not doing infrastructure work.
        """
        # States that have progress phases (infrastructure work)
        states_with_phases = ACTIVE_STATES - {StageState.AWAITING_USER_FINALIZATION}
        for state in states_with_phases:
            assert STATE_ENTRY_PHASES[state] is not None, f"{state} should have a phase"
            assert isinstance(STATE_ENTRY_PHASES[state], ProgressPhase)

        # AWAITING_USER_FINALIZATION has no phases - just waiting for user
        assert STATE_ENTRY_PHASES[StageState.AWAITING_USER_FINALIZATION] is None

    def test_terminal_states_have_none_phases(self) -> None:
        """Terminal states should have None entry phases."""
        for state in TERMINAL_STATES:
            assert STATE_ENTRY_PHASES[state] is None, f"{state} should have None phase"

    def test_specific_entry_phases(self) -> None:
        """Verify specific entry phases match spec (v1.2: renamed FINALIZING→POST_RUN)."""
        assert STATE_ENTRY_PHASES[StageState.PREPARING] == ProgressPhase.GCS_CHECK
        assert STATE_ENTRY_PHASES[StageState.BUILDING] == ProgressPhase.IMAGE_CHECK
        assert STATE_ENTRY_PHASES[StageState.LAUNCHING] == ProgressPhase.INSTANCE_CREATE
        assert STATE_ENTRY_PHASES[StageState.RUNNING] == ProgressPhase.CONTAINER_INIT
        assert STATE_ENTRY_PHASES[StageState.POST_RUN] == ProgressPhase.OUTPUT_SYNC
        # AWAITING_USER_FINALIZATION has no entry phase (user-triggered state)
        assert STATE_ENTRY_PHASES[StageState.AWAITING_USER_FINALIZATION] is None


class TestUtilityFunctions:
    """Tests for get_transitions_from_state() and get_transitions_for_event()."""

    def test_get_transitions_from_preparing(self) -> None:
        """get_transitions_from_state returns correct transitions for PREPARING."""
        transitions = get_transitions_from_state(StageState.PREPARING)
        assert len(transitions) == 6
        assert all(t.from_state == StageState.PREPARING for t in transitions)

    def test_get_transitions_from_post_run(self) -> None:
        """get_transitions_from_state returns correct transitions for POST_RUN (v1.2)."""
        transitions = get_transitions_from_state(StageState.POST_RUN)
        assert len(transitions) == 8  # Includes AI_STOP
        assert all(t.from_state == StageState.POST_RUN for t in transitions)

    def test_get_transitions_from_awaiting_user_finalization(self) -> None:
        """get_transitions_from_state returns correct transitions for AWAITING_USER_FINALIZATION (v1.2)."""
        transitions = get_transitions_from_state(StageState.AWAITING_USER_FINALIZATION)
        assert len(transitions) == 2  # USER_FINALIZE, USER_CANCEL
        assert all(t.from_state == StageState.AWAITING_USER_FINALIZATION for t in transitions)

    def test_get_transitions_from_terminal_state(self) -> None:
        """get_transitions_from_state returns empty list for terminal states."""
        transitions = get_transitions_from_state(StageState.COMPLETED)
        assert transitions == []

    def test_get_transitions_for_user_cancel(self) -> None:
        """get_transitions_for_event returns all USER_CANCEL transitions (v1.2: 6 active states)."""
        transitions = get_transitions_for_event(StageEvent.USER_CANCEL)
        # USER_CANCEL is valid in all 6 active states (including POST_RUN and AWAITING_USER_FINALIZATION)
        assert len(transitions) == 6
        assert all(t.event == StageEvent.USER_CANCEL for t in transitions)
        assert all(t.to_state == StageState.CANCELED for t in transitions)

    def test_get_transitions_for_timeout(self) -> None:
        """get_transitions_for_event returns all TIMEOUT transitions."""
        transitions = get_transitions_for_event(StageEvent.TIMEOUT)
        # TIMEOUT: 4 active (PREPARING, BUILDING, LAUNCHING, RUNNING) + 2 POST_RUN guards + 1 UNKNOWN = 7
        assert len(transitions) == 7
        assert all(t.event == StageEvent.TIMEOUT for t in transitions)

    def test_get_transitions_for_post_run_fail(self) -> None:
        """get_transitions_for_event returns both guarded POST_RUN_FAIL transitions (v1.2)."""
        transitions = get_transitions_for_event(StageEvent.POST_RUN_FAIL)
        assert len(transitions) == 2
        # One goes to FAILED (critical=True), one to AWAITING_USER_FINALIZATION (critical=False)
        to_states = {t.to_state for t in transitions}
        assert to_states == {StageState.FAILED, StageState.AWAITING_USER_FINALIZATION}

    def test_get_transitions_for_user_finalize(self) -> None:
        """get_transitions_for_event returns USER_FINALIZE transition (v1.2: new event)."""
        transitions = get_transitions_for_event(StageEvent.USER_FINALIZE)
        assert len(transitions) == 1
        assert transitions[0].from_state == StageState.AWAITING_USER_FINALIZATION
        assert transitions[0].to_state == StageState.COMPLETED


class TestGuardEvaluationOrder:
    """Tests verifying guard evaluation order."""

    def test_first_matching_guard_wins(self) -> None:
        """When first guard passes, subsequent guards are not checked."""
        # For POST_RUN_FAIL, critical=True comes first in table (v1.2)
        ctx = make_context(critical=True)
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.FAILED
        assert t.guard_name == "critical=True"

    def test_second_guard_checked_when_first_fails(self) -> None:
        """When first guard fails, second guard is checked."""
        # For POST_RUN_FAIL, critical=False is second in table (v1.2)
        ctx = make_context(critical=False)
        t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        assert t is not None
        assert t.to_state == StageState.AWAITING_USER_FINALIZATION
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
