"""Transition table and lookup functions for the Stage Execution State Machine.

This module defines:
- TRANSITIONS: The complete list of 34 valid state transitions (v1.2)
- STATE_ENTRY_PHASES: Default phases when entering each state
- find_transition(): Lookup function for finding valid transitions
- Guards: Predicate functions for conditional transitions

The transition table is the single source of truth for all valid state changes.
Any state change not in this table is invalid and will be rejected.
"""

from __future__ import annotations

from goldfish.state_machine.types import (
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    TransitionDef,
)

# =============================================================================
# Guard Functions
# =============================================================================
# Guards use explicit `is True` / `is False` to avoid None truthiness bugs.
# A guard returning False means the transition doesn't apply; try next match.


def guard_instance_confirmed_dead(ctx: EventContext) -> bool:
    """Guard: Instance must be confirmed dead for EXIT_MISSING → TERMINATED."""
    return ctx.instance_confirmed_dead is True


def guard_critical_true(ctx: EventContext) -> bool:
    """Guard: POST_RUN_FAIL with critical=True → FAILED."""
    return ctx.critical is True


def guard_critical_false(ctx: EventContext) -> bool:
    """Guard: POST_RUN_FAIL with critical=False → AWAITING_USER_FINALIZATION (v1.2)."""
    return ctx.critical is False


def guard_critical_phases_done_true(ctx: EventContext) -> bool:
    """Guard: TIMEOUT in POST_RUN with outputs saved → AWAITING_USER_FINALIZATION (v1.2)."""
    return ctx.critical_phases_done is True


def guard_critical_phases_done_false(ctx: EventContext) -> bool:
    """Guard: TIMEOUT in POST_RUN with outputs NOT saved → FAILED."""
    return ctx.critical_phases_done is False


# =============================================================================
# State Categories
# =============================================================================

TERMINAL_STATES = frozenset(
    {
        StageState.COMPLETED,
        StageState.FAILED,
        StageState.TERMINATED,
        StageState.CANCELED,
    }
)

ACTIVE_STATES = frozenset(
    {
        StageState.PREPARING,
        StageState.BUILDING,
        StageState.LAUNCHING,
        StageState.RUNNING,
        StageState.POST_RUN,  # v1.2: renamed from FINALIZING
        StageState.AWAITING_USER_FINALIZATION,  # v1.2: new state
    }
)

# UNKNOWN is special - only reachable via migration, requires manual resolution
LIMBO_STATES = frozenset({StageState.UNKNOWN})


# =============================================================================
# State Entry Phases
# =============================================================================
# When transitioning INTO a state, use this phase if context.phase is not set.

STATE_ENTRY_PHASES: dict[StageState, ProgressPhase | None] = {
    StageState.PREPARING: ProgressPhase.GCS_CHECK,
    StageState.BUILDING: ProgressPhase.IMAGE_CHECK,
    StageState.LAUNCHING: ProgressPhase.INSTANCE_CREATE,
    StageState.RUNNING: ProgressPhase.CONTAINER_INIT,
    StageState.POST_RUN: ProgressPhase.OUTPUT_SYNC,  # v1.2: renamed from FINALIZING
    StageState.AWAITING_USER_FINALIZATION: None,  # v1.2: no phase, just waiting
    # Terminal states don't have phases
    StageState.COMPLETED: None,
    StageState.FAILED: None,
    StageState.TERMINATED: None,
    StageState.CANCELED: None,
    StageState.UNKNOWN: None,
}


# =============================================================================
# Transition Table (34 transitions) - v1.2
# =============================================================================
# This is the single source of truth for all valid state changes.
# Order matters for guarded transitions - first matching guard wins.

TRANSITIONS: list[TransitionDef] = [
    # =========================================================================
    # PREPARING (6 transitions)
    # =========================================================================
    TransitionDef(StageState.PREPARING, StageEvent.BUILD_START, StageState.BUILDING),
    TransitionDef(StageState.PREPARING, StageEvent.PREPARE_FAIL, StageState.FAILED),
    TransitionDef(StageState.PREPARING, StageEvent.SVS_BLOCK, StageState.FAILED),
    TransitionDef(StageState.PREPARING, StageEvent.INSTANCE_LOST, StageState.TERMINATED),
    TransitionDef(StageState.PREPARING, StageEvent.TIMEOUT, StageState.TERMINATED),
    TransitionDef(StageState.PREPARING, StageEvent.USER_CANCEL, StageState.CANCELED),
    # =========================================================================
    # BUILDING (5 transitions)
    # =========================================================================
    TransitionDef(StageState.BUILDING, StageEvent.BUILD_OK, StageState.LAUNCHING),
    TransitionDef(StageState.BUILDING, StageEvent.BUILD_FAIL, StageState.FAILED),
    TransitionDef(StageState.BUILDING, StageEvent.INSTANCE_LOST, StageState.TERMINATED),
    TransitionDef(StageState.BUILDING, StageEvent.TIMEOUT, StageState.TERMINATED),
    TransitionDef(StageState.BUILDING, StageEvent.USER_CANCEL, StageState.CANCELED),
    # =========================================================================
    # LAUNCHING (5 transitions)
    # =========================================================================
    TransitionDef(StageState.LAUNCHING, StageEvent.LAUNCH_OK, StageState.RUNNING),
    TransitionDef(StageState.LAUNCHING, StageEvent.LAUNCH_FAIL, StageState.FAILED),
    TransitionDef(StageState.LAUNCHING, StageEvent.INSTANCE_LOST, StageState.TERMINATED),
    TransitionDef(StageState.LAUNCHING, StageEvent.TIMEOUT, StageState.TERMINATED),
    TransitionDef(StageState.LAUNCHING, StageEvent.USER_CANCEL, StageState.CANCELED),
    # =========================================================================
    # RUNNING (7 transitions)
    # =========================================================================
    TransitionDef(StageState.RUNNING, StageEvent.EXIT_SUCCESS, StageState.POST_RUN),  # v1.2: FINALIZING → POST_RUN
    TransitionDef(StageState.RUNNING, StageEvent.EXIT_FAILURE, StageState.FAILED),
    TransitionDef(
        StageState.RUNNING,
        StageEvent.EXIT_MISSING,
        StageState.TERMINATED,
        guard=guard_instance_confirmed_dead,
        guard_name="instance_confirmed_dead",
    ),
    TransitionDef(StageState.RUNNING, StageEvent.INSTANCE_LOST, StageState.TERMINATED),
    TransitionDef(StageState.RUNNING, StageEvent.TIMEOUT, StageState.TERMINATED),
    TransitionDef(StageState.RUNNING, StageEvent.USER_CANCEL, StageState.CANCELED),
    TransitionDef(StageState.RUNNING, StageEvent.AI_STOP, StageState.TERMINATED),
    # =========================================================================
    # POST_RUN (8 transitions) - v1.2: renamed from FINALIZING
    # =========================================================================
    TransitionDef(
        StageState.POST_RUN,
        StageEvent.POST_RUN_OK,  # v1.2: renamed from FINALIZE_OK
        StageState.AWAITING_USER_FINALIZATION,  # v1.2: was COMPLETED
    ),
    TransitionDef(
        StageState.POST_RUN,
        StageEvent.POST_RUN_FAIL,  # v1.2: renamed from FINALIZE_FAIL
        StageState.FAILED,
        guard=guard_critical_true,
        guard_name="critical=True",
    ),
    TransitionDef(
        StageState.POST_RUN,
        StageEvent.POST_RUN_FAIL,  # v1.2: renamed from FINALIZE_FAIL
        StageState.AWAITING_USER_FINALIZATION,  # v1.2: was COMPLETED
        guard=guard_critical_false,
        guard_name="critical=False",
    ),
    TransitionDef(StageState.POST_RUN, StageEvent.INSTANCE_LOST, StageState.TERMINATED),
    TransitionDef(
        StageState.POST_RUN,
        StageEvent.TIMEOUT,
        StageState.AWAITING_USER_FINALIZATION,  # v1.2: was COMPLETED
        guard=guard_critical_phases_done_true,
        guard_name="critical_phases_done=True",
    ),
    TransitionDef(
        StageState.POST_RUN,
        StageEvent.TIMEOUT,
        StageState.FAILED,
        guard=guard_critical_phases_done_false,
        guard_name="critical_phases_done=False",
    ),
    TransitionDef(StageState.POST_RUN, StageEvent.USER_CANCEL, StageState.CANCELED),
    TransitionDef(StageState.POST_RUN, StageEvent.AI_STOP, StageState.TERMINATED),
    # =========================================================================
    # AWAITING_USER_FINALIZATION (2 transitions) - v1.2: new state
    # =========================================================================
    TransitionDef(
        StageState.AWAITING_USER_FINALIZATION,
        StageEvent.USER_FINALIZE,  # v1.2: new event from finalize_run tool
        StageState.COMPLETED,
    ),
    TransitionDef(
        StageState.AWAITING_USER_FINALIZATION,
        StageEvent.USER_CANCEL,
        StageState.CANCELED,
    ),
    # =========================================================================
    # UNKNOWN (1 transition) - requires manual resolution
    # =========================================================================
    TransitionDef(StageState.UNKNOWN, StageEvent.TIMEOUT, StageState.TERMINATED),
]


def find_transition(
    from_state: StageState | str,
    event: StageEvent | str,
    context: EventContext,
) -> TransitionDef | None:
    """Find a valid transition definition for the given state and event.

    Iterates ALL matching (state, event) transitions in order and returns the
    first one whose guard passes (or has no guard). For events with multiple
    guarded transitions (e.g., FINALIZE_FAIL with critical=True/False), this
    ensures the correct transition is selected based on context.

    Args:
        from_state: Current state (StageState or string value)
        event: Event to handle (StageEvent or string value)
        context: Event context for guard evaluation

    Returns:
        TransitionDef if a valid transition exists, None otherwise.

    Example:
        >>> from datetime import datetime, UTC
        >>> ctx = EventContext(timestamp=datetime.now(UTC), source="executor", critical=False)
        >>> t = find_transition(StageState.POST_RUN, StageEvent.POST_RUN_FAIL, ctx)
        >>> t.to_state  # AWAITING_USER_FINALIZATION (because critical=False)
    """
    # Normalize to enum values if strings were passed
    if isinstance(from_state, str):
        try:
            from_state = StageState(from_state)
        except ValueError:
            return None
    if isinstance(event, str):
        try:
            event = StageEvent(event)
        except ValueError:
            return None

    for t in TRANSITIONS:
        if t.from_state == from_state and t.event == event:
            # If no guard, or guard passes, this is our transition
            if t.guard is None or t.guard(context):
                return t
            # Otherwise, keep looking for another matching transition

    return None


def get_transitions_from_state(state: StageState) -> list[TransitionDef]:
    """Get all transitions from a given state.

    Useful for validation and documentation.
    """
    return [t for t in TRANSITIONS if t.from_state == state]


def get_transitions_for_event(event: StageEvent) -> list[TransitionDef]:
    """Get all transitions triggered by a given event.

    Useful for validation and documentation.
    """
    return [t for t in TRANSITIONS if t.event == event]
