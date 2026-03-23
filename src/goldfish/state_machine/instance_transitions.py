"""Transition table for the Instance State Machine.

The transition table is the single source of truth for all valid instance
state changes. Any state change not in this table is invalid.
"""

from __future__ import annotations

from goldfish.state_machine.instance_types import (
    InstanceEvent,
    InstanceEventContext,
    InstanceState,
    InstanceTransitionDef,
)

# =============================================================================
# State Categories
# =============================================================================

TERMINAL_INSTANCE_STATES = frozenset({InstanceState.GONE})

ACTIVE_INSTANCE_STATES = frozenset(
    {
        InstanceState.LAUNCHING,
        InstanceState.BUSY,
        InstanceState.DRAINING,
        InstanceState.IDLE_READY,
        InstanceState.CLAIMED,
        InstanceState.DELETING,
    }
)

# States that can accept DELETE_REQUESTED
DELETABLE_STATES = frozenset(
    {
        InstanceState.LAUNCHING,
        InstanceState.BUSY,
        InstanceState.DRAINING,
        InstanceState.IDLE_READY,
        InstanceState.CLAIMED,
    }
)

# =============================================================================
# Transition Table
# =============================================================================

INSTANCE_TRANSITIONS: list[InstanceTransitionDef] = [
    # launching
    InstanceTransitionDef(InstanceState.LAUNCHING, InstanceEvent.BOOT_REGISTERED, InstanceState.BUSY),
    InstanceTransitionDef(InstanceState.LAUNCHING, InstanceEvent.LAUNCH_FAILED, InstanceState.DELETING),
    InstanceTransitionDef(InstanceState.LAUNCHING, InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
    # busy
    InstanceTransitionDef(InstanceState.BUSY, InstanceEvent.JOB_FINISHED, InstanceState.DRAINING),
    InstanceTransitionDef(InstanceState.BUSY, InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
    # draining
    InstanceTransitionDef(InstanceState.DRAINING, InstanceEvent.DRAIN_COMPLETE, InstanceState.IDLE_READY),
    InstanceTransitionDef(InstanceState.DRAINING, InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
    # idle_ready
    InstanceTransitionDef(InstanceState.IDLE_READY, InstanceEvent.CLAIM_SENT, InstanceState.CLAIMED),
    InstanceTransitionDef(InstanceState.IDLE_READY, InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
    # claimed
    InstanceTransitionDef(InstanceState.CLAIMED, InstanceEvent.CLAIM_ACKED, InstanceState.BUSY),
    InstanceTransitionDef(InstanceState.CLAIMED, InstanceEvent.CLAIM_TIMEOUT, InstanceState.DELETING),
    InstanceTransitionDef(InstanceState.CLAIMED, InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
    # deleting
    InstanceTransitionDef(InstanceState.DELETING, InstanceEvent.DELETE_CONFIRMED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.DELETING, InstanceEvent.DELETE_FAILED, InstanceState.DELETING),
    # * + PREEMPTED → gone (from any non-terminal state)
    InstanceTransitionDef(InstanceState.LAUNCHING, InstanceEvent.PREEMPTED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.BUSY, InstanceEvent.PREEMPTED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.DRAINING, InstanceEvent.PREEMPTED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.IDLE_READY, InstanceEvent.PREEMPTED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.CLAIMED, InstanceEvent.PREEMPTED, InstanceState.GONE),
    InstanceTransitionDef(InstanceState.DELETING, InstanceEvent.PREEMPTED, InstanceState.GONE),
]


def find_instance_transition(
    from_state: InstanceState | str,
    event: InstanceEvent | str,
    context: InstanceEventContext | None = None,
) -> InstanceTransitionDef | None:
    """Find a valid transition for the given state and event.

    Args:
        from_state: Current instance state.
        event: Event to handle.
        context: Event context (unused for now, reserved for guards).

    Returns:
        InstanceTransitionDef if valid, None otherwise.
    """
    if isinstance(from_state, str):
        try:
            from_state = InstanceState(from_state)
        except ValueError:
            return None
    if isinstance(event, str):
        try:
            event = InstanceEvent(event)
        except ValueError:
            return None

    for t in INSTANCE_TRANSITIONS:
        if t.from_state == from_state and t.event == event:
            return t

    return None
