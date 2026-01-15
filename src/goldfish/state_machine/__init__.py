"""Stage Execution State Machine.

This module implements the state machine for managing stage run lifecycle,
replacing the ad-hoc if/then/else state management scattered across Goldfish.

The state machine becomes the single source of truth for stage run state,
with all existing state-mutation code rewritten to emit events instead.

Key components:
- StageState: All possible states (PREPARING, BUILDING, LAUNCHING, etc.)
- StageEvent: Events that trigger transitions (BUILD_START, BUILD_OK, etc.)
- EventContext: Context attached to each event for audit and decision-making
- transition(): Atomically transition state using CAS semantics
- find_transition(): Find valid transition for (state, event, context) triple

See docs/state-machine-spec.md for the full specification.
"""

from goldfish.state_machine.core import transition, update_phase
from goldfish.state_machine.transitions import (
    ACTIVE_STATES,
    LIMBO_STATES,
    STATE_ENTRY_PHASES,
    TERMINAL_STATES,
    TRANSITIONS,
    find_transition,
    get_transitions_for_event,
    get_transitions_from_state,
)
from goldfish.state_machine.types import (
    EventContext,
    ProgressPhase,
    SourceType,
    StageEvent,
    StageState,
    TerminationCause,
    TransitionDef,
    TransitionResult,
)

__all__ = [
    # Types
    "StageState",
    "StageEvent",
    "TerminationCause",
    "ProgressPhase",
    "SourceType",
    "EventContext",
    "TransitionResult",
    "TransitionDef",
    # Constants
    "TRANSITIONS",
    "STATE_ENTRY_PHASES",
    "TERMINAL_STATES",
    "ACTIVE_STATES",
    "LIMBO_STATES",
    # Functions
    "find_transition",
    "get_transitions_from_state",
    "get_transitions_for_event",
    "transition",
    "update_phase",
]
