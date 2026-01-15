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

from goldfish.state_machine.admin_tools import (
    AdminTransitionError,
    force_complete_run,
    force_fail_run,
    force_terminate_run,
)
from goldfish.state_machine.backwards_compat import (
    get_legacy_status,
    state_from_legacy,
)
from goldfish.state_machine.cancel import cancel_run
from goldfish.state_machine.core import transition, update_phase
from goldfish.state_machine.event_emission import (
    clear_gcs_outage_started,
    detect_termination_cause,
    determine_exit_event,
    determine_instance_event,
    get_gcs_outage_started,
    set_gcs_outage_started,
    verify_instance_stopped,
)
from goldfish.state_machine.exit_code import (
    ExitCodeResult,
    get_exit_code_docker,
    get_exit_code_gce,
)
from goldfish.state_machine.finalization import (
    FinalizationTracker,
    get_critical_phases_done,
)
from goldfish.state_machine.leader_election import DaemonLeaderElection
from goldfish.state_machine.stage_daemon import StageDaemon
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
from goldfish.state_machine.utils import format_transition_result

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
    "ExitCodeResult",
    # Constants
    "TRANSITIONS",
    "STATE_ENTRY_PHASES",
    "TERMINAL_STATES",
    "ACTIVE_STATES",
    "LIMBO_STATES",
    # Functions - core
    "find_transition",
    "get_transitions_from_state",
    "get_transitions_for_event",
    "transition",
    "update_phase",
    # Functions - exit code
    "get_exit_code_gce",
    "get_exit_code_docker",
    # Functions - event emission
    "determine_exit_event",
    "determine_instance_event",
    "verify_instance_stopped",
    "detect_termination_cause",
    "get_gcs_outage_started",
    "set_gcs_outage_started",
    "clear_gcs_outage_started",
    # Classes - daemon
    "DaemonLeaderElection",
    "StageDaemon",
    # Functions - cancel
    "cancel_run",
    # Finalization tracking
    "FinalizationTracker",
    "get_critical_phases_done",
    # Utility functions
    "format_transition_result",
    # Admin tools
    "AdminTransitionError",
    "force_terminate_run",
    "force_complete_run",
    "force_fail_run",
    # Backwards compatibility (deprecated)
    "get_legacy_status",
    "state_from_legacy",
]
