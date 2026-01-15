"""Backwards compatibility layer for legacy status strings.

This module provides mappings from new state machine types to legacy
status strings used in the previous implementation. Use this during
migration to maintain API compatibility.

DEPRECATION: All functions in this module emit DeprecationWarning.
New code should use StageState, TerminationCause, and ProgressPhase directly.
"""

from __future__ import annotations

import warnings

from goldfish.state_machine.types import (
    StageState,
    TerminationCause,
)

# Mapping from StageState to legacy status strings
_STATE_TO_LEGACY_STATUS: dict[StageState, str] = {
    StageState.PREPARING: "pending",
    StageState.BUILDING: "building",
    StageState.LAUNCHING: "launching",
    StageState.RUNNING: "running",
    StageState.FINALIZING: "finalizing",
    StageState.COMPLETED: "completed",
    StageState.FAILED: "failed",
    StageState.TERMINATED: "terminated",  # Overridden by termination_cause mapping
    StageState.CANCELED: "canceled",
    StageState.UNKNOWN: "unknown",
}

# Mapping from TerminationCause to legacy status strings
# These override the TERMINATED state mapping when a cause is present
_TERMINATION_CAUSE_TO_STATUS: dict[TerminationCause, str] = {
    TerminationCause.PREEMPTED: "preempted",
    TerminationCause.CRASHED: "crashed",
    TerminationCause.ORPHANED: "orphaned",
    TerminationCause.TIMEOUT: "timed_out",
    TerminationCause.AI_STOPPED: "ai_stopped",
    TerminationCause.MANUAL: "terminated",
}


def get_legacy_status(
    state: StageState,
    termination_cause: TerminationCause | None = None,
) -> str:
    """Convert state machine types to legacy status string.

    DEPRECATED: Use StageState and TerminationCause directly in new code.

    Args:
        state: Current StageState.
        termination_cause: Optional termination cause (for TERMINATED state).

    Returns:
        Legacy status string compatible with previous implementation.
    """
    warnings.warn(
        "get_legacy_status is deprecated. Use StageState and TerminationCause directly.",
        DeprecationWarning,
        stacklevel=2,
    )

    # For TERMINATED state with a cause, use the cause-specific status
    if state == StageState.TERMINATED and termination_cause is not None:
        return _TERMINATION_CAUSE_TO_STATUS[termination_cause]

    return _STATE_TO_LEGACY_STATUS[state]


def state_from_legacy(legacy_status: str) -> tuple[StageState, TerminationCause | None]:
    """Convert legacy status string to state machine types.

    DEPRECATED: Use StageState and TerminationCause directly in new code.

    Args:
        legacy_status: Legacy status string.

    Returns:
        Tuple of (StageState, optional TerminationCause).

    Raises:
        ValueError: If legacy_status is not recognized.
    """
    warnings.warn(
        "state_from_legacy is deprecated. Use StageState and TerminationCause directly.",
        DeprecationWarning,
        stacklevel=2,
    )

    # Check termination causes first (they map to TERMINATED state)
    for cause, status in _TERMINATION_CAUSE_TO_STATUS.items():
        if status == legacy_status and legacy_status != "terminated":
            return StageState.TERMINATED, cause

    # Check state mappings
    for state, status in _STATE_TO_LEGACY_STATUS.items():
        if status == legacy_status:
            # For "terminated" string, return None cause for roundtrip consistency
            # (get_legacy_status(TERMINATED, None) → "terminated" → state_from_legacy → (TERMINATED, None))
            return state, None

    raise ValueError(f"Unknown legacy status: {legacy_status}")
