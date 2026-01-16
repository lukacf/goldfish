"""Backwards compatibility layer for legacy stage run status/progress.

During migration, some parts of Goldfish still read the legacy `stage_runs.status`
and `stage_runs.progress` columns. The state machine (`state`/`phase`) is the
source of truth, but we keep the legacy columns consistent for compatibility.
"""

from __future__ import annotations

from goldfish.models import StageRunProgress, StageRunStatus
from goldfish.state_machine.types import (
    ProgressPhase,
    StageState,
    TerminationCause,
)

# Mapping from StageState to legacy *status* strings (StageRunStatus values)
# Spec mapping:
# - preparing -> pending
# - building/launching/running/finalizing -> running
# - completed -> completed
# - failed/terminated/unknown -> failed
# - canceled -> canceled
_STATE_TO_LEGACY_STATUS: dict[StageState, str] = {
    StageState.PREPARING: StageRunStatus.PENDING.value,
    StageState.BUILDING: StageRunStatus.RUNNING.value,
    StageState.LAUNCHING: StageRunStatus.RUNNING.value,
    StageState.RUNNING: StageRunStatus.RUNNING.value,
    StageState.FINALIZING: StageRunStatus.RUNNING.value,
    StageState.COMPLETED: StageRunStatus.COMPLETED.value,
    StageState.FAILED: StageRunStatus.FAILED.value,
    StageState.TERMINATED: StageRunStatus.FAILED.value,
    StageState.CANCELED: StageRunStatus.CANCELED.value,
    StageState.UNKNOWN: StageRunStatus.FAILED.value,
}

# Mapping from TerminationCause to legacy status strings
# These override the TERMINATED state mapping when a cause is present
# Older deployments may have used extended terminal status strings.
_TERMINATION_CAUSE_TO_STATUS: dict[TerminationCause, str] = {
    TerminationCause.PREEMPTED: "preempted",
    TerminationCause.CRASHED: "crashed",
    TerminationCause.ORPHANED: "orphaned",
    TerminationCause.TIMEOUT: "timed_out",
    TerminationCause.AI_STOPPED: "ai_stopped",
    TerminationCause.MANUAL: "terminated",
}

# Mapping from StageState to legacy *progress* strings (StageRunProgress values)
_STATE_TO_LEGACY_PROGRESS: dict[StageState, str | None] = {
    StageState.BUILDING: StageRunProgress.BUILD.value,
    StageState.LAUNCHING: StageRunProgress.LAUNCH.value,
    StageState.RUNNING: StageRunProgress.RUNNING.value,
    StageState.FINALIZING: StageRunProgress.FINALIZING.value,
    # No progress for preparing/terminal/unknown
    StageState.PREPARING: None,
    StageState.COMPLETED: None,
    StageState.FAILED: None,
    StageState.TERMINATED: None,
    StageState.CANCELED: None,
    StageState.UNKNOWN: None,
}


def get_legacy_status(
    state: StageState,
    termination_cause: TerminationCause | None = None,
) -> str:
    """Convert state machine types to legacy status string (StageRunStatus value).

    Args:
        state: Current StageState.
        termination_cause: Optional termination cause (for TERMINATED state).

    Returns:
        Legacy status string compatible with previous implementation.
    """
    # Spec: status is coarse; termination_cause does not change status.
    return _STATE_TO_LEGACY_STATUS[state]


def get_legacy_progress(state: StageState, phase: ProgressPhase | None = None) -> str | None:
    """Convert state/phase to legacy progress string (StageRunProgress value).

    Note: `phase` is currently unused; progress is derived from the coarse state.
    """
    return _STATE_TO_LEGACY_PROGRESS[state]


def state_from_legacy(legacy_status: str) -> tuple[StageState, TerminationCause | None]:
    """Convert legacy status string to state machine types.

    Args:
        legacy_status: Legacy status string.

    Returns:
        Tuple of (StageState, optional TerminationCause).

    Raises:
        ValueError: If legacy_status is not recognized.
    """
    # Check termination causes first (they map to TERMINATED state)
    for cause, status in _TERMINATION_CAUSE_TO_STATUS.items():
        if status == legacy_status and legacy_status != "terminated":
            return StageState.TERMINATED, cause

    # Older deployments may have written an explicit "terminated" status string.
    # We map it to TERMINATED with no specific cause for backwards compatibility.
    if legacy_status == "terminated":
        return StageState.TERMINATED, None

    # Standard StageRunStatus values
    if legacy_status == StageRunStatus.PENDING.value:
        return StageState.PREPARING, None
    if legacy_status == StageRunStatus.RUNNING.value:
        return StageState.RUNNING, None
    if legacy_status == StageRunStatus.COMPLETED.value:
        return StageState.COMPLETED, None
    if legacy_status == StageRunStatus.FAILED.value:
        return StageState.FAILED, None
    if legacy_status == StageRunStatus.CANCELED.value:
        return StageState.CANCELED, None

    # Older status strings that correspond to progress-only phases.
    if legacy_status == StageRunProgress.BUILD.value:
        return StageState.BUILDING, None
    if legacy_status == StageRunProgress.LAUNCH.value:
        return StageState.LAUNCHING, None
    if legacy_status == StageRunProgress.FINALIZING.value:
        return StageState.FINALIZING, None

    # Check state mappings
    for state, status in _STATE_TO_LEGACY_STATUS.items():
        if status == legacy_status:
            # For "terminated" string, return None cause for roundtrip consistency
            # (get_legacy_status(TERMINATED, None) → "terminated" → state_from_legacy → (TERMINATED, None))
            return state, None

    raise ValueError(f"Unknown legacy status: {legacy_status}")
