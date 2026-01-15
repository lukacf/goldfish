"""Admin tools for forcing state machine transitions.

These tools are intended for administrative use only, such as:
- Recovering from stuck runs
- Forcing completion of runs that are in limbo states
- Manual intervention when automatic state transitions fail

All operations require explicit reason strings (min 15 chars) for audit trails.
Admin tools bypass the normal state machine transitions and directly update state.

NOTE: Admin tools do NOT perform backend cleanup (stopping containers, deleting
GCE instances). Use these tools only for database state correction, then clean
up infrastructure manually if needed.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from goldfish.errors import GoldfishError, validate_reason
from goldfish.state_machine.types import (
    StageState,
    TerminationCause,
)
from goldfish.validation import validate_stage_run_id

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)


class AdminTransitionError(GoldfishError):
    """Error during admin state transition."""

    pass


def _admin_transition(
    db: Database,
    run_id: str,
    new_state: StageState,
    reason: str,
    termination_cause: TerminationCause | None = None,
    allowed_states: set[StageState] | None = None,
) -> dict[str, Any]:
    """Atomically read current state, validate, and update in a single transaction.

    This function performs the complete read-validate-write cycle within a single
    database transaction to prevent race conditions.

    Args:
        db: Database instance.
        run_id: Stage run ID to transition.
        new_state: Target state.
        reason: Why this transition is being forced.
        termination_cause: Cause for TERMINATED state (optional).
        allowed_states: If set, only allow transition from these states.

    Returns:
        Dict with success status and transition details.

    Raises:
        AdminTransitionError: If run not found or state validation fails.
    """
    with db._conn() as conn:
        # Read current state within transaction
        row = conn.execute(
            "SELECT state FROM stage_runs WHERE id = ?",
            (run_id,),
        ).fetchone()

        if row is None:
            raise AdminTransitionError(
                f"Run '{run_id}' not found",
                details={"run_id": run_id},
            )

        previous_state = StageState(row["state"])

        # Validate allowed states if specified
        if allowed_states is not None and previous_state not in allowed_states:
            raise AdminTransitionError(
                f"Cannot transition from state '{previous_state.value}'. "
                f"Allowed states: {', '.join(s.value for s in sorted(allowed_states, key=lambda s: s.value))}",
                details={
                    "run_id": run_id,
                    "current_state": previous_state.value,
                    "allowed_states": [s.value for s in allowed_states],
                },
            )

        # Update state atomically (same transaction as read)
        now = datetime.now(UTC)
        if termination_cause:
            conn.execute(
                """UPDATE stage_runs
                   SET state = ?, termination_cause = ?, state_entered_at = ?
                   WHERE id = ?""",
                (new_state.value, termination_cause.value, now.isoformat(), run_id),
            )
        else:
            conn.execute(
                """UPDATE stage_runs
                   SET state = ?, state_entered_at = ?
                   WHERE id = ?""",
                (new_state.value, now.isoformat(), run_id),
            )

        # Record in audit table with full details
        # Note: audit.reason is required (CHECK >= 15 chars), details is JSON string
        audit_details = json.dumps(
            {
                "run_id": run_id,
                "previous_state": previous_state.value,
                "new_state": new_state.value,
                "termination_cause": termination_cause.value if termination_cause else None,
            }
        )
        try:
            conn.execute(
                """INSERT INTO audit (timestamp, operation, slot, workspace, reason, details)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (now.isoformat(), "admin_state_change", None, None, reason, audit_details),
            )
        except Exception as e:
            logger.warning("Failed to record admin audit for run %s: %s", run_id, e)

    # Build result dict
    result: dict[str, Any] = {
        "success": True,
        "run_id": run_id,
        "previous_state": previous_state.value,
        "new_state": new_state.value,
    }
    if termination_cause:
        result["termination_cause"] = termination_cause.value
    return result


def force_terminate_run(
    db: Database,
    run_id: str,
    reason: str,
    termination_cause: TerminationCause = TerminationCause.MANUAL,
) -> dict[str, Any]:
    """Force a run into TERMINATED state.

    Can be called from any state. Use for runs stuck in non-terminal states
    that cannot be recovered.

    Args:
        db: Database instance.
        run_id: Stage run ID to terminate.
        reason: Why this run is being terminated (min 15 chars).
        termination_cause: Cause for termination (default: MANUAL).

    Returns:
        Dict with success status and transition details.

    Raises:
        InvalidStageRunIdError: If run_id format is invalid.
        ReasonTooShortError: If reason is less than 15 characters.
        AdminTransitionError: If run not found.
    """
    validate_stage_run_id(run_id)
    validate_reason(reason)

    return _admin_transition(
        db=db,
        run_id=run_id,
        new_state=StageState.TERMINATED,
        reason=reason,
        termination_cause=termination_cause,
        allowed_states=None,  # Can terminate from any state
    )


def force_complete_run(
    db: Database,
    run_id: str,
    reason: str,
) -> dict[str, Any]:
    """Force a run into COMPLETED state.

    Only allowed from FINALIZING or UNKNOWN states. Use when a run completed
    successfully but the state transition didn't happen (e.g., daemon crash).

    Args:
        db: Database instance.
        run_id: Stage run ID to complete.
        reason: Why this run is being force-completed (min 15 chars).

    Returns:
        Dict with success status and transition details.

    Raises:
        InvalidStageRunIdError: If run_id format is invalid.
        ReasonTooShortError: If reason is less than 15 characters.
        AdminTransitionError: If run not found or not in allowed state.
    """
    validate_stage_run_id(run_id)
    validate_reason(reason)

    return _admin_transition(
        db=db,
        run_id=run_id,
        new_state=StageState.COMPLETED,
        reason=reason,
        allowed_states={StageState.FINALIZING, StageState.UNKNOWN},
    )


def force_fail_run(
    db: Database,
    run_id: str,
    reason: str,
) -> dict[str, Any]:
    """Force a run into FAILED state.

    Only allowed from UNKNOWN state. Use when a run failed but the failure
    wasn't properly recorded (e.g., daemon crash during failure handling).

    Args:
        db: Database instance.
        run_id: Stage run ID to fail.
        reason: Why this run is being force-failed (min 15 chars).

    Returns:
        Dict with success status and transition details.

    Raises:
        InvalidStageRunIdError: If run_id format is invalid.
        ReasonTooShortError: If reason is less than 15 characters.
        AdminTransitionError: If run not found or not in UNKNOWN state.
    """
    validate_stage_run_id(run_id)
    validate_reason(reason)

    return _admin_transition(
        db=db,
        run_id=run_id,
        new_state=StageState.FAILED,
        reason=reason,
        allowed_states={StageState.UNKNOWN},
    )
