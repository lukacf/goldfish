"""Core state machine logic with CAS semantics.

This module implements:
- transition(): Atomically transition a stage run's state using CAS semantics
- update_phase(): Update the phase within a state with CAS guard

All state mutations go through these functions to ensure consistency
and create audit trails.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import TYPE_CHECKING

from goldfish.state_machine.transitions import (
    STATE_ENTRY_PHASES,
    TRANSITIONS,
    find_transition,
)
from goldfish.state_machine.types import (
    EventContext,
    ProgressPhase,
    StageEvent,
    StageState,
    TerminationCause,
    TransitionResult,
)

if TYPE_CHECKING:
    from goldfish.db.database import Database


def transition(
    db: Database,
    run_id: str,
    event: StageEvent,
    context: EventContext,
) -> TransitionResult:
    """Atomically transition a stage run's state.

    Uses CAS (Compare-And-Swap) semantics to prevent race conditions:
    1. Read current state INSIDE transaction
    2. Find valid transition for (state, event, context)
    3. Update state with WHERE clause checking expected state
    4. Insert audit record in same transaction
    5. Return result

    Args:
        db: Database instance
        run_id: Stage run ID (e.g., "stage-abc123")
        event: Event to process
        context: Event context with details for guards and audit

    Returns:
        TransitionResult with success status and new state or error details.

    Example:
        >>> ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        >>> result = transition(db, "stage-abc", StageEvent.BUILD_START, ctx)
        >>> if result.success:
        ...     print(f"Now in {result.new_state}")
    """
    with db._conn() as conn:
        # 1. Read current state INSIDE transaction (prevents TOCTOU)
        row = conn.execute(
            "SELECT state, completed_with_warnings FROM stage_runs WHERE id = ?",
            (run_id,),
        ).fetchone()

        if row is None:
            return TransitionResult(
                success=False,
                reason="not_found",
                details=f"Stage run '{run_id}' not found",
            )

        current_state_str = row["state"]

        # Handle case where state column doesn't exist yet (migration not done)
        if current_state_str is None:
            return TransitionResult(
                success=False,
                reason="state_not_set",
                details="State column not populated (migration required)",
            )

        try:
            current_state = StageState(current_state_str)
        except ValueError:
            return TransitionResult(
                success=False,
                reason="invalid_state",
                details=f"Unknown state value: {current_state_str}",
            )

        # 2. Find valid transition
        trans_def = find_transition(current_state, event, context)

        # 3. Handle idempotency (guard-aware)
        if trans_def is None:
            # Spec: If current state is a valid target for this event AND the guard
            # passes for THIS context, treat as idempotent success.
            for t in TRANSITIONS:
                if t.event == event and t.to_state == current_state:
                    if t.guard is None or t.guard(context):
                        return TransitionResult(
                            success=True,
                            new_state=current_state,
                            reason="already_in_target_state",
                        )

            # No valid transition
            return TransitionResult(
                success=False,
                reason="no_transition",
                details=f"No valid transition from {current_state} on {event}",
            )

        to_state = trans_def.to_state

        # 4. Determine new phase (use STATE_ENTRY_PHASES for the new state)
        new_phase = context.phase
        if new_phase is None:
            new_phase = STATE_ENTRY_PHASES.get(to_state)

        # 5. Determine if completed_with_warnings should be set or preserved
        # When transitioning from AWAITING_USER_FINALIZATION to COMPLETED, preserve the flag
        if current_state == StageState.AWAITING_USER_FINALIZATION and to_state == StageState.COMPLETED:
            completed_with_warnings = bool(row["completed_with_warnings"])
        else:
            completed_with_warnings = _should_set_completed_with_warnings(event, context, to_state)

        # 6. Build the UPDATE query with CAS
        timestamp_str = context.timestamp.isoformat()
        phase_str = new_phase.value if new_phase else None
        error_str = context.error_message
        completed_at_str = (
            timestamp_str
            if to_state in (StageState.COMPLETED, StageState.FAILED, StageState.TERMINATED, StageState.CANCELED)
            else None
        )

        # Spec: termination_cause is ONLY for TERMINATED state
        termination_cause_str: str | None = None
        if to_state == StageState.TERMINATED:
            if context.termination_cause is not None:
                termination_cause_str = context.termination_cause.value
            elif event == StageEvent.TIMEOUT:
                termination_cause_str = TerminationCause.TIMEOUT.value

        # CAS UPDATE: only update if state matches expected
        # NOTE: status column is deprecated - only state is updated
        result = conn.execute(
            """
            UPDATE stage_runs
            SET state = ?,
                phase = ?,
                state_entered_at = ?,
                phase_updated_at = ?,
                termination_cause = ?,
                error = ?,
                completed_with_warnings = ?,
                completed_at = ?
            WHERE id = ? AND state = ?
            """,
            (
                to_state.value,
                phase_str,
                timestamp_str,
                timestamp_str,
                termination_cause_str,
                error_str,
                1 if completed_with_warnings else 0,
                completed_at_str,
                run_id,
                current_state.value,
            ),
        )

        # 7. Check CAS result
        if result.rowcount == 0:
            # State changed between read and update (race condition)
            return TransitionResult(
                success=False,
                reason="stale_state",
                details=f"State changed from {current_state} before update completed",
            )

        # 8. Insert audit record (normalized columns; no context_json)
        conn.execute(
            """
            INSERT INTO stage_state_transitions
            (stage_run_id, from_state, to_state, event,
             phase, termination_cause, exit_code, exit_code_exists, error_message,
             svs_review_id, source, created_at)
            VALUES (?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?)
            """,
            (
                run_id,
                current_state.value,
                to_state.value,
                event.value,
                phase_str,
                termination_cause_str,
                context.exit_code,
                1 if context.exit_code_exists else 0,
                error_str,
                context.svs_review_id,
                context.source,
                timestamp_str,
            ),
        )

        return TransitionResult(
            success=True,
            new_state=to_state,
            reason="ok",
        )


def update_phase(
    db: Database,
    run_id: str,
    expected_state: StageState,
    new_phase: ProgressPhase,
    timestamp: datetime,
    *,
    source: str = "executor",
) -> bool:
    """Update phase within a state with CAS guard.

    Only updates the phase if the run is still in the expected state.
    This prevents updating phase for a run that has already transitioned.

    Args:
        db: Database instance
        run_id: Stage run ID
        expected_state: State the run must be in for update to proceed
        new_phase: New phase to set
        timestamp: When the phase change occurred

    Returns:
        True if update succeeded, False if state didn't match (CAS failure).

    Example:
        >>> success = update_phase(
        ...     db, "stage-abc",
        ...     expected_state=StageState.PREPARING,
        ...     new_phase=ProgressPhase.VERSIONING,
        ...     timestamp=datetime.now(UTC)
        ... )
    """
    with db._conn() as conn:
        result = conn.execute(
            """
            UPDATE stage_runs
            SET phase = ?, phase_updated_at = ?
            WHERE id = ? AND state = ?
              AND (phase_updated_at IS NULL OR phase_updated_at <= ?)
            """,
            (
                new_phase.value,
                timestamp.isoformat(),
                run_id,
                expected_state.value,
                timestamp.isoformat(),
            ),
        )

        if result.rowcount == 0:
            return False

        # Record phase_update pseudo-event for audit/provenance.
        conn.execute(
            """
            INSERT INTO stage_state_transitions
            (stage_run_id, from_state, to_state, event,
             phase, termination_cause, exit_code, exit_code_exists, error_message,
             svs_review_id, source, created_at)
            VALUES (?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?)
            """,
            (
                run_id,
                expected_state.value,
                expected_state.value,
                "phase_update",
                new_phase.value,
                None,
                None,
                None,
                None,
                None,  # svs_review_id (always NULL for phase updates)
                source,
                timestamp.isoformat(),
            ),
        )

        return True


def _should_set_completed_with_warnings(event: StageEvent, context: EventContext, to_state: StageState) -> bool:
    """Determine if completed_with_warnings flag should be set.

    v1.2: With the unified finalization model, non-critical failures and timeouts
    with critical phases done now go to AWAITING_USER_FINALIZATION instead of COMPLETED.
    The completed_with_warnings flag is set when transitioning to AWAITING_USER_FINALIZATION
    for these specific events, so it persists through to COMPLETED when USER_FINALIZE is called.

    Set when:
    - POST_RUN_FAIL with critical=False goes to AWAITING_USER_FINALIZATION
    - TIMEOUT in POST_RUN with critical_phases_done=True goes to AWAITING_USER_FINALIZATION
    """
    if to_state != StageState.AWAITING_USER_FINALIZATION:
        return False

    if event == StageEvent.POST_RUN_FAIL and context.critical is False:
        return True

    if event == StageEvent.TIMEOUT and context.critical_phases_done is True:
        return True

    return False


def _serialize_context(context: EventContext) -> str:
    """Serialize EventContext to JSON for audit trail."""
    data = asdict(context)

    # Convert datetime objects to ISO strings
    if data.get("timestamp"):
        data["timestamp"] = data["timestamp"].isoformat()
    if data.get("gcs_outage_started"):
        data["gcs_outage_started"] = data["gcs_outage_started"].isoformat()

    # Convert enums to values
    if data.get("termination_cause"):
        data["termination_cause"] = data["termination_cause"].value
    if data.get("phase"):
        data["phase"] = data["phase"].value

    return json.dumps(data)
