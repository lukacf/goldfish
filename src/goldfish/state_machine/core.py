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
        row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", (run_id,)).fetchone()

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

        # 3. Handle idempotency for terminal states
        if trans_def is None:
            # Check if we're already in the target state (idempotency)
            # For events that always lead to the same state (like FINALIZE_OK → COMPLETED),
            # if we're already in that state, it's idempotent
            potential_target = _find_potential_target_state(event)

            if potential_target is not None and potential_target == current_state:
                # Already in target state - idempotent success
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

        # 5. Determine if completed_with_warnings should be set
        completed_with_warnings = _should_set_completed_with_warnings(event, context, to_state)

        # 6. Build the UPDATE query with CAS
        timestamp_str = context.timestamp.isoformat()
        phase_str = new_phase.value if new_phase else None
        termination_cause_str = context.termination_cause.value if context.termination_cause else None
        error_str = context.error_message

        # CAS UPDATE: only update if state matches expected
        if completed_with_warnings:
            result = conn.execute(
                """
                UPDATE stage_runs
                SET state = ?,
                    phase = ?,
                    state_entered_at = ?,
                    phase_updated_at = ?,
                    termination_cause = COALESCE(?, termination_cause),
                    error = COALESCE(?, error),
                    completed_with_warnings = 1
                WHERE id = ? AND state = ?
                """,
                (
                    to_state.value,
                    phase_str,
                    timestamp_str,
                    timestamp_str,
                    termination_cause_str,
                    error_str,
                    run_id,
                    current_state.value,
                ),
            )
        else:
            result = conn.execute(
                """
                UPDATE stage_runs
                SET state = ?,
                    phase = ?,
                    state_entered_at = ?,
                    phase_updated_at = ?,
                    termination_cause = COALESCE(?, termination_cause),
                    error = COALESCE(?, error)
                WHERE id = ? AND state = ?
                """,
                (
                    to_state.value,
                    phase_str,
                    timestamp_str,
                    timestamp_str,
                    termination_cause_str,
                    error_str,
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

        # 8. Insert audit record
        context_json = _serialize_context(context)
        conn.execute(
            """
            INSERT INTO stage_state_transitions
            (stage_run_id, from_state, to_state, event, context_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                current_state.value,
                to_state.value,
                event.value,
                context_json,
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
            """,
            (new_phase.value, timestamp.isoformat(), run_id, expected_state.value),
        )

        return result.rowcount > 0


def _find_potential_target_state(event: StageEvent) -> StageState | None:
    """Find the most common target state for an event, ignoring guards.

    Used for idempotency checking - to see what state an event
    would normally lead to, regardless of which state we're in.

    For events that can lead to multiple states (like FINALIZE_FAIL),
    this returns None since idempotency is guard-dependent.
    """
    target_states: set[StageState] = set()
    for t in TRANSITIONS:
        if t.event == event:
            target_states.add(t.to_state)

    # If event leads to exactly one state, return it
    # Otherwise, idempotency depends on guards, return None
    if len(target_states) == 1:
        return target_states.pop()
    return None


def _should_set_completed_with_warnings(event: StageEvent, context: EventContext, to_state: StageState) -> bool:
    """Determine if completed_with_warnings flag should be set.

    Set when:
    - FINALIZE_FAIL with critical=False goes to COMPLETED
    - TIMEOUT in FINALIZING with critical_phases_done=True goes to COMPLETED
    """
    if to_state != StageState.COMPLETED:
        return False

    if event == StageEvent.FINALIZE_FAIL and context.critical is False:
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
