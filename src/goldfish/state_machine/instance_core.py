"""Core instance state machine logic with CAS semantics.

Implements instance_transition() — the single entry point for all
warm_instances state changes. Mirrors the CAS pattern in core.py.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from goldfish.state_machine.instance_transitions import (
    INSTANCE_TRANSITIONS,
    find_instance_transition,
)
from goldfish.state_machine.instance_types import (
    InstanceEvent,
    InstanceEventContext,
    InstanceState,
    InstanceTransitionResult,
)

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)


def instance_transition(
    db: Database,
    instance_name: str,
    event: InstanceEvent,
    context: InstanceEventContext,
) -> InstanceTransitionResult:
    """Atomically transition a warm instance's state using CAS semantics.

    1. Read current state INSIDE transaction
    2. Find valid transition for (state, event)
    3. Update state with WHERE clause checking expected state
    4. Insert audit record in same transaction
    5. Return result

    Args:
        db: Database instance.
        instance_name: Warm instance name.
        event: Event to process.
        context: Event context for audit trail.

    Returns:
        InstanceTransitionResult with success status.
    """
    with db._conn() as conn:
        # 1. Read current state
        row = conn.execute(
            "SELECT state FROM warm_instances WHERE instance_name = ?",
            (instance_name,),
        ).fetchone()

        if row is None:
            return InstanceTransitionResult(
                success=False,
                reason="not_found",
                details=f"Instance '{instance_name}' not found",
            )

        current_state_str = row["state"]
        if current_state_str is None:
            return InstanceTransitionResult(
                success=False,
                reason="state_not_set",
                details="State column not populated",
            )

        try:
            current_state = InstanceState(current_state_str)
        except ValueError:
            return InstanceTransitionResult(
                success=False,
                reason="invalid_state",
                details=f"Unknown state value: {current_state_str}",
            )

        # 2. Find valid transition
        trans_def = find_instance_transition(current_state, event, context)

        # 3. Handle idempotency
        if trans_def is None:
            # Check if already in target state for this event
            for t in INSTANCE_TRANSITIONS:
                if t.event == event and t.to_state == current_state:
                    return InstanceTransitionResult(
                        success=True,
                        new_state=current_state,
                        reason="already_in_target_state",
                    )

            return InstanceTransitionResult(
                success=False,
                reason="no_transition",
                details=f"No valid transition from {current_state} on {event}",
            )

        to_state = trans_def.to_state
        timestamp_str = context.timestamp.isoformat()

        # 4. CAS UPDATE
        result = conn.execute(
            """
            UPDATE warm_instances
            SET state = ?, state_entered_at = ?
            WHERE instance_name = ? AND state = ?
            """,
            (to_state.value, timestamp_str, instance_name, current_state.value),
        )

        if result.rowcount == 0:
            return InstanceTransitionResult(
                success=False,
                reason="stale_state",
                details=f"State changed from {current_state} before update",
            )

        # 5. Insert audit record
        conn.execute(
            """
            INSERT INTO instance_state_transitions
            (instance_name, from_state, to_state, event,
             stage_run_id, error_message, reason, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                instance_name,
                current_state.value,
                to_state.value,
                event.value,
                context.stage_run_id,
                context.error_message,
                context.reason,
                context.source,
                timestamp_str,
            ),
        )

        return InstanceTransitionResult(
            success=True,
            new_state=to_state,
            reason="ok",
        )
