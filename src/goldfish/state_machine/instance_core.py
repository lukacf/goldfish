"""Core instance state machine logic with CAS semantics.

Implements instance_transition() — the single entry point for all
warm_instances state changes. Mirrors the CAS pattern in core.py.

Lease operations (acquire/release) are performed atomically in the same
UPDATE statement as the state transition, eliminating partial-failure windows.
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

# Sentinel: "don't touch the lease column"
_LEASE_UNCHANGED = object()


def instance_transition(
    db: Database,
    instance_name: str,
    event: InstanceEvent,
    context: InstanceEventContext,
    *,
    set_lease_run_id: str | None | object = _LEASE_UNCHANGED,
) -> InstanceTransitionResult:
    """Atomically transition a warm instance's state using CAS semantics.

    The state change and optional lease operation happen in a single UPDATE,
    so there is no window where state and lease are inconsistent.

    Args:
        db: Database instance.
        instance_name: Warm instance name.
        event: Event to process.
        context: Event context for audit trail.
        set_lease_run_id: Optional lease operation performed atomically:
            - _LEASE_UNCHANGED (default): don't touch current_lease_run_id
            - "stage-xxx": set current_lease_run_id to this value (acquire lease)
            - None: clear current_lease_run_id (release lease)

    Returns:
        InstanceTransitionResult with success status.
    """
    with db._conn() as conn:
        # 1. Read current state
        row = conn.execute(
            "SELECT state, current_lease_run_id FROM warm_instances WHERE instance_name = ?",
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
            for t in INSTANCE_TRANSITIONS:
                if t.event == event and t.to_state == current_state and t.from_state != current_state:
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

        # 4. CAS UPDATE — state + lease in one atomic operation
        if set_lease_run_id is _LEASE_UNCHANGED:
            result = conn.execute(
                """
                UPDATE warm_instances
                SET state = ?, state_entered_at = ?
                WHERE instance_name = ? AND state = ?
                """,
                (to_state.value, timestamp_str, instance_name, current_state.value),
            )
        else:
            # Acquire (set to run_id) or release (set to NULL) the lease
            result = conn.execute(
                """
                UPDATE warm_instances
                SET state = ?, state_entered_at = ?, current_lease_run_id = ?
                WHERE instance_name = ? AND state = ?
                """,
                (to_state.value, timestamp_str, set_lease_run_id, instance_name, current_state.value),
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

        # 6. Append to instance_leases audit log if lease changed
        if set_lease_run_id is not _LEASE_UNCHANGED:
            if set_lease_run_id is not None:
                # Acquiring lease — insert or reactivate if same pair was released
                conn.execute(
                    """
                    INSERT INTO instance_leases
                        (instance_name, stage_run_id, lease_state, claimed_at)
                    VALUES (?, ?, 'active', ?)
                    ON CONFLICT(instance_name, stage_run_id) DO UPDATE
                        SET lease_state = 'active', claimed_at = excluded.claimed_at, released_at = NULL
                    """,
                    (instance_name, set_lease_run_id, timestamp_str),
                )
            else:
                # Releasing lease — mark any active lease as released
                old_lease_run_id = row["current_lease_run_id"]
                if old_lease_run_id:
                    conn.execute(
                        """
                        UPDATE instance_leases
                        SET lease_state = 'released', released_at = ?
                        WHERE instance_name = ? AND stage_run_id = ? AND lease_state = 'active'
                        """,
                        (timestamp_str, instance_name, old_lease_run_id),
                    )

        return InstanceTransitionResult(
            success=True,
            new_state=to_state,
            reason="ok",
        )
