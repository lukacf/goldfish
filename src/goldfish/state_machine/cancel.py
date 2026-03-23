"""Cancel functionality using state machine transitions.

This module provides the cancel_run() function that uses the state machine
to emit USER_CANCEL events and trigger backend cleanup.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from goldfish.errors import validate_reason
from goldfish.state_machine.core import transition
from goldfish.state_machine.types import (
    EventContext,
    SourceType,
    StageEvent,
)
from goldfish.state_machine.utils import format_transition_result
from goldfish.validation import (
    validate_stage_run_id,
)

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Source type for MCP tool calls
SOURCE_MCP_TOOL: SourceType = "mcp_tool"


def cancel_run(
    db: Database,
    run_id: str,
    reason: str,
) -> dict[str, Any]:
    """Cancel a running stage using the state machine.

    Emits USER_CANCEL event to transition to CANCELED state,
    then triggers best-effort backend cleanup.

    Args:
        db: Database instance.
        run_id: Stage run ID to cancel.
        reason: Why the run is being canceled (min 15 chars).

    Returns:
        Dict with success status, previous_state, new_state, reason,
        and optional cleanup_error.

    Raises:
        InvalidStageRunIdError: If run_id format is invalid.
        ReasonTooShortError: If reason is less than 15 characters.
    """
    validate_stage_run_id(run_id)
    validate_reason(reason)

    # Get run info before transition (for cleanup and result)
    run_info = _get_run_info(db, run_id)
    if run_info is None:
        return {
            "success": False,
            "run_id": run_id,
            "previous_state": None,
            "new_state": None,
            "reason": "not_found",
        }

    previous_state = run_info["state"]
    backend_type = run_info.get("backend_type")
    backend_handle = run_info.get("backend_handle")

    # Create event context
    context = EventContext(
        timestamp=datetime.now(UTC),
        source=SOURCE_MCP_TOOL,
        error_message=f"Canceled: {reason}",
    )

    # Emit USER_CANCEL event
    result = transition(db, run_id, StageEvent.USER_CANCEL, context)

    # Format response
    response = format_transition_result(result, run_id, previous_state)

    # Route through InstanceController for warm pool lifecycle.
    # If the controller handles it (active lease found → DELETE_REQUESTED emitted),
    # skip direct backend cleanup to avoid redundant synchronous gcloud deletes.
    warm_pool_handled = False
    if result.success:
        try:
            from goldfish.state_machine.instance_controller import InstanceController

            controller = InstanceController(db)
            ctrl_result = controller.on_run_terminal(run_id, "canceled", source="controller")
            if ctrl_result is not None:
                warm_pool_handled = True
        except Exception as e:
            logger.debug("Instance controller on_run_terminal skipped for %s: %s", run_id, e)

    # Best-effort backend cleanup (only if transition succeeded AND warm pool
    # didn't already handle it — avoids redundant synchronous gcloud delete)
    if result.success and backend_type and backend_handle and not warm_pool_handled:
        try:
            _cleanup_backend(run_id, backend_type, backend_handle)
        except Exception as e:
            logger.warning("Failed to cleanup backend for %s (%s:%s): %s", run_id, backend_type, backend_handle, e)
            response["cleanup_error"] = "Backend cleanup failed"

    return response


def _cleanup_backend(run_id: str, backend_type: str, backend_handle: str) -> None:
    """Cleanup backend resources after cancellation.

    This is best-effort - failures are logged but don't affect the cancel result.
    Uses the RunBackend protocol for backend-agnostic termination.

    Args:
        run_id: Stage run ID (for logging).
        backend_type: "local" or "gce".
        backend_handle: Container ID or instance name.

    Raises:
        InvalidContainerIdError: If backend_handle is invalid for local backend.
        InvalidInstanceNameError: If backend_handle is invalid for GCE backend.
        ValueError: If backend_type is unknown.
    """
    # Import here to avoid circular dependencies
    from goldfish.cloud.contracts import RunHandle
    from goldfish.cloud.factory import create_backend_for_cleanup, validate_backend_handle

    # Delegate to adapter via factory - all backend logic is in the adapter
    try:
        backend = create_backend_for_cleanup(backend_type)
    except ValueError:
        logger.warning("Unknown backend type %s for run %s", backend_type, run_id)
        return

    # Validate at the boundary using backend-aware rules.
    validate_backend_handle(backend_type, backend_handle)

    handle = RunHandle.from_dict(
        {
            "stage_run_id": run_id,
            "backend_type": backend_type,
            "backend_handle": backend_handle,
        }
    )

    backend.terminate(handle)
    logger.info("Terminated %s resource %s for run %s", backend_type, backend_handle, run_id)


def _get_run_info(db: Database, run_id: str) -> dict[str, Any] | None:
    """Get run info needed for cancel operation.

    Args:
        db: Database instance.
        run_id: Stage run ID.

    Returns:
        Dict with keys 'state', 'backend_type', 'backend_handle',
        or None if run not found.
    """
    with db._conn() as conn:
        row = conn.execute(
            "SELECT state, backend_type, backend_handle FROM stage_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        return dict(row) if row else None
