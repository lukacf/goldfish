"""Event emission layer for state machine transitions.

This module provides functions to determine what events to emit based on
exit codes, GCS status, and backend state. It replaces direct status updates
with event-driven state machine transitions.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from goldfish.cloud.contracts import StorageURI
from goldfish.state_machine.exit_code import ExitCodeResult
from goldfish.state_machine.types import (
    EventContext,
    StageEvent,
    StageState,
    TerminationCause,
)

if TYPE_CHECKING:
    from goldfish.cloud.protocols import ObjectStorage
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# GCS outage threshold before giving up (1 hour)
GCS_OUTAGE_THRESHOLD = timedelta(hours=1)

# Default grace period before checking liveness during LAUNCHING.
# Configurable via defaults.launching_grace_seconds in goldfish.yaml.
# Must exceed the longest VM boot time (a3-megagpu-8g: 5-7 min GPU driver loading).
DEFAULT_LAUNCHING_GRACE_SECONDS = 600  # 10 minutes


def determine_exit_event(
    run: dict[str, Any],
    exit_result: ExitCodeResult | None,
    *,
    db: Database | None = None,
    project_id: str | None = None,
    zone: str | None = None,
) -> tuple[StageEvent, EventContext] | None:
    """Determine what exit event to emit based on exit code result.

    This function is called by the daemon when a stage is in RUNNING state
    and we've checked for an exit code.

    Args:
        run: Stage run dictionary with state, id, gcs_outage_started, etc.
        exit_result: Result of exit code retrieval.

    Returns:
        Tuple of (event, context) to emit, or None if no event should be emitted.
    """
    if exit_result is None:
        return None

    # Only emit exit events for states where a backend handle exists and
    # the job may have run. LAUNCHING is included to recover exit codes
    # when the executor crashes after launch() but before LAUNCH_OK.
    state = run.get("state")
    allowed_states = {StageState.RUNNING.value, StageState.LAUNCHING.value}
    if state and state not in allowed_states:
        return None

    now = datetime.now(UTC)

    # Handle GCS errors specially
    if exit_result.gcs_error:
        return _handle_gcs_error(
            run,
            exit_result,
            now,
            db=db,
            project_id=project_id,
            zone=zone,
        )

    # If GCS was previously erroring but is now reachable, clear outage marker.
    if db is not None and get_gcs_outage_started(run) is not None:
        clear_gcs_outage_started(db, run_id=run.get("id", ""))

    # Create base context
    context = EventContext(
        timestamp=now,
        source="daemon",
        exit_code=exit_result.code,
        exit_code_exists=exit_result.exists,
    )

    # Determine event based on exit result
    if exit_result.exists and exit_result.code == 0:
        # Check if this was an AI-requested stop
        ai_stop_info = check_ai_stop_requested(run, db=db, project_id=project_id)
        if ai_stop_info is not None:
            context.termination_cause = TerminationCause.AI_STOPPED
            context.svs_review_id = ai_stop_info.get("svs_review_id")
            return (StageEvent.AI_STOP, context)
        return (StageEvent.EXIT_SUCCESS, context)
    elif exit_result.exists and exit_result.code is not None:
        return (StageEvent.EXIT_FAILURE, context)
    elif not exit_result.exists:
        # Spec: Only emit EXIT_MISSING once we've verified the instance/container is stopped.
        backend_handle = run.get("backend_handle")
        if not backend_handle:
            return None

        backend_type = run.get("backend_type", "local")
        is_stopped = verify_instance_stopped(
            run_id=run.get("id", ""),
            backend_type=backend_type,
            backend_handle=backend_handle,
            project_id=project_id,
            zone=zone,
        )
        if not is_stopped:
            return None

        cause = detect_termination_cause(
            run_id=run.get("id", ""),
            backend_type=backend_type,
            backend_handle=backend_handle,
            project_id=project_id,
            zone=zone,
        )
        context.instance_confirmed_dead = True
        context.termination_cause = cause
        context.exit_code = None
        context.exit_code_exists = False
        return (StageEvent.EXIT_MISSING, context)

    return None


def _handle_gcs_error(
    run: dict[str, Any],
    exit_result: ExitCodeResult,
    now: datetime,
    *,
    db: Database | None,
    project_id: str | None,
    zone: str | None,
) -> tuple[StageEvent, EventContext] | None:
    """Handle GCS error during exit code retrieval.

    GCS errors get special handling:
    - First error: Don't emit event, just track outage start
    - Under 1 hour: Keep waiting
    - Over 1 hour: Emit EXIT_MISSING with gcs_error flag

    Args:
        run: Stage run dictionary.
        exit_result: Exit code result with gcs_error=True.
        now: Current timestamp.

    Returns:
        Tuple of (event, context) or None.
    """
    outage_started = get_gcs_outage_started(run)

    if outage_started is None:
        # First GCS error - start outage clock and don't emit yet.
        if db is not None:
            set_gcs_outage_started(db, run_id=run.get("id", ""), timestamp=now)
        return None

    # Check if we've exceeded the threshold
    elapsed = now - outage_started
    if elapsed < GCS_OUTAGE_THRESHOLD:
        # Keep waiting
        return None

    # Exceeded threshold - only emit EXIT_MISSING once instance is confirmed dead.
    backend_handle = run.get("backend_handle")
    if not backend_handle:
        return None

    backend_type = run.get("backend_type", "local")
    is_stopped = verify_instance_stopped(
        run_id=run.get("id", ""),
        backend_type=backend_type,
        backend_handle=backend_handle,
        project_id=project_id,
        zone=zone,
    )
    if not is_stopped:
        return None

    cause = detect_termination_cause(
        run_id=run.get("id", ""),
        backend_type=backend_type,
        backend_handle=backend_handle,
        project_id=project_id,
        zone=zone,
    )

    # Exceeded threshold - give up and emit EXIT_MISSING
    context = EventContext(
        timestamp=now,
        source="daemon",
        exit_code=None,
        exit_code_exists=False,
        gcs_error=True,
        gcs_outage_started=outage_started,
        error_message=exit_result.error,
        instance_confirmed_dead=True,
        termination_cause=cause,
    )
    return (StageEvent.EXIT_MISSING, context)


def get_gcs_outage_started(run: dict[str, Any]) -> datetime | None:
    """Get the GCS outage start time from run data.

    Args:
        run: Stage run dictionary.

    Returns:
        Datetime when GCS outage started, or None.
    """
    value = run.get("gcs_outage_started")
    if value is None:
        return None

    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def set_gcs_outage_started(db: Database, run_id: str, timestamp: datetime) -> None:
    """Set the GCS outage start time in database.

    Args:
        db: Database instance.
        run_id: Stage run ID.
        timestamp: When the outage started.
    """
    db.update_stage_run_gcs_outage(run_id, timestamp.isoformat())


def clear_gcs_outage_started(db: Database, run_id: str) -> None:
    """Clear the GCS outage start time in database.

    Called when GCS becomes available again.

    Args:
        db: Database instance.
        run_id: Stage run ID.
    """
    db.update_stage_run_gcs_outage(run_id, None)


def verify_instance_stopped(
    run_id: str,
    backend_type: str,
    backend_handle: str,
    project_id: str | None = None,
    zone: str | None = None,
) -> bool:
    """Verify that an instance/container has stopped.

    Used to confirm that EXIT_MISSING is due to actual termination,
    not just GCS lag.

    Args:
        run_id: Stage run ID.
        backend_type: "gce" or "local".
        backend_handle: Instance name or container ID.
        project_id: GCP project ID (for GCE).
        zone: GCE zone (for GCE).

    Returns:
        True if instance is confirmed stopped/missing, False if still running.
    """
    from goldfish.cloud.contracts import RunHandle
    from goldfish.cloud.factory import create_backend_for_cleanup, validate_backend_handle
    from goldfish.errors import NotFoundError

    try:
        backend = create_backend_for_cleanup(backend_type, project_id=project_id)
    except ValueError:
        logger.warning("Unknown backend type %s for run %s", backend_type, run_id)
        return False

    validate_backend_handle(backend_type, backend_handle)

    handle = RunHandle.from_dict(
        {
            "stage_run_id": run_id,
            "backend_type": backend_type,
            "backend_handle": backend_handle,
            "zone": zone,
        }
    )
    try:
        status = backend.get_status(handle)
    except NotFoundError:
        return True
    except Exception as e:
        logger.warning(
            "Error checking backend status for %s (%s:%s): %s",
            run_id,
            backend_type,
            backend_handle,
            e,
        )
        return False

    return status.status.is_terminal()


def detect_termination_cause(
    run_id: str,
    backend_type: str,
    backend_handle: str,
    project_id: str | None = None,
    zone: str | None = None,
) -> TerminationCause:
    """Detect why an instance/container terminated.

    Delegates backend probing to the RunBackend adapter and infers a
    TerminationCause from the normalized BackendStatus.

    Args:
        run_id: Stage run ID.
        backend_type: "gce" or "local".
        backend_handle: Instance name or container ID.
        project_id: GCP project ID (for GCE).
        zone: GCE zone (for GCE).

    Returns:
        TerminationCause enum value.
    """
    from goldfish.cloud.contracts import RunHandle
    from goldfish.cloud.factory import create_backend_for_cleanup, validate_backend_handle
    from goldfish.errors import NotFoundError

    try:
        backend = create_backend_for_cleanup(backend_type, project_id=project_id)
    except ValueError:
        logger.warning("Unknown backend type %s for run %s", backend_type, run_id)
        return TerminationCause.ORPHANED

    validate_backend_handle(backend_type, backend_handle)

    handle = RunHandle.from_dict(
        {
            "stage_run_id": run_id,
            "backend_type": backend_type,
            "backend_handle": backend_handle,
            "zone": zone,
        }
    )
    try:
        status = backend.get_status(handle)
    except NotFoundError:
        return TerminationCause.ORPHANED
    except Exception as e:
        logger.warning(
            "Error getting backend status for %s (%s:%s): %s",
            run_id,
            backend_type,
            backend_handle,
            e,
        )
        return TerminationCause.ORPHANED

    raw_cause = (status.termination_cause or "").strip().lower()
    cause_map: dict[str, TerminationCause] = {
        "preemption": TerminationCause.PREEMPTED,
        "oom": TerminationCause.CRASHED,
        "timeout": TerminationCause.TIMEOUT,
        "user": TerminationCause.MANUAL,
    }
    if raw_cause:
        mapped = cause_map.get(raw_cause)
        if mapped is not None:
            return mapped

    # Preserve legacy default behavior when the backend can't provide a cause.
    default_by_backend: dict[str, TerminationCause] = {
        "gce": TerminationCause.CRASHED,
        "local": TerminationCause.ORPHANED,
    }
    return default_by_backend.get(backend_type, TerminationCause.ORPHANED)


def check_ai_stop_requested(
    run: dict[str, Any],
    *,
    db: Database | None = None,
    project_id: str | None = None,
    storage: ObjectStorage | None = None,
    bucket_uri: StorageURI | None = None,
) -> dict[str, Any] | None:
    """Check if AI/SVS requested a stop for this run.

    Checks for the existence of a stop_requested file, which is written
    by during-run SVS when it detects issues.

    Args:
        run: Stage run dictionary.
        db: Database instance (to look up svs_review_id).
        project_id: GCP project ID (for GCE).

    Returns:
        Dict with stop info including svs_review_id, or None if no stop requested.
    """
    run_id = run.get("id", "")
    stop_requested = False

    # Local: check outputs directory (preferred when available).
    try:
        outputs_dir = run.get("outputs_dir")
        if outputs_dir:
            from pathlib import Path

            stop_file = Path(outputs_dir) / ".goldfish" / "stop_requested"
            stop_requested = stop_file.exists()
    except Exception as e:
        logger.debug("Error checking local stop_requested for %s: %s", run_id, e)

    # Remote: check via the storage adapter (requires bucket_uri).
    if not stop_requested and storage is not None and bucket_uri is not None:
        try:
            stop_uri = bucket_uri.join("runs", run_id, "outputs", ".goldfish", "stop_requested")
            stop_requested = storage.exists(stop_uri)
        except Exception as e:
            logger.debug("Error checking remote stop_requested for %s: %s", run_id, e)

    if not stop_requested:
        return None

    # Stop was requested - look up the svs_review_id that triggered it
    svs_review_id = None
    if db is not None:
        try:
            with db._conn() as conn:
                # Find the most recent during_run review for this run
                row = conn.execute(
                    """
                    SELECT id FROM svs_reviews
                    WHERE stage_run_id = ? AND review_type = 'during_run'
                    ORDER BY reviewed_at DESC
                    LIMIT 1
                    """,
                    (run_id,),
                ).fetchone()
                if row:
                    svs_review_id = str(row["id"])
        except Exception as e:
            logger.debug("Error looking up svs_review_id for %s: %s", run_id, e)

    return {"stop_requested": True, "svs_review_id": svs_review_id}


def determine_instance_event(
    run: dict[str, Any],
    project_id: str | None = None,
    launching_grace_seconds: int | None = None,
) -> tuple[StageEvent, EventContext] | None:
    """Determine if instance has been lost.

    Checks if the backend instance/container has unexpectedly disappeared.

    Checks for RUNNING and LAUNCHING states. LAUNCHING is included because
    preemption CAN occur after the instance is created but before ACK is received.
    The backend_handle check guards against checking before the instance exists.

    The following states are excluded from INSTANCE_LOST checks:
    - PREPARING: Code syncing, no instance yet
    - BUILDING: Docker build, no instance yet
    - POST_RUN: Instance stopping is EXPECTED after the process exits.
      The executor handles POST_RUN → AWAITING_USER_FINALIZATION via POST_RUN_OK.
    - AWAITING_USER_FINALIZATION: Instance may have been cleaned up after post-run
    - UNKNOWN: Cannot assume anything about instance existence

    NOTE: The daemon polls every 2 seconds. For LAUNCHING state, the instance
    might not be visible yet if backend_handle is not set. The check below
    (if not backend_handle: return None) handles this case.

    Args:
        run: Stage run dictionary.
        project_id: GCP project ID (for GCE).

    Returns:
        Tuple of (INSTANCE_LOST, context) if instance is gone, None otherwise.
    """
    # Check instance status for RUNNING state only.
    #
    # States where we should NOT emit INSTANCE_LOST:
    # - PREPARING: syncing code, no instance yet
    # - BUILDING: building Docker image, no instance yet
    # - LAUNCHING: backend_handle is set but instance may not be visible in GCE
    #   API yet (5+ second propagation delay on GPU VMs). The executor handles
    #   launch timeouts via wait_for_completion(). See stage-3a4ce565 where the
    #   daemon killed the instance 5s after launch due to this race.
    # - POST_RUN: instance stopping is EXPECTED after process exits
    # - AWAITING_USER_FINALIZATION: instance may have been cleaned up after post-run
    # - UNKNOWN: can't assume anything about instance existence
    #
    # States where we SHOULD check for INSTANCE_LOST:
    # - RUNNING: instance is confirmed running, should still exist
    #
    # CRITICAL: POST_RUN must be excluded because the instance stopping is EXPECTED
    # after the process exits. The post-run phase handles output sync and finalization,
    # and the executor will emit POST_RUN_OK when done. Emitting INSTANCE_LOST here
    # would race with POST_RUN_OK and incorrectly terminate successful runs.
    state_str = run.get("state")
    if state_str:
        try:
            state = StageState(state_str)
            # States where INSTANCE_LOST should NEVER be emitted
            states_skip_instance_lost = {
                StageState.PREPARING,  # No instance yet
                StageState.BUILDING,  # No instance yet
                StageState.POST_RUN,  # Instance stopping is EXPECTED after exit
                StageState.AWAITING_USER_FINALIZATION,  # Instance may be cleaned up
                StageState.UNKNOWN,  # Can't assume anything
            }
            if state in states_skip_instance_lost:
                return None

            # LAUNCHING: check liveness only after a grace period.
            # Must exceed the longest VM boot time (a3-megagpu-8g: 5-7 min
            # for GPU driver loading). Configurable via goldfish.yaml
            # defaults.launching_grace_seconds (default 10 min).
            if state == StageState.LAUNCHING:
                entered_str = run.get("state_entered_at")
                if not entered_str:
                    return None  # No timestamp — can't determine age, skip conservatively
                grace = timedelta(
                    seconds=launching_grace_seconds
                    if launching_grace_seconds is not None
                    else DEFAULT_LAUNCHING_GRACE_SECONDS
                )
                try:
                    entered_at = datetime.fromisoformat(entered_str)
                    if entered_at.tzinfo is None:
                        entered_at = entered_at.replace(tzinfo=UTC)
                    if datetime.now(UTC) - entered_at < grace:
                        return None  # Still within grace period
                except (ValueError, TypeError):
                    return None  # Unparseable timestamp — skip conservatively
                # Grace period elapsed — fall through to liveness check below

        except ValueError:
            pass  # Unknown state string, proceed with check

    backend_type = run.get("backend_type", "local")
    backend_handle = run.get("backend_handle")

    if not backend_handle:
        return None

    is_stopped = verify_instance_stopped(
        run_id=run.get("id", ""),
        backend_type=backend_type,
        backend_handle=backend_handle,
        project_id=project_id,
    )

    if is_stopped:
        # Detect why it stopped
        cause = detect_termination_cause(
            run_id=run.get("id", ""),
            backend_type=backend_type,
            backend_handle=backend_handle,
            project_id=project_id,
        )

        context = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            termination_cause=cause,
            instance_confirmed_dead=True,
        )
        return (StageEvent.INSTANCE_LOST, context)

    return None
