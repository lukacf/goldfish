"""Event emission layer for state machine transitions.

This module provides functions to determine what events to emit based on
exit codes, GCS status, and backend state. It replaces direct status updates
with event-driven state machine transitions.
"""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from goldfish.state_machine.exit_code import ExitCodeResult
from goldfish.state_machine.types import (
    EventContext,
    StageEvent,
    StageState,
    TerminationCause,
)
from goldfish.validation import (
    validate_container_id,
    validate_instance_name,
    validate_project_id,
    validate_zone,
)

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# GCS outage threshold before giving up (1 hour)
GCS_OUTAGE_THRESHOLD = timedelta(hours=1)


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

    # Only emit exit events for RUNNING state
    state = run.get("state")
    if state and state != StageState.RUNNING.value:
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

    Raises:
        InvalidInstanceNameError: If GCE instance name is invalid.
        InvalidContainerIdError: If Docker container ID is invalid.
    """
    # Validate inputs before subprocess calls (security)
    if backend_type == "gce":
        validate_instance_name(backend_handle)
        if project_id:
            validate_project_id(project_id)
        if zone:
            validate_zone(zone)
        return _verify_gce_instance_stopped(backend_handle, project_id, zone)
    else:
        validate_container_id(backend_handle)
        return _verify_docker_container_stopped(backend_handle)


def _verify_gce_instance_stopped(
    instance_name: str,
    project_id: str | None,
    zone: str | None = None,
) -> bool:
    """Verify GCE instance status.

    Args:
        instance_name: GCE instance name.
        project_id: GCP project ID.
        zone: GCE zone.

    Returns:
        True if instance is stopped/terminated/not found.
    """
    try:
        cmd = ["gcloud", "compute", "instances", "describe", instance_name]
        if project_id:
            cmd.extend(["--project", project_id])
        if zone:
            cmd.extend(["--zone", zone])
        cmd.extend(["--format", "value(status)"])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

        status = result.stdout.strip().upper()
        # RUNNING, STAGING, PROVISIONING = still alive
        # TERMINATED, STOPPED = dead
        return status not in ("RUNNING", "STAGING", "PROVISIONING")

    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").lower()
        if "not found" in stderr:
            return True  # Instance doesn't exist = confirmed dead
        logger.warning("Error checking GCE instance %s: %s", instance_name, e.stderr)
        return False  # Can't confirm, assume still running

    except Exception as e:
        logger.warning("Error checking GCE instance %s: %s", instance_name, e)
        return False


def _verify_docker_container_stopped(container_id: str) -> bool:
    """Verify Docker container status.

    Args:
        container_id: Docker container ID or name.

    Returns:
        True if container is stopped/exited/not found.
    """
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", container_id],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )

        status = result.stdout.strip().lower()
        # running, restarting, paused = still alive
        # exited, dead, removing, created = not running
        return status not in ("running", "restarting", "paused")

    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").lower()
        if "no such container" in stderr:
            return True  # Container doesn't exist = confirmed dead
        logger.warning("Error checking Docker container %s: %s", container_id, e.stderr)
        return False

    except Exception as e:
        logger.warning("Error checking Docker container %s: %s", container_id, e)
        return False


def detect_termination_cause(
    run_id: str,
    backend_type: str,
    backend_handle: str,
    project_id: str | None = None,
    zone: str | None = None,
) -> TerminationCause:
    """Detect why an instance/container terminated.

    Checks backend APIs for preemption, OOM, etc.

    Args:
        run_id: Stage run ID.
        backend_type: "gce" or "local".
        backend_handle: Instance name or container ID.
        project_id: GCP project ID (for GCE).
        zone: GCE zone (for GCE).

    Returns:
        TerminationCause enum value.

    Raises:
        InvalidInstanceNameError: If GCE instance name is invalid.
        InvalidContainerIdError: If Docker container ID is invalid.
    """
    # Validate inputs before subprocess calls (security)
    if backend_type == "gce":
        validate_instance_name(backend_handle)
        if project_id:
            validate_project_id(project_id)
        return _detect_gce_termination_cause(backend_handle, project_id, zone)
    else:
        validate_container_id(backend_handle)
        return _detect_docker_termination_cause(backend_handle)


def _detect_gce_termination_cause(
    instance_name: str,
    project_id: str | None,
    zone: str | None = None,
) -> TerminationCause:
    """Detect GCE instance termination cause.

    Checks GCE operations API for preemption events.

    Args:
        instance_name: GCE instance name.
        project_id: GCP project ID.
        zone: GCE zone.

    Returns:
        TerminationCause.PREEMPTED or TerminationCause.CRASHED.
    """
    try:
        # Check for preemption via operations API
        cmd = [
            "gcloud",
            "compute",
            "operations",
            "list",
            "--filter",
            f'targetLink:"{instance_name}" AND operationType:compute.instances.preempted',
            "--format",
            "json",
        ]
        if project_id:
            cmd.extend(["--project", project_id])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

        operations = json.loads(result.stdout or "[]")
        if operations:
            return TerminationCause.PREEMPTED

        return TerminationCause.CRASHED

    except Exception as e:
        logger.warning("Error detecting GCE termination cause for %s: %s", instance_name, e)
        return TerminationCause.ORPHANED


def _detect_docker_termination_cause(container_id: str) -> TerminationCause:
    """Detect Docker container termination cause.

    Checks for OOM kill.

    Args:
        container_id: Docker container ID or name.

    Returns:
        TerminationCause.CRASHED (OOM) or TerminationCause.ORPHANED.
    """
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.OOMKilled}}", container_id],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )

        oom_killed = result.stdout.strip().lower()
        if oom_killed == "true":
            return TerminationCause.CRASHED

        return TerminationCause.ORPHANED

    except Exception as e:
        logger.warning("Error detecting Docker termination cause for %s: %s", container_id, e)
        return TerminationCause.ORPHANED


def determine_instance_event(
    run: dict[str, Any],
    project_id: str | None = None,
) -> tuple[StageEvent, EventContext] | None:
    """Determine if instance has been lost.

    Checks if the backend instance/container has unexpectedly disappeared.

    Args:
        run: Stage run dictionary.
        project_id: GCP project ID (for GCE).

    Returns:
        Tuple of (INSTANCE_LOST, context) if instance is gone, None otherwise.
    """
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
