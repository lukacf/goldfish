"""Stage daemon with event-driven state machine architecture.

This module replaces the if/then/else status update logic in the old daemon
with a clean event-driven architecture that uses the state machine.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from goldfish.state_machine.core import transition
from goldfish.state_machine.event_emission import (
    detect_termination_cause,
    verify_instance_stopped,
)
from goldfish.state_machine.leader_election import (
    _VALID_ID_PATTERN,
    DaemonLeaderElection,
)
from goldfish.state_machine.transitions import TERMINAL_STATES
from goldfish.state_machine.types import (
    EventContext,
    SourceType,
    StageEvent,
    StageState,
    TerminationCause,
)

if TYPE_CHECKING:
    from goldfish.config import GoldfishConfig
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Maximum number of active runs to query (prevents memory exhaustion)
DEFAULT_ACTIVE_RUNS_LIMIT = 1000

# Backend type constants
BACKEND_GCE = "gce"
BACKEND_LOCAL = "local"

# Event source identifier (typed for mypy)
SOURCE_DAEMON: SourceType = "daemon"

# Default run ID when not available
UNKNOWN_RUN_ID = "unknown"

# Timeout durations for each state - how long a run can remain in a state
# before the daemon considers it stuck and emits a TIMEOUT event.
# NOTE: These are different from GOLDFISH_GCE_NOT_FOUND_TIMEOUT (5min) which
# controls how long to wait for a missing GCE instance before declaring it lost.
# ML training jobs legitimately run for hours/days, so RUNNING timeout is 24h.
STATE_TIMEOUTS: dict[StageState, timedelta] = {
    StageState.PREPARING: timedelta(minutes=15),
    StageState.BUILDING: timedelta(minutes=30),
    StageState.LAUNCHING: timedelta(minutes=20),
    StageState.RUNNING: timedelta(hours=24),
    StageState.FINALIZING: timedelta(minutes=30),
    StageState.UNKNOWN: timedelta(hours=24),
}


class StageDaemon:
    """Event-driven stage daemon using state machine.

    This daemon polls active runs and emits appropriate events to the
    state machine based on run state, backend status, and timeouts.
    """

    def __init__(
        self,
        db: Database,
        config: GoldfishConfig | None = None,
        holder_id: str | None = None,
    ) -> None:
        """Initialize the stage daemon.

        Args:
            db: Database instance.
            config: Goldfish configuration.
            holder_id: Unique identifier for leader election. Auto-generated if None.

        Raises:
            ValueError: If holder_id is provided but invalid.
        """
        self._db = db
        self._config = config

        # Validate holder_id if provided, otherwise auto-generate
        if holder_id is not None:
            if not _VALID_ID_PATTERN.match(holder_id):
                raise ValueError(f"Invalid holder_id: must match {_VALID_ID_PATTERN.pattern}")
            self._holder_id = holder_id
        else:
            self._holder_id = DaemonLeaderElection.generate_holder_id()

        self._leader = DaemonLeaderElection(db)

    def poll_active_runs(self) -> None:
        """Poll active runs and emit events.

        Acquires leader lease before processing to prevent duplicate
        event emission from multiple daemon instances.
        """
        # Try to acquire lease
        if not self._leader.try_acquire_lease(self._holder_id):
            logger.debug("Could not acquire lease, skipping poll")
            return

        try:
            runs = self.get_active_runs()
            logger.debug("Found %d active runs to process", len(runs))

            for run in runs:
                self._process_run(run)

        except Exception as e:
            logger.exception("Error polling active runs: %s", e)
        # Note: We don't release the lease - it will auto-expire if we crash

    def _process_run(self, run: dict[str, Any]) -> None:
        """Process a single run and emit events if needed.

        Args:
            run: Stage run dictionary from database.
        """
        run_id = run.get("id", UNKNOWN_RUN_ID)

        try:
            result = self._determine_event(run)
            if result is not None:
                event, context = result
                logger.info(
                    "Emitting event %s for run %s (state=%s)",
                    event.name,
                    run_id,
                    run.get("state"),
                )
                self._transition(run_id, event, context)

        except Exception as e:
            logger.exception("Error processing run %s: %s", run_id, e)

    def _transition(
        self,
        run_id: str,
        event: StageEvent,
        context: EventContext,
    ) -> None:
        """Emit a transition to the state machine.

        This is a separate method to allow mocking in tests.

        Args:
            run_id: Stage run ID.
            event: Event to emit.
            context: Event context.
        """
        transition(self._db, run_id, event, context)

    def _determine_event(
        self,
        run: dict[str, Any],
    ) -> tuple[StageEvent, EventContext] | None:
        """Determine what event to emit for a run.

        This is pure logic with no side effects - it only examines
        the run state and determines the appropriate event.

        Args:
            run: Stage run dictionary.

        Returns:
            Tuple of (event, context) or None if no event should be emitted.
        """
        state_str = run.get("state")
        if not state_str:
            return None

        try:
            state = StageState(state_str)
        except ValueError:
            return None

        # Terminal states don't need processing
        if state in TERMINAL_STATES:
            return None

        # Check for timeout first (applies to all active states)
        if self._check_timeout(run):
            # For FINALIZING timeout, determine critical_phases_done
            critical_phases_done: bool | None = None
            if state == StageState.FINALIZING:
                output_sync_done = run.get("output_sync_done", 0)
                output_recording_done = run.get("output_recording_done", 0)
                critical_phases_done = bool(output_sync_done) and bool(output_recording_done)

            context = EventContext(
                timestamp=datetime.now(UTC),
                source=SOURCE_DAEMON,
                critical_phases_done=critical_phases_done,
            )
            return (StageEvent.TIMEOUT, context)

        # Backend-specific event detection
        backend_type = run.get("backend_type", BACKEND_LOCAL)
        project_id = None
        if backend_type == BACKEND_GCE and self._config and self._config.gce:
            try:
                project_id = self._config.gce.effective_project_id
            except ValueError:
                pass
        return self._determine_backend_event(run, backend_type, project_id)

    def _determine_backend_event(
        self,
        run: dict[str, Any],
        backend_type: str,
        project_id: str | None = None,
    ) -> tuple[StageEvent, EventContext] | None:
        """Determine event for a backend (GCE or local Docker).

        Checks if the instance/container is still running and determines
        the appropriate event if not.

        Args:
            run: Stage run dictionary.
            backend_type: Either "gce" or "local".
            project_id: GCP project ID (only for GCE backend).

        Returns:
            Tuple of (event, context) or None.
        """
        backend_handle = run.get("backend_handle")
        if not backend_handle:
            return None

        run_id = run.get("id", UNKNOWN_RUN_ID)
        backend_label = "GCE instance" if backend_type == BACKEND_GCE else "Docker container"

        # Check if instance/container is stopped
        try:
            is_stopped = verify_instance_stopped(
                run_id=run_id,
                backend_type=backend_type,
                backend_handle=backend_handle,
                project_id=project_id,
            )
        except Exception as e:
            logger.warning("Error checking %s %s: %s", backend_label, backend_handle, e)
            return None

        if is_stopped:
            # Detect termination cause
            try:
                cause = detect_termination_cause(
                    run_id=run_id,
                    backend_type=backend_type,
                    backend_handle=backend_handle,
                    project_id=project_id,
                )
            except Exception as e:
                logger.warning(
                    "Error detecting termination cause for %s %s: %s, " "defaulting to ORPHANED",
                    backend_label,
                    backend_handle,
                    e,
                )
                cause = TerminationCause.ORPHANED

            context = EventContext(
                timestamp=datetime.now(UTC),
                source=SOURCE_DAEMON,
                termination_cause=cause,
                instance_confirmed_dead=True,
            )
            return (StageEvent.INSTANCE_LOST, context)

        return None

    def _check_timeout(self, run: dict[str, Any]) -> bool:
        """Check if a run has timed out in its current state.

        Args:
            run: Stage run dictionary.

        Returns:
            True if the run has exceeded the timeout for its state.
        """
        state_str = run.get("state")
        state_entered_at_str = run.get("state_entered_at")

        if not state_str or not state_entered_at_str:
            return False

        try:
            state = StageState(state_str)
        except ValueError:
            return False

        timeout = STATE_TIMEOUTS.get(state)
        if timeout is None:
            return False

        try:
            state_entered_at = datetime.fromisoformat(state_entered_at_str)
            # Ensure timezone-aware comparison
            if state_entered_at.tzinfo is None:
                state_entered_at = state_entered_at.replace(tzinfo=UTC)

            elapsed = datetime.now(UTC) - state_entered_at
            return elapsed > timeout

        except (ValueError, TypeError):
            return False

    def get_active_runs(self, limit: int = DEFAULT_ACTIVE_RUNS_LIMIT) -> list[dict[str, Any]]:
        """Get all runs in active states.

        Uses the partial index on state for efficient querying.
        Limited to prevent memory exhaustion with large result sets.

        Args:
            limit: Maximum number of runs to return (default 1000).

        Returns:
            List of stage run dictionaries.
        """
        active_states = (
            StageState.PREPARING.value,
            StageState.BUILDING.value,
            StageState.LAUNCHING.value,
            StageState.RUNNING.value,
            StageState.FINALIZING.value,
            StageState.UNKNOWN.value,  # Include UNKNOWN for cleanup
        )

        # Only select columns actually used by _determine_event
        with self._db._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, state, state_entered_at, backend_type, backend_handle,
                       output_sync_done, output_recording_done
                FROM stage_runs
                WHERE state IN ({','.join('?' * len(active_states))})
                ORDER BY started_at ASC
                LIMIT ?
                """,
                (*active_states, limit),
            ).fetchall()

        return [dict(row) for row in rows]
