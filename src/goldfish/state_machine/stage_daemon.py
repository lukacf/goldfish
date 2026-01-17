"""Stage daemon with event-driven state machine architecture.

This module replaces the if/then/else status update logic in the old daemon
with a clean event-driven architecture that uses the state machine.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from goldfish.state_machine.core import transition
from goldfish.state_machine.event_emission import determine_exit_event, determine_instance_event
from goldfish.state_machine.exit_code import get_exit_code_docker, get_exit_code_gce
from goldfish.state_machine.leader_election import (
    DaemonLeaderElection,
    validate_holder_id,
)
from goldfish.state_machine.transitions import TERMINAL_STATES
from goldfish.state_machine.types import (
    EventContext,
    SourceType,
    StageEvent,
    StageState,
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
    StageState.POST_RUN: timedelta(minutes=30),  # v1.2: renamed from FINALIZING
    # Note: AWAITING_USER_FINALIZATION has no timeout - it requires user action
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
            validate_holder_id(holder_id)  # Raises ValueError if invalid
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
            # Backfill missing state_entered_at for robustness (most commonly affects UNKNOWN
            # rows created/modified during migrations).
            self._ensure_state_entered_at(run)

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

    def _ensure_state_entered_at(self, run: dict[str, Any]) -> None:
        """Ensure state_entered_at is populated for active/limbo runs.

        Some legacy/migrated rows may have `state` set but `state_entered_at` missing.
        The daemon's timeout logic relies on `state_entered_at`, so we backfill it
        using the run's `started_at` when available.
        """
        state_str = run.get("state")
        if not state_str:
            return

        # If already present, nothing to do.
        if run.get("state_entered_at"):
            return

        try:
            state = StageState(state_str)
        except ValueError:
            return

        if state in TERMINAL_STATES:
            return

        run_id = run.get("id", UNKNOWN_RUN_ID)
        started_at = run.get("started_at")
        if not started_at:
            started_at = datetime.now(UTC).isoformat()

        try:
            with self._db._conn() as conn:
                result = conn.execute(
                    """
                    UPDATE stage_runs
                    SET state_entered_at = ?
                    WHERE id = ? AND state = ?
                      AND (state_entered_at IS NULL OR state_entered_at = '')
                    """,
                    (started_at, run_id, state.value),
                )
        except Exception:
            # Best-effort only: a failure to backfill should not block event processing.
            return

        rowcount = getattr(result, "rowcount", 0)
        if isinstance(rowcount, int) and rowcount > 0:
            run["state_entered_at"] = started_at

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
            # For POST_RUN timeout, determine critical_phases_done (v1.2: renamed from FINALIZING)
            critical_phases_done: bool | None = None
            if state == StageState.POST_RUN:
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
        zone = None
        if backend_type == BACKEND_GCE and self._config and self._config.gce:
            try:
                project_id = self._config.gce.effective_project_id
            except ValueError:
                pass
            zone = getattr(self._config.gce, "zone", None)

        # 1) RUNNING: check for exit code (success/failure/missing) first
        if state == StageState.RUNNING:
            exit_result = None
            try:
                if backend_type == BACKEND_LOCAL:
                    backend_handle = run.get("backend_handle")
                    if backend_handle:
                        exit_result = get_exit_code_docker(backend_handle)
                elif backend_type == BACKEND_GCE and self._config and self._config.gcs and self._config.gcs.bucket:
                    bucket = self._config.gcs.bucket
                    bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
                    exit_result = get_exit_code_gce(bucket_uri, run.get("id", ""), project_id=project_id)
            except Exception as e:
                logger.warning("Error retrieving exit code for run %s: %s", run.get("id", UNKNOWN_RUN_ID), e)
                exit_result = None
            event_ctx = determine_exit_event(
                run,
                exit_result,
                db=self._db,
                project_id=project_id,
                zone=zone,
            )
            if event_ctx is not None:
                return event_ctx

        # 2) Any active state: instance/container lost
        instance_event = determine_instance_event(run, project_id=project_id)
        if instance_event is not None:
            return instance_event

        return None

    # (backend event detection moved to determine_exit_event/determine_instance_event)

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
            StageState.POST_RUN.value,  # v1.2: renamed from FINALIZING
            StageState.AWAITING_USER_FINALIZATION.value,  # v1.2: new state
            StageState.UNKNOWN.value,  # Include UNKNOWN for cleanup
        )

        # Only select columns actually used by _determine_event
        with self._db._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, state, state_entered_at, started_at, backend_type, backend_handle,
                       output_sync_done, output_recording_done, gcs_outage_started
                FROM stage_runs
                WHERE state IN ({','.join('?' * len(active_states))})
                ORDER BY started_at ASC
                LIMIT ?
                """,
                (*active_states, limit),
            ).fetchall()

        return [dict(row) for row in rows]
