"""Stage daemon with event-driven state machine architecture.

This module replaces the if/then/else status update logic in the old daemon
with a clean event-driven architecture that uses the state machine.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from goldfish.cloud.contracts import RunHandle
from goldfish.cloud.factory import AdapterFactory
from goldfish.errors import NotFoundError
from goldfish.state_machine.core import transition
from goldfish.state_machine.event_emission import determine_exit_event, determine_instance_event
from goldfish.state_machine.exit_code import ExitCodeResult
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
    from goldfish.cloud.protocols import RunBackend
    from goldfish.config import GoldfishConfig
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Maximum number of active runs to query (prevents memory exhaustion)
DEFAULT_ACTIVE_RUNS_LIMIT = 1000

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
        self._backend_cache: dict[str, RunBackend] = {}

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

            # Poll warm instances (replaces reap_idle, recover, retry_deleting)
            self.poll_warm_instances()

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
            # Backfill missing state_entered_at for robustness.
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

                # After daemon-driven transition, route through InstanceController.
                # The executor handles its own finalization; daemon-driven transitions
                # (e.g., after restart) also need to release/delete warm instances.
                self._try_instance_controller_finalize(run_id)

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

    def _get_backend(self, backend_type: str, *, project_id: str | None) -> RunBackend | None:
        """Get (or create) a RunBackend for a backend_type."""
        cached = self._backend_cache.get(backend_type)
        if cached is not None:
            return cached

        if self._config is None:
            return None

        configured_backend = getattr(getattr(self._config, "jobs", None), "backend", None)
        if configured_backend and configured_backend != backend_type:
            # Only monitor runs for the configured backend. This prevents creating
            # partially-configured backends (missing bucket/zones) that can produce
            # misleading statuses.
            return None

        try:
            backend = AdapterFactory(self._config, db=self._db).create_run_backend()
        except Exception as e:
            logger.warning("Failed to create run backend for stage daemon (%s): %s", backend_type, e)
            return None

        self._backend_cache[backend_type] = backend
        return backend

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
            # RACE CONDITION FIX: Re-read state to avoid emitting TIMEOUT based on stale data.
            # The 'run' dict was read at poll start; the state may have changed since then.
            # Without this check, we could timeout a run that just transitioned to a new state.
            run_id = run.get("id", UNKNOWN_RUN_ID)
            fresh_run = self._db.get_stage_run(run_id)
            if fresh_run is None:
                # Run was deleted - skip
                return None
            if not self._check_timeout(fresh_run):
                # State changed and no longer timed out - skip
                logger.debug(
                    "Run %s state changed from %s to %s, no longer timed out",
                    run_id,
                    run.get("state"),
                    fresh_run.get("state"),
                )
                return None

            # Use fresh data for the timeout event
            fresh_state_str = fresh_run.get("state")
            try:
                fresh_state = StageState(fresh_state_str) if fresh_state_str else state
            except ValueError:
                fresh_state = state

            # For POST_RUN timeout, determine critical_phases_done (v1.2: renamed from FINALIZING)
            critical_phases_done: bool | None = None
            if fresh_state == StageState.POST_RUN:
                output_sync_done = fresh_run.get("output_sync_done", 0)
                output_recording_done = fresh_run.get("output_recording_done", 0)
                critical_phases_done = bool(output_sync_done) and bool(output_recording_done)

            context = EventContext(
                timestamp=datetime.now(UTC),
                source=SOURCE_DAEMON,
                critical_phases_done=critical_phases_done,
            )
            return (StageEvent.TIMEOUT, context)

        project_id = None
        if self._config and self._config.gce:
            try:
                project_id = self._config.gce.effective_project_id
            except ValueError:
                pass

        # 1) RUNNING: check for exit code (success/failure/missing) first
        if state == StageState.RUNNING:
            exit_result: ExitCodeResult | None = None
            backend_handle = run.get("backend_handle")
            backend_type = run.get("backend_type", "local")
            if backend_handle:
                backend = self._get_backend(str(backend_type), project_id=project_id)
                if backend is not None:
                    handle = RunHandle.from_dict(
                        {
                            "stage_run_id": str(run.get("id", "")),
                            "backend_type": str(backend_type),
                            "backend_handle": str(backend_handle),
                            "zone": None,
                        }
                    )
                    try:
                        status = backend.get_status(handle)
                        if status.exit_code is not None:
                            exit_result = ExitCodeResult.from_code(status.exit_code)
                    except NotFoundError:
                        exit_result = ExitCodeResult.from_not_found()
                    except Exception as e:
                        logger.warning(
                            "Error retrieving backend status for run %s: %s",
                            run.get("id", UNKNOWN_RUN_ID),
                            e,
                        )

            event_ctx = determine_exit_event(
                run,
                exit_result,
                db=self._db,
                project_id=project_id,
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

    def _try_instance_controller_finalize(self, run_id: str) -> None:
        """Route run terminal event through InstanceController.

        Called after daemon-driven transitions. The executor handles its own
        finalization, but daemon-driven transitions (restart recovery, timeouts)
        also need to release/delete warm instances.
        """
        try:
            run = self._db.get_stage_run(run_id)
            if not run:
                return
            state = run.get("state", "")

            # Only finalize for terminal or non-owning states
            terminal_values = {s.value for s in TERMINAL_STATES}
            non_owning = terminal_values | {"awaiting_user_finalization"}
            if state not in non_owning:
                return

            from goldfish.state_machine.instance_controller import InstanceController

            controller = InstanceController(self._db)
            controller.on_run_terminal(run_id, state, source="daemon")
        except Exception as e:
            logger.debug("Instance controller finalize skipped for %s: %s", run_id, e)

    def poll_warm_instances(self) -> None:
        """Poll warm instances and emit events based on observed facts.

        Replaces recover_after_restart(), reap_idle(), retry_deleting().
        Observes GCE API, metadata, leases, timestamps → emits events.
        """
        if self._config is None:
            return

        # Get warm pool manager (if available)
        warm_pool = self._get_warm_pool_manager()
        if warm_pool is None:
            return

        from goldfish.state_machine.instance_types import InstanceState

        controller = warm_pool.controller
        instances = self._db.list_warm_instances()

        for inst in instances:
            try:
                name = inst["instance_name"]
                state_str = inst["state"]
                zone = inst["zone"]

                try:
                    state = InstanceState(state_str)
                except ValueError:
                    continue

                if state == InstanceState.GONE:
                    # Clean up gone rows
                    self._db.delete_warm_instance(name)
                    continue

                if state == InstanceState.IDLE_READY:
                    # Check idle timeout
                    if self._instance_timed_out(inst, warm_pool._config.idle_timeout_minutes * 60):
                        controller.on_delete_requested(name, reason="idle timeout")
                    continue

                if state == InstanceState.CLAIMED:
                    # Check stale claims (30s ACK timeout + 60s buffer = 90s)
                    if self._instance_timed_out(inst, 90):
                        # Release lease for stale claims
                        lease = self._db.get_active_lease_for_instance(name)
                        if lease:
                            controller.on_claim_timeout(name, lease["stage_run_id"])
                        else:
                            controller.on_delete_requested(name, reason="stale claim, no lease")
                    continue

                if state == InstanceState.DELETING:
                    # Retry gcloud delete
                    if warm_pool.delete_gce_instance(name, zone):
                        controller.on_delete_confirmed(name)
                        self._db.delete_warm_instance(name)
                    else:
                        controller.on_delete_failed(name, error="gcloud delete retry failed")
                    continue

                if state == InstanceState.DRAINING:
                    lease = self._db.get_active_lease_for_instance(name)

                    # If the lease is still active but the owning run is already
                    # terminal, the executor crashed before calling on_run_terminal.
                    # Release the stale lease so draining can proceed.
                    if lease is not None:
                        run = self._db.get_stage_run(lease["stage_run_id"])
                        run_state = run.get("state", "") if run else ""
                        terminal_values = {s.value for s in TERMINAL_STATES}
                        non_owning = terminal_values | {"awaiting_user_finalization"}
                        if run_state in non_owning:
                            logger.info(
                                "Releasing stale lease for draining instance %s (run %s in %s)",
                                name,
                                lease["stage_run_id"],
                                run_state,
                            )
                            self._db.release_instance_lease(name, lease["stage_run_id"])
                            lease = None  # Allow drain check below

                    # Check if VM reports idle_ready metadata AND no active lease
                    if lease is None:
                        metadata = warm_pool.get_instance_metadata(name, zone)
                        if metadata.get("goldfish_instance_state") == "idle_ready":
                            controller.on_drain_complete(name)
                            continue

                    # Check if VM is dead
                    if not warm_pool.check_instance_alive(name, zone):
                        controller.on_preempted(name)
                    continue

                if state == InstanceState.LAUNCHING:
                    # Don't check liveness on launching instances — the VM is still
                    # booting and the executor handles launch failures via on_launch_failed().
                    # But DO enforce a timeout: if the row has been in launching for
                    # too long (e.g. executor died before calling on_launch_failed),
                    # transition to deleting to prevent permanent capacity leak.
                    _LAUNCHING_TIMEOUT_SECONDS = 900  # 15 minutes
                    if self._instance_timed_out(inst, _LAUNCHING_TIMEOUT_SECONDS):
                        logger.warning(
                            "Instance %s stuck in launching for >%ds — transitioning to deleting",
                            name,
                            _LAUNCHING_TIMEOUT_SECONDS,
                        )
                        controller.on_delete_requested(name, reason="launching timeout")
                    continue

                if state == InstanceState.BUSY:
                    # Check if VM is dead (preemption/crash)
                    if not warm_pool.check_instance_alive(name, zone):
                        controller.on_preempted(name)
                    continue

            except Exception as e:
                logger.debug("poll_warm_instances error for %s: %s", inst.get("instance_name", "?"), e)

    def _instance_timed_out(self, inst: dict[str, Any] | Any, timeout_seconds: float) -> bool:
        """Check if an instance has been in its current state longer than timeout_seconds."""
        entered_at_str = inst.get("state_entered_at")
        if not entered_at_str:
            return False
        try:
            from datetime import UTC, datetime

            entered_at = datetime.fromisoformat(entered_at_str)
            if entered_at.tzinfo is None:
                entered_at = entered_at.replace(tzinfo=UTC)
            elapsed = (datetime.now(UTC) - entered_at).total_seconds()
            return elapsed > timeout_seconds
        except (ValueError, TypeError):
            return False

    def _get_warm_pool_manager(self) -> Any:
        """Get the WarmPoolManager if available."""
        if self._config is None:
            return None
        gce = getattr(self._config, "gce", None)
        if gce is None:
            return None
        warm_pool_cfg = getattr(gce, "warm_pool", None)
        if warm_pool_cfg is None or not warm_pool_cfg.enabled:
            return None

        try:
            from goldfish.cloud.factory import create_warm_pool_manager

            return create_warm_pool_manager(self._db, self._config)
        except Exception:
            return None

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
