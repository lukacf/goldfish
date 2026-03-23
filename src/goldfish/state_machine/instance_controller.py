"""InstanceController — single entry point for mapping run events to instance events.

All warm pool instance lifecycle decisions go through this controller.
No code should UPDATE warm_instances.state directly.

Lease operations are atomic with state transitions — both happen in the
same CAS UPDATE via instance_transition(set_lease_run_id=...).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from goldfish.state_machine.instance_core import instance_transition
from goldfish.state_machine.instance_types import (
    InstanceEvent,
    InstanceEventContext,
    InstanceSourceType,
    InstanceTransitionResult,
)

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)


class InstanceController:
    """Maps run lifecycle events to instance state transitions.

    This is the ONLY place that calls instance_transition().
    External code calls controller methods, never instance_transition() directly.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # =========================================================================
    # Fresh launch
    # =========================================================================

    def on_fresh_launch(
        self,
        instance_name: str,
        stage_run_id: str,
        *,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult:
        """Fresh launch succeeded: launching → busy + acquire lease.

        Atomically transitions state AND sets current_lease_run_id.
        """
        ctx = self._ctx(source=source, stage_run_id=stage_run_id)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.BOOT_REGISTERED,
            ctx,
            set_lease_run_id=stage_run_id,
        )
        if not result.success:
            logger.warning(
                "on_fresh_launch: BOOT_REGISTERED failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_launch_failed(
        self,
        instance_name: str,
        stage_run_id: str | None = None,
        *,
        error: str | None = None,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult:
        """Fresh launch failed: launching → deleting + release lease.

        The instance may be partially created, so we go to deleting
        (not gone) to let the daemon retry gcloud delete.
        """
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, error_message=error)
        return instance_transition(
            self._db,
            instance_name,
            InstanceEvent.LAUNCH_FAILED,
            ctx,
            set_lease_run_id=None,  # Release lease atomically
        )

    # =========================================================================
    # Claim (reuse)
    # =========================================================================

    def on_claim_start(
        self,
        instance_name: str,
        stage_run_id: str,
        *,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult:
        """Claim initiated: idle_ready → claimed + acquire lease."""
        ctx = self._ctx(source=source, stage_run_id=stage_run_id)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.CLAIM_SENT,
            ctx,
            set_lease_run_id=stage_run_id,
        )
        if not result.success:
            logger.warning(
                "on_claim_start: CLAIM_SENT failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_claim_acked(
        self,
        instance_name: str,
        stage_run_id: str,
        *,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult:
        """Claim ACKed: claimed → busy (lease already held)."""
        ctx = self._ctx(source=source, stage_run_id=stage_run_id)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.CLAIM_ACKED,
            ctx,
        )
        if not result.success:
            logger.warning(
                "on_claim_acked: CLAIM_ACKED failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_claim_timeout(
        self,
        instance_name: str,
        stage_run_id: str,
        *,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult:
        """Claim timed out: claimed → deleting + release lease."""
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, reason="ACK timeout")
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.CLAIM_TIMEOUT,
            ctx,
            set_lease_run_id=None,  # Release lease atomically
        )
        if not result.success:
            logger.warning(
                "on_claim_timeout: CLAIM_TIMEOUT failed for %s: %s",
                instance_name,
                result.details,
            )
            # Always release even if transition failed — caller abandoned the claim
            self._force_release_lease(instance_name)
        return result

    # =========================================================================
    # Run terminal → instance lifecycle
    # =========================================================================

    def on_run_terminal(
        self,
        stage_run_id: str,
        terminal_state: str,
        *,
        source: InstanceSourceType = "controller",
    ) -> InstanceTransitionResult | None:
        """Run reached terminal state → transition + release lease atomically.

        For COMPLETED/FAILED/AWAITING: JOB_FINISHED (busy → draining)
        For TERMINATED/CANCELED: DELETE_REQUESTED (→ deleting)
        """
        # Find the instance via the lease column
        inst = self._find_instance_for_run(stage_run_id)
        if inst is None:
            logger.debug("on_run_terminal: no leased instance for run %s", stage_run_id)
            return None

        instance_name = inst["instance_name"]
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, reason=f"run {terminal_state}")

        delete_states = ("terminated", "canceled")
        event = InstanceEvent.DELETE_REQUESTED if terminal_state in delete_states else InstanceEvent.JOB_FINISHED

        result = instance_transition(
            self._db,
            instance_name,
            event,
            ctx,
            set_lease_run_id=None,  # Release lease atomically
        )
        if not result.success:
            logger.warning(
                "on_run_terminal: %s failed for %s (run=%s, terminal=%s): %s",
                event.name,
                instance_name,
                stage_run_id,
                terminal_state,
                result.details,
            )
        return result

    # =========================================================================
    # Daemon-observed events
    # =========================================================================

    def on_drain_complete(
        self,
        instance_name: str,
        *,
        source: InstanceSourceType = "daemon",
    ) -> InstanceTransitionResult:
        """VM reports idle_ready AND instance is in draining → idle_ready."""
        ctx = self._ctx(source=source)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.DRAIN_COMPLETE,
            ctx,
        )
        if not result.success:
            logger.debug(
                "on_drain_complete: DRAIN_COMPLETE not accepted for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_preempted(
        self,
        instance_name: str,
        *,
        source: InstanceSourceType = "daemon",
    ) -> InstanceTransitionResult:
        """VM is dead (preemption/crash/not found) → gone + release lease."""
        ctx = self._ctx(source=source, reason="preempted/dead")
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.PREEMPTED,
            ctx,
            set_lease_run_id=None,  # Release lease atomically
        )
        if not result.success:
            logger.warning(
                "on_preempted: PREEMPTED failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_delete_requested(
        self,
        instance_name: str,
        *,
        reason: str = "",
        source: InstanceSourceType = "daemon",
    ) -> InstanceTransitionResult:
        """Request deletion (idle timeout, manual, etc.) → deleting + release lease."""
        ctx = self._ctx(source=source, reason=reason)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.DELETE_REQUESTED,
            ctx,
            set_lease_run_id=None,  # Release lease atomically
        )
        if not result.success:
            logger.debug(
                "on_delete_requested: DELETE_REQUESTED not accepted for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_delete_confirmed(
        self,
        instance_name: str,
        *,
        source: InstanceSourceType = "daemon",
    ) -> InstanceTransitionResult:
        """gcloud delete succeeded → gone."""
        ctx = self._ctx(source=source)
        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.DELETE_CONFIRMED,
            ctx,
        )
        if not result.success:
            logger.warning(
                "on_delete_confirmed: DELETE_CONFIRMED failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

    def on_delete_failed(
        self,
        instance_name: str,
        *,
        error: str | None = None,
        source: InstanceSourceType = "daemon",
    ) -> InstanceTransitionResult:
        """gcloud delete failed → stay in deleting (retry later)."""
        ctx = self._ctx(source=source, error_message=error)
        return instance_transition(
            self._db,
            instance_name,
            InstanceEvent.DELETE_FAILED,
            ctx,
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _find_instance_for_run(self, stage_run_id: str) -> dict | None:
        """Find the warm instance currently leased to a run."""
        with self._db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM warm_instances WHERE current_lease_run_id = ?",
                (stage_run_id,),
            ).fetchone()
            return dict(row) if row else None

    def _force_release_lease(self, instance_name: str) -> None:
        """Unconditionally clear current_lease_run_id and audit table. Used as fallback."""
        from datetime import UTC, datetime

        now = datetime.now(UTC).isoformat()
        with self._db._conn() as conn:
            # Clear the authoritative lease column
            conn.execute(
                "UPDATE warm_instances SET current_lease_run_id = NULL WHERE instance_name = ?",
                (instance_name,),
            )
            # Also release any active audit rows to keep them consistent
            conn.execute(
                "UPDATE instance_leases SET lease_state = 'released', released_at = ? "
                "WHERE instance_name = ? AND lease_state = 'active'",
                (now, instance_name),
            )

    def _ctx(
        self,
        *,
        source: InstanceSourceType = "controller",
        stage_run_id: str | None = None,
        error_message: str | None = None,
        reason: str | None = None,
    ) -> InstanceEventContext:
        return InstanceEventContext(
            timestamp=datetime.now(UTC),
            source=source,
            stage_run_id=stage_run_id,
            error_message=error_message,
            reason=reason,
        )
