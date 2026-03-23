"""InstanceController — single entry point for mapping run events to instance events.

All warm pool instance lifecycle decisions go through this controller.
No code should UPDATE warm_instances.state directly.
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
        """Fresh launch succeeded: launching → busy, create lease.

        Called after gcloud create succeeds for a pre-registered instance.
        """
        ctx = self._ctx(source=source, stage_run_id=stage_run_id)

        # Create lease first (binds run to instance).
        # If lease creation fails, do NOT transition to busy — a busy instance
        # without a lease can never be released by on_run_terminal, stranding it.
        try:
            self._db.create_instance_lease(instance_name, stage_run_id)
        except Exception as e:
            logger.warning("Failed to create lease for %s/%s: %s", instance_name, stage_run_id, e)
            return InstanceTransitionResult(
                success=False,
                reason="lease_failed",
                details=f"Could not create lease: {e}",
            )

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.BOOT_REGISTERED,
            ctx,
        )
        if not result.success:
            # Roll back the lease since we didn't transition
            self._db.release_instance_lease(instance_name, stage_run_id)
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
        """Fresh launch failed: launching → deleting.

        The instance may be partially created, so we go to deleting
        (not gone) to let the daemon retry gcloud delete.
        """
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, error_message=error)

        # Release lease if one exists
        if stage_run_id:
            self._db.release_instance_lease(instance_name, stage_run_id)

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.LAUNCH_FAILED,
            ctx,
        )
        if not result.success:
            logger.warning(
                "on_launch_failed: LAUNCH_FAILED failed for %s: %s",
                instance_name,
                result.details,
            )
        return result

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
        """Claim initiated: idle_ready → claimed, create lease."""
        ctx = self._ctx(source=source, stage_run_id=stage_run_id)

        # Create lease
        try:
            self._db.create_instance_lease(instance_name, stage_run_id)
        except Exception as e:
            logger.warning("Failed to create lease for claim %s/%s: %s", instance_name, stage_run_id, e)
            return InstanceTransitionResult(
                success=False,
                reason="lease_failed",
                details=str(e),
            )

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.CLAIM_SENT,
            ctx,
        )
        if not result.success:
            # Roll back lease
            self._db.release_instance_lease(instance_name, stage_run_id)
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
        """Claim ACKed: claimed → busy."""
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
        """Claim timed out: claimed → deleting, release lease."""
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, reason="ACK timeout")

        # Release lease
        self._db.release_instance_lease(instance_name, stage_run_id)

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.CLAIM_TIMEOUT,
            ctx,
        )
        if not result.success:
            logger.warning(
                "on_claim_timeout: CLAIM_TIMEOUT failed for %s: %s",
                instance_name,
                result.details,
            )
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
        """Run reached terminal state → release lease, emit instance event.

        For COMPLETED/FAILED/AWAITING: JOB_FINISHED (busy → draining)
        For TERMINATED/CANCELED: DELETE_REQUESTED (→ deleting)
        """
        # Find the lease for this run
        lease = self._db.get_active_lease_for_run(stage_run_id)
        if lease is None:
            logger.debug("on_run_terminal: no active lease for run %s", stage_run_id)
            return None

        instance_name = lease["instance_name"]
        ctx = self._ctx(source=source, stage_run_id=stage_run_id, reason=f"run {terminal_state}")

        # Release the lease
        self._db.release_instance_lease(instance_name, stage_run_id)

        # Choose event based on terminal state
        delete_states = ("terminated", "canceled")
        if terminal_state in delete_states:
            event = InstanceEvent.DELETE_REQUESTED
        else:
            # completed, failed, awaiting_user_finalization
            event = InstanceEvent.JOB_FINISHED

        result = instance_transition(self._db, instance_name, event, ctx)
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
        """VM is dead (preemption/crash/not found) → gone.

        Also releases any active lease.
        """
        ctx = self._ctx(source=source, reason="preempted/dead")

        # Release any active lease
        lease = self._db.get_active_lease_for_instance(instance_name)
        if lease:
            self._db.release_instance_lease(instance_name, lease["stage_run_id"])

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.PREEMPTED,
            ctx,
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
        """Request deletion (idle timeout, manual, etc.) → deleting."""
        ctx = self._ctx(source=source, reason=reason)

        # Release any active lease
        lease = self._db.get_active_lease_for_instance(instance_name)
        if lease:
            self._db.release_instance_lease(instance_name, lease["stage_run_id"])

        result = instance_transition(
            self._db,
            instance_name,
            InstanceEvent.DELETE_REQUESTED,
            ctx,
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
