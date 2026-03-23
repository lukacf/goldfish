"""Tests for InstanceController (Phase B).

Tests controller methods that map run events → instance events.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.instance_controller import InstanceController
from goldfish.state_machine.instance_types import InstanceState


def _insert_instance(db, name="inst-1", state="launching", lease_run_id=None):
    """Helper to insert a warm instance directly."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
            VALUES (?, 'us-central1-a', 'proj', 'n1-standard-1', 0,
                    'debian-12', 'debian-cloud', 0, ?, ?, ?, ?)
            """,
            (name, state, now, lease_run_id, now),
        )


@pytest.fixture
def test_db(tmp_path) -> Database:
    db_path = tmp_path / "test_ctrl.db"
    return Database(db_path)


@pytest.fixture
def controller(test_db) -> InstanceController:
    return InstanceController(test_db)


class TestOnFreshLaunch:
    def test_creates_lease_and_transitions(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "launching")
        result = controller.on_fresh_launch("inst-1", "stage-abc")
        assert result.success
        assert result.new_state == InstanceState.BUSY

        # Lease set atomically on instance row
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] == "stage-abc"

    def test_idempotent_if_already_busy(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy")
        result = controller.on_fresh_launch("inst-1", "stage-abc")
        assert result.success
        assert result.reason == "already_in_target_state"


class TestOnLaunchFailed:
    def test_transitions_to_deleting(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "launching", lease_run_id="stage-abc")

        result = controller.on_launch_failed("inst-1", "stage-abc", error="gcloud failed")
        assert result.success
        assert result.new_state == InstanceState.DELETING

        # Lease released atomically
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] is None


class TestOnClaimStart:
    def test_creates_lease_and_transitions(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "idle_ready")
        result = controller.on_claim_start("inst-1", "stage-xyz")
        assert result.success
        assert result.new_state == InstanceState.CLAIMED

        # Lease set atomically on instance row
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] == "stage-xyz"

    def test_fails_if_not_idle_ready(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy")
        result = controller.on_claim_start("inst-1", "stage-xyz")
        assert not result.success


class TestOnClaimAcked:
    def test_transitions_to_busy(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "claimed", lease_run_id="stage-xyz")

        result = controller.on_claim_acked("inst-1", "stage-xyz")
        assert result.success
        assert result.new_state == InstanceState.BUSY


class TestOnClaimTimeout:
    def test_transitions_to_deleting_and_releases_lease(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "claimed", lease_run_id="stage-xyz")

        result = controller.on_claim_timeout("inst-1", "stage-xyz")
        assert result.success
        assert result.new_state == InstanceState.DELETING

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] is None


class TestOnRunTerminal:
    def test_completed_emits_job_finished(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_run_terminal("stage-abc", "completed")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

        # Lease released atomically
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] is None

    def test_failed_emits_job_finished(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_run_terminal("stage-abc", "failed")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

    def test_awaiting_emits_job_finished(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_run_terminal("stage-abc", "awaiting_user_finalization")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

    def test_terminated_emits_delete_requested(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_run_terminal("stage-abc", "terminated")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DELETING

    def test_canceled_emits_delete_requested(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_run_terminal("stage-abc", "canceled")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DELETING

    def test_no_lease_returns_none(self, test_db, controller):
        result = controller.on_run_terminal("stage-nonexistent", "completed")
        assert result is None


class TestOnDrainComplete:
    def test_draining_to_idle_ready(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "draining")
        result = controller.on_drain_complete("inst-1")
        assert result.success
        assert result.new_state == InstanceState.IDLE_READY

    def test_rejected_from_busy(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy")
        result = controller.on_drain_complete("inst-1")
        assert not result.success


class TestOnPreempted:
    def test_transitions_to_gone_and_releases_lease(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")

        result = controller.on_preempted("inst-1")
        assert result.success
        assert result.new_state == InstanceState.GONE

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] is None


class TestOnDeleteRequested:
    def test_idle_ready_to_deleting(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "idle_ready")
        result = controller.on_delete_requested("inst-1", reason="idle timeout")
        assert result.success
        assert result.new_state == InstanceState.DELETING


class TestOnDeleteConfirmed:
    def test_deleting_to_gone(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "deleting")
        result = controller.on_delete_confirmed("inst-1")
        assert result.success
        assert result.new_state == InstanceState.GONE


class TestOnDeleteFailed:
    def test_stays_in_deleting(self, test_db, controller):
        _insert_instance(test_db, "inst-1", "deleting")
        result = controller.on_delete_failed("inst-1", error="transient")
        assert result.success
        assert result.new_state == InstanceState.DELETING


class TestFullLifecycleViaController:
    """End-to-end lifecycle test using only controller methods."""

    def test_fresh_launch_to_reuse_to_delete(self, test_db, controller):
        # Pre-register
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="us-central1-a",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=2,
        )
        assert ok

        # Fresh launch
        r = controller.on_fresh_launch("inst-1", "stage-run1")
        assert r.success and r.new_state == InstanceState.BUSY

        # Run completes
        r = controller.on_run_terminal("stage-run1", "completed")
        assert r is not None and r.success and r.new_state == InstanceState.DRAINING

        # VM drains
        r = controller.on_drain_complete("inst-1")
        assert r.success and r.new_state == InstanceState.IDLE_READY

        # Claim for reuse
        r = controller.on_claim_start("inst-1", "stage-run2")
        assert r.success and r.new_state == InstanceState.CLAIMED

        # ACK received
        r = controller.on_claim_acked("inst-1", "stage-run2")
        assert r.success and r.new_state == InstanceState.BUSY

        # Second run canceled
        r = controller.on_run_terminal("stage-run2", "canceled")
        assert r is not None and r.success and r.new_state == InstanceState.DELETING

        # Delete confirmed
        r = controller.on_delete_confirmed("inst-1")
        assert r.success and r.new_state == InstanceState.GONE

        # Clean up
        test_db.delete_warm_instance("inst-1")
        assert test_db.get_warm_instance("inst-1") is None
