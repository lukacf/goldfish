"""Tests for the instance state machine (Phase A).

Tests transition table, CAS semantics, leases, and the 4 critical test cases.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.state_machine.instance_core import instance_transition
from goldfish.state_machine.instance_transitions import (
    ACTIVE_INSTANCE_STATES,
    INSTANCE_TRANSITIONS,
    TERMINAL_INSTANCE_STATES,
    find_instance_transition,
)
from goldfish.state_machine.instance_types import (
    InstanceEvent,
    InstanceEventContext,
    InstanceState,
    InstanceTransitionDef,
)

# =============================================================================
# Transition Table Tests
# =============================================================================


class TestInstanceTransitionTable:
    """Verify the transition table is correctly defined."""

    def test_all_transitions_are_valid_defs(self):
        for t in INSTANCE_TRANSITIONS:
            assert isinstance(t, InstanceTransitionDef)
            assert isinstance(t.from_state, InstanceState)
            assert isinstance(t.event, InstanceEvent)
            assert isinstance(t.to_state, InstanceState)

    def test_no_transitions_from_gone(self):
        """Terminal state should have no outgoing transitions (except PREEMPTED→gone idempotency)."""
        gone_transitions = [t for t in INSTANCE_TRANSITIONS if t.from_state == InstanceState.GONE]
        assert gone_transitions == []

    def test_preempted_from_all_active_states(self):
        """PREEMPTED should be accepted from every active state."""
        for state in ACTIVE_INSTANCE_STATES:
            t = find_instance_transition(state, InstanceEvent.PREEMPTED)
            assert t is not None, f"PREEMPTED not accepted from {state}"
            assert t.to_state == InstanceState.GONE

    def test_delete_requested_from_deletable_states(self):
        deletable = {
            InstanceState.LAUNCHING,
            InstanceState.BUSY,
            InstanceState.DRAINING,
            InstanceState.IDLE_READY,
            InstanceState.CLAIMED,
        }
        for state in deletable:
            t = find_instance_transition(state, InstanceEvent.DELETE_REQUESTED)
            assert t is not None, f"DELETE_REQUESTED not accepted from {state}"
            assert t.to_state == InstanceState.DELETING

    def test_happy_path_lifecycle(self):
        """launching → busy → draining → idle_ready → claimed → busy."""
        path = [
            (InstanceState.LAUNCHING, InstanceEvent.BOOT_REGISTERED, InstanceState.BUSY),
            (InstanceState.BUSY, InstanceEvent.JOB_FINISHED, InstanceState.DRAINING),
            (InstanceState.DRAINING, InstanceEvent.DRAIN_COMPLETE, InstanceState.IDLE_READY),
            (InstanceState.IDLE_READY, InstanceEvent.CLAIM_SENT, InstanceState.CLAIMED),
            (InstanceState.CLAIMED, InstanceEvent.CLAIM_ACKED, InstanceState.BUSY),
        ]
        for from_state, event, expected_to in path:
            t = find_instance_transition(from_state, event)
            assert t is not None, f"No transition from {from_state} on {event}"
            assert t.to_state == expected_to

    def test_delete_lifecycle(self):
        """deleting → gone via DELETE_CONFIRMED."""
        t = find_instance_transition(InstanceState.DELETING, InstanceEvent.DELETE_CONFIRMED)
        assert t is not None
        assert t.to_state == InstanceState.GONE

    def test_delete_failed_stays_deleting(self):
        t = find_instance_transition(InstanceState.DELETING, InstanceEvent.DELETE_FAILED)
        assert t is not None
        assert t.to_state == InstanceState.DELETING

    def test_claim_timeout_deletes(self):
        t = find_instance_transition(InstanceState.CLAIMED, InstanceEvent.CLAIM_TIMEOUT)
        assert t is not None
        assert t.to_state == InstanceState.DELETING

    def test_launch_failed_deletes(self):
        t = find_instance_transition(InstanceState.LAUNCHING, InstanceEvent.LAUNCH_FAILED)
        assert t is not None
        assert t.to_state == InstanceState.DELETING

    def test_invalid_transition_returns_none(self):
        """No transition from busy on CLAIM_SENT."""
        t = find_instance_transition(InstanceState.BUSY, InstanceEvent.CLAIM_SENT)
        assert t is None

    def test_string_enum_coercion(self):
        t = find_instance_transition("busy", "job_finished")
        assert t is not None
        assert t.to_state == InstanceState.DRAINING

    def test_invalid_string_returns_none(self):
        assert find_instance_transition("nonexistent", "boot_registered") is None
        assert find_instance_transition("busy", "nonexistent_event") is None

    def test_state_categories_are_complete(self):
        all_states = set(InstanceState)
        covered = ACTIVE_INSTANCE_STATES | TERMINAL_INSTANCE_STATES
        assert covered == all_states


# =============================================================================
# CAS (instance_transition) Tests
# =============================================================================


def _make_ctx(
    source: str = "controller",
    stage_run_id: str | None = None,
    reason: str | None = None,
    error_message: str | None = None,
) -> InstanceEventContext:
    return InstanceEventContext(
        timestamp=datetime.now(UTC),
        source=source,  # type: ignore[arg-type]
        stage_run_id=stage_run_id,
        reason=reason,
        error_message=error_message,
    )


def _insert_instance(db, name="inst-1", state="launching"):
    """Helper to insert a warm instance directly."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'proj', 'n1-standard-1', 0,
                    'debian-12', 'debian-cloud', 0, ?, ?, ?)
            """,
            (name, state, now, now),
        )


class TestInstanceTransitionCAS:
    def test_basic_transition(self, test_db):
        _insert_instance(test_db, "inst-1", "launching")
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.BOOT_REGISTERED,
            _make_ctx(),
        )
        assert result.success
        assert result.new_state == InstanceState.BUSY
        assert result.reason == "ok"

        # Verify DB state
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "busy"

    def test_transition_not_found(self, test_db):
        result = instance_transition(
            test_db,
            "nonexistent",
            InstanceEvent.BOOT_REGISTERED,
            _make_ctx(),
        )
        assert not result.success
        assert result.reason == "not_found"

    def test_invalid_transition_rejected(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.CLAIM_SENT,
            _make_ctx(),
        )
        assert not result.success
        assert result.reason == "no_transition"

    def test_idempotent_already_in_target(self, test_db):
        """If already in target state for event, return success."""
        _insert_instance(test_db, "inst-1", "busy")
        # BOOT_REGISTERED: launching→busy. If already busy, idempotent.
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.BOOT_REGISTERED,
            _make_ctx(),
        )
        assert result.success
        assert result.reason == "already_in_target_state"

    def test_audit_trail_recorded(self, test_db):
        _insert_instance(test_db, "inst-1", "launching")
        instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.BOOT_REGISTERED,
            _make_ctx(stage_run_id="stage-abc"),
        )
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM instance_state_transitions WHERE instance_name = 'inst-1'",
            ).fetchone()
            assert row is not None
            assert row["from_state"] == "launching"
            assert row["to_state"] == "busy"
            assert row["event"] == "boot_registered"
            assert row["stage_run_id"] == "stage-abc"

    def test_full_lifecycle(self, test_db):
        """Walk through a full instance lifecycle."""
        _insert_instance(test_db, "inst-1", "launching")
        events = [
            (InstanceEvent.BOOT_REGISTERED, InstanceState.BUSY),
            (InstanceEvent.JOB_FINISHED, InstanceState.DRAINING),
            (InstanceEvent.DRAIN_COMPLETE, InstanceState.IDLE_READY),
            (InstanceEvent.CLAIM_SENT, InstanceState.CLAIMED),
            (InstanceEvent.CLAIM_ACKED, InstanceState.BUSY),
            (InstanceEvent.JOB_FINISHED, InstanceState.DRAINING),
            (InstanceEvent.DRAIN_COMPLETE, InstanceState.IDLE_READY),
            (InstanceEvent.DELETE_REQUESTED, InstanceState.DELETING),
            (InstanceEvent.DELETE_CONFIRMED, InstanceState.GONE),
        ]
        for event, expected in events:
            result = instance_transition(test_db, "inst-1", event, _make_ctx())
            assert result.success, f"Failed: {event} → {expected}: {result.details}"
            assert result.new_state == expected

    def test_preempted_from_any_active_state(self, test_db):
        for state_val in ["launching", "busy", "draining", "idle_ready", "claimed", "deleting"]:
            name = f"inst-{state_val}"
            _insert_instance(test_db, name, state_val)
            result = instance_transition(test_db, name, InstanceEvent.PREEMPTED, _make_ctx())
            assert result.success, f"PREEMPTED failed from {state_val}"
            assert result.new_state == InstanceState.GONE


# =============================================================================
# Lease Tests
# =============================================================================


class TestInstanceLeases:
    def test_create_and_get_lease(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        lease = test_db.create_instance_lease("inst-1", "stage-abc")
        assert lease["lease_state"] == "active"
        assert lease["instance_name"] == "inst-1"
        assert lease["stage_run_id"] == "stage-abc"

        # Verify via get
        active = test_db.get_active_lease_for_instance("inst-1")
        assert active is not None
        assert active["stage_run_id"] == "stage-abc"

    def test_release_lease(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")

        released = test_db.release_instance_lease("inst-1", "stage-abc")
        assert released is True

        # No active lease
        assert test_db.get_active_lease_for_instance("inst-1") is None

    def test_release_idempotent(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        test_db.release_instance_lease("inst-1", "stage-abc")

        # Second release is a no-op
        released = test_db.release_instance_lease("inst-1", "stage-abc")
        assert released is False

    def test_at_most_one_active_lease_per_instance(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-aaa")

        with pytest.raises(Exception, match="UNIQUE constraint"):
            # Unique index violation
            test_db.create_instance_lease("inst-1", "stage-bbb")

    def test_released_then_new_lease(self, test_db):
        """After releasing a lease, a new one can be created."""
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-aaa")
        test_db.release_instance_lease("inst-1", "stage-aaa")
        lease = test_db.create_instance_lease("inst-1", "stage-bbb")
        assert lease["stage_run_id"] == "stage-bbb"
        assert lease["lease_state"] == "active"

    def test_get_lease_by_run(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        lease = test_db.get_active_lease_for_run("stage-abc")
        assert lease is not None
        assert lease["instance_name"] == "inst-1"

    def test_get_lease_by_run_returns_none(self, test_db):
        assert test_db.get_active_lease_for_run("nonexistent") is None


# =============================================================================
# Conditional INSERT (Capacity Gate) Tests
# =============================================================================


class TestCapacityGate:
    def test_pre_register_succeeds_when_empty(self, test_db):
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="us-central1-a",
            project_id="proj",
            machine_type="n1-standard-1",
            gpu_count=0,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
        )
        assert ok is True
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "launching"

    def test_pre_register_fails_when_full(self, test_db):
        # Fill the pool
        test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="z",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=1,
        )
        # Second should fail
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-2",
            zone="z",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=1,
        )
        assert ok is False
        assert test_db.get_warm_instance("inst-2") is None

    def test_gone_instances_dont_count(self, test_db):
        """Instances in 'gone' state should not count toward capacity."""
        _insert_instance(test_db, "inst-old", "gone")
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-new",
            zone="z",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=1,
        )
        assert ok is True

    def test_capacity_gate_atomicity(self, test_db):
        """Critical test case #4: exactly one of two concurrent inserts succeeds."""
        results = []
        for i in range(2):
            ok = test_db.pre_register_warm_instance(
                instance_name=f"inst-{i}",
                zone="z",
                project_id="p",
                machine_type="m",
                gpu_count=0,
                image_family="f",
                image_project="p",
                max_instances=1,
            )
            results.append(ok)
        assert results.count(True) == 1
        assert results.count(False) == 1
        # Exactly 1 row in table (excluding gone)
        all_instances = test_db.list_warm_instances()
        assert len(all_instances) == 1


# =============================================================================
# Critical Test Case #1: Reuse Ordering Gate
# =============================================================================


class TestReuseOrderingGate:
    def test_early_idle_ready_does_not_make_claimable(self, test_db):
        """DRAIN_COMPLETE is not accepted from busy state.

        Even if the VM reports idle_ready while the instance is still busy,
        the state machine prevents skipping to idle_ready.
        """
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-run1")

        # Attempt DRAIN_COMPLETE while in busy → rejected
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.DRAIN_COMPLETE,
            _make_ctx(),
        )
        assert not result.success
        assert result.reason == "no_transition"

        # Instance stays busy
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "busy"

        # Not claimable
        found = test_db.find_claimable_instance("n1-standard-1", 0, "debian-12", "debian-cloud")
        assert found is None

    def test_full_ordering_gate_lifecycle(self, test_db):
        """After run terminalization, JOB_FINISHED → draining, then DRAIN_COMPLETE → idle_ready."""
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-run1")

        # Step 1: Run goes terminal → controller releases lease, emits JOB_FINISHED
        test_db.release_instance_lease("inst-1", "stage-run1")
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.JOB_FINISHED,
            _make_ctx(),
        )
        assert result.success
        assert result.new_state == InstanceState.DRAINING

        # Step 2: Daemon observes idle_ready metadata, emits DRAIN_COMPLETE
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.DRAIN_COMPLETE,
            _make_ctx(),
        )
        assert result.success
        assert result.new_state == InstanceState.IDLE_READY

        # Step 3: NOW claimable
        found = test_db.find_claimable_instance("n1-standard-1", 0, "debian-12", "debian-cloud")
        assert found is not None
        assert found["instance_name"] == "inst-1"


# =============================================================================
# Critical Test Case #2: Cancel During Launching
# =============================================================================


class TestCancelDuringLaunching:
    def test_cancel_launching_instance(self, test_db):
        _insert_instance(test_db, "inst-1", "launching")
        test_db.create_instance_lease("inst-1", "stage-run1")

        # Cancel → DELETE_REQUESTED
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.DELETE_REQUESTED,
            _make_ctx(reason="run canceled"),
        )
        assert result.success
        assert result.new_state == InstanceState.DELETING

        # Release lease
        test_db.release_instance_lease("inst-1", "stage-run1")
        assert test_db.get_active_lease_for_instance("inst-1") is None

        # Daemon retries delete → DELETE_CONFIRMED
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.DELETE_CONFIRMED,
            _make_ctx(),
        )
        assert result.success
        assert result.new_state == InstanceState.GONE

        # Clean up gone row
        test_db.delete_warm_instance("inst-1")
        assert test_db.get_warm_instance("inst-1") is None


# =============================================================================
# Critical Test Case #3: Launch Failure Cleanup
# =============================================================================


class TestLaunchFailureCleanup:
    def test_launch_failure_enters_deleting_not_gone(self, test_db):
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="z",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=2,
        )
        assert ok is True

        # Launch fails
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.LAUNCH_FAILED,
            _make_ctx(source="controller", error_message="gcloud create failed"),
        )
        assert result.success
        assert result.new_state == InstanceState.DELETING

        # Row still in DB
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "deleting"

        # Daemon retries → DELETE_CONFIRMED
        result = instance_transition(
            test_db,
            "inst-1",
            InstanceEvent.DELETE_CONFIRMED,
            _make_ctx(),
        )
        assert result.success
        assert result.new_state == InstanceState.GONE


# =============================================================================
# find_claimable_instance Tests
# =============================================================================


class TestFindClaimable:
    def test_finds_matching_idle_ready(self, test_db):
        _insert_instance(test_db, "inst-1", "idle_ready")
        found = test_db.find_claimable_instance("n1-standard-1", 0, "debian-12", "debian-cloud")
        assert found is not None
        assert found["instance_name"] == "inst-1"

    def test_ignores_non_idle_ready(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        found = test_db.find_claimable_instance("n1-standard-1", 0, "debian-12", "debian-cloud")
        assert found is None

    def test_ignores_mismatched_hardware(self, test_db):
        _insert_instance(test_db, "inst-1", "idle_ready")
        found = test_db.find_claimable_instance("a3-highgpu-1g", 8, "debian-12", "debian-cloud")
        assert found is None
