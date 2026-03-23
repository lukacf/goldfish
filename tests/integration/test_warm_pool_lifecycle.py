"""Integration tests for warm pool lifecycle (v2: state-machine-driven).

Tests full lifecycle through InstanceController:
- Fresh launch → busy → draining → idle_ready → claim → busy → cancel → deleting → gone
- Pre-registration capacity gate
- Multiple claim/reuse cycles
"""

from __future__ import annotations

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.instance_controller import InstanceController
from goldfish.state_machine.instance_types import InstanceState


@pytest.fixture
def test_db(tmp_path) -> Database:
    db_path = tmp_path / "test_lifecycle.db"
    return Database(db_path)


@pytest.fixture
def controller(test_db) -> InstanceController:
    return InstanceController(test_db)


class TestFullLifecycle:
    """Full lifecycle through two claim/reuse cycles."""

    def test_full_lifecycle_two_reuse_cycles(self, test_db, controller):
        # 1. Pre-register instance
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
            image_tag="train:v1",
        )
        assert ok

        # 2. Fresh launch succeeds
        r = controller.on_fresh_launch("inst-1", "stage-run-001")
        assert r.success and r.new_state == InstanceState.BUSY

        # 3. First run completes → draining
        r = controller.on_run_terminal("stage-run-001", "completed")
        assert r is not None and r.success and r.new_state == InstanceState.DRAINING

        # 4. VM finishes drain → idle_ready
        r = controller.on_drain_complete("inst-1")
        assert r.success and r.new_state == InstanceState.IDLE_READY

        # 5. Second run → claim
        found = test_db.find_claimable_instance("a3-highgpu-1g", 1, "debian-12", "debian-cloud")
        assert found is not None

        r = controller.on_claim_start("inst-1", "stage-run-002")
        assert r.success and r.new_state == InstanceState.CLAIMED

        # 6. ACK → busy
        r = controller.on_claim_acked("inst-1", "stage-run-002")
        assert r.success and r.new_state == InstanceState.BUSY

        # 7. Second run completes → draining
        r = controller.on_run_terminal("stage-run-002", "completed")
        assert r is not None and r.success and r.new_state == InstanceState.DRAINING

        # 8. Drain → idle_ready again
        r = controller.on_drain_complete("inst-1")
        assert r.success and r.new_state == InstanceState.IDLE_READY

        # 9. Third run → claim → ack → busy → canceled → deleting
        r = controller.on_claim_start("inst-1", "stage-run-003")
        assert r.success
        r = controller.on_claim_acked("inst-1", "stage-run-003")
        assert r.success

        r = controller.on_run_terminal("stage-run-003", "canceled")
        assert r is not None and r.success and r.new_state == InstanceState.DELETING

        # 10. Delete confirmed → gone
        r = controller.on_delete_confirmed("inst-1")
        assert r.success and r.new_state == InstanceState.GONE

        # Cleanup
        test_db.delete_warm_instance("inst-1")
        assert test_db.get_warm_instance("inst-1") is None

    def test_ack_timeout_deletes_instance(self, test_db, controller):
        """ACK timeout: claimed → deleting, lease released."""
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-1",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
        )
        assert ok

        # Launch + complete to idle_ready
        controller.on_fresh_launch("inst-1", "stage-init")
        controller.on_run_terminal("stage-init", "completed")
        controller.on_drain_complete("inst-1")

        # Claim starts
        r = controller.on_claim_start("inst-1", "stage-run-001")
        assert r.success and r.new_state == InstanceState.CLAIMED

        # ACK times out
        r = controller.on_claim_timeout("inst-1", "stage-run-001")
        assert r.success and r.new_state == InstanceState.DELETING
        assert test_db.get_active_lease_for_instance("inst-1") is None

    def test_pool_capacity_enforcement(self, test_db):
        """Cannot register beyond max_instances."""
        for i in range(3):
            ok = test_db.pre_register_warm_instance(
                instance_name=f"inst-{i}",
                zone="z",
                project_id="p",
                machine_type="m",
                gpu_count=0,
                image_family="f",
                image_project="p",
                max_instances=3,
            )
            assert ok is True

        # Fourth fails
        ok = test_db.pre_register_warm_instance(
            instance_name="inst-3",
            zone="z",
            project_id="p",
            machine_type="m",
            gpu_count=0,
            image_family="f",
            image_project="p",
            max_instances=3,
        )
        assert ok is False

    def test_preemption_during_busy(self, test_db, controller):
        """Preemption during busy: lease released, instance → gone."""
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
        assert ok
        controller.on_fresh_launch("inst-1", "stage-run-001")

        r = controller.on_preempted("inst-1")
        assert r.success and r.new_state == InstanceState.GONE
        assert test_db.get_active_lease_for_instance("inst-1") is None
