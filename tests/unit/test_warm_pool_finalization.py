"""Tests for warm pool lifecycle via InstanceController (replaces old _warm_pool_finalization).

Verifies that run terminal states correctly map to instance events
through the controller.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.instance_controller import InstanceController
from goldfish.state_machine.instance_types import InstanceState


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_fin.db")


def _insert_instance(db, name="inst-1", state="busy"):
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


class TestWarmPoolFinalization:
    """Test that run terminal states correctly route through InstanceController."""

    def test_completed_run_emits_job_finished(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        result = ctrl.on_run_terminal("stage-abc", "completed")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

    def test_failed_run_emits_job_finished(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        result = ctrl.on_run_terminal("stage-abc", "failed")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

    def test_awaiting_user_finalization_emits_job_finished(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        result = ctrl.on_run_terminal("stage-abc", "awaiting_user_finalization")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DRAINING

    def test_terminated_run_emits_delete_requested(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        result = ctrl.on_run_terminal("stage-abc", "terminated")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DELETING

    def test_canceled_run_emits_delete_requested(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        result = ctrl.on_run_terminal("stage-abc", "canceled")
        assert result is not None
        assert result.success
        assert result.new_state == InstanceState.DELETING

    def test_non_warm_instance_no_effect(self, test_db):
        """Run without a lease → no instance action."""
        ctrl = InstanceController(test_db)
        result = ctrl.on_run_terminal("stage-abc", "completed")
        assert result is None

    def test_lease_released_after_terminal(self, test_db):
        _insert_instance(test_db, "inst-1", "busy")
        test_db.create_instance_lease("inst-1", "stage-abc")
        ctrl = InstanceController(test_db)

        ctrl.on_run_terminal("stage-abc", "completed")
        assert test_db.get_active_lease_for_instance("inst-1") is None
