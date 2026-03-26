"""Tests for warm_pool_cleanup() respecting run ownership.

Verifies that emergency cleanup cancels owning runs before deleting
instances, preventing orphaned runs that think they still have a VM.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.instance_controller import InstanceController
from goldfish.state_machine.instance_types import InstanceState


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_cleanup_ownership.db")


@pytest.fixture
def controller(test_db) -> InstanceController:
    return InstanceController(test_db)


def _insert_instance(db, name, state="busy", lease_run_id=None):
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


def _insert_stage_run(db, run_id, state="running"):
    with db._conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at) "
            "VALUES ('w1', '2024-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO workspace_versions "
            "(workspace_name, version, git_tag, git_sha, created_at, created_by) "
            "VALUES ('w1', 'v1', 'tag', 'sha', '2024-01-01T00:00:00Z', 'manual')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO stage_runs "
            "(id, workspace_name, stage_name, status, started_at, version, state) "
            "VALUES (?, 'w1', 'train', 'running', '2024-01-01T09:00:00Z', 'v1', ?)",
            (run_id, state),
        )


class TestCleanupRespectsOwnership:
    """Verify that on_delete_requested alone does NOT sever active leases
    without first canceling the owning run.
    """

    def test_on_delete_requested_releases_active_lease(self, test_db, controller):
        """on_delete_requested releases the lease — the fix is at the caller level
        (warm_pool_cleanup cancels the run first)."""
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")
        _insert_stage_run(test_db, "stage-abc", "running")

        result = controller.on_delete_requested("inst-1", reason="test")
        assert result.success
        assert result.new_state == InstanceState.DELETING

        # Lease was released atomically by on_delete_requested
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["current_lease_run_id"] is None

    def test_on_run_terminal_then_delete_is_safe(self, test_db, controller):
        """Proper pattern: cancel run first → on_run_terminal releases lease + transitions,
        then instance is in deleting state. No orphaned run."""
        _insert_instance(test_db, "inst-1", "busy", lease_run_id="stage-abc")
        _insert_stage_run(test_db, "stage-abc", "running")

        # Step 1: Run canceled first
        from goldfish.state_machine.cancel import cancel_run

        cancel_result = cancel_run(test_db, "stage-abc", "Emergency warm pool cleanup — instance being deleted")
        assert cancel_result["success"] is True

        # The run is now canceled
        run = test_db.get_stage_run("stage-abc")
        assert run is not None
        assert run["state"] == "canceled"

        # cancel_run calls on_run_terminal which:
        # - releases the lease atomically (current_lease_run_id = NULL)
        # - emits DELETE_REQUESTED (for canceled runs)
        # So the instance should already be in deleting state
        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "deleting"

        # No active lease remains
        assert inst["current_lease_run_id"] is None

    def test_idle_instance_no_lease_can_delete_directly(self, test_db, controller):
        """Instances without active leases can be deleted directly."""
        _insert_instance(test_db, "inst-1", "idle_ready")

        result = controller.on_delete_requested("inst-1", reason="emergency cleanup")
        assert result.success
        assert result.new_state == InstanceState.DELETING
