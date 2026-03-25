"""Tests for daemon warm pool handlers — assignment-start deadline.

Tests the poll_warm_instances() BUSY handler's assignment-start deadline
which detects instances that were assigned a job (JOB_ASSIGNED → busy)
but the VM never picked it up (metadata still idle_ready or unreachable).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.instance_controller import InstanceController


def _insert_busy_instance(
    db: Database,
    name: str = "inst-1",
    *,
    lease_run_id: str = "stage-run-001",
    minutes_ago: int = 10,
    run_state: str = "running",
) -> None:
    """Insert a busy warm instance with state_entered_at in the past.

    Also creates a matching stage run so the daemon's stale-lease check
    doesn't fire before the assignment-start deadline.
    """
    entered = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat()
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        # Create workspace scaffolding for the stage run FK
        conn.execute(
            "INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at) VALUES ('w1', ?)",
            (now,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by) "
            "VALUES ('w1', 'v1', 'w1-v1', 'abc', ?, 'test')",
            (now,),
        )
        # Create a stage run in the given state so the stale-lease check doesn't fire
        conn.execute(
            """INSERT OR IGNORE INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at,
                 backend_type, backend_handle)
            VALUES (?, 'w1', 'v1', 'train', 'running', ?, ?, ?, 'gce', ?)""",
            (lease_run_id, now, run_state, now, name),
        )
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
            VALUES (?, 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, 'busy', ?, ?, ?)
            """,
            (name, entered, lease_run_id, now),
        )


def _insert_idle_instance(
    db: Database,
    name: str = "inst-idle",
    *,
    minutes_ago: int = 10,
) -> None:
    entered = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat()
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, 'idle_ready', ?, ?)
            """,
            (name, entered, now),
        )


def _insert_launching_instance(
    db: Database,
    name: str = "inst-launching",
    *,
    minutes_ago: int = 10,
) -> None:
    entered = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat()
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, 'launching', ?, ?)
            """,
            (name, entered, now),
        )


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_daemon_warm.db")


def _make_warm_pool_mock(db, *, metadata_return=None):
    """Build a WarmPoolManager mock with real controller backed by real DB."""
    controller = InstanceController(db)
    warm_pool = MagicMock()
    warm_pool._config.idle_timeout_minutes = 30
    warm_pool._db = db
    warm_pool.controller = controller
    warm_pool.get_instance_metadata.return_value = metadata_return or {}
    warm_pool.check_instance_status.return_value = "alive"
    return warm_pool


def _make_daemon(db):
    """Create a StageDaemon for testing."""
    from goldfish.state_machine.stage_daemon import StageDaemon

    config = MagicMock()
    config.gce.warm_pool.enabled = True
    config.gce.warm_pool.idle_timeout_minutes = 30
    config.jobs.backend = "gce"

    return StageDaemon(db=db, config=config)


def _run_poll(db, warm_pool_mock, *, daemon=None):
    """Run poll_warm_instances with a mocked warm pool manager."""
    if daemon is None:
        daemon = _make_daemon(db)

    with patch.object(daemon, "_get_warm_pool_manager", return_value=warm_pool_mock):
        daemon.poll_warm_instances()

    return daemon


class TestAssignmentStartDeadline:
    """Tests for the BUSY handler's assignment-start deadline."""

    def test_busy_idle_ready_metadata_triggers_delete(self, test_db):
        """Busy >5min + VM metadata says idle_ready → delete (job never picked up)."""
        _insert_busy_instance(test_db, "inst-1", minutes_ago=10)
        warm_pool = _make_warm_pool_mock(test_db, metadata_return={"goldfish_instance_state": "idle_ready"})

        _run_poll(test_db, warm_pool)

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "deleting"

    def test_busy_metadata_unreachable_does_not_delete(self, test_db):
        """Busy >5min + empty metadata → stay busy (fall through to liveness check).

        Empty metadata is a transient gcloud failure, not proof the VM is stuck.
        Only positive evidence (idle_ready) should trigger deletion. The liveness
        check below (check_instance_status) handles actually dead VMs.
        """
        _insert_busy_instance(test_db, "inst-1", minutes_ago=10)
        warm_pool = _make_warm_pool_mock(test_db, metadata_return={})

        # Even after multiple polls, empty metadata alone must not trigger delete
        daemon = _make_daemon(test_db)
        _run_poll(test_db, warm_pool, daemon=daemon)
        _run_poll(test_db, warm_pool, daemon=daemon)
        _run_poll(test_db, warm_pool, daemon=daemon)

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "busy"

    def test_busy_metadata_busy_no_delete(self, test_db):
        """Busy >5min + VM metadata says busy → VM is working, leave it alone."""
        _insert_busy_instance(test_db, "inst-1", minutes_ago=10)
        warm_pool = _make_warm_pool_mock(test_db, metadata_return={"goldfish_instance_state": "busy"})

        _run_poll(test_db, warm_pool)

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "busy"

    def test_busy_under_deadline_no_delete(self, test_db):
        """Busy <5min → deadline not reached, leave alone regardless of metadata."""
        _insert_busy_instance(test_db, "inst-1", minutes_ago=2)
        warm_pool = _make_warm_pool_mock(test_db, metadata_return={"goldfish_instance_state": "idle_ready"})

        _run_poll(test_db, warm_pool)

        inst = test_db.get_warm_instance("inst-1")
        assert inst is not None
        assert inst["state"] == "busy"


class TestWarmPoolDaemonRecovery:
    def test_idle_dead_vm_transitions_to_deleting(self, test_db):
        """Stopped idle VMs must be deleted, not marked gone directly."""
        _insert_idle_instance(test_db, "inst-idle")
        warm_pool = _make_warm_pool_mock(test_db)
        warm_pool.check_instance_status.return_value = "dead"

        daemon = _make_daemon(test_db)
        _run_poll(test_db, warm_pool, daemon=daemon)
        _run_poll(test_db, warm_pool, daemon=daemon)
        _run_poll(test_db, warm_pool, daemon=daemon)

        inst = test_db.get_warm_instance("inst-idle")
        assert inst is not None
        assert inst["state"] == "deleting"

    @patch.dict("os.environ", {"GOLDFISH_GCE_LAUNCH_TIMEOUT": "1200"})
    def test_launching_timeout_matches_gce_launch_window(self, test_db):
        """Launching rows must not be reaped before the configured 20-minute launch timeout."""
        _insert_launching_instance(test_db, "inst-launching", minutes_ago=16)
        warm_pool = _make_warm_pool_mock(test_db)

        _run_poll(test_db, warm_pool)

        inst = test_db.get_warm_instance("inst-launching")
        assert inst is not None
        assert inst["state"] == "launching"
