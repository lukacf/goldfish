"""Unit tests for warm pool dispatch + completion detection (v2).

Tests cover:
- try_claim: disabled returns None, no idle instance returns None
- get_status: exit code detection uses stage_run_id
- cleanup: warm instance no-op, regular instance proceeds
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.cloud.contracts import RunHandle, RunStatus
from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database
from goldfish.state_machine.exit_code import ExitCodeResult
from goldfish.state_machine.types import StageState

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_db(tmp_path) -> Database:
    db_path = tmp_path / "test_dispatch.db"
    return Database(db_path)


@pytest.fixture
def enabled_config() -> WarmPoolConfig:
    return WarmPoolConfig(enabled=True, max_instances=5, idle_timeout_minutes=30)


@pytest.fixture
def manager(test_db, enabled_config):
    from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

    return WarmPoolManager(
        db=test_db,
        config=enabled_config,
        bucket="test-bucket",
        project_id="test-project",
    )


def _insert_instance(db: Database, name: str, state: str = "idle_ready") -> None:
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'test-project', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, ?, ?, ?)
            """,
            (name, state, now, now),
        )


# =============================================================================
# try_claim() Tests
# =============================================================================


class TestTryClaim:
    def test_try_claim_disabled(self, test_db):
        from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

        config = WarmPoolConfig(enabled=False)
        mgr = WarmPoolManager(db=test_db, config=config)

        handle = mgr.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
        )
        assert handle is None


# =============================================================================
# get_status() Tests — exit code detection
# =============================================================================


class TestGetStatusExitCode:
    def _make_backend(self, launcher_mock):
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = launcher_mock
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = "test-bucket"
        backend._warm_pool = None
        backend._profile_overrides = None
        return backend

    def test_get_status_running_no_exit_code(self):
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher._get_exit_code.return_value = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)

        backend = self._make_backend(launcher)
        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.RUNNING
        launcher._get_exit_code.assert_called_once_with("stage-run-001")

    def test_get_status_running_with_exit_code_0(self):
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher._get_exit_code.return_value = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)

        backend = self._make_backend(launcher)
        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.COMPLETED
        assert status.exit_code == 0

    def test_get_status_running_with_exit_code_1(self):
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher._get_exit_code.return_value = ExitCodeResult(exists=True, code=1, gcs_error=False, error=None)

        backend = self._make_backend(launcher)
        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1

    def test_get_status_uses_stage_run_id_not_instance_name(self):
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher._get_exit_code.return_value = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)

        backend = self._make_backend(launcher)
        handle = RunHandle(
            stage_run_id="stage-abc123",
            backend_type="gce",
            backend_handle="goldfish-instance-name",
            zone="us-central1-a",
        )

        backend.get_status(handle)
        launcher._get_exit_code.assert_called_once_with("stage-abc123")


# =============================================================================
# cleanup() Tests
# =============================================================================


class TestCleanupWarmInstance:
    def test_cleanup_warm_instance_noop(self, test_db, enabled_config):
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

        warm_pool = WarmPoolManager(db=test_db, config=enabled_config, bucket="test-bucket", project_id="test-project")
        _insert_instance(test_db, "goldfish-warm-001", "busy")

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = MagicMock()
        backend._warm_pool = warm_pool
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = "test-bucket"
        backend._profile_overrides = None

        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        backend.cleanup(handle)
        backend._launcher.delete_instance.assert_not_called()
        assert test_db.get_warm_instance("goldfish-warm-001") is not None

    def test_cleanup_regular_instance(self):
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = MagicMock()
        backend._warm_pool = None
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = "test-bucket"
        backend._profile_overrides = None

        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-regular-001",
            zone="us-central1-a",
        )

        backend.cleanup(handle)
