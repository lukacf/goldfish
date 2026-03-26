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

from goldfish.cloud.contracts import RunHandle, RunSpec, RunStatus
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


class TestWarmPoolClaimJobSpec:
    def test_claim_job_spec_includes_goldfish_runtime_env(self):
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        warm_pool = MagicMock()
        warm_pool.try_claim.return_value = None

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = MagicMock()
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = "test-bucket"
        backend._warm_pool = warm_pool
        backend._profile_overrides = None

        spec = RunSpec(
            stage_run_id="stage-run-001",
            workspace_name="w1",
            stage_name="train",
            image="test:latest",
            profile="cpu-small",
            machine_type="n1-standard-4",
            inputs={},
            env={"USER_ENV": "present"},
            timeout_seconds=1800,
        )

        backend._try_warm_pool_claim(spec)

        job_spec = warm_pool.try_claim.call_args.kwargs["job_spec"]
        assert job_spec["env"]["USER_ENV"] == "present"
        assert job_spec["env"]["GOLDFISH_STAGE_CONFIG"] == '{"inputs": {}, "compute": {"max_runtime_seconds": 1800}}'
        assert job_spec["env"]["GOLDFISH_RUN_ID"] == "stage-run-001"
        assert job_spec["env"]["GOLDFISH_INPUTS_DIR"] == "/mnt/inputs"
        assert job_spec["env"]["GOLDFISH_OUTPUTS_DIR"] == "/mnt/outputs"
        assert job_spec["stage_run_id"] == "stage-run-001"
        assert job_spec["container_name"] == "goldfish-stage-run-001"

    def test_claim_preserves_executor_stage_config(self):
        """Warm-pool dispatch must preserve the full GOLDFISH_STAGE_CONFIG from the executor."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        warm_pool = MagicMock()
        warm_pool.try_claim.return_value = None

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = MagicMock()
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = "test-bucket"
        backend._warm_pool = warm_pool
        backend._profile_overrides = None

        # Simulate executor-provided env with full stage config
        full_config = '{"inputs": {"data": {"type": "dataset", "format": "npy"}}, "outputs": {"model": {"type": "directory"}}, "compute": {"max_runtime_seconds": 3600}}'
        spec = RunSpec(
            stage_run_id="stage-run-001",
            workspace_name="w1",
            stage_name="train",
            image="test:latest",
            profile="cpu-small",
            machine_type="n1-standard-4",
            inputs={},
            env={"GOLDFISH_STAGE_CONFIG": full_config, "GOLDFISH_RUN_ID": "stage-run-001"},
            timeout_seconds=3600,
        )

        backend._try_warm_pool_claim(spec)

        job_spec = warm_pool.try_claim.call_args.kwargs["job_spec"]
        # The executor's full config must be preserved, NOT overwritten with a minimal one
        assert job_spec["env"]["GOLDFISH_STAGE_CONFIG"] == full_config


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

    def test_get_status_running_uses_matching_metadata_exit_code_fallback(self):
        """Warm VMs may finish even if exit_code.txt upload to GCS fails.

        In that case the startup loop publishes goldfish_exit_code plus the
        matching goldfish_exit_run_id in instance metadata. get_status() should
        use that fallback only when the run IDs match.
        """
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher.bucket_uri = "gs://test-bucket"
        launcher._get_exit_code.return_value = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)

        warm_pool = MagicMock()
        warm_pool.get_instance_metadata.return_value = {
            "goldfish_exit_code": "1",
            "goldfish_exit_run_id": "stage-run-001",
        }

        backend = self._make_backend(launcher)
        backend._warm_pool = warm_pool
        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1

    def test_get_status_running_ignores_stale_metadata_from_previous_run(self):
        """Instance-scoped exit metadata from a previous lease must not complete a new run."""
        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher.bucket_uri = "gs://test-bucket"
        launcher._get_exit_code.return_value = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)

        warm_pool = MagicMock()
        warm_pool.get_instance_metadata.return_value = {
            "goldfish_exit_code": "0",
            "goldfish_exit_run_id": "stage-old-run",
        }

        backend = self._make_backend(launcher)
        backend._warm_pool = warm_pool
        handle = RunHandle(
            stage_run_id="stage-new-run",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.RUNNING

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

    def test_get_status_running_no_bucket_stays_running(self):
        """Without a bucket, _get_exit_code returns synthetic code 0.

        get_status must NOT trust that — a running VM with no bucket should
        stay RUNNING, not be misclassified as COMPLETED.
        """
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.RUNNING
        launcher.bucket_uri = None  # No bucket (cleanup backend)
        launcher._get_exit_code.return_value = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = launcher
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = None
        backend._warm_pool = None
        backend._profile_overrides = None

        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.RUNNING
        # _get_exit_code should NOT have been called
        launcher._get_exit_code.assert_not_called()

    def test_get_status_not_found_no_bucket_raises(self):
        """Without a bucket, not_found must raise NotFoundError, not COMPLETED."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.errors import NotFoundError

        launcher = MagicMock()
        launcher.get_instance_status.return_value = "not_found"
        launcher.bucket_uri = None  # No bucket (cleanup backend)

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = launcher
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = None
        backend._warm_pool = None
        backend._profile_overrides = None

        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        with pytest.raises(NotFoundError):
            backend.get_status(handle)
        launcher._get_exit_code.assert_not_called()

    def test_get_status_terminated_no_bucket_returns_failed(self):
        """Without a bucket, terminated instance returns FAILED (not synthetic COMPLETED)."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        launcher = MagicMock()
        launcher.get_instance_status.return_value = StageState.COMPLETED
        launcher.bucket_uri = None

        backend = GCERunBackend.__new__(GCERunBackend)
        backend._launcher = launcher
        backend._project_id = "test-project"
        backend._zones = ["us-central1-a"]
        backend._bucket = None
        backend._warm_pool = None
        backend._profile_overrides = None

        handle = RunHandle(
            stage_run_id="stage-run-001",
            backend_type="gce",
            backend_handle="goldfish-warm-001",
            zone="us-central1-a",
        )

        status = backend.get_status(handle)
        assert status.status == RunStatus.FAILED
        assert status.exit_code == 1
        launcher._get_exit_code.assert_not_called()


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
