"""Adapter-level tests for WarmPoolManager.try_claim().

Exercises the real claim path through the manager→controller→DB chain
with mocked gcloud/gsutil subprocess calls. Covers:
- Success: find_claimable → JOB_ASSIGNED → upload → signal → return handle
- No idle instance: returns None immediately
- GCS upload failure: dispatch rolled back via LAUNCH_FAILED → deleting, raises
- Metadata uses --metadata-from-file (JSON-safe signaling)
- Cross-layer: launch() does NOT fall through to fresh VM on dispatch failure
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager
from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database
from goldfish.errors import LaunchError


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_claim_adapter.db")


@pytest.fixture
def enabled_config() -> WarmPoolConfig:
    return WarmPoolConfig(enabled=True, max_instances=5, idle_timeout_minutes=30)


@pytest.fixture
def manager(test_db, enabled_config) -> WarmPoolManager:
    return WarmPoolManager(
        db=test_db,
        config=enabled_config,
        bucket="test-bucket",
        project_id="test-project",
    )


def _insert_idle_instance(db: Database, name: str = "goldfish-warm-001") -> None:
    """Insert an idle_ready instance matching a3-highgpu-1g."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'test-project', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, 'idle_ready', ?, ?)
            """,
            (name, now, now),
        )


def _mock_gcloud_success():
    """Create subprocess.run side_effect that simulates gcloud + gsutil calls."""

    def side_effect(*args, **kwargs):
        cmd = args[0] if args else kwargs.get("args", [])
        result = MagicMock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""

        if "gsutil" in cmd:
            # gsutil cp — succeed
            return result

        if "add-metadata" in cmd:
            # gcloud add-metadata — succeed
            return result

        return result

    return side_effect


class TestTryClaimSuccess:
    """Full claim success path through the adapter."""

    @patch("subprocess.run")
    def test_claim_success_returns_handle(self, mock_run, manager, test_db):
        _insert_idle_instance(test_db)

        mock_run.side_effect = _mock_gcloud_success()

        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
            job_spec={"image": "test:latest"},
        )

        assert handle is not None
        assert handle.stage_run_id == "stage-run-001"
        assert handle.backend_handle == "goldfish-warm-001"
        assert handle.zone == "us-central1-a"

        # Instance should be in busy state (idle_ready → JOB_ASSIGNED → busy)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "busy"

        # Lease should be active
        lease = test_db.get_active_lease_for_instance("goldfish-warm-001")
        assert lease is not None
        assert lease["stage_run_id"] == "stage-run-001"

    @patch("subprocess.run")
    def test_claim_uses_metadata_from_file(self, mock_run, manager, test_db):
        """Verify --metadata-from-file is used instead of --metadata."""
        _insert_idle_instance(test_db)

        mock_run.side_effect = _mock_gcloud_success()

        manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
            job_spec={"image": "test:latest"},
        )

        # Find the add-metadata call
        metadata_calls = [
            c
            for c in mock_run.call_args_list
            if any("add-metadata" in str(arg) for arg in (c.args[0] if c.args else []))
        ]
        assert len(metadata_calls) >= 1
        cmd = metadata_calls[0].args[0] if metadata_calls[0].args else metadata_calls[0][0]

        # Should use --metadata-from-file, NOT --metadata
        assert "--metadata-from-file" in cmd
        assert (
            "--metadata" not in cmd or cmd.index("--metadata-from-file") < cmd.index("--metadata")
            if "--metadata" in cmd and "--metadata-from-file" in cmd
            else True
        )


class TestTryClaimNoIdle:
    def test_returns_none_when_no_idle(self, manager):
        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
        )
        assert handle is None


class TestTryClaimUploadFailure:
    """GCS upload failure after JOB_ASSIGNED: instance → deleting, exception raised."""

    @patch("subprocess.run")
    def test_upload_failure_raises_and_rolls_back(self, mock_run, manager, test_db):
        _insert_idle_instance(test_db)

        def side_effect(*args, **kwargs):
            cmd = args[0] if args else kwargs.get("args", [])
            if "gsutil" in cmd:
                raise OSError("gsutil not found")
            result = MagicMock()
            result.returncode = 0
            return result

        mock_run.side_effect = side_effect

        # try_claim raises on dispatch failure after JOB_ASSIGNED
        with pytest.raises(OSError, match="gsutil not found"):
            manager.try_claim(
                machine_type="a3-highgpu-1g",
                gpu_count=1,
                image_family="debian-12",
                image_project="debian-cloud",
                stage_run_id="stage-run-001",
            )

        # Instance should be in deleting state (dispatch failure)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "deleting"

        # Lease should be released
        assert test_db.get_active_lease_for_instance("goldfish-warm-001") is None


class TestTryClaimDeadVMFallback:
    """Dead VM after JOB_ASSIGNED: returns None so caller can fall back to fresh launch."""

    @patch("subprocess.run")
    def test_dead_vm_returns_none(self, mock_run, manager, test_db):
        _insert_idle_instance(test_db)

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            cmd = args[0] if args else kwargs.get("args", [])
            call_count += 1

            if "gsutil" in cmd:
                result = MagicMock()
                result.returncode = 0
                return result

            if "add-metadata" in cmd:
                # Simulate gcloud error for a deleted VM
                import subprocess

                raise subprocess.CalledProcessError(
                    1,
                    cmd,
                    stderr="ERROR: (gcloud.compute.instances.add-metadata) "
                    "The resource 'projects/p/zones/z/instances/goldfish-warm-001' was not found",
                )

            result = MagicMock()
            result.returncode = 0
            return result

        mock_run.side_effect = side_effect

        # Should return None (dead VM), not raise
        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
            job_spec={"image": "test:latest"},
        )

        assert handle is None

        # Instance should be in deleting (cleanup happened)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "deleting"


class TestLaunchNoFallbackOnDispatchFailure:
    """Cross-layer test: launch() must NOT fall through to fresh VM on dispatch failure."""

    def test_launch_raises_on_warm_dispatch_failure(self, test_db, enabled_config):
        """When try_claim raises after JOB_ASSIGNED, launch() raises LaunchError."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.cloud.contracts import RunSpec

        warm_pool = WarmPoolManager(
            db=test_db,
            config=enabled_config,
            bucket="test-bucket",
            project_id="test-project",
        )
        _insert_idle_instance(test_db)

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
            profile="h100-spot",
            machine_type="a3-highgpu-1g",
            inputs={},
            env={},
            timeout_seconds=1800,
            gpu_count=1,
        )

        # Make the profile resolver return matching hardware
        with patch("goldfish.cloud.adapters.gcp.profiles.ProfileResolver") as mock_resolver_cls:
            mock_resolver = MagicMock()
            mock_resolver.resolve.return_value = {
                "machine_type": "a3-highgpu-1g",
                "boot_disk": {"image_family": "debian-12", "image_project": "debian-cloud"},
            }
            mock_resolver_cls.return_value = mock_resolver

            # Make spec upload fail (after JOB_ASSIGNED succeeds)
            with patch("subprocess.run") as mock_run:

                def side_effect(*args, **kwargs):
                    cmd = args[0] if args else kwargs.get("args", [])
                    if "gsutil" in cmd:
                        raise OSError("GCS unavailable")
                    result = MagicMock()
                    result.returncode = 0
                    return result

                mock_run.side_effect = side_effect

                # launch() must raise LaunchError, NOT fall through to fresh launch
                with pytest.raises(LaunchError, match="dispatch failed after assignment"):
                    backend.launch(spec)

            # The fresh launch path must NOT have been called
            backend._launcher.launch_instance.assert_not_called()

            # Instance should be in deleting (cleanup happened)
            inst = test_db.get_warm_instance("goldfish-warm-001")
            assert inst is not None
            assert inst["state"] == "deleting"
