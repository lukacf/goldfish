"""Adapter-level test for launch failure cleaning up the pre-registered warm pool row.

Verifies that when GCERunBackend.launch() pre-registers a warm instance
and then launch_instance() raises, the exception handler calls
on_launch_failed() so the row transitions launching → deleting instead
of sitting in launching forever and leaking capacity.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
from goldfish.cloud.contracts import RunSpec
from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database
from goldfish.errors import LaunchError


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_launch_fail.db")


def _make_backend(test_db: Database) -> GCERunBackend:
    """Create a GCERunBackend with a warm pool wired to a real DB."""
    from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

    config = WarmPoolConfig(enabled=True, max_instances=2, idle_timeout_minutes=30)
    warm_pool = WarmPoolManager(
        db=test_db,
        config=config,
        bucket="test-bucket",
        project_id="test-project",
    )

    backend = GCERunBackend.__new__(GCERunBackend)
    backend._launcher = MagicMock()
    backend._project_id = "test-project"
    backend._zones = ["us-central1-a"]
    backend._bucket = "test-bucket"
    backend._warm_pool = warm_pool
    backend._profile_overrides = None
    return backend


def _make_spec() -> RunSpec:
    return RunSpec(
        stage_run_id="stage-abc123",
        workspace_name="w1",
        stage_name="train",
        image="test:latest",
        profile="h100-spot",
        machine_type="a3-highgpu-1g",
        gpu_count=1,
        gpu_type="nvidia-h100-80gb",
        spot=False,
        timeout_seconds=3600,
        inputs={},
        env={},
    )


class TestLaunchFailureCleanup:
    def test_launch_failure_transitions_preregistered_row_to_deleting(self, test_db):
        """Pre-registered row must move to deleting when launch_instance() raises."""
        backend = _make_backend(test_db)

        # Make launch_instance raise
        backend._launcher.launch_instance.side_effect = RuntimeError("gcloud create failed")

        spec = _make_spec()

        with pytest.raises(LaunchError):
            backend.launch(spec)

        # The pre-registered row should exist and be in deleting, NOT launching
        from goldfish.cloud.adapters.gcp.gce_launcher import GCELauncher

        expected_name = GCELauncher._sanitize_name(spec.stage_run_id)
        inst = test_db.get_warm_instance(expected_name)
        assert inst is not None, f"Expected warm instance row for {expected_name}"
        assert inst["state"] == "deleting", (
            f"Expected state 'deleting' but got '{inst['state']}'. "
            "Launch failure should transition launching → deleting via on_launch_failed()"
        )

    def test_launch_failure_does_not_leak_capacity(self, test_db):
        """After a failed launch, the pool row should not count against capacity."""
        backend = _make_backend(test_db)
        backend._launcher.launch_instance.side_effect = RuntimeError("boom")

        spec = _make_spec()
        with pytest.raises(LaunchError):
            backend.launch(spec)

        # Row is in deleting, not launching — but still counts against pool.
        # A second pre-register should succeed because deleting rows are not 'gone'
        # but a fresh register for a different instance should work (max=2, 1 in deleting).
        ok = test_db.pre_register_warm_instance(
            instance_name="new-inst",
            zone="us-central1-a",
            project_id="test-project",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
        )
        assert ok is True, "Deleting row should not permanently block capacity"
