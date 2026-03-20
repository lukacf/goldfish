"""Integration tests for warm pool lifecycle.

Tests the full claim → signal → release → reap cycle with real DB
but mocked GCE/metadata layer.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager
from goldfish.config import WarmPoolConfig


@pytest.fixture
def warm_config() -> WarmPoolConfig:
    return WarmPoolConfig(enabled=True, max_instances=2, idle_timeout_minutes=30)


@pytest.fixture
def manager(test_db, warm_config) -> WarmPoolManager:
    return WarmPoolManager(
        db=test_db,
        config=warm_config,
        signal_bus=None,
        bucket="test-bucket",
        project_id="test-project",
    )


class TestWarmPoolLifecycle:
    def test_register_and_claim_roundtrip(self, test_db, manager) -> None:
        """Register an instance, claim it, verify state transitions."""
        # Register
        assert manager.register_instance("stage-abc", "us-central1-a", "a3-highgpu-1g", 1)

        # Verify registered as running (first job active)
        instances = test_db.list_warm_instances(status="running")
        assert len(instances) == 1

        # Release (simulates first job completing → idle loop)
        test_db.release_warm_instance("stage-abc")
        instances = test_db.list_warm_instances(status="idle")
        assert len(instances) == 1

        # Claim
        claimed = test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)
        assert claimed is not None
        assert claimed["status"] == "claimed"

        # Release again
        test_db.release_warm_instance("stage-abc")
        instances = test_db.list_warm_instances(status="idle")
        assert len(instances) == 1

    def test_pool_size_cap_enforced(self, test_db, manager) -> None:
        """Cannot register more instances than max_instances."""
        assert manager.register_instance("stage-1", "zone-a", "a3-highgpu-1g", 1)
        assert manager.register_instance("stage-2", "zone-b", "a3-highgpu-1g", 1)
        # Third should be rejected (max=2)
        assert not manager.register_instance("stage-3", "zone-c", "a3-highgpu-1g", 1)

    def test_reap_idle_deletes_expired(self, test_db, manager) -> None:
        """Reaper should delete instances past idle timeout."""
        manager.register_instance("stage-old", "us-central1-a", "a3-highgpu-1g", 1)
        test_db.release_warm_instance("stage-old")  # Must be idle to be reaped

        # Backdate idle_since
        with test_db._conn() as conn:
            old_time = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
            conn.execute("UPDATE warm_instances SET idle_since = ?", (old_time,))

        with patch.object(manager, "_delete_gce_instance"):
            reaped = manager.reap_idle()
            assert reaped == 1
            assert test_db.list_warm_instances() == []

    def test_reap_idle_skips_active(self, test_db, manager) -> None:
        """Reaper should not delete instances that are running or recently idle."""
        manager.register_instance("stage-active", "us-central1-a", "a3-highgpu-1g", 1)
        # Claim it (status = claimed, not idle)
        test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)

        with patch.object(manager, "_delete_gce_instance"):
            reaped = manager.reap_idle()
            assert reaped == 0
            assert len(test_db.list_warm_instances()) == 1

    def test_reap_all_emergency_cleanup(self, test_db, manager) -> None:
        """Emergency reap should delete all instances regardless of state."""
        manager.register_instance("stage-1", "zone-a", "a3-highgpu-1g", 1)
        manager.register_instance("stage-2", "zone-b", "a3-highgpu-1g", 1)
        test_db.release_warm_instance("stage-1")  # One idle, one running

        with patch.object(manager, "_delete_gce_instance"):
            reaped = manager.reap_all()
            assert reaped == 2
            assert test_db.list_warm_instances() == []

    def test_try_claim_without_signal_bus_returns_none(self, test_db, manager) -> None:
        """Without signal bus, try_claim should release and return None."""
        manager.register_instance("stage-abc", "us-central1-a", "a3-highgpu-1g", 1)
        test_db.release_warm_instance("stage-abc")  # Must be idle to claim

        result = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            stage_run_id="stage-new",
            image="img:v1",
            env_map={},
            pre_run_script="echo pre",
            post_run_script="echo post",
            docker_cmd_script="echo docker",
            run_path="runs/stage-new",
        )
        # No signal bus → falls through
        assert result is None
        # Instance should be released back to idle
        instances = test_db.list_warm_instances(status="idle")
        assert len(instances) == 1

    def test_try_claim_with_signal_bus_and_ack(self, test_db, warm_config) -> None:
        """With signal bus and ACK, try_claim should return a RunHandle."""
        # Create a stage_run so the warm_instances FK constraint is satisfied
        with test_db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, state, backend_type, started_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("stage-new", "ws", "v1", "train", "running", "running", "gce"),
            )
            conn.execute("PRAGMA foreign_keys = ON")

        mock_bus = MagicMock()
        # ACK immediately
        mock_bus.get_ack.return_value = "stage-new"

        mgr = WarmPoolManager(
            db=test_db,
            config=warm_config,
            signal_bus=mock_bus,
            bucket="test-bucket",
            project_id="test-project",
        )
        mgr.register_instance("stage-warm", "us-central1-a", "a3-highgpu-1g", 1)
        test_db.release_warm_instance("stage-warm")  # Must be idle to claim

        with patch.object(mgr, "_upload_job_spec", return_value="gs://test-bucket/warm_pool/stage-warm/jobs/stage-new"):
            result = mgr.try_claim(
                machine_type="a3-highgpu-1g",
                gpu_count=1,
                stage_run_id="stage-new",
                image="img:v2",
                env_map={"FOO": "bar"},
                pre_run_script="echo pre",
                post_run_script="echo post",
                docker_cmd_script="echo docker",
                run_path="runs/stage-new",
            )

        assert result is not None
        assert result.warm_instance is True
        assert result.backend_handle == "stage-warm"
        assert result.zone == "us-central1-a"

        # Instance should be in 'running' state
        instances = test_db.list_warm_instances(status="running")
        assert len(instances) == 1
        assert instances[0]["current_stage_run_id"] == "stage-new"

    def test_try_claim_with_ack_timeout_releases(self, test_db, warm_config) -> None:
        """If ACK times out, instance should be released."""
        mock_bus = MagicMock()
        mock_bus.get_ack.return_value = None  # Never ACKs

        mgr = WarmPoolManager(
            db=test_db,
            config=warm_config,
            signal_bus=mock_bus,
            bucket="test-bucket",
            project_id="test-project",
        )
        mgr.register_instance("stage-slow", "us-central1-a", "a3-highgpu-1g", 1)
        test_db.release_warm_instance("stage-slow")  # Must be idle to claim

        with (
            patch.object(mgr, "_upload_job_spec", return_value="gs://bucket/spec"),
            patch("goldfish.cloud.adapters.gcp.warm_pool.time.sleep"),  # Skip waiting
        ):
            result = mgr.try_claim(
                machine_type="a3-highgpu-1g",
                gpu_count=1,
                stage_run_id="stage-timeout",
                image="img:v1",
                env_map={},
                pre_run_script="",
                post_run_script="",
                docker_cmd_script="",
                run_path="runs/stage-timeout",
            )

        assert result is None
        # Instance should be back to idle
        instances = test_db.list_warm_instances(status="idle")
        assert len(instances) == 1
