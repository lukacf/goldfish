"""Unit tests for warm pool feature (Phase 1: Config + DB + Reaper).

Tests the foundation: configuration model, database CRUD, and reaper logic.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

# --- Config Tests ---


class TestWarmPoolConfig:
    def test_defaults_disabled(self) -> None:
        """Warm pool is disabled by default."""
        from goldfish.config import WarmPoolConfig

        config = WarmPoolConfig()
        assert config.enabled is False
        assert config.max_instances == 2
        assert config.idle_timeout_minutes == 30
        assert config.profiles == []
        assert config.watchdog_seconds == 21600

    def test_enabled_with_profiles(self) -> None:
        from goldfish.config import WarmPoolConfig

        config = WarmPoolConfig(enabled=True, profiles=["h100-spot", "a100-spot"])
        assert config.enabled is True
        assert config.profiles == ["h100-spot", "a100-spot"]

    def test_max_instances_bounds(self) -> None:
        from pydantic import ValidationError

        from goldfish.config import WarmPoolConfig

        with pytest.raises(ValidationError):
            WarmPoolConfig(max_instances=0)
        with pytest.raises(ValidationError):
            WarmPoolConfig(max_instances=11)

    def test_idle_timeout_bounds(self) -> None:
        from pydantic import ValidationError

        from goldfish.config import WarmPoolConfig

        with pytest.raises(ValidationError):
            WarmPoolConfig(idle_timeout_minutes=4)  # Min 5
        with pytest.raises(ValidationError):
            WarmPoolConfig(idle_timeout_minutes=121)  # Max 120

    def test_gce_config_includes_warm_pool(self) -> None:
        """GCEConfig should have an optional warm_pool field."""
        from goldfish.config import GCEConfig

        gce = GCEConfig(project_id="test")
        assert gce.warm_pool is not None
        assert gce.warm_pool.enabled is False

    def test_gce_config_with_warm_pool(self) -> None:
        from goldfish.config import GCEConfig

        gce = GCEConfig(
            project_id="test",
            warm_pool={"enabled": True, "max_instances": 3},
        )
        assert gce.warm_pool.enabled is True
        assert gce.warm_pool.max_instances == 3


# --- DB CRUD Tests ---


class TestWarmPoolDB:
    def test_register_warm_instance(self, test_db) -> None:
        test_db.register_warm_instance(
            instance_name="stage-abc123",
            zone="us-central1-a",
            project_id="my-project",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_tag="us-docker.pkg.dev/proj/repo/img:v1",
        )

        instances = test_db.list_warm_instances()
        assert len(instances) == 1
        assert instances[0]["instance_name"] == "stage-abc123"
        assert instances[0]["machine_type"] == "a3-highgpu-1g"
        assert instances[0]["status"] == "idle"

    def test_claim_warm_instance_returns_match(self, test_db) -> None:
        test_db.register_warm_instance(
            instance_name="stage-abc",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )

        claimed = test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)
        assert claimed is not None
        assert claimed["instance_name"] == "stage-abc"
        assert claimed["status"] == "claimed"

    def test_claim_warm_instance_no_match(self, test_db) -> None:
        test_db.register_warm_instance(
            instance_name="stage-abc",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )

        # Wrong machine type
        claimed = test_db.claim_warm_instance(machine_type="a3-highgpu-8g", gpu_count=8)
        assert claimed is None

    def test_claim_warm_instance_atomic(self, test_db) -> None:
        """Only one caller can claim a given instance."""
        test_db.register_warm_instance(
            instance_name="stage-abc",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )

        first = test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)
        second = test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)
        assert first is not None
        assert second is None

    def test_release_warm_instance(self, test_db) -> None:
        test_db.register_warm_instance(
            instance_name="stage-abc",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )
        test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)
        test_db.release_warm_instance("stage-abc")

        instances = test_db.list_warm_instances(status="idle")
        assert len(instances) == 1

    def test_delete_warm_instance(self, test_db) -> None:
        test_db.register_warm_instance(
            instance_name="stage-abc",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )
        test_db.delete_warm_instance("stage-abc")
        assert test_db.list_warm_instances() == []

    def test_count_warm_instances(self, test_db) -> None:
        for i in range(3):
            test_db.register_warm_instance(
                instance_name=f"stage-{i}",
                zone="us-central1-a",
                project_id="proj",
                machine_type="a3-highgpu-1g",
                gpu_count=1,
            )
        test_db.claim_warm_instance(machine_type="a3-highgpu-1g", gpu_count=1)

        assert test_db.count_warm_instances() == 3
        assert test_db.count_warm_instances(statuses=("idle",)) == 2
        assert test_db.count_warm_instances(statuses=("claimed",)) == 1

    def test_list_expired_warm_instances(self, test_db) -> None:
        """Should find instances idle longer than the timeout."""
        test_db.register_warm_instance(
            instance_name="stage-old",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )
        # Backdate the idle_since to 2 hours ago
        with test_db._conn() as conn:
            old_time = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
            conn.execute(
                "UPDATE warm_instances SET idle_since = ? WHERE instance_name = ?",
                (old_time, "stage-old"),
            )

        expired = test_db.list_expired_warm_instances(idle_timeout_minutes=30)
        assert len(expired) == 1
        assert expired[0]["instance_name"] == "stage-old"

        # Fresh instance should not be expired
        test_db.register_warm_instance(
            instance_name="stage-fresh",
            zone="us-central1-a",
            project_id="proj",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
        )
        expired = test_db.list_expired_warm_instances(idle_timeout_minutes=30)
        assert len(expired) == 1  # Only the old one
