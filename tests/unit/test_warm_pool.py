"""Unit tests for warm pool: config and DB CRUD (v2 state-machine schema)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from goldfish.config import GCEConfig, WarmPoolConfig
from goldfish.db.database import Database

# =============================================================================
# Config Tests
# =============================================================================


class TestWarmPoolConfig:
    """Tests for WarmPoolConfig model."""

    def test_defaults(self):
        """WarmPoolConfig has sane defaults: disabled, 2 max, 30min idle."""
        cfg = WarmPoolConfig()
        assert cfg.enabled is False
        assert cfg.max_instances == 2
        assert cfg.idle_timeout_minutes == 30
        assert cfg.profiles == []
        assert cfg.watchdog_seconds == 21600
        assert cfg.preserve_paths == []

    def test_custom_values(self):
        """WarmPoolConfig accepts valid custom values."""
        cfg = WarmPoolConfig(
            enabled=True,
            max_instances=5,
            idle_timeout_minutes=60,
            profiles=["h100-spot", "a100-spot"],
            watchdog_seconds=7200,
            preserve_paths=["/data/cache"],
        )
        assert cfg.enabled is True
        assert cfg.max_instances == 5
        assert cfg.idle_timeout_minutes == 60
        assert cfg.profiles == ["h100-spot", "a100-spot"]
        assert cfg.watchdog_seconds == 7200
        assert cfg.preserve_paths == ["/data/cache"]

    def test_max_instances_ge_1(self):
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            WarmPoolConfig(max_instances=0)

    def test_max_instances_le_10(self):
        with pytest.raises(ValidationError, match="less than or equal to 10"):
            WarmPoolConfig(max_instances=11)

    def test_idle_timeout_ge_5(self):
        with pytest.raises(ValidationError, match="greater than or equal to 5"):
            WarmPoolConfig(idle_timeout_minutes=4)

    def test_idle_timeout_le_120(self):
        with pytest.raises(ValidationError, match="less than or equal to 120"):
            WarmPoolConfig(idle_timeout_minutes=121)

    def test_watchdog_ge_3600(self):
        with pytest.raises(ValidationError, match="greater than or equal to 3600"):
            WarmPoolConfig(watchdog_seconds=3599)

    def test_extra_field_rejected(self):
        with pytest.raises(ValidationError, match="extra_forbidden"):
            WarmPoolConfig(bogus_field="nope")  # type: ignore[call-arg]

    def test_parse_from_dict(self):
        data = {
            "enabled": True,
            "max_instances": 3,
            "idle_timeout_minutes": 15,
            "profiles": ["cpu-small"],
            "watchdog_seconds": 3600,
            "preserve_paths": [],
        }
        cfg = WarmPoolConfig(**data)
        assert cfg.enabled is True
        assert cfg.max_instances == 3


class TestGCEConfigWarmPool:
    """Tests for WarmPoolConfig nested in GCEConfig."""

    def test_default_warm_pool(self):
        gce = GCEConfig(project_id="test-project")
        assert isinstance(gce.warm_pool, WarmPoolConfig)
        assert gce.warm_pool.enabled is False

    def test_custom_warm_pool(self):
        gce = GCEConfig(
            project_id="test-project",
            warm_pool=WarmPoolConfig(enabled=True, max_instances=4),
        )
        assert gce.warm_pool.enabled is True
        assert gce.warm_pool.max_instances == 4

    def test_warm_pool_from_dict(self):
        gce = GCEConfig(
            project_id="test-project",
            warm_pool={"enabled": True, "max_instances": 3},  # type: ignore[arg-type]
        )
        assert gce.warm_pool.enabled is True
        assert gce.warm_pool.max_instances == 3


# =============================================================================
# DB CRUD Tests (v2 schema)
# =============================================================================


@pytest.fixture
def test_db(tmp_path) -> Database:
    """Create a test database with schema initialized."""
    db_path = tmp_path / "test_warm_pool.db"
    return Database(db_path)


def _insert_instance(db: Database, name: str = "goldfish-warm-001", state: str = "launching") -> None:
    """Insert instance directly for testing."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'my-project', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, ?, ?, ?)
            """,
            (name, state, now, now),
        )


class TestGetWarmInstance:
    def test_get_existing(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001")
        row = test_db.get_warm_instance("goldfish-warm-001")
        assert row is not None
        assert row["instance_name"] == "goldfish-warm-001"
        assert row["state"] == "launching"

    def test_get_nonexistent(self, test_db: Database):
        assert test_db.get_warm_instance("does-not-exist") is None


class TestDeleteWarmInstance:
    def test_delete_existing(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001")
        test_db.delete_warm_instance("goldfish-warm-001")
        assert test_db.get_warm_instance("goldfish-warm-001") is None

    def test_delete_nonexistent(self, test_db: Database):
        test_db.delete_warm_instance("does-not-exist")  # no error


class TestListWarmInstances:
    def test_list_all(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001", "busy")
        _insert_instance(test_db, "goldfish-warm-002", "idle_ready")
        rows = test_db.list_warm_instances()
        assert len(rows) == 2

    def test_list_with_state_filter(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001", "busy")
        _insert_instance(test_db, "goldfish-warm-002", "idle_ready")

        busy = test_db.list_warm_instances(state="busy")
        assert len(busy) == 1
        assert busy[0]["instance_name"] == "goldfish-warm-001"

        idle = test_db.list_warm_instances(state="idle_ready")
        assert len(idle) == 1
        assert idle[0]["instance_name"] == "goldfish-warm-002"

    def test_list_empty(self, test_db: Database):
        assert test_db.list_warm_instances() == []


class TestPreRegisterWarmInstance:
    def test_pre_register_success(self, test_db: Database):
        ok = test_db.pre_register_warm_instance(
            instance_name="goldfish-warm-001",
            zone="us-central1-a",
            project_id="my-project",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
        )
        assert ok is True
        row = test_db.get_warm_instance("goldfish-warm-001")
        assert row is not None
        assert row["state"] == "launching"
        assert row["machine_type"] == "a3-highgpu-1g"
        assert row["gpu_count"] == 1

    def test_pre_register_with_image_tag(self, test_db: Database):
        ok = test_db.pre_register_warm_instance(
            instance_name="goldfish-warm-001",
            zone="us-central1-a",
            project_id="my-project",
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            max_instances=2,
            image_tag="us-docker.pkg.dev/proj/repo/img:v1",
        )
        assert ok is True
        row = test_db.get_warm_instance("goldfish-warm-001")
        assert row is not None
        assert row["image_tag"] == "us-docker.pkg.dev/proj/repo/img:v1"


class TestFindClaimableInstance:
    def test_find_matching_idle_ready(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001", "idle_ready")
        row = test_db.find_claimable_instance("a3-highgpu-1g", 1, "debian-12", "debian-cloud")
        assert row is not None
        assert row["instance_name"] == "goldfish-warm-001"

    def test_no_match_when_busy(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001", "busy")
        assert test_db.find_claimable_instance("a3-highgpu-1g", 1, "debian-12", "debian-cloud") is None

    def test_no_match_machine_type(self, test_db: Database):
        _insert_instance(test_db, "goldfish-warm-001", "idle_ready")
        assert test_db.find_claimable_instance("n2-standard-4", 0, "debian-12", "debian-cloud") is None
