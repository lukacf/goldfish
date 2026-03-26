"""Unit tests for WarmPoolManager (v2: state-machine-driven).

Tests cover:
- is_enabled_for() profile filtering
- pool_status() summary counts
- try_claim / delete_instance with gcloud mock
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_db(tmp_path) -> Database:
    db_path = tmp_path / "test_warm_pool_mgr.db"
    return Database(db_path)


@pytest.fixture
def enabled_config() -> WarmPoolConfig:
    return WarmPoolConfig(enabled=True, max_instances=2, idle_timeout_minutes=30)


@pytest.fixture
def manager(test_db, enabled_config):
    from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

    return WarmPoolManager(
        db=test_db,
        config=enabled_config,
        bucket="test-bucket",
        project_id="test-project",
    )


def _insert_instance(db: Database, name: str, state: str = "busy") -> None:
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
# is_enabled_for() Tests
# =============================================================================


class TestIsEnabledFor:
    def test_is_enabled_for_empty_profiles(self, manager):
        assert manager.is_enabled_for("h100-spot") is True
        assert manager.is_enabled_for("cpu-small") is True

    def test_is_enabled_for_specific_profiles(self, test_db):
        from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

        config = WarmPoolConfig(enabled=True, profiles=["h100-spot", "a100-spot"])
        mgr = WarmPoolManager(db=test_db, config=config)
        assert mgr.is_enabled_for("h100-spot") is True
        assert mgr.is_enabled_for("cpu-small") is False

    def test_is_enabled_for_disabled(self, test_db):
        from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

        config = WarmPoolConfig(enabled=False)
        mgr = WarmPoolManager(db=test_db, config=config)
        assert mgr.is_enabled_for("h100-spot") is False


# =============================================================================
# pool_status() Tests
# =============================================================================


class TestPoolStatus:
    def test_pool_status_counts(self, manager, test_db):
        _insert_instance(test_db, "goldfish-warm-001", "busy")
        _insert_instance(test_db, "goldfish-warm-002", "idle_ready")

        status = manager.pool_status()
        assert status["enabled"] is True
        assert status["max_instances"] == 2
        assert status["total"] == 2
        assert status["by_state"]["busy"] == 1
        assert status["by_state"]["idle_ready"] == 1

    def test_pool_status_empty(self, manager):
        status = manager.pool_status()
        assert status["total"] == 0
        assert status["by_state"] == {}


# =============================================================================
# get_instance() Tests
# =============================================================================


class TestGetInstance:
    def test_get_existing(self, manager, test_db):
        _insert_instance(test_db, "goldfish-warm-001")
        inst = manager.get_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["instance_name"] == "goldfish-warm-001"

    def test_get_nonexistent(self, manager):
        assert manager.get_instance("nonexistent") is None


# =============================================================================
# delete_instance() Tests
# =============================================================================


class TestDeleteInstance:
    @patch("subprocess.run")
    def test_delete_instance_gcloud_succeeds(self, mock_run, manager, test_db):
        _insert_instance(test_db, "goldfish-warm-001", "deleting")
        mock_run.return_value = type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()

        manager.delete_instance("goldfish-warm-001")
        assert test_db.get_warm_instance("goldfish-warm-001") is None

    @patch("subprocess.run")
    def test_delete_nonexistent_no_error(self, mock_run, manager):
        manager.delete_instance("does-not-exist")
        mock_run.assert_not_called()
