"""Unit tests for database-driven base image version tracking."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.infra.base_images.manager import BaseImageManager
from goldfish.infra.profiles import BASE_IMAGE_VERSION


@pytest.fixture
def test_db(tmp_path: Path):
    """Create a test database."""
    from goldfish.db.database import Database

    db_path = tmp_path / ".goldfish" / "goldfish.db"
    return Database(db_path)


@pytest.fixture
def mock_config_with_gce() -> GoldfishConfig:
    """Create a mock GoldfishConfig with GCE configured."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig()
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    config.gce = gce
    return config


class TestBaseImageVersionsSchema:
    """Tests for base_image_versions table schema."""

    def test_table_exists_after_init(self, test_db) -> None:
        """base_image_versions table should exist after DB init."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='base_image_versions'"
            ).fetchone()
        assert result is not None

    def test_can_insert_version(self, test_db) -> None:
        """Should be able to insert a base image version."""
        test_db.set_base_image_version("gpu", "v10", "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v10")

        result = test_db.get_current_base_image_version("gpu")
        assert result is not None
        assert result["version"] == "v10"
        assert result["image_type"] == "gpu"

    def test_can_update_current_version(self, test_db) -> None:
        """Setting a new version should update the current version."""
        test_db.set_base_image_version("gpu", "v10", "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v10")
        test_db.set_base_image_version("gpu", "v11", "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v11")

        result = test_db.get_current_base_image_version("gpu")
        assert result is not None
        assert result["version"] == "v11"

    def test_different_image_types_independent(self, test_db) -> None:
        """CPU and GPU versions should be tracked independently."""
        test_db.set_base_image_version("gpu", "v10", "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v10")
        test_db.set_base_image_version("cpu", "v5", "us-docker.pkg.dev/proj/goldfish/goldfish-base-cpu:v5")

        gpu_result = test_db.get_current_base_image_version("gpu")
        cpu_result = test_db.get_current_base_image_version("cpu")

        assert gpu_result["version"] == "v10"
        assert cpu_result["version"] == "v5"

    def test_returns_none_for_unknown_type(self, test_db) -> None:
        """Should return None for unknown image type."""
        result = test_db.get_current_base_image_version("gpu")
        assert result is None


class TestBaseImageVersionHistory:
    """Tests for base image version history tracking."""

    def test_history_preserved_on_update(self, test_db) -> None:
        """All versions should be preserved in history."""
        test_db.set_base_image_version("gpu", "v10", "tag1")
        test_db.set_base_image_version("gpu", "v11", "tag2")
        test_db.set_base_image_version("gpu", "v12", "tag3")

        history = test_db.list_base_image_versions("gpu")
        assert len(history) == 3
        versions = [h["version"] for h in history]
        assert "v10" in versions
        assert "v11" in versions
        assert "v12" in versions

    def test_history_ordered_by_created_at(self, test_db) -> None:
        """History should be ordered by created_at descending (newest first)."""
        test_db.set_base_image_version("gpu", "v10", "tag1")
        test_db.set_base_image_version("gpu", "v11", "tag2")
        test_db.set_base_image_version("gpu", "v12", "tag3")

        history = test_db.list_base_image_versions("gpu")
        # Newest first
        assert history[0]["version"] == "v12"
        assert history[0]["is_current"] == 1
        assert history[1]["version"] == "v11"
        assert history[1]["is_current"] == 0


class TestManagerUsesDatabase:
    """Tests for BaseImageManager using database versions."""

    def test_uses_db_version_when_available(
        self, tmp_path: Path, test_db, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should use database version instead of hardcoded constant."""
        # Set a version in DB
        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=test_db)

        # Should use v15 from DB, not hardcoded BASE_IMAGE_VERSION
        registry_tag = manager._get_goldfish_base_registry_tag("gpu")
        assert ":v15" in registry_tag
        assert f":{BASE_IMAGE_VERSION}" not in registry_tag or BASE_IMAGE_VERSION == "v15"

    def test_falls_back_to_constant_when_no_db(self, tmp_path: Path, mock_config_with_gce: GoldfishConfig) -> None:
        """Manager should use hardcoded constant when no DB provided."""
        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=None)

        registry_tag = manager._get_goldfish_base_registry_tag("gpu")
        assert f":{BASE_IMAGE_VERSION}" in registry_tag

    def test_falls_back_to_constant_when_no_db_version(
        self, tmp_path: Path, test_db, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should use hardcoded constant when no version in DB."""
        # Don't set any version in DB
        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=test_db)

        registry_tag = manager._get_goldfish_base_registry_tag("gpu")
        assert f":{BASE_IMAGE_VERSION}" in registry_tag


class TestAutoIncrementVersion:
    """Tests for automatic version incrementing on build."""

    def test_get_next_version_starts_at_v1(self, test_db) -> None:
        """First version should be v1."""
        next_version = test_db.get_next_base_image_version("gpu")
        assert next_version == "v1"

    def test_get_next_version_increments(self, test_db) -> None:
        """Should increment version number correctly."""
        test_db.set_base_image_version("gpu", "v5", "tag1")
        next_version = test_db.get_next_base_image_version("gpu")
        assert next_version == "v6"

    def test_get_next_version_handles_gaps(self, test_db) -> None:
        """Should use max version + 1, not count."""
        test_db.set_base_image_version("gpu", "v10", "tag1")
        next_version = test_db.get_next_base_image_version("gpu")
        assert next_version == "v11"
