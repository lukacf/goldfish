"""Unit tests for database-driven PROJECT image version tracking.

This follows TDD RED phase - tests should FAIL with current code but
PASS once the project image versioning feature is implemented.

Project images ({project}-{cpu,gpu}) need independent version tracking,
similar to base images (goldfish-base-{cpu,gpu}), to enable:
1. Automatic version incrementing on build
2. Version history for rollback
3. Per-project tracking in the database

Note: Each project has its own SQLite database, so project_name is implicit
(not passed to DB methods). This mirrors the base_image_versions pattern.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.infra.base_images.manager import BaseImageManager


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
    config.project_name = "mlm"
    config.docker = DockerConfig()
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    config.gce = gce
    return config


# =============================================================================
# Database Schema Tests
# =============================================================================


class TestProjectImageVersionsSchema:
    """Tests for project_image_versions table schema."""

    def test_table_exists_after_init(self, test_db) -> None:
        """project_image_versions table should exist after DB init."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='project_image_versions'"
            ).fetchone()
        assert result is not None, "project_image_versions table should exist"


# =============================================================================
# Database CRUD Tests
# =============================================================================


class TestSetProjectImageVersion:
    """Tests for Database.set_project_image_version()."""

    def test_set_project_image_version_creates_record(self, test_db) -> None:
        """Setting a project image version should create a database record."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v1",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v1",
        )

        result = test_db.get_current_project_image_version("mlm", "gpu")
        assert result is not None
        assert result["version"] == "v1"
        assert result["project_name"] == "mlm"
        assert result["image_type"] == "gpu"

    def test_set_project_image_version_marks_current(self, test_db) -> None:
        """Setting a project image version should mark it as current."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v1",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v1",
        )

        result = test_db.get_current_project_image_version("mlm", "gpu")
        assert result is not None
        assert result["is_current"] == 1

    def test_set_project_image_version_unmarks_previous(self, test_db) -> None:
        """Setting a new version should unmark the previous current version."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v1",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v1",
        )
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v2",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v2",
        )

        # Current should be v2
        current = test_db.get_current_project_image_version("mlm", "gpu")
        assert current is not None
        assert current["version"] == "v2"
        assert current["is_current"] == 1

        # v1 should still exist but not be current
        history = test_db.list_project_image_versions("mlm", "gpu")
        v1_entry = next((h for h in history if h["version"] == "v1"), None)
        assert v1_entry is not None
        assert v1_entry["is_current"] == 0


# =============================================================================
# Database Query Tests
# =============================================================================


class TestGetCurrentProjectImageVersion:
    """Tests for Database.get_current_project_image_version()."""

    def test_get_current_project_image_version(self, test_db) -> None:
        """Should return the current version info for a project/type."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v3",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v3",
        )

        result = test_db.get_current_project_image_version("mlm", "gpu")
        assert result is not None
        assert result["version"] == "v3"
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/goldfish/mlm-gpu:v3"

    def test_get_current_project_image_version_none_when_empty(self, test_db) -> None:
        """Should return None when no versions exist for project/type."""
        result = test_db.get_current_project_image_version("mlm", "gpu")
        assert result is None

    def test_different_image_types_independent(self, test_db) -> None:
        """CPU and GPU versions should be tracked independently per project."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v10",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-gpu:v10",
        )
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="cpu",
            version="v3",
            registry_tag="us-docker.pkg.dev/proj/goldfish/mlm-cpu:v3",
        )

        gpu_result = test_db.get_current_project_image_version("mlm", "gpu")
        cpu_result = test_db.get_current_project_image_version("mlm", "cpu")

        assert gpu_result["version"] == "v10"
        assert cpu_result["version"] == "v3"


# =============================================================================
# Database List/History Tests
# =============================================================================


class TestListProjectImageVersions:
    """Tests for Database.list_project_image_versions()."""

    def test_list_project_image_versions(self, test_db) -> None:
        """Should return all versions for a project/type."""
        test_db.set_project_image_version("mlm", "gpu", "v1", "tag1")
        test_db.set_project_image_version("mlm", "gpu", "v2", "tag2")
        test_db.set_project_image_version("mlm", "gpu", "v3", "tag3")

        history = test_db.list_project_image_versions("mlm", "gpu")
        assert len(history) == 3
        versions = [h["version"] for h in history]
        assert "v1" in versions
        assert "v2" in versions
        assert "v3" in versions

    def test_list_project_image_versions_ordered(self, test_db) -> None:
        """History should be ordered by id descending (newest first)."""
        test_db.set_project_image_version("mlm", "gpu", "v1", "tag1")
        test_db.set_project_image_version("mlm", "gpu", "v2", "tag2")
        test_db.set_project_image_version("mlm", "gpu", "v3", "tag3")

        history = test_db.list_project_image_versions("mlm", "gpu")
        # Newest first
        assert history[0]["version"] == "v3"
        assert history[0]["is_current"] == 1
        assert history[1]["version"] == "v2"
        assert history[1]["is_current"] == 0

    def test_list_project_image_versions_empty(self, test_db) -> None:
        """Should return empty list when no versions exist."""
        history = test_db.list_project_image_versions("mlm", "gpu")
        assert history == []


# =============================================================================
# Auto-Increment Version Tests
# =============================================================================


class TestGetNextProjectImageVersion:
    """Tests for Database.get_next_project_image_version()."""

    def test_get_next_project_image_version_starts_at_v1(self, test_db) -> None:
        """First version should be v1."""
        next_version = test_db.get_next_project_image_version("mlm", "gpu")
        assert next_version == "v1"

    def test_get_next_project_image_version_increments(self, test_db) -> None:
        """Should increment version number correctly."""
        test_db.set_project_image_version("mlm", "gpu", "v5", "tag1")
        next_version = test_db.get_next_project_image_version("mlm", "gpu")
        assert next_version == "v6"

    def test_get_next_project_image_version_handles_gaps(self, test_db) -> None:
        """Should use max version + 1, not count."""
        test_db.set_project_image_version("mlm", "gpu", "v10", "tag1")
        next_version = test_db.get_next_project_image_version("mlm", "gpu")
        assert next_version == "v11"

    def test_get_next_project_image_version_per_type(self, test_db) -> None:
        """Different image types should have independent version sequences."""
        test_db.set_project_image_version("mlm", "gpu", "v10", "tag1")
        test_db.set_project_image_version("mlm", "cpu", "v3", "tag2")

        gpu_next = test_db.get_next_project_image_version("mlm", "gpu")
        cpu_next = test_db.get_next_project_image_version("mlm", "cpu")

        assert gpu_next == "v11"
        assert cpu_next == "v4"


# =============================================================================
# Manager Integration Tests
# =============================================================================


class TestManagerUsesProjectDatabase:
    """Tests for BaseImageManager using database versions for project images."""

    def test_project_image_tag_uses_db_version(
        self, tmp_path: Path, test_db, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should use database version for project image tags."""
        # Set a version in DB
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v7",
            registry_tag="us-docker.pkg.dev/my-project/goldfish/mlm-gpu:v7",
        )

        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=test_db)

        # Should use v7 from DB, not hardcoded v1
        local_tag = manager._get_project_image_tag("gpu", for_registry=False)
        assert local_tag == "mlm-gpu:v7", f"Expected 'mlm-gpu:v7', got '{local_tag}'"

    def test_project_image_registry_tag_uses_db_version(
        self, tmp_path: Path, test_db, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should use database version for project registry tags."""
        test_db.set_project_image_version(
            project_name="mlm",
            image_type="gpu",
            version="v7",
            registry_tag="us-docker.pkg.dev/my-project/goldfish/mlm-gpu:v7",
        )

        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=test_db)

        registry_tag = manager._get_project_image_tag("gpu", for_registry=True)
        assert registry_tag is not None, "Expected registry_tag, got None"
        assert ":v7" in registry_tag, f"Expected ':v7' in '{registry_tag}'"

    def test_project_image_returns_none_when_no_db(
        self, tmp_path: Path, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should return None when no DB provided.

        Project images are user-built, not Goldfish-shipped. There is NO default.
        When no version exists, _get_project_image_tag returns None.
        """
        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=None)

        local_tag = manager._get_project_image_tag("gpu", for_registry=False)
        assert local_tag is None, "Project image tag should be None when no version exists"

    def test_project_image_returns_none_when_no_db_version(
        self, tmp_path: Path, test_db, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """Manager should return None when no version in DB.

        Project images are user-built, not Goldfish-shipped. There is NO default.
        When no version exists in DB, _get_project_image_tag returns None.
        """
        # Don't set any version in DB
        manager = BaseImageManager(tmp_path, mock_config_with_gce, db=test_db)

        local_tag = manager._get_project_image_tag("gpu", for_registry=False)
        assert local_tag is None, "Project image tag should be None when no DB version exists"
