"""Unit tests for Docker builds database CRUD methods."""

from datetime import UTC, datetime
from pathlib import Path

import pytest


@pytest.fixture
def test_db(tmp_path: Path):
    """Create a test database with schema."""
    from goldfish.db.database import Database

    db_path = tmp_path / "test.db"
    return Database(db_path)


class TestInsertDockerBuild:
    """Tests for insert_docker_build."""

    def test_insert_build_creates_record(self, test_db):
        """insert_docker_build should create a new record."""
        build_id = "build-abc12345"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="local",
            started_at=datetime.now(UTC).isoformat(),
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        assert result["id"] == build_id
        assert result["image_type"] == "gpu"
        assert result["target"] == "base"
        assert result["backend"] == "local"
        assert result["status"] == "pending"

    def test_insert_build_with_tags(self, test_db):
        """insert_docker_build should store tags."""
        build_id = "build-xyz12345"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="cpu",
            target="project",
            backend="cloud",
            started_at=datetime.now(UTC).isoformat(),
            image_tag="my-project-cpu:v1",
            registry_tag="us-docker.pkg.dev/proj/goldfish/my-project-cpu:v1",
            cloud_build_id="abc-123-xyz",
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        assert result["image_tag"] == "my-project-cpu:v1"
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/goldfish/my-project-cpu:v1"
        assert result["cloud_build_id"] == "abc-123-xyz"


class TestGetDockerBuild:
    """Tests for get_docker_build."""

    def test_get_nonexistent_returns_none(self, test_db):
        """get_docker_build should return None for unknown build."""
        result = test_db.get_docker_build("build-00000000")
        assert result is None

    def test_get_returns_all_fields(self, test_db):
        """get_docker_build should return all fields."""
        build_id = "build-test1234"
        now = datetime.now(UTC).isoformat()
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="cloud",
            started_at=now,
            cloud_build_id="gcp-build-id",
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        # Check all required fields exist
        assert "id" in result
        assert "image_type" in result
        assert "target" in result
        assert "backend" in result
        assert "cloud_build_id" in result
        assert "status" in result
        assert "image_tag" in result
        assert "registry_tag" in result
        assert "started_at" in result
        assert "completed_at" in result
        assert "error" in result
        assert "logs_uri" in result
        assert "created_at" in result


class TestUpdateDockerBuildStatus:
    """Tests for update_docker_build_status."""

    def test_update_status(self, test_db):
        """update_docker_build_status should update status."""
        build_id = "build-upd12345"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="local",
            started_at=datetime.now(UTC).isoformat(),
        )

        result = test_db.update_docker_build_status(build_id, "building")
        assert result is True

        build = test_db.get_docker_build(build_id)
        assert build is not None
        assert build["status"] == "building"

    def test_update_with_error(self, test_db):
        """update_docker_build_status should store error on failure."""
        build_id = "build-fail1234"
        now = datetime.now(UTC)
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="project",
            backend="cloud",
            started_at=now.isoformat(),
        )

        test_db.update_docker_build_status(
            build_id,
            "failed",
            error="Build failed: out of memory",
            completed_at=now.isoformat(),
        )

        build = test_db.get_docker_build(build_id)
        assert build is not None
        assert build["status"] == "failed"
        assert build["error"] == "Build failed: out of memory"
        assert build["completed_at"] is not None

    def test_update_with_image_tags(self, test_db):
        """update_docker_build_status should store image tags on success."""
        build_id = "build-succ1234"
        now = datetime.now(UTC)
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now.isoformat(),
        )

        test_db.update_docker_build_status(
            build_id,
            "completed",
            completed_at=now.isoformat(),
            image_tag="goldfish-base-gpu:v4",
            registry_tag="us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v4",
        )

        build = test_db.get_docker_build(build_id)
        assert build is not None
        assert build["status"] == "completed"
        assert build["image_tag"] == "goldfish-base-gpu:v4"
        assert build["registry_tag"] == "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v4"

    def test_update_nonexistent_returns_false(self, test_db):
        """update_docker_build_status should return False for unknown build."""
        result = test_db.update_docker_build_status("build-00000000", "completed")
        assert result is False


class TestListDockerBuilds:
    """Tests for list_docker_builds."""

    def test_list_empty(self, test_db):
        """list_docker_builds should return empty list when no builds."""
        result = test_db.list_docker_builds()
        assert result == []

    def test_list_returns_all(self, test_db):
        """list_docker_builds should return all builds."""
        now = datetime.now(UTC).isoformat()
        for i in range(3):
            test_db.insert_docker_build(
                build_id=f"build-list{i:04d}",
                image_type="gpu" if i % 2 == 0 else "cpu",
                target="base",
                backend="local",
                started_at=now,
            )

        result = test_db.list_docker_builds()
        assert len(result) == 3

    def test_list_filter_by_status(self, test_db):
        """list_docker_builds should filter by status."""
        now = datetime.now(UTC).isoformat()
        test_db.insert_docker_build(
            build_id="build-pend0001",
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now,
        )
        test_db.insert_docker_build(
            build_id="build-comp0001",
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now,
        )
        test_db.update_docker_build_status("build-comp0001", "completed")

        pending = test_db.list_docker_builds(status="pending")
        assert len(pending) == 1
        assert pending[0]["id"] == "build-pend0001"

        completed = test_db.list_docker_builds(status="completed")
        assert len(completed) == 1
        assert completed[0]["id"] == "build-comp0001"

    def test_list_filter_by_backend(self, test_db):
        """list_docker_builds should filter by backend."""
        now = datetime.now(UTC).isoformat()
        test_db.insert_docker_build(
            build_id="build-local001",
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now,
        )
        test_db.insert_docker_build(
            build_id="build-cloud001",
            image_type="gpu",
            target="base",
            backend="cloud",
            started_at=now,
        )

        local = test_db.list_docker_builds(backend="local")
        assert len(local) == 1
        assert local[0]["backend"] == "local"

        cloud = test_db.list_docker_builds(backend="cloud")
        assert len(cloud) == 1
        assert cloud[0]["backend"] == "cloud"


class TestGetActiveDockerBuilds:
    """Tests for get_active_docker_builds."""

    def test_get_active_returns_pending_and_building(self, test_db):
        """get_active_docker_builds should return pending and building builds."""
        now = datetime.now(UTC).isoformat()

        # Create builds in various states
        test_db.insert_docker_build(
            build_id="build-actpend1",
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now,
        )  # pending

        test_db.insert_docker_build(
            build_id="build-actbldg1",
            image_type="cpu",
            target="project",
            backend="cloud",
            started_at=now,
        )
        test_db.update_docker_build_status("build-actbldg1", "building")

        test_db.insert_docker_build(
            build_id="build-actcomp1",
            image_type="gpu",
            target="base",
            backend="local",
            started_at=now,
        )
        test_db.update_docker_build_status("build-actcomp1", "completed")

        active = test_db.get_active_docker_builds()
        assert len(active) == 2
        active_ids = {b["id"] for b in active}
        assert "build-actpend1" in active_ids
        assert "build-actbldg1" in active_ids
        assert "build-actcomp1" not in active_ids


class TestDeleteDockerBuild:
    """Tests for delete_docker_build."""

    def test_delete_removes_build(self, test_db):
        """delete_docker_build should remove the build."""
        build_id = "build-del12345"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="local",
            started_at=datetime.now(UTC).isoformat(),
        )

        result = test_db.delete_docker_build(build_id)
        assert result is True

        build = test_db.get_docker_build(build_id)
        assert build is None

    def test_delete_nonexistent_returns_false(self, test_db):
        """delete_docker_build should return False for unknown build."""
        result = test_db.delete_docker_build("build-00000000")
        assert result is False
