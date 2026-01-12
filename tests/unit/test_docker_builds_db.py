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


class TestWorkspaceBuilds:
    """Tests for workspace-related Docker build features."""

    def test_insert_workspace_build(self, test_db):
        """insert_docker_build should store workspace_name and version."""
        build_id = "build-ws123456"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=datetime.now(UTC).isoformat(),
            workspace_name="baseline_lstm",
            version="v5",
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        assert result["target"] == "workspace"
        assert result["workspace_name"] == "baseline_lstm"
        assert result["version"] == "v5"

    def test_insert_base_build_has_null_workspace(self, test_db):
        """Base builds should have NULL workspace_name and version."""
        build_id = "build-base1234"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="base",
            backend="local",
            started_at=datetime.now(UTC).isoformat(),
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        assert result["workspace_name"] is None
        assert result["version"] is None

    def test_get_docker_build_returns_workspace_fields(self, test_db):
        """get_docker_build should include workspace_name and version fields."""
        build_id = "build-wsfield1"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=datetime.now(UTC).isoformat(),
            workspace_name="test_ws",
            version="v10",
        )

        result = test_db.get_docker_build(build_id)
        assert result is not None
        # Check new fields exist
        assert "workspace_name" in result
        assert "version" in result
        assert result["workspace_name"] == "test_ws"
        assert result["version"] == "v10"

    def test_get_docker_build_by_workspace(self, test_db):
        """get_docker_build_by_workspace should find build by workspace+version."""
        build_id = "build-bylookup"
        test_db.insert_docker_build(
            build_id=build_id,
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=datetime.now(UTC).isoformat(),
            workspace_name="my_workspace",
            version="v3",
        )

        result = test_db.get_docker_build_by_workspace("my_workspace", "v3")
        assert result is not None
        assert result["id"] == build_id
        assert result["workspace_name"] == "my_workspace"
        assert result["version"] == "v3"

    def test_get_docker_build_by_workspace_returns_none_if_not_found(self, test_db):
        """get_docker_build_by_workspace should return None if not found."""
        result = test_db.get_docker_build_by_workspace("nonexistent", "v1")
        assert result is None

    def test_get_docker_build_by_workspace_returns_latest(self, test_db):
        """get_docker_build_by_workspace should return most recent build."""
        now = datetime.now(UTC)
        # Insert older build
        test_db.insert_docker_build(
            build_id="build-older001",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="multi_build",
            version="v1",
        )
        test_db.update_docker_build_status("build-older001", "completed")

        # Insert newer build for same workspace+version
        from datetime import timedelta

        later = (now + timedelta(hours=1)).isoformat()
        test_db.insert_docker_build(
            build_id="build-newer001",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=later,
            workspace_name="multi_build",
            version="v1",
        )

        result = test_db.get_docker_build_by_workspace("multi_build", "v1")
        assert result is not None
        # Should return the newer build
        assert result["id"] == "build-newer001"

    def test_list_docker_builds_includes_workspace_fields(self, test_db):
        """list_docker_builds should include workspace fields."""
        test_db.insert_docker_build(
            build_id="build-list-ws1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=datetime.now(UTC).isoformat(),
            workspace_name="list_test_ws",
            version="v2",
        )

        result = test_db.list_docker_builds()
        assert len(result) == 1
        assert "workspace_name" in result[0]
        assert "version" in result[0]
        assert result[0]["workspace_name"] == "list_test_ws"
        assert result[0]["version"] == "v2"
