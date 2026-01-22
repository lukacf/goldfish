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


class TestGetLatestDockerBuildForWorkspace:
    """REGRESSION TESTS for cross-version Docker layer caching.

    THE BUG (Fixed 2025-01-18):
    - get_docker_build_by_workspace(workspace, version) filtered by exact version
    - When building v5, it searched for v5 builds which don't exist yet
    - Result: cache-from was never populated, images rebuilt from scratch each time

    THE FIX:
    - get_latest_docker_build_for_workspace(workspace) finds ANY previous build
    - This enables cache-from to work across version boundaries (v4 → v5)
    - Docker layers are workspace-specific, not version-specific

    These tests ensure cross-version caching works correctly.
    """

    def test_returns_none_when_no_builds_exist(self, test_db):
        """Should return None when workspace has no builds."""
        result = test_db.get_latest_docker_build_for_workspace("nonexistent_ws")
        assert result is None

    def test_returns_most_recent_completed_build(self, test_db):
        """Should return the most recent completed build for the workspace."""
        now = datetime.now(UTC)

        # Insert v1 build (completed)
        test_db.insert_docker_build(
            build_id="build-cache-v1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="cache_test_ws",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-cache-v1",
            "completed",
            registry_tag="us-docker.pkg.dev/proj/repo/cache_test_ws:v1",
        )

        result = test_db.get_latest_docker_build_for_workspace("cache_test_ws")
        assert result is not None
        assert result["version"] == "v1"
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/repo/cache_test_ws:v1"

    def test_returns_latest_across_versions(self, test_db):
        """REGRESSION: Should return the most recent build ACROSS versions.

        When building v3, should find v2's image for cache-from.
        """
        from datetime import timedelta

        now = datetime.now(UTC)

        # Insert v1 build (older)
        test_db.insert_docker_build(
            build_id="build-cross-v1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="cross_version_ws",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-cross-v1",
            "completed",
            completed_at=now.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/cross_version_ws:v1",
        )

        # Insert v2 build (newer)
        later = now + timedelta(hours=1)
        test_db.insert_docker_build(
            build_id="build-cross-v2",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=later.isoformat(),
            workspace_name="cross_version_ws",
            version="v2",
        )
        test_db.update_docker_build_status(
            "build-cross-v2",
            "completed",
            completed_at=later.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/cross_version_ws:v2",
        )

        # When building v3, should get v2's tag for cache
        result = test_db.get_latest_docker_build_for_workspace("cross_version_ws")
        assert result is not None
        assert result["version"] == "v2"  # Most recent version
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/repo/cross_version_ws:v2"

    def test_only_returns_completed_builds(self, test_db):
        """Should only return completed builds (not pending/building/failed)."""
        now = datetime.now(UTC)

        # Insert failed build (should be ignored)
        test_db.insert_docker_build(
            build_id="build-failed-v1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="status_test_ws",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-failed-v1",
            "failed",
            registry_tag=None,  # No registry tag for failed builds
        )

        # Insert pending build (should be ignored)
        test_db.insert_docker_build(
            build_id="build-pending-v2",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="status_test_ws",
            version="v2",
        )
        # Leave as pending (no status update)

        result = test_db.get_latest_docker_build_for_workspace("status_test_ws")
        # Should be None - no completed builds
        assert result is None

    def test_only_returns_builds_with_registry_tag(self, test_db):
        """Should only return builds that have a registry_tag (pushed to registry)."""
        now = datetime.now(UTC)

        # Insert completed build WITHOUT registry tag (local build never pushed)
        test_db.insert_docker_build(
            build_id="build-local-only",
            image_type="gpu",
            target="workspace",
            backend="local",
            started_at=now.isoformat(),
            workspace_name="tag_test_ws",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-local-only",
            "completed",
            registry_tag=None,  # No push to registry
        )

        result = test_db.get_latest_docker_build_for_workspace("tag_test_ws")
        # Should be None - no builds with registry_tag
        assert result is None

    def test_workspaces_are_independent(self, test_db):
        """Builds from different workspaces should not affect each other."""
        now = datetime.now(UTC)

        # Build for workspace A
        test_db.insert_docker_build(
            build_id="build-ws-a-v1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="workspace_a",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-ws-a-v1",
            "completed",
            registry_tag="us-docker.pkg.dev/proj/repo/workspace_a:v1",
        )

        # Build for workspace B
        test_db.insert_docker_build(
            build_id="build-ws-b-v1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="workspace_b",
            version="v1",
        )
        test_db.update_docker_build_status(
            "build-ws-b-v1",
            "completed",
            registry_tag="us-docker.pkg.dev/proj/repo/workspace_b:v1",
        )

        # Query workspace A - should only get A's build
        result_a = test_db.get_latest_docker_build_for_workspace("workspace_a")
        assert result_a is not None
        assert result_a["workspace_name"] == "workspace_a"

        # Query workspace B - should only get B's build
        result_b = test_db.get_latest_docker_build_for_workspace("workspace_b")
        assert result_b is not None
        assert result_b["workspace_name"] == "workspace_b"

        # Query workspace C (no builds) - should return None
        result_c = test_db.get_latest_docker_build_for_workspace("workspace_c")
        assert result_c is None


class TestGetDockerBuildByContentHash:
    """REGRESSION TESTS for content-based Docker build caching.

    THE BUG (Fixed 2025-01-18):
    - Workspace images were rebuilt on every run even when no files changed
    - This happened because builds were keyed by version (v1, v2, etc.)
    - When running v5, it checked for goldfish-ws-v5 which doesn't exist yet

    THE FIX:
    - Compute SHA256 hash of build context (workspace files + Dockerfile + base image)
    - Store content_hash in docker_builds table
    - Before building, check if we've built this exact content before
    - If content_hash matches a completed build, skip building and reuse the image

    These tests ensure content-based caching works correctly.
    """

    def test_returns_none_when_no_matching_hash(self, test_db):
        """Should return None when no build matches the content hash."""
        result = test_db.get_docker_build_by_content_hash("some_ws", "sha256_that_doesnt_exist")
        assert result is None

    def test_finds_build_by_content_hash(self, test_db):
        """Should find a completed build by its content hash."""
        now = datetime.now(UTC)
        content_hash = "abc123def456789"

        test_db.insert_docker_build(
            build_id="build-hash-test1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="hash_test_ws",
            version="v1",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-hash-test1",
            "completed",
            registry_tag="us-docker.pkg.dev/proj/repo/hash_test_ws:v1",
        )

        result = test_db.get_docker_build_by_content_hash("hash_test_ws", content_hash)
        assert result is not None
        assert result["content_hash"] == content_hash
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/repo/hash_test_ws:v1"

    def test_only_returns_completed_builds_with_hash(self, test_db):
        """Should only return completed builds (not pending/failed)."""
        now = datetime.now(UTC)
        content_hash = "pending_hash_123"

        # Insert pending build with hash
        test_db.insert_docker_build(
            build_id="build-pending-hash",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="pending_hash_ws",
            version="v1",
            content_hash=content_hash,
        )
        # Leave as pending

        result = test_db.get_docker_build_by_content_hash("pending_hash_ws", content_hash)
        assert result is None  # Should not find pending build

    def test_only_returns_builds_with_registry_tag_by_hash(self, test_db):
        """Should only return builds that have a registry_tag."""
        now = datetime.now(UTC)
        content_hash = "local_only_hash_456"

        # Insert completed build WITHOUT registry tag
        test_db.insert_docker_build(
            build_id="build-local-hash",
            image_type="gpu",
            target="workspace",
            backend="local",
            started_at=now.isoformat(),
            workspace_name="local_hash_ws",
            version="v1",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-local-hash",
            "completed",
            registry_tag=None,  # No push to registry
        )

        result = test_db.get_docker_build_by_content_hash("local_hash_ws", content_hash)
        assert result is None  # Should not find build without registry_tag

    def test_hash_is_workspace_scoped(self, test_db):
        """Same content_hash in different workspaces should not match."""
        now = datetime.now(UTC)
        content_hash = "shared_hash_789"

        # Build for workspace A
        test_db.insert_docker_build(
            build_id="build-hash-ws-a",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="workspace_a_hash",
            version="v1",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-hash-ws-a",
            "completed",
            registry_tag="us-docker.pkg.dev/proj/repo/workspace_a_hash:v1",
        )

        # Query with workspace B - should NOT find workspace A's build
        result = test_db.get_docker_build_by_content_hash("workspace_b_hash", content_hash)
        assert result is None

        # Query with workspace A - should find it
        result = test_db.get_docker_build_by_content_hash("workspace_a_hash", content_hash)
        assert result is not None
        assert result["workspace_name"] == "workspace_a_hash"

    def test_reuses_older_version_with_same_hash(self, test_db):
        """REGRESSION: When v5 has same content as v3, should reuse v3's image.

        This is the core use case - if nothing changed between versions,
        we should skip the build entirely and reuse the existing image.
        """
        from datetime import timedelta

        now = datetime.now(UTC)
        content_hash = "unchanged_content_hash_abc"

        # Build v3 with content_hash
        test_db.insert_docker_build(
            build_id="build-v3-unchanged",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="unchanged_ws",
            version="v3",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-v3-unchanged",
            "completed",
            completed_at=now.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/unchanged_ws:v3",
        )

        # v4 changed something (different hash, not relevant to this test)
        later = now + timedelta(hours=1)
        test_db.insert_docker_build(
            build_id="build-v4-changed",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=later.isoformat(),
            workspace_name="unchanged_ws",
            version="v4",
            content_hash="different_hash_for_v4",
        )
        test_db.update_docker_build_status(
            "build-v4-changed",
            "completed",
            completed_at=later.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/unchanged_ws:v4",
        )

        # Now for v5, content matches v3 (same hash)
        # When we query by content_hash, should find v3's build
        result = test_db.get_docker_build_by_content_hash("unchanged_ws", content_hash)
        assert result is not None
        assert result["version"] == "v3"  # Found v3, not v4
        assert result["registry_tag"] == "us-docker.pkg.dev/proj/repo/unchanged_ws:v3"

    def test_returns_most_recent_matching_hash(self, test_db):
        """If multiple builds have same hash, return most recent."""
        from datetime import timedelta

        now = datetime.now(UTC)
        content_hash = "duplicate_hash_test"

        # First build with this hash
        test_db.insert_docker_build(
            build_id="build-dup-1",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=now.isoformat(),
            workspace_name="dup_hash_ws",
            version="v1",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-dup-1",
            "completed",
            completed_at=now.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/dup_hash_ws:v1",
        )

        # Second build with same hash (maybe rollback scenario)
        later = now + timedelta(hours=2)
        test_db.insert_docker_build(
            build_id="build-dup-2",
            image_type="gpu",
            target="workspace",
            backend="cloud",
            started_at=later.isoformat(),
            workspace_name="dup_hash_ws",
            version="v3",
            content_hash=content_hash,
        )
        test_db.update_docker_build_status(
            "build-dup-2",
            "completed",
            completed_at=later.isoformat(),
            registry_tag="us-docker.pkg.dev/proj/repo/dup_hash_ws:v3",
        )

        # Should return the most recent one
        result = test_db.get_docker_build_by_content_hash("dup_hash_ws", content_hash)
        assert result is not None
        assert result["version"] == "v3"  # Most recent


class TestContentHashMigration:
    """Tests for content_hash column migration.

    These tests verify that:
    1. The content_hash column exists in the docker_builds schema
    2. The content_hash index exists
    3. Builds can be inserted and queried with content_hash

    Note: Full migration testing (from old schema) is complex because the
    Database class requires many tables to exist. These tests verify the
    final state after migration rather than simulating the migration itself.
    """

    def test_content_hash_column_exists(self, test_db):
        """docker_builds table should have content_hash column."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(docker_builds)")}
            assert "content_hash" in columns

    def test_content_hash_index_exists(self, test_db):
        """docker_builds table should have index on content_hash column."""
        with test_db._conn() as conn:
            indexes = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='docker_builds'"
            ).fetchall()
            index_names = {row["name"] for row in indexes}
            assert "idx_docker_builds_content_hash" in index_names

    def test_content_hash_in_migration_config(self):
        """Verify content_hash is listed in migration required_columns.

        This ensures that existing databases without the column will get it
        added during schema migration.
        """
        import inspect

        from goldfish.db.database import Database

        # Get the source of _migrate_schema to verify content_hash is there
        source = inspect.getsource(Database._migrate_schema)
        assert '"docker_builds"' in source
        assert '"content_hash"' in source
        assert "TEXT" in source  # content_hash should be TEXT type
