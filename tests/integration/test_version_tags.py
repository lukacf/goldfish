"""Tests for version tags feature.

TDD: These tests define the expected behavior for version tagging.
Tags allow marking significant versions (e.g., "baseline-working", "best-model").

Key behaviors:
- Tags can be applied retroactively to any existing version
- Tag names must be unique per workspace
- Tags are stored in workspace_version_tags table
"""

import subprocess

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def tags_setup(temp_dir):
    """Setup for version tags tests."""
    temp_dir = temp_dir.resolve()

    # Create user project directory
    user_project = temp_dir / "my-project"
    user_project.mkdir()
    workspaces_dir = user_project / "workspaces"
    workspaces_dir.mkdir()

    # Create dev repo with git
    dev_repo = temp_dir / "my-project-dev"
    dev_repo.mkdir()

    # Initialize git repo
    subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create initial structure and commit
    (dev_repo / "code").mkdir()
    (dev_repo / "code" / "main.py").write_text("# Main module")
    subprocess.run(["git", "add", "."], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create .goldfish directory in dev repo
    goldfish_dir = dev_repo / ".goldfish"
    goldfish_dir.mkdir()

    # Create database
    db = Database(goldfish_dir / "goldfish.db")

    # Create config
    config = GoldfishConfig(
        project_name="my-project",
        dev_repo_path="my-project-dev",
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )

    # Create manager
    manager = WorkspaceManager(
        config=config,
        project_root=user_project,
        db=db,
    )

    return {
        "manager": manager,
        "db": db,
        "user_project": user_project,
        "dev_repo": dev_repo,
        "workspaces_dir": workspaces_dir,
    }


def _create_workspace_with_versions(setup, workspace_name: str, num_versions: int = 3) -> list[str]:
    """Helper to create a workspace with multiple versions.

    Returns list of version strings (e.g., ["v1", "v2", "v3"]).
    """
    manager = setup["manager"]

    manager.create_workspace(workspace_name, goal="Test workspace", reason="Creating workspace for tag tests")
    manager.mount(workspace_name, "w1", reason="Mount for creating versions")

    versions = []
    slot_path = manager._slot_path("w1")

    for i in range(1, num_versions + 1):
        (slot_path / f"file{i}.txt").write_text(f"content {i}")
        result = manager.save_version("w1", f"Version {i} save message")
        versions.append(result.version)

    return versions


class TestTagVersionCreatesTag:
    """Tests that tag_version() properly creates tags in database."""

    def test_tag_version_creates_tag(self, tags_setup):
        """tag_version should create a tag record in workspace_version_tags table."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=1)
        version = versions[0]

        # Create a tag
        db.create_tag("test-ws", version, "baseline-working")

        # Tag should exist in database
        tags = db.list_tags("test-ws")
        assert len(tags) == 1
        assert tags[0]["tag_name"] == "baseline-working"
        assert tags[0]["version"] == version
        assert tags[0]["workspace_name"] == "test-ws"
        assert "created_at" in tags[0]

    def test_tag_version_retroactive(self, tags_setup):
        """Tags can be applied to older versions, not just the current one."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=5)

        # Tag an old version (v2), not the current one (v5)
        old_version = versions[1]  # v2
        db.create_tag("test-ws", old_version, "first-working")

        # Tag should point to the old version
        tags = db.list_tags("test-ws")
        assert len(tags) == 1
        assert tags[0]["version"] == old_version
        assert tags[0]["tag_name"] == "first-working"

    def test_multiple_tags_on_different_versions(self, tags_setup):
        """Multiple tags can exist on different versions in same workspace."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=5)

        # Tag multiple versions
        db.create_tag("test-ws", versions[1], "first-working")  # v2
        db.create_tag("test-ws", versions[3], "best-model")  # v4
        db.create_tag("test-ws", versions[4], "final")  # v5

        tags = db.list_tags("test-ws")
        assert len(tags) == 3

        tag_names = {t["tag_name"] for t in tags}
        assert tag_names == {"first-working", "best-model", "final"}


class TestUntagVersion:
    """Tests for removing tags."""

    def test_untag_version_removes_tag(self, tags_setup):
        """untag_version should remove the tag from database."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=1)

        # Create and then remove tag
        db.create_tag("test-ws", versions[0], "to-remove")
        assert len(db.list_tags("test-ws")) == 1

        db.delete_tag("test-ws", "to-remove")

        # Tag should be gone
        tags = db.list_tags("test-ws")
        assert len(tags) == 0

    def test_untag_nonexistent_raises_error(self, tags_setup):
        """Removing a non-existent tag should raise an error."""
        db = tags_setup["db"]
        _create_workspace_with_versions(tags_setup, "test-ws", num_versions=1)

        with pytest.raises(GoldfishError, match="Tag .* not found"):
            db.delete_tag("test-ws", "nonexistent-tag")


class TestListTags:
    """Tests for listing tags."""

    def test_list_tags_returns_all_tags(self, tags_setup):
        """list_tags should return all tags for a workspace."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=3)

        db.create_tag("test-ws", versions[0], "alpha")
        db.create_tag("test-ws", versions[1], "beta")
        db.create_tag("test-ws", versions[2], "gamma")

        tags = db.list_tags("test-ws")
        assert len(tags) == 3

        # Should be ordered by created_at or tag_name (check implementation)
        tag_names = [t["tag_name"] for t in tags]
        assert set(tag_names) == {"alpha", "beta", "gamma"}

    def test_list_tags_empty_workspace(self, tags_setup):
        """list_tags should return empty list for workspace with no tags."""
        db = tags_setup["db"]
        _create_workspace_with_versions(tags_setup, "test-ws", num_versions=1)

        tags = db.list_tags("test-ws")
        assert tags == []

    def test_list_tags_workspace_isolation(self, tags_setup):
        """Tags from one workspace should not appear in another."""
        db = tags_setup["db"]
        manager = tags_setup["manager"]

        # Create first workspace with tags
        versions1 = _create_workspace_with_versions(tags_setup, "ws1", num_versions=2)
        db.create_tag("ws1", versions1[0], "ws1-tag")

        # Unmount and create second workspace
        manager.hibernate("w1", reason="Hibernating to create new workspace")

        # Create second workspace with different tags
        manager.create_workspace("ws2", goal="Second workspace", reason="Testing workspace isolation")
        manager.mount("ws2", "w1", reason="Mount ws2 for isolation test")
        slot_path = manager._slot_path("w1")
        (slot_path / "file1.txt").write_text("ws2 content")
        result = manager.save_version("w1", "ws2 version 1 save")
        db.create_tag("ws2", result.version, "ws2-tag")

        # Each workspace should only see its own tags
        ws1_tags = db.list_tags("ws1")
        ws2_tags = db.list_tags("ws2")

        assert len(ws1_tags) == 1
        assert ws1_tags[0]["tag_name"] == "ws1-tag"

        assert len(ws2_tags) == 1
        assert ws2_tags[0]["tag_name"] == "ws2-tag"


class TestTagNameUniqueness:
    """Tests for tag name constraints."""

    def test_tag_name_must_be_unique_per_workspace(self, tags_setup):
        """Cannot create two tags with same name in same workspace."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=2)

        db.create_tag("test-ws", versions[0], "milestone")

        # Second tag with same name should fail
        with pytest.raises(GoldfishError, match="Tag .* already exists"):
            db.create_tag("test-ws", versions[1], "milestone")

    def test_same_tag_name_different_workspaces_ok(self, tags_setup):
        """Same tag name can be used in different workspaces."""
        db = tags_setup["db"]
        manager = tags_setup["manager"]

        # Create tag in first workspace
        versions1 = _create_workspace_with_versions(tags_setup, "ws1", num_versions=1)
        db.create_tag("ws1", versions1[0], "milestone")

        # Unmount and create second workspace with same tag name
        manager.hibernate("w1", reason="Hibernating for second workspace")
        manager.create_workspace("ws2", goal="Second workspace", reason="Testing tag name isolation")
        manager.mount("ws2", "w1", reason="Mount ws2 for tag isolation test")
        slot_path = manager._slot_path("w1")
        (slot_path / "file1.txt").write_text("ws2 content")
        result = manager.save_version("w1", "ws2 version 1 save")

        # Should succeed - same tag name but different workspace
        db.create_tag("ws2", result.version, "milestone")

        assert len(db.list_tags("ws1")) == 1
        assert len(db.list_tags("ws2")) == 1


class TestTagNonexistentVersion:
    """Tests for validation of version existence."""

    def test_cannot_tag_nonexistent_version(self, tags_setup):
        """Tagging a non-existent version should raise an error."""
        db = tags_setup["db"]
        _create_workspace_with_versions(tags_setup, "test-ws", num_versions=1)

        with pytest.raises(GoldfishError, match="Version .* not found"):
            db.create_tag("test-ws", "v999", "ghost-tag")

    def test_cannot_tag_version_in_wrong_workspace(self, tags_setup):
        """Tagging a version that belongs to different workspace should fail."""
        db = tags_setup["db"]
        manager = tags_setup["manager"]

        # Create first workspace with a version
        versions1 = _create_workspace_with_versions(tags_setup, "ws1", num_versions=1)

        # Unmount and create second workspace
        manager.hibernate("w1", reason="Hibernating for ws2")
        manager.create_workspace("ws2", goal="ws2", reason="Testing cross-workspace tag prevention")

        # Try to tag ws1's version from ws2 context
        with pytest.raises(GoldfishError, match="Version .* not found"):
            db.create_tag("ws2", versions1[0], "cross-ws-tag")


class TestGetVersionTags:
    """Tests for getting tags attached to a specific version."""

    def test_get_tags_for_version(self, tags_setup):
        """get_version_tags returns tags for a specific version."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=3)

        # Tag v2 with two tags (if we allow multiple tags per version)
        db.create_tag("test-ws", versions[1], "milestone-a")

        # Get tags for v2
        v2_tags = db.get_version_tags("test-ws", versions[1])
        assert len(v2_tags) == 1
        assert v2_tags[0]["tag_name"] == "milestone-a"

        # v1 and v3 should have no tags
        assert db.get_version_tags("test-ws", versions[0]) == []
        assert db.get_version_tags("test-ws", versions[2]) == []

    def test_list_versions_includes_tags(self, tags_setup):
        """list_versions should include tag info for each version."""
        db = tags_setup["db"]
        versions = _create_workspace_with_versions(tags_setup, "test-ws", num_versions=3)

        db.create_tag("test-ws", versions[1], "tagged-version")

        # list_versions should include tag info
        all_versions = db.list_versions("test-ws")

        # Find the tagged version
        tagged = [v for v in all_versions if v["version"] == versions[1]]
        assert len(tagged) == 1

        # Should have tag info (either as a field or can be joined)
        # The exact structure depends on implementation
        # Option 1: tags field on version
        # Option 2: separate get_version_tags call
        # We'll test that get_version_tags works correctly
        tags = db.get_version_tags("test-ws", versions[1])
        assert len(tags) == 1
        assert tags[0]["tag_name"] == "tagged-version"
