"""Tests for version pruning feature.

TDD: These tests define the expected behavior for version pruning.
Pruning allows hiding noise versions while preserving the audit trail.

Key behaviors:
- Pruned versions are hidden from list_versions() by default
- Tagged versions cannot be pruned (protected)
- Pruning is reversible via unprune
- Flexible pruning: single version, range, or "before tag"
"""

import subprocess

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def pruning_setup(temp_dir):
    """Setup for version pruning tests."""
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


def _create_workspace_with_versions(setup, workspace_name: str, num_versions: int = 5) -> list[str]:
    """Helper to create a workspace with multiple versions.

    Returns list of version strings (e.g., ["v1", "v2", "v3", "v4", "v5"]).
    """
    manager = setup["manager"]

    manager.create_workspace(workspace_name, goal="Test workspace", reason="Creating workspace for pruning tests")
    manager.mount(workspace_name, "w1", reason="Mount for creating versions")

    versions = []
    slot_path = manager._slot_path("w1")

    for i in range(1, num_versions + 1):
        (slot_path / f"file{i}.txt").write_text(f"content {i}")
        result = manager.save_version("w1", f"Version {i} save message")
        versions.append(result.version)

    return versions


class TestPruneVersionHidesFromList:
    """Tests that pruned versions are hidden from list_versions() by default."""

    def test_prune_version_hides_from_list(self, pruning_setup):
        """Pruned versions should not appear in list_versions() by default."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # All 5 versions should be visible initially
        all_versions = db.list_versions("test-ws")
        assert len(all_versions) == 5

        # Prune v2 and v3
        db.prune_version("test-ws", versions[1], "Pruning failed experiment v2")
        db.prune_version("test-ws", versions[2], "Pruning failed experiment v3")

        # Now only 3 versions should be visible
        visible_versions = db.list_versions("test-ws")
        assert len(visible_versions) == 3

        visible_version_names = [v["version"] for v in visible_versions]
        assert versions[0] in visible_version_names  # v1 visible
        assert versions[1] not in visible_version_names  # v2 pruned
        assert versions[2] not in visible_version_names  # v3 pruned
        assert versions[3] in visible_version_names  # v4 visible
        assert versions[4] in visible_version_names  # v5 visible

    def test_list_versions_include_pruned_flag(self, pruning_setup):
        """list_versions(include_pruned=True) should return all versions including pruned."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Prune some versions
        db.prune_version("test-ws", versions[1], "Pruning for include flag test")
        db.prune_version("test-ws", versions[2], "Pruning second version for test")

        # Default should hide pruned
        default_versions = db.list_versions("test-ws")
        assert len(default_versions) == 3

        # With include_pruned=True should show all
        all_versions = db.list_versions("test-ws", include_pruned=True)
        assert len(all_versions) == 5

        # Pruned versions should have pruned_at set
        pruned = [v for v in all_versions if v.get("pruned_at") is not None]
        assert len(pruned) == 2


class TestPruneVersionsRange:
    """Tests for pruning a range of versions."""

    def test_prune_versions_range(self, pruning_setup):
        """prune_versions should prune a range of versions (inclusive)."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=10)

        # Prune v3 through v7
        result = db.prune_versions("test-ws", versions[2], versions[6], "Pruning failed experiments v3-v7")

        assert result["pruned_count"] == 5

        # Should have 5 visible versions (v1, v2, v8, v9, v10)
        visible = db.list_versions("test-ws")
        assert len(visible) == 5

        visible_names = [v["version"] for v in visible]
        assert versions[0] in visible_names  # v1
        assert versions[1] in visible_names  # v2
        assert versions[7] in visible_names  # v8
        assert versions[8] in visible_names  # v9
        assert versions[9] in visible_names  # v10


class TestPruneBeforeTag:
    """Tests for pruning all versions before a tagged milestone."""

    def test_prune_before_tag_stops_at_tag(self, pruning_setup):
        """prune_before_tag should prune all versions before the tagged version."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=10)

        # Tag v5 as a milestone
        db.create_tag("test-ws", versions[4], "first-working")

        # Prune all versions before the tag
        result = db.prune_before_tag("test-ws", "first-working", "Pruning failed attempts before milestone")

        # Should prune v1, v2, v3, v4 (4 versions before v5)
        assert result["pruned_count"] == 4

        # Should have 6 visible versions (v5-v10)
        visible = db.list_versions("test-ws")
        assert len(visible) == 6

        # v5 (the tagged version) should still be visible
        visible_names = [v["version"] for v in visible]
        assert versions[4] in visible_names

    def test_prune_before_tag_does_not_prune_tagged_version(self, pruning_setup):
        """prune_before_tag should NOT prune the tagged version itself."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Tag v3
        db.create_tag("test-ws", versions[2], "milestone")

        # Prune before tag
        db.prune_before_tag("test-ws", "milestone", "Pruning versions before milestone")

        # v3 should still be visible
        visible = db.list_versions("test-ws")
        visible_names = [v["version"] for v in visible]
        assert versions[2] in visible_names


class TestCannotPruneTaggedVersion:
    """Tests that tagged versions are protected from pruning."""

    def test_cannot_prune_tagged_version(self, pruning_setup):
        """Attempting to prune a tagged version should raise an error."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=3)

        # Tag v2
        db.create_tag("test-ws", versions[1], "protected-milestone")

        # Trying to prune v2 should fail
        with pytest.raises(GoldfishError, match="Cannot prune version .* because it has tags"):
            db.prune_version("test-ws", versions[1], "Attempting to prune protected version")

    def test_prune_range_skips_tagged_versions(self, pruning_setup):
        """Pruning a range should skip tagged versions within the range."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=10)

        # Tag v5 (middle of range we'll prune)
        db.create_tag("test-ws", versions[4], "milestone")

        # Prune v3 through v7 - should skip v5
        result = db.prune_versions("test-ws", versions[2], versions[6], "Pruning range with tagged version")

        # Should prune v3, v4, v6, v7 (4 versions, skipping v5)
        assert result["pruned_count"] == 4
        assert result.get("skipped_tagged", 0) == 1

        # v5 should still be visible
        visible = db.list_versions("test-ws")
        visible_names = [v["version"] for v in visible]
        assert versions[4] in visible_names  # v5 still there


class TestUnpruneVersion:
    """Tests for restoring pruned versions."""

    def test_unprune_restores_version(self, pruning_setup):
        """Unpruning should make a version visible again."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Prune v3
        db.prune_version("test-ws", versions[2], "Pruning for unprune test")

        # Verify it's hidden
        visible = db.list_versions("test-ws")
        assert len(visible) == 4

        # Unprune v3
        db.unprune_version("test-ws", versions[2])

        # Should be visible again
        restored = db.list_versions("test-ws")
        assert len(restored) == 5

        # v3 should have no pruned_at
        all_with_pruned = db.list_versions("test-ws", include_pruned=True)
        v3 = [v for v in all_with_pruned if v["version"] == versions[2]][0]
        assert v3.get("pruned_at") is None

    def test_unprune_range(self, pruning_setup):
        """unprune_versions should restore a range of versions."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=10)

        # Prune v3-v7
        db.prune_versions("test-ws", versions[2], versions[6], "Pruning range for unprune test")

        # Verify only 5 visible
        assert len(db.list_versions("test-ws")) == 5

        # Unprune v4-v6 (subset of pruned range)
        result = db.unprune_versions("test-ws", versions[3], versions[5])

        assert result["unpruned_count"] == 3

        # Should now have 8 visible (v1, v2, v4, v5, v6, v8, v9, v10)
        visible = db.list_versions("test-ws")
        assert len(visible) == 8


class TestVersionNumberingAfterPrune:
    """Tests that version numbering continues correctly after pruning."""

    def test_version_numbering_continues_after_prune(self, pruning_setup):
        """New versions should continue numbering even after pruning old ones."""
        db = pruning_setup["db"]
        manager = pruning_setup["manager"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Prune v2, v3, v4
        db.prune_versions("test-ws", versions[1], versions[3], "Pruning for numbering test")

        # Create a new version
        slot_path = manager._slot_path("w1")
        (slot_path / "new_file.txt").write_text("new content")
        result = manager.save_version("w1", "New version after pruning")

        # New version should be v6, not v3
        assert result.version == "v6"

        # Verify with list_versions (include_pruned to see all)
        all_versions = db.list_versions("test-ws", include_pruned=True)
        assert len(all_versions) == 6

        version_names = [v["version"] for v in all_versions]
        assert "v6" in version_names


class TestPrunedNotInStatusOutput:
    """Tests that pruned versions don't appear in status() output."""

    def test_pruned_not_in_status_output(self, pruning_setup):
        """Pruned versions should not appear in status/lineage output."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Prune some versions
        db.prune_version("test-ws", versions[1], "Pruning for status test v2")
        db.prune_version("test-ws", versions[2], "Pruning for status test v3")

        # Get latest version (should not return a pruned version)
        latest = db.get_latest_version("test-ws")
        assert latest is not None
        assert latest["version"] == versions[4]  # v5

    def test_get_latest_version_skips_pruned(self, pruning_setup):
        """get_latest_version should return the latest non-pruned version."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=5)

        # Prune the last two versions (v4, v5)
        db.prune_version("test-ws", versions[3], "Pruning v4 for latest test")
        db.prune_version("test-ws", versions[4], "Pruning v5 for latest test")

        # get_latest_version should return v3 (the last non-pruned)
        latest = db.get_latest_version("test-ws")
        assert latest is not None
        assert latest["version"] == versions[2]  # v3


class TestPruneNonexistentVersion:
    """Tests for validation when pruning."""

    def test_prune_nonexistent_version_raises_error(self, pruning_setup):
        """Pruning a non-existent version should raise an error."""
        db = pruning_setup["db"]
        _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=3)

        with pytest.raises(GoldfishError, match="Version .* not found"):
            db.prune_version("test-ws", "v999", "Attempting to prune nonexistent")


class TestGetPrunedCount:
    """Tests for getting the count of pruned versions."""

    def test_get_pruned_count(self, pruning_setup):
        """get_pruned_count should return the number of pruned versions."""
        db = pruning_setup["db"]
        versions = _create_workspace_with_versions(pruning_setup, "test-ws", num_versions=10)

        # Initially no pruned versions
        assert db.get_pruned_count("test-ws") == 0

        # Prune some versions
        db.prune_version("test-ws", versions[1], "Pruning v2 for count test")
        db.prune_version("test-ws", versions[2], "Pruning v3 for count test")
        db.prune_version("test-ws", versions[3], "Pruning v4 for count test")

        # Should report 3 pruned
        assert db.get_pruned_count("test-ws") == 3
