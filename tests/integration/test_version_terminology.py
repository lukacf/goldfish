"""Tests for version terminology refactor.

TDD: Tests for renaming checkpoint() to save_version() and using "version"
as the primary identifier instead of "snapshot_id".

Goals:
- save_version() returns version (v1, v2) as primary identifier
- rollback() accepts version string instead of snapshot_id
- Old checkpoint() tool is deprecated but still works
"""

import subprocess
import warnings

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def version_test_setup(temp_dir):
    """Setup for version terminology tests."""
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


class TestSaveVersionReturnsVersionAsPrimary:
    """Tests that save_version() returns version as primary identifier."""

    def test_save_version_returns_version_field(self, version_test_setup):
        """save_version() should return version (v1, v2, etc.) as primary identifier."""
        manager = version_test_setup["manager"]

        # Create and mount workspace
        manager.create_workspace("test-ws", goal="Test save_version", reason="Testing version terminology")
        manager.mount("test-ws", "w1", reason="Testing save_version")

        # Make a change
        slot_path = manager._slot_path("w1")
        (slot_path / "feature.py").write_text("# New feature")

        # Call save_version (new name for checkpoint)
        result = manager.save_version("w1", "First version with new feature")

        # Should return version as primary identifier
        assert hasattr(result, "version"), "Response should have 'version' field"
        assert result.version == "v1", f"First version should be 'v1', got {result.version}"

    def test_save_version_returns_git_tag(self, version_test_setup):
        """save_version() should return git_tag as secondary/internal identifier."""
        manager = version_test_setup["manager"]

        manager.create_workspace("test-ws", goal="Test git_tag", reason="Testing git_tag in response")
        manager.mount("test-ws", "w1", reason="Testing git_tag field")

        slot_path = manager._slot_path("w1")
        (slot_path / "code.py").write_text("# Code")

        result = manager.save_version("w1", "Version with git_tag test")

        # Should return git_tag (the snap-xxx format)
        assert hasattr(result, "git_tag"), "Response should have 'git_tag' field"
        assert result.git_tag.startswith("snap-"), f"git_tag should start with 'snap-', got {result.git_tag}"

    def test_save_version_returns_git_sha(self, version_test_setup):
        """save_version() should return git_sha for full provenance."""
        manager = version_test_setup["manager"]

        manager.create_workspace("test-ws", goal="Test git_sha", reason="Testing git_sha in response")
        manager.mount("test-ws", "w1", reason="Testing git_sha field")

        slot_path = manager._slot_path("w1")
        (slot_path / "module.py").write_text("# Module")

        result = manager.save_version("w1", "Version with git_sha test")

        # Should return full git SHA
        assert hasattr(result, "git_sha"), "Response should have 'git_sha' field"
        assert len(result.git_sha) == 40, f"git_sha should be 40 chars, got {len(result.git_sha)}"

    def test_save_version_increments_version_numbers(self, version_test_setup):
        """Multiple save_version() calls should increment version numbers."""
        manager = version_test_setup["manager"]

        manager.create_workspace("test-ws", goal="Test version increment", reason="Testing version numbering")
        manager.mount("test-ws", "w1", reason="Testing version increment")

        slot_path = manager._slot_path("w1")

        # First version
        (slot_path / "v1.py").write_text("# V1")
        result1 = manager.save_version("w1", "First version created")
        assert result1.version == "v1"

        # Second version
        (slot_path / "v2.py").write_text("# V2")
        result2 = manager.save_version("w1", "Second version created")
        assert result2.version == "v2"

        # Third version
        (slot_path / "v3.py").write_text("# V3")
        result3 = manager.save_version("w1", "Third version created")
        assert result3.version == "v3"


class TestRollbackAcceptsVersion:
    """Tests that rollback() accepts version string instead of snapshot_id."""

    def test_rollback_accepts_version_string(self, version_test_setup):
        """rollback() should accept version like 'v1' instead of snapshot_id."""
        manager = version_test_setup["manager"]

        manager.create_workspace("test-ws", goal="Test rollback", reason="Testing rollback with version")
        manager.mount("test-ws", "w1", reason="Testing rollback accepts version")

        slot_path = manager._slot_path("w1")

        # Create v1
        (slot_path / "original.py").write_text("# Original")
        result1 = manager.save_version("w1", "Original version v1")

        # Create v2 with changes
        (slot_path / "changed.py").write_text("# Changed")
        manager.save_version("w1", "Changed version v2")

        # Rollback to v1 using version string
        rollback_result = manager.rollback("w1", version="v1", reason="Rolling back to original version")

        assert rollback_result.success
        assert rollback_result.version == "v1"
        # changed.py should be gone after rollback
        assert not (slot_path / "changed.py").exists()

    def test_rollback_response_includes_version(self, version_test_setup):
        """rollback() response should include version field."""
        manager = version_test_setup["manager"]

        manager.create_workspace("test-ws", goal="Test rollback response", reason="Testing rollback response fields")
        manager.mount("test-ws", "w1", reason="Testing rollback response")

        slot_path = manager._slot_path("w1")

        # Create version
        (slot_path / "file.py").write_text("# File")
        manager.save_version("w1", "Version to rollback to")

        # Make more changes
        (slot_path / "extra.py").write_text("# Extra")
        manager.save_version("w1", "Version with extra file")

        # Rollback
        result = manager.rollback("w1", version="v1", reason="Testing response fields")

        assert hasattr(result, "version"), "Response should have 'version' field"
        assert result.version == "v1"


class TestDeprecatedCheckpoint:
    """Tests that old checkpoint() still works but is deprecated."""

    def test_checkpoint_still_works(self, version_test_setup):
        """checkpoint() should still work for backwards compatibility."""
        manager = version_test_setup["manager"]

        manager.create_workspace(
            "test-ws", goal="Test deprecated checkpoint", reason="Testing checkpoint backwards compat"
        )
        manager.mount("test-ws", "w1", reason="Testing checkpoint still works")

        slot_path = manager._slot_path("w1")
        (slot_path / "file.py").write_text("# File")

        # Old checkpoint() should still work
        result = manager.checkpoint("w1", "Checkpoint using old API")

        assert result.success
        # Old response still has snapshot_id for backwards compat
        assert hasattr(result, "snapshot_id")
        assert result.snapshot_id.startswith("snap-")

    def test_checkpoint_emits_deprecation_warning(self, version_test_setup):
        """checkpoint() should emit deprecation warning."""
        manager = version_test_setup["manager"]

        manager.create_workspace(
            "test-ws", goal="Test deprecation warning", reason="Testing deprecation warning emission"
        )
        manager.mount("test-ws", "w1", reason="Testing deprecation warning")

        slot_path = manager._slot_path("w1")
        (slot_path / "file.py").write_text("# File")

        # Should emit deprecation warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            manager.checkpoint("w1", "Should emit deprecation warning")

            # Check for deprecation warning
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) >= 1, "checkpoint() should emit DeprecationWarning"
            assert "save_version" in str(deprecation_warnings[0].message).lower()


class TestVersionInLineage:
    """Tests that versions appear correctly in lineage."""

    def test_save_version_appears_in_lineage(self, version_test_setup):
        """Versions from save_version() should appear in get_workspace_lineage()."""
        from goldfish.lineage.manager import LineageManager

        manager = version_test_setup["manager"]
        db = version_test_setup["db"]

        manager.create_workspace("test-ws", goal="Test lineage", reason="Testing versions in lineage")
        manager.mount("test-ws", "w1", reason="Testing lineage integration")

        slot_path = manager._slot_path("w1")
        (slot_path / "file.py").write_text("# File")

        manager.save_version("w1", "Version for lineage test")

        # Check lineage
        lineage_mgr = LineageManager(db=db, workspace_manager=manager)
        lineage = lineage_mgr.get_workspace_lineage("test-ws")

        assert len(lineage["versions"]) >= 1
        version = lineage["versions"][0]
        assert version["version"] == "v1"
        assert version["created_by"] == "save_version"
