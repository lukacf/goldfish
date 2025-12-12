"""Tests for checkpoint versioning - ensuring checkpoints register as versions.

TDD: These tests capture the expected behavior.

Bug: checkpoint() creates git tags but doesn't register versions in workspace_versions table.
This breaks:
- get_workspace_lineage() returning empty versions list
- branch_workspace() failing with "Version not found"
"""

import subprocess

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def checkpoint_setup(temp_dir):
    """Setup for checkpoint versioning tests."""
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


class TestCheckpointCreatesVersion:
    """Tests that checkpoint() properly registers versions in database."""

    def test_checkpoint_creates_version_in_database(self, checkpoint_setup):
        """Checkpoint should create a version record in workspace_versions table."""
        manager = checkpoint_setup["manager"]
        db = checkpoint_setup["db"]

        # Create workspace
        manager.create_workspace(
            "test-ws", goal="Test checkpoint versioning", reason="Testing checkpoint version registration"
        )
        manager.mount("test-ws", "w1", reason="Testing checkpoint")

        # Create a checkpoint
        slot_path = manager._slot_path("w1")
        (slot_path / "test.txt").write_text("test content")
        result = manager.checkpoint("w1", "First checkpoint message")

        # Version should be registered in database
        versions = db.list_versions("test-ws")
        assert len(versions) >= 1, "Checkpoint should create a version in database"

        # Find the version created by checkpoint (internally uses save_version)
        checkpoint_versions = [v for v in versions if v["created_by"] == "save_version"]
        assert len(checkpoint_versions) == 1, "Should have exactly one version"

        version = checkpoint_versions[0]
        assert version["workspace_name"] == "test-ws"
        assert version["version"] is not None  # e.g., "v1"
        assert version["git_tag"] is not None
        assert version["git_sha"] is not None

    def test_checkpoint_version_appears_in_lineage(self, checkpoint_setup):
        """Checkpoint versions should appear in get_workspace_lineage()."""
        from goldfish.lineage.manager import LineageManager

        manager = checkpoint_setup["manager"]
        db = checkpoint_setup["db"]

        # Create workspace and checkpoint
        manager.create_workspace("test-ws", goal="Test lineage", reason="Testing lineage with checkpoints")
        manager.mount("test-ws", "w1", reason="Testing lineage features")
        slot_path = manager._slot_path("w1")
        (slot_path / "test.txt").write_text("content")
        manager.checkpoint("w1", "Checkpoint for lineage test")

        # Check lineage
        lineage_mgr = LineageManager(db=db, workspace_manager=manager)
        lineage = lineage_mgr.get_workspace_lineage("test-ws")

        assert len(lineage["versions"]) >= 1, "Lineage should include checkpoint versions"


class TestBranchFromCheckpoint:
    """Tests that branch_workspace works with checkpoint versions."""

    def test_branch_workspace_from_checkpoint_version(self, checkpoint_setup):
        """branch_workspace should work with versions created by checkpoint."""
        from goldfish.lineage.manager import LineageManager

        manager = checkpoint_setup["manager"]
        db = checkpoint_setup["db"]

        # Create workspace and checkpoint
        manager.create_workspace("parent-ws", goal="Parent workspace", reason="Testing branch from checkpoint")
        manager.mount("parent-ws", "w1", reason="Testing branch feature")
        slot_path = manager._slot_path("w1")
        (slot_path / "feature.py").write_text("# New feature")
        manager.checkpoint("w1", "Checkpoint with new feature")

        # Get the version created by checkpoint (internally uses save_version)
        versions = db.list_versions("parent-ws")
        checkpoint_versions = [v for v in versions if v["created_by"] == "save_version"]
        assert len(checkpoint_versions) >= 1, "Should have version"
        version = checkpoint_versions[0]["version"]

        # Branch from that version
        lineage_mgr = LineageManager(db=db, workspace_manager=manager)
        # This should NOT raise "Version not found"
        lineage_mgr.branch_workspace(
            from_workspace="parent-ws",
            from_version=version,
            new_workspace="child-ws",
            reason="Branch from checkpoint",
        )

        # Verify branch was created
        child_lineage = db.get_workspace_lineage("child-ws")
        assert child_lineage is not None
        assert child_lineage["parent_workspace"] == "parent-ws"
        assert child_lineage["parent_version"] == version


class TestListSnapshotsValidation:
    """Tests for list_snapshots validation bug."""

    def test_list_snapshots_accepts_valid_limit(self, checkpoint_setup):
        """list_snapshots should accept limit=50 (a valid value between 1-200)."""
        manager = checkpoint_setup["manager"]

        # Create workspace
        manager.create_workspace("test-ws", goal="Test list_snapshots", reason="Testing list_snapshots validation")

        # This should NOT raise "limit must be between 1 and 200"
        snapshots = manager.list_snapshots("test-ws", limit=50, offset=0)
        # Empty list is fine, just shouldn't error
        assert isinstance(snapshots, list)

    def test_list_snapshots_rejects_invalid_limits(self, checkpoint_setup):
        """list_snapshots should reject limits outside 1-200."""
        from goldfish.errors import GoldfishError

        manager = checkpoint_setup["manager"]

        manager.create_workspace("test-ws", goal="Test", reason="Testing invalid limits")

        with pytest.raises(GoldfishError, match="limit must be between 1 and 200"):
            manager.list_snapshots("test-ws", limit=0)

        with pytest.raises(GoldfishError, match="limit must be between 1 and 200"):
            manager.list_snapshots("test-ws", limit=201)
