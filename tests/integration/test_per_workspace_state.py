"""Tests for Phase 4: Per-Workspace STATE.md.

Each mounted workspace should have its own STATE.md in the slot directory,
providing workspace-specific context for Claude's compaction recovery.
"""

import subprocess

import pytest

from goldfish.config import (
    AuditConfig,
    GoldfishConfig,
    JobsConfig,
    StateMdConfig,
)
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager


@pytest.fixture
def state_md_setup(temp_dir):
    """Set up isolated environment for per-workspace STATE.md tests."""
    # Resolve to handle macOS symlinks
    temp_dir = temp_dir.resolve()

    # Create user project directory
    user_project = temp_dir / "myproject"
    user_project.mkdir()

    # Create dev repo with git
    dev_repo = temp_dir / "myproject-dev"
    dev_repo.mkdir()
    subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create initial structure in dev repo
    (dev_repo / "code").mkdir()
    (dev_repo / "code" / "main.py").write_text("# initial")
    (dev_repo / ".goldfish").mkdir()

    subprocess.run(["git", "add", "."], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create workspaces directory in user project
    workspaces_dir = user_project / "workspaces"
    workspaces_dir.mkdir()

    # Initialize database
    db_path = dev_repo / ".goldfish" / "goldfish.db"
    db = Database(db_path)

    # Create config - dev_repo is sibling to user_project
    config = GoldfishConfig(
        project_name="test-state-md",
        dev_repo_path="myproject-dev",  # Sibling to myproject
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="exp"),
        invariants=["Never modify core data format"],
    )

    # Initialize manager
    manager = WorkspaceManager(
        db=db,
        config=config,
        project_root=user_project,
    )

    return {
        "user_project": user_project,
        "dev_repo": dev_repo,
        "workspaces_dir": workspaces_dir,
        "db": db,
        "config": config,
        "manager": manager,
    }


class TestPerWorkspaceStateMd:
    """Tests for per-workspace STATE.md generation."""

    def test_mount_creates_state_md_in_slot(self, state_md_setup):
        """Mounting a workspace should create STATE.md in the slot directory."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        # Create and mount workspace
        manager.create_workspace("feature-a", "Build feature A", "Creating workspace for feature")
        manager.mount("feature-a", "w1", "Mount for development")

        slot_path = workspaces_dir / "w1"

        # STATE.md should exist in slot
        state_md_path = slot_path / "STATE.md"
        assert state_md_path.exists(), "STATE.md should be created in slot on mount"

    def test_state_md_contains_workspace_name(self, state_md_setup):
        """STATE.md should contain the workspace name prominently."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("experiment-lstm", "LSTM experiment", "Creating for LSTM test")
        manager.mount("experiment-lstm", "w1", "Mount for LSTM work")

        state_md_path = workspaces_dir / "w1" / "STATE.md"
        content = state_md_path.read_text()

        assert "experiment-lstm" in content

    def test_state_md_contains_goal(self, state_md_setup):
        """STATE.md should contain the workspace goal."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        goal = "Train baseline LSTM model with attention"
        manager.create_workspace("baseline", goal, "Creating baseline workspace")
        manager.mount("baseline", "w1", "Mount for baseline work")

        state_md_path = workspaces_dir / "w1" / "STATE.md"
        content = state_md_path.read_text()

        assert goal in content or "Goal" in content

    def test_state_md_contains_slot_info(self, state_md_setup):
        """STATE.md should indicate which slot it's mounted to."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("test-ws", "Testing slot info", "Creating for slot test")
        manager.mount("test-ws", "w2", "Mount to w2 slot")

        state_md_path = workspaces_dir / "w2" / "STATE.md"
        content = state_md_path.read_text()

        assert "w2" in content

    def test_state_md_updated_on_checkpoint(self, state_md_setup):
        """Checkpoint should update STATE.md with version info."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("versioned-ws", "Test versioning", "Creating versioned workspace")
        manager.mount("versioned-ws", "w1", "Mount for versioning")

        slot_path = workspaces_dir / "w1"

        # Make a change and checkpoint
        (slot_path / "code" / "main.py").write_text("# modified")
        manager.checkpoint("w1", "First checkpoint for testing")

        state_md_path = slot_path / "STATE.md"
        content = state_md_path.read_text()

        # Should have version info
        assert "snap-" in content or "checkpoint" in content.lower()

    def test_state_md_updated_on_version(self, state_md_setup):
        """sync_and_version should update STATE.md with version info."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("version-test", "Test sync_and_version", "Creating version test workspace")
        manager.mount("version-test", "w1", "Mount for version test")

        slot_path = workspaces_dir / "w1"

        # Make a change and version
        (slot_path / "code" / "main.py").write_text("# for version")
        manager.sync_and_version("w1", "train", "Version for training")

        state_md_path = slot_path / "STATE.md"
        content = state_md_path.read_text()

        # Should have version info
        assert "v1" in content or "version" in content.lower()

    def test_state_md_contains_recent_actions(self, state_md_setup):
        """STATE.md should track recent actions for this workspace."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("action-test", "Test recent actions", "Creating for action test")
        manager.mount("action-test", "w1", "Mount for action test")

        slot_path = workspaces_dir / "w1"

        # Perform some actions
        (slot_path / "code" / "main.py").write_text("# change 1")
        manager.checkpoint("w1", "First checkpoint message")

        (slot_path / "code" / "main.py").write_text("# change 2")
        manager.checkpoint("w1", "Second checkpoint message")

        state_md_path = slot_path / "STATE.md"
        content = state_md_path.read_text()

        # Should have recent actions section
        assert "Recent" in content or "Actions" in content

    def test_state_md_preserved_on_hibernate_remount(self, state_md_setup):
        """STATE.md content should be preserved through hibernate/remount cycle."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        manager.create_workspace("persist-test", "Test persistence", "Creating for persistence test")
        manager.mount("persist-test", "w1", "Initial mount testing")

        slot_path = workspaces_dir / "w1"

        # Make changes and checkpoint
        (slot_path / "code" / "main.py").write_text("# persisted code")
        manager.checkpoint("w1", "Checkpoint before hibernate")

        # Hibernate
        manager.hibernate("w1", "Hibernate for testing persistence")

        # Remount
        manager.mount("persist-test", "w2", "Remount after hibernate")

        state_md_path = workspaces_dir / "w2" / "STATE.md"
        content = state_md_path.read_text()

        # Should still have workspace info
        assert "persist-test" in content

    def test_different_workspaces_have_independent_state_md(self, state_md_setup):
        """Each workspace should have its own independent STATE.md."""
        manager = state_md_setup["manager"]
        workspaces_dir = state_md_setup["workspaces_dir"]

        # Create and mount two different workspaces
        manager.create_workspace("workspace-a", "Goal for A", "Creating workspace A")
        manager.create_workspace("workspace-b", "Goal for B", "Creating workspace B")

        manager.mount("workspace-a", "w1", "Mount workspace A to w1")
        manager.mount("workspace-b", "w2", "Mount workspace B to w2")

        state_a = (workspaces_dir / "w1" / "STATE.md").read_text()
        state_b = (workspaces_dir / "w2" / "STATE.md").read_text()

        # Each should have its own workspace info
        assert "workspace-a" in state_a
        assert "workspace-b" in state_b
        assert "workspace-b" not in state_a
        assert "workspace-a" not in state_b
