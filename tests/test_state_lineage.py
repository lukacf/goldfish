"""Tests for STATE.md lineage display enhancements - TDD Phase 7."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from goldfish.state.state_md import StateManager
from goldfish.config import GoldfishConfig
from goldfish.models import SlotInfo, SlotState, DirtyState


class TestWorkspaceLineageDisplay:
    """Test displaying workspace lineage in STATE.md."""

    def test_state_shows_workspace_version(self, temp_dir, test_config):
        """STATE.md should show current version for each workspace."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="test_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v3"  # Add version info
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should show version
        assert "test_ws" in content
        assert "v3" in content

    def test_state_shows_workspace_parent(self, temp_dir, test_config):
        """STATE.md should show parent workspace if branched."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="experiment_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v1",
                parent_workspace="main_ws",  # Branched from main_ws
                parent_version="v5"
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should show parent info
        assert "experiment_ws" in content
        assert "main_ws" in content or "branched" in content.lower()

    def test_state_shows_version_count(self, temp_dir, test_config):
        """STATE.md should show total number of versions."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="test_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v5",
                version_count=5  # Has 5 versions total
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should show version count
        assert "5" in content or "versions" in content.lower()


class TestVersionHistoryDisplay:
    """Test displaying version evolution history."""

    def test_state_shows_recent_versions(self, temp_dir, test_config):
        """STATE.md should show recent version history."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        # Mock LineageManager to return version history
        version_history = [
            {"version": "v3", "created_at": "2024-01-03", "description": "Added attention"},
            {"version": "v2", "created_at": "2024-01-02", "description": "Increased batch size"},
            {"version": "v1", "created_at": "2024-01-01", "description": "Initial pipeline"}
        ]

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="test_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v3",
                version_history=version_history  # Recent versions
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should show version evolution
        assert "v3" in content
        assert "v2" in content or "v1" in content


class TestBranchDisplay:
    """Test displaying workspace branches."""

    def test_state_shows_child_branches(self, temp_dir, test_config):
        """STATE.md should show child workspaces branched from this one."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        branches = [
            {"workspace": "experiment1", "branched_from": "v3"},
            {"workspace": "experiment2", "branched_from": "v3"}
        ]

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="main_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v5",
                branches=branches  # Child branches
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should show branches
        assert "experiment1" in content or "experiment2" in content or "branch" in content.lower()


class TestCompactLineageDisplay:
    """Test compact lineage display for STATE.md."""

    def test_lineage_display_is_concise(self, temp_dir, test_config):
        """Lineage display should be concise to avoid bloat."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        # Workspace with lots of versions - should only show recent ones
        version_history = [{"version": f"v{i}", "created_at": f"2024-01-{i:02d}"}
                          for i in range(1, 21)]  # 20 versions

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="test_ws",
                dirty=DirtyState.CLEAN,
                context="",
                last_checkpoint="2024-01-01",
                current_version="v20",
                version_history=version_history
            )
        ]

        # Execute
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify - should not show all 20 versions (too verbose)
        # Should show maybe last 3-5
        version_mentions = content.count("v")
        assert version_mentions < 15  # Not showing all versions


class TestNoLineageWhenEmpty:
    """Test STATE.md when workspace has no lineage yet."""

    def test_state_handles_no_versions(self, temp_dir, test_config):
        """STATE.md should handle workspace with no versions yet."""
        # Setup
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, test_config)

        slots = [
            SlotInfo(
                slot="w1",
                state=SlotState.MOUNTED,
                workspace="new_ws",
                dirty=DirtyState.DIRTY,
                context="",
                last_checkpoint=None,
                current_version=None  # No versions yet
            )
        ]

        # Execute - should not crash
        content = manager.regenerate(slots=slots, jobs=[], source_count=0)

        # Verify
        assert "new_ws" in content
        assert "DIRTY" in content
