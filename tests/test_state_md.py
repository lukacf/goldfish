"""Tests for state_md.py - STATE.md generation and maintenance."""

import pytest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

from goldfish.config import GoldfishConfig, StateMdConfig
from goldfish.models import SlotInfo, SlotState, DirtyState
from goldfish.state.state_md import StateManager


@pytest.fixture
def basic_config():
    """Create a basic config for testing."""
    return GoldfishConfig(
        project_name="test-project",
        dev_repo_path="../test-dev",
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=5),
        invariants=["Keep X constant", "Never change Y"],
    )


@pytest.fixture
def state_manager(temp_dir, basic_config):
    """Create a StateManager instance."""
    state_path = temp_dir / "STATE.md"
    return StateManager(state_path, basic_config)


class TestStateManagerInit:
    """Tests for StateManager initialization."""

    def test_creates_with_empty_state(self, temp_dir, basic_config):
        """Should initialize with empty recent actions and default goal."""
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, basic_config)

        assert manager.state_path == state_path
        assert manager.config == basic_config
        assert manager.max_recent == 5
        assert manager._active_goal == "Not set"
        assert len(manager._recent_actions) == 0

    def test_loads_existing_goal(self, temp_dir, basic_config):
        """Should load existing goal from STATE.md."""
        state_path = temp_dir / "STATE.md"
        state_path.write_text("""# test-project

## Active Goal
Build a better model

## Recent Actions
- No recent actions
""")

        manager = StateManager(state_path, basic_config)

        assert manager._active_goal == "Build a better model"

    def test_loads_existing_recent_actions(self, temp_dir, basic_config):
        """Should load existing recent actions from STATE.md."""
        state_path = temp_dir / "STATE.md"
        state_path.write_text("""# test-project

## Active Goal
Some goal

## Recent Actions
- [10:00] First action
- [10:05] Second action
- [10:10] Third action

## Background Jobs
""")

        manager = StateManager(state_path, basic_config)

        assert len(manager._recent_actions) == 3
        assert "[10:00] First action" in manager._recent_actions
        assert "[10:10] Third action" in manager._recent_actions

    def test_handles_missing_file(self, temp_dir, basic_config):
        """Should handle missing STATE.md gracefully."""
        state_path = temp_dir / "STATE.md"
        # Don't create the file

        manager = StateManager(state_path, basic_config)

        assert manager._active_goal == "Not set"
        assert len(manager._recent_actions) == 0


class TestSetGoal:
    """Tests for set_goal() method."""

    def test_set_goal_updates_internal_state(self, state_manager):
        """Should update the active goal."""
        state_manager.set_goal("Train better embeddings")

        assert state_manager._active_goal == "Train better embeddings"

    def test_set_goal_persists_across_instances(self, temp_dir, basic_config):
        """Goal should persist when loading new manager."""
        state_path = temp_dir / "STATE.md"
        manager1 = StateManager(state_path, basic_config)
        manager1.set_goal("Improve accuracy")
        # Force a write by regenerating
        manager1.regenerate(slots=[], jobs=[])

        manager2 = StateManager(state_path, basic_config)

        assert manager2._active_goal == "Improve accuracy"


class TestAddAction:
    """Tests for add_action() method."""

    def test_add_action_includes_timestamp(self, state_manager):
        """Should add action with timestamp prefix."""
        with patch("goldfish.state.state_md.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "14:30"
            mock_dt.now.return_value = datetime(2025, 1, 1, 14, 30, tzinfo=timezone.utc)
            state_manager.add_action("Did something")

        assert "[14:30] Did something" in state_manager._recent_actions

    def test_add_action_respects_max_recent(self, temp_dir, basic_config):
        """Should drop oldest actions when max reached."""
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, basic_config)  # max_recent=5

        for i in range(10):
            manager.add_action(f"Action {i}")

        assert len(manager._recent_actions) == 5
        # Should have most recent 5 (actions 5-9, not 0-4)
        assert "Action 4" not in str(list(manager._recent_actions))
        assert "Action 5" in str(list(manager._recent_actions))
        assert "Action 9" in str(list(manager._recent_actions))


class TestRead:
    """Tests for read() method."""

    def test_read_returns_file_content(self, temp_dir, basic_config):
        """Should return STATE.md file content."""
        state_path = temp_dir / "STATE.md"
        content = "# Test\n\nSome content"
        state_path.write_text(content)

        manager = StateManager(state_path, basic_config)
        result = manager.read()

        assert result == content

    def test_read_returns_placeholder_when_missing(self, temp_dir, basic_config):
        """Should return placeholder when file doesn't exist."""
        state_path = temp_dir / "STATE.md"
        # Don't create file

        manager = StateManager(state_path, basic_config)
        result = manager.read()

        assert "test-project" in result
        assert "not initialized" in result.lower()


class TestRegenerate:
    """Tests for regenerate() method."""

    def test_regenerate_includes_project_name(self, state_manager):
        """Should include project name as title."""
        content = state_manager.regenerate(slots=[], jobs=[])

        assert "# test-project" in content

    def test_regenerate_includes_active_goal(self, state_manager):
        """Should include active goal section."""
        state_manager.set_goal("Build something great")
        content = state_manager.regenerate(slots=[], jobs=[])

        assert "## Active Goal" in content
        assert "Build something great" in content

    def test_regenerate_includes_workspace_slots(self, state_manager):
        """Should include workspace slots."""
        slots = [
            SlotInfo(slot="w1", state=SlotState.EMPTY),
            SlotInfo(
                slot="w2",
                state=SlotState.MOUNTED,
                workspace="my-ws",
                dirty=DirtyState.CLEAN,
                context="Working on feature",
            ),
            SlotInfo(
                slot="w3",
                state=SlotState.MOUNTED,
                workspace="other-ws",
                dirty=DirtyState.DIRTY,
                last_checkpoint="snap-abc1234-20251205-100000",
            ),
        ]
        content = state_manager.regenerate(slots=slots, jobs=[])

        assert "## Workspaces" in content
        assert "w1: [empty]" in content
        assert "w2: my-ws (CLEAN)" in content
        assert "Working on feature" in content
        assert "w3: other-ws (DIRTY)" in content
        assert "snap-abc1234" in content

    def test_regenerate_includes_invariants(self, state_manager):
        """Should include invariants from config."""
        content = state_manager.regenerate(slots=[], jobs=[])

        assert "## Configuration Invariants" in content
        assert "Keep X constant" in content
        assert "Never change Y" in content

    def test_regenerate_includes_data_sources_count(self, state_manager):
        """Should include data sources count when > 0."""
        content = state_manager.regenerate(slots=[], jobs=[], source_count=5)

        assert "## Data Sources" in content
        assert "5 sources" in content

    def test_regenerate_skips_sources_when_zero(self, state_manager):
        """Should skip data sources section when count is 0."""
        content = state_manager.regenerate(slots=[], jobs=[], source_count=0)

        assert "## Data Sources" not in content

    def test_regenerate_includes_recent_actions(self, state_manager):
        """Should include recent actions in reverse chronological order."""
        state_manager._recent_actions.append("[10:00] First")
        state_manager._recent_actions.append("[10:05] Second")
        state_manager._recent_actions.append("[10:10] Third")

        content = state_manager.regenerate(slots=[], jobs=[])

        assert "## Recent Actions" in content
        # Third should appear before First (reverse order)
        third_pos = content.find("[10:10] Third")
        first_pos = content.find("[10:00] First")
        assert third_pos < first_pos

    def test_regenerate_includes_active_jobs(self, state_manager):
        """Should include active (pending/running) jobs."""
        jobs = [
            {"id": "job-a1b2c3d4", "script": "train.py", "status": "running"},
            {"id": "job-b2c3d4e5", "script": "eval.py", "status": "pending"},
            {"id": "job-c3d4e5f6", "script": "old.py", "status": "completed"},  # Should be excluded
        ]
        content = state_manager.regenerate(slots=[], jobs=jobs)

        assert "## Background Jobs" in content
        assert "job-a1b2c3d4" in content
        assert "train.py" in content
        assert "job-b2c3d4e5" in content
        assert "eval.py" in content
        assert "job-c3d4e5f6" not in content  # Completed job excluded

    def test_regenerate_shows_no_active_jobs_message(self, state_manager):
        """Should show message when no active jobs."""
        content = state_manager.regenerate(slots=[], jobs=[])

        assert "No active jobs" in content

    def test_regenerate_writes_to_file(self, state_manager):
        """Should write content to STATE.md file."""
        state_manager.regenerate(slots=[], jobs=[])

        assert state_manager.state_path.exists()
        content = state_manager.state_path.read_text()
        assert "# test-project" in content


class TestWriteContent:
    """Tests for _write_content() method (atomic writes)."""

    def test_write_creates_parent_directories(self, temp_dir, basic_config):
        """Should create parent directories if needed."""
        state_path = temp_dir / "subdir" / "STATE.md"
        manager = StateManager(state_path, basic_config)

        manager._write_content("test content")

        assert state_path.exists()
        assert state_path.read_text() == "test content"

    def test_write_is_atomic(self, temp_dir, basic_config):
        """Should use atomic write pattern."""
        state_path = temp_dir / "STATE.md"
        state_path.write_text("original content")

        manager = StateManager(state_path, basic_config)
        manager._write_content("new content")

        # Should have new content (not corrupted)
        assert state_path.read_text() == "new content"


class TestCreateInitial:
    """Tests for create_initial() class method."""

    def test_creates_state_manager(self, temp_dir, basic_config):
        """Should return a StateManager instance."""
        state_path = temp_dir / "STATE.md"

        manager = StateManager.create_initial(state_path, basic_config)

        assert isinstance(manager, StateManager)
        assert manager.state_path == state_path

    def test_creates_initial_file(self, temp_dir, basic_config):
        """Should create the STATE.md file."""
        state_path = temp_dir / "STATE.md"

        StateManager.create_initial(state_path, basic_config)

        assert state_path.exists()

    def test_initial_file_has_project_name(self, temp_dir, basic_config):
        """Should include project name in initial file."""
        state_path = temp_dir / "STATE.md"

        StateManager.create_initial(state_path, basic_config)

        content = state_path.read_text()
        assert "# test-project" in content

    def test_initial_file_has_empty_slots(self, temp_dir, basic_config):
        """Should show all slots as empty initially."""
        state_path = temp_dir / "STATE.md"
        # Config has default slots: w1, w2, w3

        StateManager.create_initial(state_path, basic_config)

        content = state_path.read_text()
        assert "w1: [empty]" in content
        assert "w2: [empty]" in content
        assert "w3: [empty]" in content

    def test_initial_file_has_invariants(self, temp_dir, basic_config):
        """Should include configured invariants."""
        state_path = temp_dir / "STATE.md"

        StateManager.create_initial(state_path, basic_config)

        content = state_path.read_text()
        assert "Keep X constant" in content
        assert "Never change Y" in content

    def test_initial_file_has_initialized_action(self, temp_dir, basic_config):
        """Should record project initialized action."""
        state_path = temp_dir / "STATE.md"

        StateManager.create_initial(state_path, basic_config)

        content = state_path.read_text()
        assert "Project initialized" in content

    def test_initial_file_has_no_active_jobs(self, temp_dir, basic_config):
        """Should show no active jobs initially."""
        state_path = temp_dir / "STATE.md"

        StateManager.create_initial(state_path, basic_config)

        content = state_path.read_text()
        assert "No active jobs" in content
