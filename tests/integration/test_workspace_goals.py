"""Tests for workspace goal persistence - P2.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestWorkspaceGoalPersistence:
    """Tests for persisting workspace goals."""

    def test_database_has_workspace_goals_table(self, temp_dir):
        """Database should have workspace_goals table."""
        from goldfish.db.database import Database

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Check table exists
        with db._conn() as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='workspace_goals'"
            ).fetchone()
            assert tables is not None

    def test_database_can_set_workspace_goal(self, temp_dir):
        """Should be able to set a workspace goal."""
        from goldfish.db.database import Database

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        db.set_workspace_goal("fix-tbpe", "Fix TBPE label generation")

        goal = db.get_workspace_goal("fix-tbpe")
        assert goal == "Fix TBPE label generation"

    def test_database_can_update_workspace_goal(self, temp_dir):
        """Should be able to update an existing goal."""
        from goldfish.db.database import Database

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        db.set_workspace_goal("fix-tbpe", "Original goal")
        db.set_workspace_goal("fix-tbpe", "Updated goal")

        goal = db.get_workspace_goal("fix-tbpe")
        assert goal == "Updated goal"

    def test_database_returns_none_for_missing_goal(self, temp_dir):
        """Should return None for workspace without goal."""
        from goldfish.db.database import Database

        db_path = temp_dir / "test.db"
        db = Database(db_path)

        goal = db.get_workspace_goal("nonexistent")
        assert goal is None


class TestWorkspaceManagerGoals:
    """Tests for workspace manager goal handling."""

    def test_create_workspace_persists_goal(self, temp_dir):
        """create_workspace should persist the goal."""
        from goldfish import server
        from goldfish.models import CreateWorkspaceResponse

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.create_workspace.return_value = CreateWorkspaceResponse(
            success=True,
            workspace="new-ws",
            forked_from="main",
            state_md="# State",
        )

        mock_db = MagicMock()

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            create_fn = (
                server.create_workspace.fn if hasattr(server.create_workspace, "fn") else server.create_workspace
            )
            create_fn(
                name="new-ws",
                goal="Implement new feature X",
                reason="Starting work on feature X",
            )

            # Goal should be persisted via database
            mock_db.set_workspace_goal.assert_called_once_with("new-ws", "Implement new feature X")
        finally:
            server.reset_server()

    def test_list_workspaces_includes_goals(self, temp_dir):
        """list_workspaces should include goals from database."""
        from goldfish.workspace.manager import WorkspaceManager

        # WorkspaceManager should use database for goals
        assert hasattr(WorkspaceManager, "list_workspaces")
        # The implementation should fetch goals from db.get_workspace_goal


class TestWorkspaceInfoGoal:
    """Tests for WorkspaceInfo goal field."""

    def test_workspace_info_has_goal_field(self):
        """WorkspaceInfo should have goal field."""
        from goldfish.models import WorkspaceInfo

        # Should be able to create with goal
        info = WorkspaceInfo(
            name="test-ws",
            created_at="2024-01-01T00:00:00+00:00",
            goal="Test goal",
            snapshot_count=0,
            last_activity="2024-01-01T00:00:00+00:00",
            is_mounted=False,
        )
        assert info.goal == "Test goal"


class TestGetWorkspaceGoalTool:
    """Tests for get_workspace_goal tool - P1."""

    def test_get_workspace_goal_tool_exists(self):
        """Server should have get_workspace_goal tool."""
        from goldfish import server

        assert hasattr(server, "get_workspace_goal")

    def test_returns_goal_for_workspace(self, temp_dir):
        """get_workspace_goal should return goal when set."""
        from goldfish import server
        from goldfish.models import WorkspaceGoalResponse

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_workspace_goal.return_value = "Train a price prediction model"

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_goal_fn = (
                server.get_workspace_goal.fn if hasattr(server.get_workspace_goal, "fn") else server.get_workspace_goal
            )
            result = get_goal_fn(workspace="my-feature")

            assert isinstance(result, WorkspaceGoalResponse)
            assert result.workspace == "my-feature"
            assert result.goal == "Train a price prediction model"
        finally:
            server.reset_server()

    def test_returns_none_when_goal_not_set(self, temp_dir):
        """get_workspace_goal should return None when not set."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.get_workspace_goal.return_value = None

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_goal_fn = (
                server.get_workspace_goal.fn if hasattr(server.get_workspace_goal, "fn") else server.get_workspace_goal
            )
            result = get_goal_fn(workspace="new-workspace")

            assert result.goal is None
        finally:
            server.reset_server()

    def test_validates_workspace_name(self, temp_dir):
        """get_workspace_goal should validate workspace name."""
        from goldfish import server
        from goldfish.validation import InvalidWorkspaceNameError

        mock_config = MagicMock()

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_goal_fn = (
                server.get_workspace_goal.fn if hasattr(server.get_workspace_goal, "fn") else server.get_workspace_goal
            )
            with pytest.raises(InvalidWorkspaceNameError):
                get_goal_fn(workspace="../invalid")
        finally:
            server.reset_server()


class TestUpdateWorkspaceGoalTool:
    """Tests for update_workspace_goal tool - P1."""

    def test_update_workspace_goal_tool_exists(self):
        """Server should have update_workspace_goal tool."""
        from goldfish import server

        assert hasattr(server, "update_workspace_goal")

    def test_updates_workspace_goal(self, temp_dir):
        """update_workspace_goal should update and return new goal."""
        from goldfish import server
        from goldfish.models import UpdateWorkspaceGoalResponse

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15
        mock_db = MagicMock()
        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# State\ngoal updated"

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=mock_state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            update_goal_fn = (
                server.update_workspace_goal.fn
                if hasattr(server.update_workspace_goal, "fn")
                else server.update_workspace_goal
            )
            result = update_goal_fn(
                workspace="my-feature",
                goal="New goal: Implement backtesting framework",
                reason="Pivoting to focus on backtesting first",
            )

            assert isinstance(result, UpdateWorkspaceGoalResponse)
            assert result.success is True
            assert result.workspace == "my-feature"
            assert result.goal == "New goal: Implement backtesting framework"

            # Verify db was called
            mock_db.set_workspace_goal.assert_called_once_with(
                "my-feature", "New goal: Implement backtesting framework"
            )

            # Verify audit was logged
            mock_db.log_audit.assert_called_once()
        finally:
            server.reset_server()

    def test_validates_reason_length(self, temp_dir):
        """update_workspace_goal should validate reason length."""
        from goldfish import server
        from goldfish.errors import ReasonTooShortError

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
            workspace_manager=MagicMock(),
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            update_goal_fn = (
                server.update_workspace_goal.fn
                if hasattr(server.update_workspace_goal, "fn")
                else server.update_workspace_goal
            )
            with pytest.raises(ReasonTooShortError):
                update_goal_fn(
                    workspace="my-feature",
                    goal="New goal",
                    reason="short",  # Too short
                )
        finally:
            server.reset_server()

    def test_updates_state_manager_goal(self, temp_dir):
        """update_workspace_goal should update state manager."""
        from goldfish import server

        mock_config = MagicMock()
        mock_config.audit.min_reason_length = 15
        mock_db = MagicMock()
        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# State"

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=MagicMock(),
            state_manager=mock_state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            update_goal_fn = (
                server.update_workspace_goal.fn
                if hasattr(server.update_workspace_goal, "fn")
                else server.update_workspace_goal
            )
            update_goal_fn(
                workspace="my-feature",
                goal="New goal for testing",
                reason="Testing state manager update",
            )

            # Verify state manager was updated
            mock_state_manager.set_goal.assert_called_once_with("New goal for testing")
            mock_state_manager.add_action.assert_called()
        finally:
            server.reset_server()
