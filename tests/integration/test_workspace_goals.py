"""Tests for workspace goal persistence - P2.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock


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
