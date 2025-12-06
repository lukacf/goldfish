"""Tests for list_snapshots() tool - P1.

TDD: Write failing tests first, then implement.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


class TestListSnapshotsTool:
    """Tests for list_snapshots tool."""

    def test_list_snapshots_tool_exists(self):
        """Server should have list_snapshots tool."""
        from goldfish import server

        assert hasattr(server, "list_snapshots")

    def test_returns_snapshots_for_workspace(self, temp_dir):
        """list_snapshots should return snapshot list."""
        from goldfish import server
        from goldfish.models import ListSnapshotsResponse

        mock_config = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.list_snapshots.return_value = [
            {
                "snapshot_id": "snap-abc1234-20251205-120000",
                "created_at": datetime(2025, 12, 5, 12, 0, 0, tzinfo=timezone.utc),
                "message": "First checkpoint",
            },
            {
                "snapshot_id": "snap-def5678-20251205-130000",
                "created_at": datetime(2025, 12, 5, 13, 0, 0, tzinfo=timezone.utc),
                "message": "Second checkpoint",
            },
        ]

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
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
            list_snapshots_fn = server.list_snapshots.fn if hasattr(server.list_snapshots, 'fn') else server.list_snapshots
            result = list_snapshots_fn(workspace="my-feature")

            assert isinstance(result, ListSnapshotsResponse)
            assert result.workspace == "my-feature"
            assert result.total_count == 2
            assert len(result.snapshots) == 2
            assert result.snapshots[0].snapshot_id == "snap-abc1234-20251205-120000"
            assert result.snapshots[0].message == "First checkpoint"
        finally:
            server.reset_server()

    def test_returns_empty_for_workspace_without_snapshots(self, temp_dir):
        """list_snapshots should return empty list for workspace with no snapshots."""
        from goldfish import server

        mock_config = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.list_snapshots.return_value = []

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
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
            list_snapshots_fn = server.list_snapshots.fn if hasattr(server.list_snapshots, 'fn') else server.list_snapshots
            result = list_snapshots_fn(workspace="empty-workspace")

            assert result.total_count == 0
            assert len(result.snapshots) == 0
        finally:
            server.reset_server()

    def test_validates_workspace_name(self, temp_dir):
        """list_snapshots should validate workspace name."""
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
            list_snapshots_fn = server.list_snapshots.fn if hasattr(server.list_snapshots, 'fn') else server.list_snapshots
            with pytest.raises(InvalidWorkspaceNameError):
                list_snapshots_fn(workspace="../invalid")
        finally:
            server.reset_server()

    def test_filters_snapshots_without_dates(self, temp_dir):
        """list_snapshots should filter out snapshots without valid dates."""
        from goldfish import server

        mock_config = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.list_snapshots.return_value = [
            {
                "snapshot_id": "snap-abc1234-20251205-120000",
                "created_at": datetime(2025, 12, 5, 12, 0, 0, tzinfo=timezone.utc),
                "message": "Valid checkpoint",
            },
            {
                "snapshot_id": "snap-invalid-00000000-000000",
                "created_at": None,  # No date
                "message": "Invalid checkpoint",
            },
        ]

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=MagicMock(),
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
            list_snapshots_fn = server.list_snapshots.fn if hasattr(server.list_snapshots, 'fn') else server.list_snapshots
            result = list_snapshots_fn(workspace="test-ws")

            # Should only include the valid one
            assert result.total_count == 1
            assert result.snapshots[0].snapshot_id == "snap-abc1234-20251205-120000"
        finally:
            server.reset_server()


class TestWorkspaceManagerListSnapshots:
    """Tests for workspace manager list_snapshots method."""

    def test_workspace_manager_has_list_snapshots_method(self):
        """WorkspaceManager should have list_snapshots method."""
        from goldfish.workspace.manager import WorkspaceManager

        assert hasattr(WorkspaceManager, "list_snapshots")


class TestGitLayerListSnapshots:
    """Tests for git layer list_snapshots method."""

    def test_git_layer_has_list_snapshots_method(self):
        """GitLayer should have list_snapshots method."""
        from goldfish.workspace.git_layer import GitLayer

        assert hasattr(GitLayer, "list_snapshots")

    def test_git_layer_has_get_snapshot_info_method(self):
        """GitLayer should have get_snapshot_info method."""
        from goldfish.workspace.git_layer import GitLayer

        assert hasattr(GitLayer, "get_snapshot_info")
"""Tests for list_snapshots pagination - P1.

TDD: Write failing tests first, then implement.
"""

import pytest


class TestListSnapshotsPagination:
    """Tests for list_snapshots pagination - P1."""

    def test_list_snapshots_with_limit(self, temp_dir, temp_git_repo):
        """Test list_snapshots respects limit parameter."""
        from goldfish.config import GoldfishConfig, JobsConfig, StateMdConfig, AuditConfig
        from goldfish.db.database import Database
        from goldfish.workspace.manager import WorkspaceManager

        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        manager = WorkspaceManager(config=config, project_root=project_root, db=db)

        # Create workspace and mount it
        manager.create_workspace(
            "test-ws",
            goal="Test workspace",
            reason="Testing pagination in snapshots",
        )
        manager.mount("test-ws", "w1", "Testing pagination in snapshots")

        # Create 7 snapshots (with file changes to make them unique)
        import time
        workspace_path = project_root / "workspaces" / "w1"
        (workspace_path / "code").mkdir(parents=True, exist_ok=True)
        for i in range(7):
            # Make a change to the workspace
            (workspace_path / "code" / f"file{i}.py").write_text(f"# File {i}")
            manager.checkpoint("w1", f"Checkpoint {i} for testing pagination")
            time.sleep(1.1)  # Ensure unique timestamps (seconds granularity)

        # Request only 3
        snapshots = manager.list_snapshots("test-ws", limit=3)
        assert len(snapshots) == 3

    def test_list_snapshots_with_offset(self, temp_dir, temp_git_repo):
        """Test list_snapshots respects offset parameter."""
        from goldfish.config import GoldfishConfig, JobsConfig, StateMdConfig, AuditConfig
        from goldfish.db.database import Database
        from goldfish.workspace.manager import WorkspaceManager

        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        manager = WorkspaceManager(config=config, project_root=project_root, db=db)

        # Create workspace and mount it
        manager.create_workspace(
            "test-ws",
            goal="Test workspace",
            reason="Testing pagination in snapshots",
        )
        manager.mount("test-ws", "w1", "Testing pagination in snapshots")

        # Create 7 snapshots (with file changes to make them unique)
        import time
        workspace_path = project_root / "workspaces" / "w1"
        (workspace_path / "code").mkdir(parents=True, exist_ok=True)
        for i in range(7):
            # Make a change to the workspace
            (workspace_path / "code" / f"file{i}.py").write_text(f"# File {i}")
            manager.checkpoint("w1", f"Checkpoint {i} for testing pagination")
            time.sleep(1.1)  # Ensure unique timestamps (seconds granularity)

        # Get first 3
        first_page = manager.list_snapshots("test-ws", limit=3, offset=0)
        # Get next 3
        second_page = manager.list_snapshots("test-ws", limit=3, offset=3)

        assert len(first_page) == 3
        assert len(second_page) == 3
        # Should be different snapshots
        first_ids = {s["snapshot_id"] for s in first_page}
        second_ids = {s["snapshot_id"] for s in second_page}
        assert first_ids.isdisjoint(second_ids)

    def test_list_snapshots_validates_bounds(self, temp_dir, temp_git_repo):
        """Test that limit and offset are validated."""
        from goldfish.config import GoldfishConfig, JobsConfig, StateMdConfig, AuditConfig
        from goldfish.db.database import Database
        from goldfish.workspace.manager import WorkspaceManager
        from goldfish.errors import GoldfishError

        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        manager = WorkspaceManager(config=config, project_root=project_root, db=db)

        # Create a workspace
        manager.create_workspace(
            "test-ws",
            goal="Test workspace",
            reason="Testing bounds validation",
        )

        # limit < 1 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_snapshots("test-ws", limit=0)
        assert "limit" in str(exc_info.value).lower()

        # limit > 200 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_snapshots("test-ws", limit=201)
        assert "limit" in str(exc_info.value).lower()

        # offset < 0 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_snapshots("test-ws", offset=-1)
        assert "offset" in str(exc_info.value).lower()
