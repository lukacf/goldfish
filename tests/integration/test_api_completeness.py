"""Tests for API completeness - pagination and filtering - P1.

TDD: Write failing tests first, then implement.
"""

import pytest


class TestListSourcesPagination:
    """Tests for list_sources pagination - P1."""

    def test_list_sources_with_limit(self, temp_dir):
        """Test list_sources respects limit parameter."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create 10 sources
        for i in range(10):
            db.create_source(
                source_id=f"source-{i:02d}",
                name=f"Source {i}",
                gcs_location=f"gs://bucket/source-{i}",
                created_by="external",
            )

        # Request only 5
        sources = db.list_sources(limit=5)
        assert len(sources) == 5

    def test_list_sources_with_offset(self, temp_dir):
        """Test list_sources respects offset parameter."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create 10 sources with predictable names
        for i in range(10):
            db.create_source(
                source_id=f"source-{i:02d}",
                name=f"Source {i:02d}",
                gcs_location=f"gs://bucket/source-{i}",
                created_by="external",
            )

        # Get first 5
        first_page = db.list_sources(limit=5, offset=0)
        # Get next 5
        second_page = db.list_sources(limit=5, offset=5)

        assert len(first_page) == 5
        assert len(second_page) == 5
        # Should be different sources
        first_ids = {s["id"] for s in first_page}
        second_ids = {s["id"] for s in second_page}
        assert first_ids.isdisjoint(second_ids)

    def test_list_sources_pagination_with_total_count(self, temp_dir):
        """Test that count_sources returns correct total."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create 150 sources
        for i in range(150):
            db.create_source(
                source_id=f"source-{i:03d}",
                name=f"Source {i}",
                gcs_location=f"gs://bucket/source-{i}",
                created_by="external" if i % 2 == 0 else "internal",
            )

        # Count all sources
        total = db.count_sources()
        assert total == 150

        # Count with filter
        external_count = db.count_sources(created_by="external")
        assert external_count == 75

    def test_list_sources_validates_limit_bounds(self, temp_dir):
        """Test that limit parameter is validated."""
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError

        db = Database(temp_dir / "test.db")

        # limit < 1 should raise
        with pytest.raises(GoldfishError) as exc_info:
            db.list_sources(limit=0)
        assert "limit" in str(exc_info.value).lower()

        # limit > 200 should raise
        with pytest.raises(GoldfishError) as exc_info:
            db.list_sources(limit=201)
        assert "limit" in str(exc_info.value).lower()

    def test_list_sources_validates_offset_bounds(self, temp_dir):
        """Test that offset parameter is validated."""
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError

        db = Database(temp_dir / "test.db")

        # offset < 0 should raise
        with pytest.raises(GoldfishError) as exc_info:
            db.list_sources(offset=-1)
        assert "offset" in str(exc_info.value).lower()


class TestListSourcesFiltering:
    """Tests for list_sources filtering - P1."""

    def test_list_sources_filter_by_status(self, temp_dir):
        """Test filtering sources by status."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create sources with different statuses
        for i in range(5):
            db.create_source(
                source_id=f"available-{i}",
                name=f"Available {i}",
                gcs_location=f"gs://bucket/avail-{i}",
                created_by="external",
                status="available",
            )
        for i in range(3):
            db.create_source(
                source_id=f"processing-{i}",
                name=f"Processing {i}",
                gcs_location=f"gs://bucket/proc-{i}",
                created_by="external",
                status="processing",
            )

        # Filter by available
        available = db.list_sources(status="available")
        assert len(available) == 5
        assert all(s["status"] == "available" for s in available)

        # Filter by processing
        processing = db.list_sources(status="processing")
        assert len(processing) == 3
        assert all(s["status"] == "processing" for s in processing)

    def test_list_sources_filter_by_created_by(self, temp_dir):
        """Test filtering sources by created_by."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create sources with different creators
        for i in range(7):
            db.create_source(
                source_id=f"external-{i}",
                name=f"External {i}",
                gcs_location=f"gs://bucket/ext-{i}",
                created_by="external",
            )
        for i in range(4):
            db.create_source(
                source_id=f"internal-{i}",
                name=f"Internal {i}",
                gcs_location=f"gs://bucket/int-{i}",
                created_by="internal",
            )

        # Filter by external
        external = db.list_sources(created_by="external")
        assert len(external) == 7
        assert all(s["created_by"] == "external" for s in external)

        # Filter by internal
        internal = db.list_sources(created_by="internal")
        assert len(internal) == 4
        assert all(s["created_by"] == "internal" for s in internal)

    def test_list_sources_combined_filters_and_pagination(self, temp_dir):
        """Test combining filters with pagination."""
        from goldfish.db.database import Database

        db = Database(temp_dir / "test.db")

        # Create 20 external available sources
        for i in range(20):
            db.create_source(
                source_id=f"ext-avail-{i:02d}",
                name=f"Ext Available {i}",
                gcs_location=f"gs://bucket/ext-avail-{i}",
                created_by="external",
                status="available",
            )

        # Create 10 internal processing sources
        for i in range(10):
            db.create_source(
                source_id=f"int-proc-{i:02d}",
                name=f"Int Processing {i}",
                gcs_location=f"gs://bucket/int-proc-{i}",
                created_by="internal",
                status="processing",
            )

        # Get first 10 external available sources
        page1 = db.list_sources(status="available", created_by="external", limit=10, offset=0)
        assert len(page1) == 10
        assert all(s["status"] == "available" for s in page1)
        assert all(s["created_by"] == "external" for s in page1)

        # Get next 10 external available sources
        page2 = db.list_sources(status="available", created_by="external", limit=10, offset=10)
        assert len(page2) == 10

        # Count should return 20 for this filter combination
        count = db.count_sources(status="available", created_by="external")
        assert count == 20


class TestListWorkspacesPagination:
    """Tests for list_workspaces pagination - P1."""

    def test_list_workspaces_with_limit(self, temp_dir, temp_git_repo):
        """Test list_workspaces respects limit parameter."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
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

        # Create 10 workspaces
        for i in range(10):
            manager.create_workspace(
                f"workspace-{i:02d}",
                goal=f"Goal {i}",
                reason="Testing pagination in list workspaces",
            )

        # Request only 5
        workspaces = manager.list_workspaces(limit=5)
        assert len(workspaces) == 5

    def test_list_workspaces_with_offset(self, temp_dir, temp_git_repo):
        """Test list_workspaces respects offset parameter."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
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

        # Create 10 workspaces
        for i in range(10):
            manager.create_workspace(
                f"workspace-{i:02d}",
                goal=f"Goal {i}",
                reason="Testing pagination in list workspaces",
            )

        # Get first 5
        first_page = manager.list_workspaces(limit=5, offset=0)
        # Get next 5
        second_page = manager.list_workspaces(limit=5, offset=5)

        assert len(first_page) == 5
        assert len(second_page) == 5
        # Should be different workspaces
        first_names = {w.name for w in first_page}
        second_names = {w.name for w in second_page}
        assert first_names.isdisjoint(second_names)

    def test_list_workspaces_validates_bounds(self, temp_dir, temp_git_repo):
        """Test that limit and offset are validated."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError
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

        # limit < 1 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_workspaces(limit=0)
        assert "limit" in str(exc_info.value).lower()

        # limit > 200 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_workspaces(limit=201)
        assert "limit" in str(exc_info.value).lower()

        # offset < 0 should raise
        with pytest.raises(GoldfishError) as exc_info:
            manager.list_workspaces(offset=-1)
        assert "offset" in str(exc_info.value).lower()


class TestGetSnapshot:
    """Tests for get_snapshot() tool - P1."""

    def test_get_snapshot_valid(self, temp_dir, temp_git_repo):
        """Test retrieving a valid snapshot."""
        from unittest.mock import MagicMock

        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.models import SnapshotInfo
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
            reason="Testing get_snapshot tool",
        )
        manager.mount("test-ws", "w1", "Testing get_snapshot tool")

        # Create a checkpoint
        workspace_path = project_root / "workspaces" / "w1"
        (workspace_path / "code").mkdir(parents=True, exist_ok=True)
        (workspace_path / "code" / "test.py").write_text("# Test file")
        checkpoint_result = manager.checkpoint("w1", "Test checkpoint for get_snapshot")
        snapshot_id = checkpoint_result.snapshot_id

        # Configure server
        server.configure_server(
            project_root=project_root,
            config=config,
            db=db,
            workspace_manager=manager,
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_snapshot_fn = server.get_snapshot.fn if hasattr(server.get_snapshot, "fn") else server.get_snapshot
            result = get_snapshot_fn(workspace="test-ws", snapshot_id=snapshot_id)

            assert isinstance(result, SnapshotInfo)
            assert result.snapshot_id == snapshot_id
            assert result.message == "Test checkpoint for get_snapshot"
            assert result.created_at is not None
        finally:
            server.reset_server()

    def test_get_snapshot_not_found(self, temp_dir, temp_git_repo):
        """Test error on non-existent snapshot."""
        from unittest.mock import MagicMock

        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError
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

        # Create workspace (no snapshots)
        manager.create_workspace(
            "test-ws",
            goal="Test workspace",
            reason="Testing get_snapshot error handling",
        )

        # Configure server
        server.configure_server(
            project_root=project_root,
            config=config,
            db=db,
            workspace_manager=manager,
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_snapshot_fn = server.get_snapshot.fn if hasattr(server.get_snapshot, "fn") else server.get_snapshot
            with pytest.raises(GoldfishError) as exc_info:
                # Use valid format but non-existent snapshot (7 hex chars)
                get_snapshot_fn(workspace="test-ws", snapshot_id="snap-abcdef7-20251205-120000")

            assert "not found" in str(exc_info.value).lower() or "does not exist" in str(exc_info.value).lower()
        finally:
            server.reset_server()

    def test_get_snapshot_wrong_workspace(self, temp_dir, temp_git_repo):
        """Test error when snapshot not in specified workspace."""
        from unittest.mock import MagicMock

        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.errors import GoldfishError
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

        # Create workspace1 and add a snapshot
        manager.create_workspace(
            "workspace1",
            goal="Workspace 1",
            reason="Testing get_snapshot cross-workspace error",
        )
        manager.mount("workspace1", "w1", "Testing get_snapshot cross-workspace error")
        workspace_path = project_root / "workspaces" / "w1"
        (workspace_path / "code").mkdir(parents=True, exist_ok=True)
        (workspace_path / "code" / "test.py").write_text("# Test file")
        checkpoint_result = manager.checkpoint("w1", "Checkpoint in workspace1")
        snapshot_id = checkpoint_result.snapshot_id
        manager.hibernate("w1", "Done with workspace1")

        # Create workspace2 (different workspace)
        manager.create_workspace(
            "workspace2",
            goal="Workspace 2",
            reason="Testing get_snapshot cross-workspace error",
        )

        # Configure server
        server.configure_server(
            project_root=project_root,
            config=config,
            db=db,
            workspace_manager=manager,
            state_manager=MagicMock(),
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            get_snapshot_fn = server.get_snapshot.fn if hasattr(server.get_snapshot, "fn") else server.get_snapshot
            # Try to get workspace1's snapshot from workspace2
            with pytest.raises(GoldfishError) as exc_info:
                get_snapshot_fn(workspace="workspace2", snapshot_id=snapshot_id)

            assert "not found" in str(exc_info.value).lower() or "does not exist" in str(exc_info.value).lower()
        finally:
            server.reset_server()
