"""Tests for server dependency injection - P1.

TDD: Write failing tests first, then implement.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest


class TestServerDependencyInjection:
    """Tests for server dependency injection support."""

    def test_configure_server_accepts_dependencies(self, temp_dir):
        """Should be able to configure server with custom dependencies."""
        from goldfish import server

        # Should have configure_server function
        assert hasattr(server, "configure_server")

    def test_configure_server_sets_components(self, temp_dir):
        """configure_server should set all components via context."""
        from goldfish import server
        from goldfish.context import get_context, has_context

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_state_manager = MagicMock()
        mock_job_launcher = MagicMock()
        mock_job_tracker = MagicMock()
        mock_pipeline_manager = MagicMock()
        mock_dataset_registry = MagicMock()
        mock_stage_executor = MagicMock()
        mock_pipeline_executor = MagicMock()

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=mock_state_manager,
            job_launcher=mock_job_launcher,
            job_tracker=mock_job_tracker,
            pipeline_manager=mock_pipeline_manager,
            dataset_registry=mock_dataset_registry,
            stage_executor=mock_stage_executor,
            pipeline_executor=mock_pipeline_executor,
        )

        try:
            # Verify context is set
            assert has_context()
            ctx = get_context()
            assert ctx.config is mock_config
            assert ctx.db is mock_db
            assert ctx.workspace_manager is mock_workspace_manager
            assert ctx.state_manager is mock_state_manager
            assert ctx.job_launcher is mock_job_launcher
            assert ctx.job_tracker is mock_job_tracker
            assert ctx.stage_executor is mock_stage_executor
            assert ctx.pipeline_executor is mock_pipeline_executor
        finally:
            server.reset_server()

    def test_reset_server_clears_state(self, temp_dir):
        """reset_server should clear all context state."""
        from goldfish import server
        from goldfish.context import has_context

        # Set some state via configure_server
        server.configure_server(
            project_root=temp_dir,
            config=MagicMock(),
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

        # Should have reset_server function
        assert hasattr(server, "reset_server")

        # Verify context is set before reset
        assert has_context()

        server.reset_server()

        # Context should be cleared
        assert not has_context()


class TestServerWithMocks:
    """Tests for server operations with mocked dependencies."""

    def test_status_uses_injected_components(self, temp_dir):
        """status() should use injected components via _get_state_md."""
        from goldfish import server

        # Create mocks
        mock_config = MagicMock()
        mock_config.project_name = "test-project"

        mock_db = MagicMock()
        mock_db.get_active_jobs.return_value = []
        mock_db.list_sources.return_value = []

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_all_slots.return_value = []

        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# Test State"

        # Configure server
        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=mock_state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            # Test via _get_state_md which is used by status()
            result = server._get_state_md()
            assert result == "# Test State"
            mock_workspace_manager.get_all_slots.assert_called_once()
        finally:
            server.reset_server()

    def test_workspace_manager_is_accessible(self, temp_dir):
        """Injected workspace_manager should be accessible via context."""
        from goldfish import server
        from goldfish.context import get_context

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.list_workspaces.return_value = []

        server.configure_server(
            project_root=temp_dir,
            config=MagicMock(),
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
            # Verify we can access the injected manager via context
            ctx = get_context()
            assert ctx.workspace_manager is mock_workspace_manager
            # Call a method on it
            ctx.workspace_manager.list_workspaces()
            mock_workspace_manager.list_workspaces.assert_called_once()
        finally:
            server.reset_server()
