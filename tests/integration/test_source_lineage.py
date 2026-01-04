"""Tests for get_source_lineage() tool - P1.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestGetSourceLineageTool:
    """Tests for manage_sources(action='lineage') tool."""

    def test_manage_sources_lineage_action_exists(self):
        """Server should have manage_sources tool."""
        from goldfish import server

        assert hasattr(server, "manage_sources")

    def test_returns_lineage_for_external_source(self, temp_dir):
        """manage_sources should return empty lineage for external source."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True
        mock_db.get_lineage.return_value = []  # External source has no parents

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
            manage_sources_fn = (
                server.manage_sources.fn if hasattr(server.manage_sources, "fn") else server.manage_sources
            )
            result = manage_sources_fn(action="lineage", name="external_data")

            assert result["source_name"] == "external_data"
            assert result["parents"] == []
            assert result["job_id"] is None
        finally:
            server.reset_server()

    def test_returns_lineage_for_promoted_artifact(self, temp_dir):
        """manage_sources should return parent sources and job for promoted artifact."""
        from goldfish import server

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True
        mock_db.get_lineage.return_value = [
            {
                "source_id": "promoted_v1",
                "parent_source_id": "raw_data",
                "job_id": "job-abc123",
            },
            {
                "source_id": "promoted_v1",
                "parent_source_id": "config_data",
                "job_id": "job-abc123",
            },
        ]

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
            manage_sources_fn = (
                server.manage_sources.fn if hasattr(server.manage_sources, "fn") else server.manage_sources
            )
            result = manage_sources_fn(action="lineage", name="promoted_v1")

            assert result["source_name"] == "promoted_v1"
            assert "raw_data" in result["parents"]
            assert "config_data" in result["parents"]
            assert result["job_id"] == "job-abc123"
        finally:
            server.reset_server()

    def test_raises_on_missing_source_name(self, temp_dir):
        """manage_sources should raise error if name missing for lineage action."""
        from goldfish import server
        from goldfish.errors import GoldfishError

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
            manage_sources_fn = (
                server.manage_sources.fn if hasattr(server.manage_sources, "fn") else server.manage_sources
            )
            with pytest.raises(GoldfishError):
                manage_sources_fn(action="lineage")  # Missing name
        finally:
            server.reset_server()

    def test_validates_source_name(self, temp_dir):
        """manage_sources should validate source name."""
        from goldfish import server
        from goldfish.validation import InvalidSourceNameError

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
            manage_sources_fn = (
                server.manage_sources.fn if hasattr(server.manage_sources, "fn") else server.manage_sources
            )
            with pytest.raises(InvalidSourceNameError):
                manage_sources_fn(action="lineage", name="../invalid")
        finally:
            server.reset_server()
