"""Tests for get_source_lineage() tool - P1.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestGetSourceLineageTool:
    """Tests for get_source_lineage tool."""

    def test_get_source_lineage_tool_exists(self):
        """Server should have get_source_lineage tool."""
        from goldfish import server

        assert hasattr(server, "get_source_lineage")

    def test_returns_lineage_for_external_source(self, temp_dir):
        """get_source_lineage should return empty lineage for external source."""
        from goldfish import server
        from goldfish.models import SourceLineage

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
            get_lineage_fn = (
                server.get_source_lineage.fn if hasattr(server.get_source_lineage, "fn") else server.get_source_lineage
            )
            result = get_lineage_fn(source_name="external_data")

            assert isinstance(result, SourceLineage)
            assert result.source_name == "external_data"
            assert result.parent_sources == []
            assert result.job_id is None
        finally:
            server.reset_server()

    def test_returns_lineage_for_promoted_artifact(self, temp_dir):
        """get_source_lineage should return parent sources and job for promoted artifact."""
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
            get_lineage_fn = (
                server.get_source_lineage.fn if hasattr(server.get_source_lineage, "fn") else server.get_source_lineage
            )
            result = get_lineage_fn(source_name="promoted_v1")

            assert result.source_name == "promoted_v1"
            assert "raw_data" in result.parent_sources
            assert "config_data" in result.parent_sources
            assert result.job_id == "job-abc123"
        finally:
            server.reset_server()

    def test_raises_on_missing_source(self, temp_dir):
        """get_source_lineage should raise SourceNotFoundError for missing source."""
        from goldfish import server
        from goldfish.errors import SourceNotFoundError

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db.source_exists.return_value = False

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
            get_lineage_fn = (
                server.get_source_lineage.fn if hasattr(server.get_source_lineage, "fn") else server.get_source_lineage
            )
            with pytest.raises(SourceNotFoundError):
                get_lineage_fn(source_name="nonexistent_source")
        finally:
            server.reset_server()

    def test_validates_source_name(self, temp_dir):
        """get_source_lineage should validate source name."""
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
            get_lineage_fn = (
                server.get_source_lineage.fn if hasattr(server.get_source_lineage, "fn") else server.get_source_lineage
            )
            with pytest.raises(InvalidSourceNameError):
                get_lineage_fn(source_name="../invalid")
        finally:
            server.reset_server()
