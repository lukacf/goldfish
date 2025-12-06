"""Tests for rollback() tool - P2.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestRollbackTool:
    """Tests for rollback tool."""

    def test_rollback_tool_exists(self):
        """Server should have rollback tool."""
        from goldfish import server

        assert hasattr(server, "rollback")

    def test_rollback_reverts_to_snapshot(self, temp_dir):
        """rollback should revert slot to specified snapshot."""
        from goldfish import server
        from goldfish.models import RollbackResponse

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]
        mock_config.audit.min_reason_length = 15

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.rollback.return_value = RollbackResponse(
            success=True,
            slot="w1",
            snapshot_id="snap-abc1234-20240101-120000",
            files_reverted=3,
            state_md="# State",
        )

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
            rollback_fn = server.rollback.fn if hasattr(server.rollback, 'fn') else server.rollback
            result = rollback_fn(
                slot="w1",
                snapshot_id="snap-abc1234-20240101-120000",
                reason="Reverting to known good state",
            )

            assert result.success is True
            assert result.snapshot_id == "snap-abc1234-20240101-120000"
            mock_workspace_manager.rollback.assert_called_once()
        finally:
            server.reset_server()

    def test_rollback_validates_slot(self, temp_dir):
        """rollback should validate slot name."""
        from goldfish import server
        from goldfish.validation import InvalidSlotNameError

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]
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
            rollback_fn = server.rollback.fn if hasattr(server.rollback, 'fn') else server.rollback
            with pytest.raises(InvalidSlotNameError):
                rollback_fn(
                    slot="invalid",
                    snapshot_id="snap-abc1234-20240101-120000",
                    reason="Testing validation works",
                )
        finally:
            server.reset_server()

    def test_rollback_validates_reason(self, temp_dir):
        """rollback should validate reason length."""
        from goldfish import server
        from goldfish.errors import GoldfishError

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]
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
            rollback_fn = server.rollback.fn if hasattr(server.rollback, 'fn') else server.rollback
            with pytest.raises(GoldfishError):
                rollback_fn(
                    slot="w1",
                    snapshot_id="snap-abc1234-20240101-120000",
                    reason="short",  # Too short
                )
        finally:
            server.reset_server()


class TestWorkspaceManagerRollback:
    """Tests for workspace manager rollback implementation."""

    def test_workspace_manager_has_rollback_method(self):
        """WorkspaceManager should have rollback method."""
        from goldfish.workspace.manager import WorkspaceManager

        assert hasattr(WorkspaceManager, "rollback")


class TestGitLayerRollback:
    """Tests for git layer rollback support."""

    def test_git_layer_has_checkout_snapshot(self):
        """GitLayer should have checkout_snapshot method."""
        from goldfish.workspace.git_layer import GitLayer

        assert hasattr(GitLayer, "checkout_snapshot")
