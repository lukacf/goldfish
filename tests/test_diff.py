"""Tests for diff() tool - P2.

TDD: Write failing tests first, then implement.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest


class TestDiffTool:
    """Tests for diff tool."""

    def test_diff_tool_exists(self):
        """Server should have diff tool."""
        from goldfish import server

        assert hasattr(server, "diff")

    def test_diff_returns_changes(self, temp_dir):
        """diff should return changes since last checkpoint."""
        from goldfish import server
        from goldfish.models import DiffResponse

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.diff.return_value = DiffResponse(
            slot="w1",
            has_changes=True,
            summary="2 files changed, 10 insertions(+), 3 deletions(-)",
            files_changed=["code/train.py", "scripts/preprocess.py"],
            diff_text="diff --git a/code/train.py...",
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
            diff_fn = server.diff.fn if hasattr(server.diff, 'fn') else server.diff
            result = diff_fn(slot="w1")

            assert result.has_changes is True
            assert len(result.files_changed) == 2
            mock_workspace_manager.diff.assert_called_once_with("w1")
        finally:
            server.reset_server()

    def test_diff_returns_no_changes(self, temp_dir):
        """diff should indicate when no changes."""
        from goldfish import server
        from goldfish.models import DiffResponse

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.diff.return_value = DiffResponse(
            slot="w1",
            has_changes=False,
            summary="No changes since last checkpoint",
            files_changed=[],
            diff_text="",
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
            diff_fn = server.diff.fn if hasattr(server.diff, 'fn') else server.diff
            result = diff_fn(slot="w1")

            assert result.has_changes is False
            assert len(result.files_changed) == 0
        finally:
            server.reset_server()

    def test_diff_validates_slot(self, temp_dir):
        """diff should validate slot name."""
        from goldfish import server
        from goldfish.validation import InvalidSlotNameError

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]

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
            diff_fn = server.diff.fn if hasattr(server.diff, 'fn') else server.diff
            with pytest.raises(InvalidSlotNameError):
                diff_fn(slot="invalid-slot")
        finally:
            server.reset_server()


class TestWorkspaceManagerDiff:
    """Tests for workspace manager diff implementation."""

    def test_workspace_manager_has_diff_method(self):
        """WorkspaceManager should have diff method."""
        from goldfish.workspace.manager import WorkspaceManager

        assert hasattr(WorkspaceManager, "diff")
