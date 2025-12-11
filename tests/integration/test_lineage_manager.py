"""Tests for LineageManager - TDD Phase 5."""

from unittest.mock import MagicMock

import pytest

from goldfish.errors import GoldfishError
from goldfish.lineage.manager import LineageManager


class TestWorkspaceLineage:
    """Test workspace lineage tracking."""

    def test_get_workspace_lineage_basic(self, test_db):
        """Should return workspace lineage with versions."""
        # Setup
        test_db.create_workspace_lineage("test_ws", description="Test workspace")
        test_db.create_version("test_ws", "v1", "tag-v1", "sha1", "run", description="First run")
        test_db.create_version("test_ws", "v2", "tag-v2", "sha2", "run", description="Second run")

        workspace_manager = MagicMock()
        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        lineage = lineage_mgr.get_workspace_lineage("test_ws")

        # Verify
        assert lineage["name"] == "test_ws"
        assert lineage["parent"] is None
        assert len(lineage["versions"]) == 2
        assert lineage["versions"][0]["version"] == "v1"
        assert lineage["versions"][1]["version"] == "v2"

    def test_get_workspace_lineage_with_parent(self, test_db):
        """Should show parent workspace relationship."""
        # Setup
        test_db.create_workspace_lineage("parent_ws", description="Parent")
        test_db.create_version("parent_ws", "v1", "tag-v1", "sha1", "run")
        test_db.create_version("parent_ws", "v2", "tag-v2", "sha2", "run")

        test_db.create_workspace_lineage(
            "child_ws", parent_workspace="parent_ws", parent_version="v2", description="Branched from parent"
        )
        test_db.create_version("child_ws", "v1", "child-v1", "sha3", "run")

        workspace_manager = MagicMock()
        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        lineage = lineage_mgr.get_workspace_lineage("child_ws")

        # Verify
        assert lineage["name"] == "child_ws"
        assert lineage["parent"] == "parent_ws"
        assert lineage["parent_version"] == "v2"
        assert len(lineage["versions"]) == 1

    def test_get_workspace_lineage_with_branches(self, test_db):
        """Should show child workspaces branched from this one."""
        # Setup
        test_db.create_workspace_lineage("main_ws", description="Main")
        test_db.create_version("main_ws", "v1", "tag-v1", "sha1", "run")

        test_db.create_workspace_lineage(
            "branch1", parent_workspace="main_ws", parent_version="v1", description="First branch"
        )
        test_db.create_workspace_lineage(
            "branch2", parent_workspace="main_ws", parent_version="v1", description="Second branch"
        )

        workspace_manager = MagicMock()
        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        lineage = lineage_mgr.get_workspace_lineage("main_ws")

        # Verify
        assert len(lineage["branches"]) == 2
        branch_names = [b["workspace"] for b in lineage["branches"]]
        assert "branch1" in branch_names
        assert "branch2" in branch_names


class TestVersionComparison:
    """Test comparing versions."""

    def test_get_version_diff_basic(self, test_db):
        """Should return diff between two versions."""
        # Setup
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-v1", "sha1", "run")
        test_db.create_version("test_ws", "v2", "tag-v2", "sha2", "run")

        workspace_manager = MagicMock()
        workspace_manager.git.diff_commits.return_value = {
            "commits": [{"sha": "sha2", "message": "Update config"}],
            "files": {"configs/preprocess.yaml": "M"},
        }

        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        diff = lineage_mgr.get_version_diff("test_ws", "v1", "v2")

        # Verify
        assert diff["from_version"] == "v1"
        assert diff["to_version"] == "v2"
        assert len(diff["commits"]) == 1
        assert diff["commits"][0]["sha"] == "sha2"
        workspace_manager.git.diff_commits.assert_called_once_with("sha1", "sha2")


class TestRunProvenance:
    """Test tracking exact provenance of runs."""

    def test_get_run_provenance_complete(self, test_db):
        """Should return complete provenance for a stage run."""
        # Setup
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-v1", "sha123", "run")

        # Create previous stage run (that produces input signal)
        prev_stage_run_id = "stage-prev"
        test_db.create_stage_run(
            stage_run_id=prev_stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="preprocess",
            config={},
        )

        # Add input signal (from previous stage)
        test_db.add_signal(
            stage_run_id=prev_stage_run_id,
            signal_name="features",
            signal_type="npy",
            storage_location="gs://bucket/features",
        )

        # Create stage run we're testing
        stage_run_id = "stage-abc123"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="tokenize",
            config={"VOCAB_SIZE": "20000"},
        )

        # Mark signal as consumed by our stage
        test_db.mark_signal_consumed(prev_stage_run_id, "features", stage_run_id)

        # Add output signal
        test_db.add_signal(
            stage_run_id=stage_run_id, signal_name="tokens", signal_type="npy", storage_location="gs://bucket/tokens"
        )

        workspace_manager = MagicMock()
        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        provenance = lineage_mgr.get_run_provenance(stage_run_id)

        # Verify
        assert provenance["stage_run_id"] == stage_run_id
        assert provenance["workspace"] == "test_ws"
        assert provenance["version"] == "v1"
        assert provenance["git_sha"] == "sha123"
        assert provenance["stage"] == "tokenize"
        assert provenance["config_override"] == {"VOCAB_SIZE": "20000"}
        assert len(provenance["inputs"]) == 1
        assert provenance["inputs"][0]["signal_name"] == "features"
        assert len(provenance["outputs"]) == 1
        assert provenance["outputs"][0]["signal_name"] == "tokens"


class TestBranchWorkspace:
    """Test branching workspaces from specific versions."""

    def test_branch_workspace_creates_branch(self, test_db):
        """Should create new workspace branched from specific version."""
        # Setup
        test_db.create_workspace_lineage("main_ws", description="Main")
        test_db.create_version("main_ws", "v1", "tag-v1", "sha1", "run")
        test_db.create_version("main_ws", "v2", "tag-v2", "sha2", "run")

        workspace_manager = MagicMock()
        workspace_manager.branch_workspace.return_value = None

        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute
        result = lineage_mgr.branch_workspace(
            from_workspace="main_ws", from_version="v2", new_workspace="experiment_ws", reason="Testing new approach"
        )

        # Verify git branch was created
        workspace_manager.branch_workspace.assert_called_once_with("main_ws", "v2", "experiment_ws")

        # Verify lineage was recorded
        lineage = lineage_mgr.get_workspace_lineage("experiment_ws")
        assert lineage["name"] == "experiment_ws"
        assert lineage["parent"] == "main_ws"
        assert lineage["parent_version"] == "v2"

    def test_branch_workspace_raises_on_missing_version(self, test_db):
        """Should raise error if version doesn't exist."""
        # Setup
        test_db.create_workspace_lineage("main_ws", description="Main")
        test_db.create_version("main_ws", "v1", "tag-v1", "sha1", "run")

        workspace_manager = MagicMock()
        lineage_mgr = LineageManager(db=test_db, workspace_manager=workspace_manager)

        # Execute - should raise
        with pytest.raises(GoldfishError, match="Version.*not found"):
            lineage_mgr.branch_workspace(
                from_workspace="main_ws", from_version="v999", new_workspace="experiment_ws", reason="Test"
            )
