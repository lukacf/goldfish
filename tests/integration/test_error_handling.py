"""Tests for error handling - P0.

TDD: Write failing tests first, then implement.
"""

import pytest

from goldfish.errors import GoldfishError


class TestConfigYAMLErrors:
    """Tests for config.py YAML parsing error handling."""

    def test_handles_malformed_yaml(self, temp_dir):
        """Should wrap YAML parsing errors in GoldfishError."""
        from goldfish.config import GoldfishConfig

        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("invalid: yaml: [unclosed")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        # Should NOT leak internal details
        assert (
            "yaml" in str(exc_info.value).lower()
            or "parse" in str(exc_info.value).lower()
            or "config" in str(exc_info.value).lower()
        )
        # Should NOT contain raw exception class names
        assert "ScannerError" not in str(exc_info.value)

    def test_handles_missing_config_file(self, temp_dir):
        """Should provide clear error for missing config."""
        from goldfish.config import GoldfishConfig
        from goldfish.errors import ProjectNotInitializedError

        # This already has proper handling - just verify it works
        with pytest.raises(ProjectNotInitializedError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "goldfish.yaml" in str(exc_info.value).lower() or "init" in str(exc_info.value).lower()

    def test_handles_invalid_config_structure(self, temp_dir):
        """Should provide clear error for wrong structure."""
        from goldfish.config import GoldfishConfig

        config_path = temp_dir / "goldfish.yaml"
        # Valid YAML but missing required fields
        config_path.write_text("random_field: true")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        # Should mention what's wrong
        assert (
            "config" in str(exc_info.value).lower()
            or "invalid" in str(exc_info.value).lower()
            or "required" in str(exc_info.value).lower()
        )


class TestExporterYAMLErrors:
    """Tests for exporter.py YAML parsing error handling."""

    def test_handles_malformed_base_config(self, temp_dir):
        """Should handle malformed base_config.yaml gracefully."""
        from goldfish.jobs.exporter import SnapshotExporter

        experiments_dir = temp_dir / "experiments"
        experiments_dir.mkdir()

        workspace_path = temp_dir / "workspace"
        workspace_path.mkdir()
        (workspace_path / "code").mkdir()

        # Create malformed base_config.yaml
        bad_config = workspace_path / "base_config.yaml"
        bad_config.write_text("invalid: yaml: [unclosed")

        exporter = SnapshotExporter(experiments_dir)

        with pytest.raises(GoldfishError) as exc_info:
            exporter.export(
                workspace_path=workspace_path,
                workspace_name="test-ws",
                snapshot_id="snap-abc1234-20251205-120000",
                script="scripts/train.py",
                reason="Testing error handling",
            )

        assert "config" in str(exc_info.value).lower() or "yaml" in str(exc_info.value).lower()


class TestLauncherExceptionHandling:
    """Tests for job launcher exception handling."""

    def test_preserves_error_context_on_checkpoint_failure(self, temp_dir):
        """Should preserve error details when checkpoint fails."""
        from unittest.mock import MagicMock

        from goldfish.errors import SlotEmptyError
        from goldfish.jobs.launcher import JobLauncher

        mock_db = MagicMock()
        mock_config = MagicMock()
        mock_config.experiments_dir = str(temp_dir / "experiments")
        mock_config.jobs.infra_path = None
        mock_config.gcs = None

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.checkpoint.side_effect = SlotEmptyError("w1 is empty")

        launcher = JobLauncher(
            db=mock_db,
            config=mock_config,
            project_root=temp_dir,
            workspace_manager=mock_workspace_manager,
        )

        with pytest.raises(GoldfishError) as exc_info:
            launcher.run_job(
                slot="w1",
                script="scripts/train.py",
                reason="Testing error context",
            )

        # Error should mention that checkpoint failed
        error_msg = str(exc_info.value).lower()
        assert "checkpoint" in error_msg or "empty" in error_msg

    def test_catches_export_errors(self, temp_dir):
        """Should catch and re-raise export errors with context."""
        from unittest.mock import MagicMock

        from goldfish.jobs.launcher import JobLauncher
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        mock_db = MagicMock()
        mock_config = MagicMock()
        mock_config.jobs.experiments_dir = "experiments"
        mock_config.jobs.infra_path = None
        mock_config.gcs = None

        # Create workspace dir with code dir but bad base_config.yaml
        workspace_path = temp_dir / "workspaces" / "w1"
        workspace_path.mkdir(parents=True)
        (workspace_path / "code").mkdir()  # Export needs this
        # Create bad base_config.yaml
        bad_config = workspace_path / "base_config.yaml"
        bad_config.write_text("invalid: yaml: [unclosed")

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1",
            state=SlotState.MOUNTED,
            workspace="test-ws",
        )
        mock_workspace_manager.get_slot_path.return_value = workspace_path
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True,
            slot="w1",
            snapshot_id="snap-abc1234-20251205-120000",
            message="test checkpoint",
            state_md="# State",
        )

        launcher = JobLauncher(
            db=mock_db,
            config=mock_config,
            project_root=temp_dir,
            workspace_manager=mock_workspace_manager,
        )

        # The export should fail due to bad YAML, and launcher should catch and wrap it
        with pytest.raises(GoldfishError) as exc_info:
            launcher.run_job(
                slot="w1",
                script="scripts/train.py",
                reason="Testing error context",
            )

        # Error should be a GoldfishError (wrapped)
        assert isinstance(exc_info.value, GoldfishError)
        assert "yaml" in str(exc_info.value).lower() or "config" in str(exc_info.value).lower()
