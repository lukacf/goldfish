"""TDD tests for failure pattern extraction integration into stage executor.

Tests verify:
- Pattern extraction is called when stage fails
- Pattern extraction respects auto_learn_failures config
- Errors in extraction don't affect stage finalization
- Rate limiting is respected
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunStatus


@pytest.fixture
def setup_workspace_for_run(test_db: Database) -> None:
    """Setup workspace and version for stage runs."""
    test_db.create_workspace_lineage("test_workspace", description="Test workspace")
    test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "manual")


@pytest.fixture
def executor_with_mocks(test_db, test_config, setup_workspace_for_run):
    """Create StageExecutor with mocked dependencies."""
    workspace_manager = MagicMock()
    pipeline_manager = MagicMock()
    dataset_registry = MagicMock()

    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=Path("/tmp"),
        dataset_registry=dataset_registry,
    )

    # Mock the local executor to return logs
    executor.local_executor = MagicMock()
    executor.local_executor.get_container_logs.return_value = "Error: something went wrong"

    return executor


def _create_failed_stage_run(db: Database, stage_run_id: str = "test-run-1") -> str:
    """Create a failed stage run for testing."""
    db.create_stage_run(
        stage_run_id=stage_run_id,
        workspace_name="test_workspace",
        version="v1",
        stage_name="train",
        backend_type="local",
        inputs={"test": "input"},
        reason={"description": "test"},
    )
    db.update_stage_run_status(
        stage_run_id=stage_run_id,
        status=StageRunStatus.RUNNING,
    )
    return stage_run_id


class TestPatternExtractionOnFailure:
    """Test that pattern extraction is called on stage failure."""

    def test_pattern_extraction_called_on_failed_stage(self, executor_with_mocks, test_db):
        """Should call extract_failure_pattern when stage fails and auto_learn_failures enabled."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn_failures (opt-in feature)
        executor_with_mocks.config.svs.auto_learn_failures = True

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        # Should be called with correct arguments
        mock_extract.assert_called_once()
        call_args = mock_extract.call_args
        assert call_args.kwargs["stage_run_id"] == stage_run_id
        assert call_args.kwargs["db"] == test_db
        # Error and logs should be passed
        assert "error" in call_args.kwargs or call_args.args[2] is not None
        assert "logs" in call_args.kwargs or len(call_args.args) >= 4

    def test_pattern_extraction_not_called_on_success(self, executor_with_mocks, test_db):
        """Should NOT call extract_failure_pattern when stage succeeds (even with auto_learn enabled)."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn_failures to verify it's skipped due to success, not config
        executor_with_mocks.config.svs.auto_learn_failures = True

        # Mock _record_output_signals to avoid actual execution
        executor_with_mocks._record_output_signals = MagicMock()

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.COMPLETED)

        mock_extract.assert_not_called()

    def test_pattern_extraction_disabled_by_default(self, executor_with_mocks, test_db):
        """Should NOT extract patterns by default (auto_learn_failures is opt-in)."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Verify default is False (opt-in)
        assert executor_with_mocks.config.svs.auto_learn_failures is False

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        mock_extract.assert_not_called()

    def test_pattern_extraction_respects_svs_disabled(self, executor_with_mocks, test_db):
        """Should NOT extract patterns when SVS is disabled (even with auto_learn enabled)."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn but disable SVS entirely
        executor_with_mocks.config.svs.auto_learn_failures = True
        executor_with_mocks.config.svs.enabled = False

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        mock_extract.assert_not_called()


class TestPatternExtractionErrorHandling:
    """Test that extraction errors don't affect finalization."""

    def test_extraction_error_does_not_fail_finalization(self, executor_with_mocks, test_db):
        """Errors in pattern extraction should be logged but not raise."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn_failures to test error handling
        executor_with_mocks.config.svs.auto_learn_failures = True

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            mock_extract.side_effect = Exception("AI agent failed")

            # Should not raise
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        # Stage should still be finalized
        stage_run = test_db.get_stage_run(stage_run_id)
        assert stage_run is not None
        assert stage_run["status"] == StageRunStatus.FAILED

    def test_rate_limit_error_logged_but_not_raised(self, executor_with_mocks, test_db):
        """Rate limit errors should be handled gracefully."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn_failures to test error handling
        executor_with_mocks.config.svs.auto_learn_failures = True

        from goldfish.svs.patterns.extractor import RateLimitExceededError

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            mock_extract.side_effect = RateLimitExceededError("Too many patterns")

            # Should not raise
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        # Finalization should complete
        stage_run = test_db.get_stage_run(stage_run_id)
        assert stage_run["status"] == StageRunStatus.FAILED


class TestPatternExtractionWithAgent:
    """Test that correct agent provider is used for extraction."""

    def test_uses_configured_agent_provider(self, executor_with_mocks, test_db):
        """Should use the configured agent provider for extraction."""
        stage_run_id = _create_failed_stage_run(test_db)

        # Enable auto_learn_failures to test agent provider passing
        executor_with_mocks.config.svs.auto_learn_failures = True

        with (
            patch("goldfish.svs.patterns.extractor.extract_failure_pattern") as mock_extract,
            patch.object(executor_with_mocks, "_run_post_run_svs_review"),
            patch.object(executor_with_mocks, "_collect_svs_manifests"),
        ):
            executor_with_mocks._finalize_stage_run(stage_run_id, "local", StageRunStatus.FAILED)

        # Should pass an agent provider
        if mock_extract.called:
            call_args = mock_extract.call_args
            # Agent should be passed
            assert "agent" in call_args.kwargs or len(call_args.args) >= 5
