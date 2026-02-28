"""Integration tests for pre-run review in stage execution.

Tests the full integration of the PreRunReviewer with:
- Real file system workspace structures
- Database records for blocked runs
- Stage executor integration
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import PreRunReviewConfig
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import (
    PipelineDef,
    ReviewIssue,
    ReviewSeverity,
    RunReason,
    RunReview,
    StageDef,
)
from goldfish.pre_run_review import PreRunReviewer
from goldfish.state_machine.types import StageState
from goldfish.svs.agent import ReviewRequest, ReviewResult
from goldfish.svs.config import SVSConfig

if TYPE_CHECKING:
    from goldfish.config import GoldfishConfig
    from goldfish.db.database import Database


class TestPreRunReviewerWithRealFiles:
    """Test PreRunReviewer with real workspace file structure."""

    def test_reads_real_pipeline_yaml(self, tmp_path: Path) -> None:
        """Should read actual pipeline.yaml from workspace."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        # Create real pipeline.yaml
        pipeline_yaml = workspace / "pipeline.yaml"
        pipeline_yaml.write_text(
            """
stages:
  - name: preprocess
    inputs:
      raw: {type: dataset, dataset: sales_v1}
    outputs:
      features: {type: npy}
"""
        )

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        content = reviewer._read_pipeline_yaml()
        assert "preprocess" in content
        assert "sales_v1" in content

    def test_reads_real_stage_module(self, tmp_path: Path) -> None:
        """Should read actual stage module from workspace."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        # Create real module directory and file
        modules = workspace / "modules"
        modules.mkdir()
        train_module = modules / "train.py"
        train_module.write_text(
            """
import torch
from goldfish.io import load_input, save_output

def run():
    features = load_input("features")
    model = train_model(features)
    save_output("model", model)
"""
        )

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        sections = reviewer._build_stage_sections(["train"])
        assert "import torch" in sections
        assert "train_model" in sections

    def test_reads_real_stage_config(self, tmp_path: Path) -> None:
        """Should read actual stage config from workspace."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        # Create real module and config
        modules = workspace / "modules"
        modules.mkdir()
        (modules / "train.py").write_text("# train module")

        configs = workspace / "configs"
        configs.mkdir()
        config_file = configs / "train.yaml"
        config_file.write_text(
            """
learning_rate: 0.001
batch_size: 32
epochs: 100
"""
        )

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        sections = reviewer._build_stage_sections(["train"])
        assert "learning_rate: 0.001" in sections
        assert "batch_size: 32" in sections

    def test_handles_missing_files_gracefully(self, tmp_path: Path) -> None:
        """Should handle missing files without crashing."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        # Should not crash, just use defaults
        pipeline = reviewer._read_pipeline_yaml()
        assert "No pipeline.yaml" in pipeline

        sections = reviewer._build_stage_sections(["nonexistent"])
        assert "not found" in sections.lower()


class TestPreRunReviewWithRunReason:
    """Test pre-run review with RunReason structured data."""

    @pytest.mark.asyncio
    async def test_review_with_full_run_reason(self, tmp_path: Path) -> None:
        """Should include RunReason in review context."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        # Create minimal workspace
        (workspace / "pipeline.yaml").write_text("stages: []")
        modules = workspace / "modules"
        modules.mkdir()
        (modules / "train.py").write_text("# train")

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        reason = RunReason(
            description="Testing larger batch sizes for stability",
            hypothesis="Increasing batch size should improve training stability",
            approach="Modified batch_size from 32 to 64, added gradient clipping",
            min_result="Lower loss variance during training",
        )

        captured_requests: list[ReviewRequest] = []

        def mock_run(request: ReviewRequest) -> ReviewResult:
            captured_requests.append(request)
            return ReviewResult(
                decision="approved",
                findings=[],
                response_text="## train\nNo issues found.",
                duration_ms=0,
            )

        mock_agent = MagicMock()
        mock_agent.run = mock_run

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            await reviewer.review(["train"], reason=reason)

        # Verify reason was included in prompt
        assert len(captured_requests) == 1
        prompt = captured_requests[0].context["prompt"]
        assert "Increasing batch size should improve training stability" in prompt
        assert "batch_size from 32 to 64" in prompt
        assert "Lower loss variance" in prompt


class TestStageExecutorReviewIntegration:
    """Test pre-run review integration with StageExecutor."""

    def test_blocked_run_creates_database_record(
        self, test_db: Database, test_config: GoldfishConfig, temp_dir: Path
    ) -> None:
        """Blocked runs should create stage_run record with FAILED status."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Enable pre-run review in config
        test_config.pre_run_review = PreRunReviewConfig(enabled=True, timeout_seconds=30)

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Mock review to return errors
        mock_review = RunReview(
            approved=False,
            summary="Review blocked: 1 error(s)",
            full_review="## preprocess\nERROR: train.py:10 - undefined variable",
            reviewed_stages=["preprocess"],
            issues=[
                ReviewIssue(
                    severity=ReviewSeverity.ERROR,
                    stage="preprocess",
                    file="train.py",
                    line=10,
                    message="undefined variable",
                )
            ],
        )
        executor._perform_pre_run_review = MagicMock(return_value=mock_review)
        executor._build_docker_image = MagicMock(return_value=("test-image", "0" * 64))
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify run was blocked
        assert result.status == StageState.FAILED

        # Verify database record was created
        stage_run = test_db.get_stage_run(result.stage_run_id)
        assert stage_run is not None
        assert stage_run["state"] == "failed"
        assert "review" in (stage_run["error"] or "").lower()

    def test_approved_run_proceeds_normally(
        self, test_db: Database, test_config: GoldfishConfig, temp_dir: Path
    ) -> None:
        """Approved runs should proceed with normal execution."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Enable pre-run review
        test_config.pre_run_review = PreRunReviewConfig(enabled=True, timeout_seconds=30)

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Mock review to return success
        mock_review = RunReview(
            approved=True,
            summary="Review passed: no issues found",
            full_review="## preprocess\nNo issues found.",
            reviewed_stages=["preprocess"],
            issues=[],
        )
        executor._perform_pre_run_review = MagicMock(return_value=mock_review)
        executor._build_docker_image = MagicMock(return_value=("test-image", "0" * 64))
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify run proceeded
        assert result.status == StageState.RUNNING
        executor._build_docker_image.assert_called_once()
        executor._launch_container.assert_called_once()

    def test_disabled_review_skips_check(self, test_db: Database, test_config: GoldfishConfig, temp_dir: Path) -> None:
        """Disabled review should skip the check entirely."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Disable pre-run review
        test_config.pre_run_review = PreRunReviewConfig(enabled=False)

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Mock methods
        executor._perform_pre_run_review = MagicMock()
        executor._build_docker_image = MagicMock(return_value=("test-image", "0" * 64))
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify review was not performed
        executor._perform_pre_run_review.assert_not_called()
        assert result.status == StageState.RUNNING

    def test_review_with_warnings_does_not_block(
        self, test_db: Database, test_config: GoldfishConfig, temp_dir: Path
    ) -> None:
        """Runs with only warnings should proceed (only errors block)."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Enable pre-run review
        test_config.pre_run_review = PreRunReviewConfig(enabled=True, timeout_seconds=30)

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Mock review to return warnings only
        mock_review = RunReview(
            approved=True,
            summary="Review passed with 1 warning(s)",
            full_review="## preprocess\nWARNING: Consider adding type hints",
            reviewed_stages=["preprocess"],
            issues=[
                ReviewIssue(
                    severity=ReviewSeverity.WARNING,
                    stage="preprocess",
                    message="Consider adding type hints",
                )
            ],
        )
        executor._perform_pre_run_review = MagicMock(return_value=mock_review)
        executor._build_docker_image = MagicMock(return_value=("test-image", "0" * 64))
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify run proceeded despite warnings
        assert result.status == StageState.RUNNING


class TestReviewTimeoutHandling:
    """Test timeout handling in pre-run review."""

    @pytest.mark.asyncio
    async def test_review_timeout_approves_and_continues(self, tmp_path: Path) -> None:
        """review_before_run timeouts should approve the run to avoid blocking."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        (workspace / "pipeline.yaml").write_text("stages: []")

        # Set a short integer timeout
        config = PreRunReviewConfig(enabled=True, timeout_seconds=1)
        svs_config = SVSConfig(agent_provider="anthropic_api")
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        # Mock agent to hang longer than timeout_seconds
        def slow_run(request: ReviewRequest) -> ReviewResult:
            import time

            time.sleep(2)  # Much more than timeout_seconds=1
            return ReviewResult(decision="approved", findings=[], response_text="OK", duration_ms=0)

        mock_agent = MagicMock()
        mock_agent.run = slow_run

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            result = await reviewer.review(["train"])

        # Should approve due to timeout
        assert result.approved is True
        assert "timed out" in result.summary.lower()

    @pytest.mark.asyncio
    async def test_review_exception_approves_and_continues(self, tmp_path: Path) -> None:
        """Exceptions should approve the run to avoid blocking."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        (workspace / "pipeline.yaml").write_text("stages: []")

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        mock_agent = MagicMock()
        mock_agent.run = MagicMock(side_effect=RuntimeError("API connection failed"))

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            result = await reviewer.review(["train"])

        # Should approve due to error
        assert result.approved is True
        assert "failed" in result.summary.lower()


class TestReviewDiffContext:
    """Test diff context in pre-run review."""

    @pytest.mark.asyncio
    async def test_review_includes_diff_text(self, tmp_path: Path) -> None:
        """Should include git diff in review context."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        (workspace / "pipeline.yaml").write_text("stages: []")
        modules = workspace / "modules"
        modules.mkdir()
        (modules / "train.py").write_text("# code")

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        diff_text = """\
diff --git a/modules/train.py b/modules/train.py
--- a/modules/train.py
+++ b/modules/train.py
@@ -1,3 +1,5 @@
 def train():
-    lr = 0.001
+    lr = 0.01  # Increased learning rate
     return model
"""

        captured_requests: list[ReviewRequest] = []

        def mock_run(request: ReviewRequest) -> ReviewResult:
            captured_requests.append(request)
            return ReviewResult(decision="approved", findings=[], response_text="OK", duration_ms=0)

        mock_agent = MagicMock()
        mock_agent.run = mock_run

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            await reviewer.review(["train"], diff_text=diff_text)

        assert len(captured_requests) == 1
        prompt = captured_requests[0].context["prompt"]
        assert "lr = 0.01" in prompt
        assert "Increased learning rate" in prompt

    @pytest.mark.asyncio
    async def test_review_handles_empty_diff(self, tmp_path: Path) -> None:
        """Should handle empty diff gracefully."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        (workspace / "pipeline.yaml").write_text("stages: []")
        modules = workspace / "modules"
        modules.mkdir()
        (modules / "train.py").write_text("# code")

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        captured_requests: list[ReviewRequest] = []

        def mock_run(request: ReviewRequest) -> ReviewResult:
            captured_requests.append(request)
            return ReviewResult(decision="approved", findings=[], response_text="OK", duration_ms=0)

        mock_agent = MagicMock()
        mock_agent.run = mock_run

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            await reviewer.review(["train"], diff_text="")

        assert len(captured_requests) == 1
        prompt = captured_requests[0].context["prompt"]
        assert "first run" in prompt.lower() or "unavailable" in prompt.lower()


class TestReviewMultipleStages:
    """Test review of multiple stages."""

    @pytest.mark.asyncio
    async def test_review_multiple_stages(self, tmp_path: Path) -> None:
        """Should review multiple stages in single call."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        dev_repo = tmp_path / "dev"
        dev_repo.mkdir()

        (workspace / "pipeline.yaml").write_text("stages: []")
        modules = workspace / "modules"
        modules.mkdir()
        (modules / "preprocess.py").write_text("# preprocess code")
        (modules / "train.py").write_text("# train code")
        (modules / "evaluate.py").write_text("# evaluate code")

        config = PreRunReviewConfig(enabled=True, timeout_seconds=30)
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace,
            dev_repo_path=dev_repo,
        )

        captured_requests: list[ReviewRequest] = []

        def mock_run(request: ReviewRequest) -> ReviewResult:
            captured_requests.append(request)
            return ReviewResult(decision="approved", findings=[], response_text="OK", duration_ms=0)

        mock_agent = MagicMock()
        mock_agent.run = mock_run

        with patch.object(reviewer, "_get_agent", return_value=mock_agent):
            await reviewer.review(["preprocess", "train"])

        assert len(captured_requests) == 1
        prompt = captured_requests[0].context["prompt"]
        assert "preprocess" in prompt
        assert "train" in prompt
