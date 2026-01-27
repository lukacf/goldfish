"""Tests for pre-run review functionality."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from goldfish.config import PreRunReviewConfig
from goldfish.models import ReviewIssue, ReviewSeverity, RunReason, RunReview
from goldfish.pre_run_review import PreRunReviewer, review_before_run
from goldfish.svs.agent import ReviewRequest, ReviewResult
from goldfish.svs.config import SVSConfig


class MockAgentProvider:
    """Mock provider for testing that captures calls and returns configured results."""

    name = "mock"

    def __init__(self, result: ReviewResult | None = None, side_effect: Exception | None = None):
        self.result = result or ReviewResult(decision="approved", response_text="OK")
        self.side_effect = side_effect
        self.call_args: ReviewRequest | None = None

    def run(self, request: ReviewRequest) -> ReviewResult:
        self.call_args = request
        if self.side_effect:
            raise self.side_effect
        return self.result


class TestPreRunReviewerParseReview:
    """Tests for the _parse_review method."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        return PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=tmp_path,
            dev_repo_path=tmp_path / "dev",
        )

    def test_parse_simple_error(self, reviewer: PreRunReviewer) -> None:
        """Parse a simple ERROR line."""
        review_text = """## train
ERROR: train.py:42 - Missing import for numpy
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].severity == ReviewSeverity.ERROR
        assert issues[0].stage == "train"
        assert issues[0].file == "train.py"
        assert issues[0].line == 42
        assert issues[0].message == "Missing import for numpy"

    def test_parse_warning_without_line(self, reviewer: PreRunReviewer) -> None:
        """Parse WARNING without line number."""
        review_text = """## preprocess
WARNING: preprocess.py - Variable 'df' may be undefined
"""
        issues = reviewer._parse_review(review_text, ["preprocess"])
        assert len(issues) == 1
        assert issues[0].severity == ReviewSeverity.WARNING
        assert issues[0].stage == "preprocess"
        assert issues[0].file == "preprocess.py"
        assert issues[0].line is None
        assert issues[0].message == "Variable 'df' may be undefined"

    def test_parse_note_without_file(self, reviewer: PreRunReviewer) -> None:
        """Parse NOTE without file reference."""
        review_text = """## train
NOTE: Consider adding type hints
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].severity == ReviewSeverity.NOTE
        assert issues[0].stage == "train"
        assert issues[0].file is None
        assert issues[0].message == "Consider adding type hints"

    def test_parse_bold_markers(self, reviewer: PreRunReviewer) -> None:
        """Parse bold markdown markers like **ERROR:**."""
        review_text = """## train
**ERROR:** model.py:10 - Division by zero risk
**WARNING:** config.yaml - Learning rate very high
**NOTE:** Add validation tests
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 3
        assert issues[0].severity == ReviewSeverity.ERROR
        assert issues[1].severity == ReviewSeverity.WARNING
        assert issues[2].severity == ReviewSeverity.NOTE

    def test_parse_multiple_stages(self, reviewer: PreRunReviewer) -> None:
        """Parse review with multiple stages."""
        review_text = """## preprocess
ERROR: preprocess.py:5 - Syntax error

## train
WARNING: train.py - Inefficient loop
NOTE: Consider vectorization

## evaluate
No issues found.
"""
        issues = reviewer._parse_review(review_text, ["preprocess", "train", "evaluate"])
        assert len(issues) == 3
        # Check preprocess issue
        preprocess_issues = [i for i in issues if i.stage == "preprocess"]
        assert len(preprocess_issues) == 1
        assert preprocess_issues[0].severity == ReviewSeverity.ERROR

        # Check train issues
        train_issues = [i for i in issues if i.stage == "train"]
        assert len(train_issues) == 2

    def test_parse_ignores_unknown_stages(self, reviewer: PreRunReviewer) -> None:
        """Ignore issues from stages not in the review list."""
        review_text = """## unknown_stage
ERROR: file.py:1 - Some error

## train
WARNING: train.py - Some warning
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].stage == "train"

    def test_parse_empty_review(self, reviewer: PreRunReviewer) -> None:
        """Handle empty review text."""
        issues = reviewer._parse_review("", ["train"])
        assert len(issues) == 0

    def test_parse_no_issues_found(self, reviewer: PreRunReviewer) -> None:
        """Handle 'No issues found' message."""
        review_text = """## train
No issues found.

## preprocess
No issues found.
"""
        issues = reviewer._parse_review(review_text, ["train", "preprocess"])
        assert len(issues) == 0

    def test_parse_yaml_config_file(self, reviewer: PreRunReviewer) -> None:
        """Parse issues referencing YAML config files."""
        review_text = """## train
ERROR: train.yaml:3 - Invalid learning rate value
WARNING: config.yml - Missing required field
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 2
        assert issues[0].file == "train.yaml"
        assert issues[0].line == 3
        assert issues[1].file == "config.yml"


class TestRunReviewModel:
    """Tests for RunReview model."""

    def test_has_blocking_issues_true(self) -> None:
        """has_blocking_issues returns True when errors present."""
        review = RunReview(
            approved=False,
            issues=[
                ReviewIssue(severity=ReviewSeverity.ERROR, stage="train", message="Error"),
                ReviewIssue(severity=ReviewSeverity.WARNING, stage="train", message="Warning"),
            ],
            summary="Found issues",
            full_review="...",
            reviewed_stages=["train"],
        )
        assert review.has_blocking_issues is True

    def test_has_blocking_issues_false(self) -> None:
        """has_blocking_issues returns False when no errors."""
        review = RunReview(
            approved=True,
            issues=[
                ReviewIssue(severity=ReviewSeverity.WARNING, stage="train", message="Warning"),
                ReviewIssue(severity=ReviewSeverity.NOTE, stage="train", message="Note"),
            ],
            summary="Passed with warnings",
            full_review="...",
            reviewed_stages=["train"],
        )
        assert review.has_blocking_issues is False

    def test_has_blocking_issues_empty(self) -> None:
        """has_blocking_issues returns False when no issues."""
        review = RunReview(
            approved=True,
            summary="No issues",
            full_review="...",
            reviewed_stages=["train"],
        )
        assert review.has_blocking_issues is False


class TestPreRunReviewerReview:
    """Tests for the main review method."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance with temp workspace."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()

        # Create minimal pipeline.yaml
        (workspace_path / "pipeline.yaml").write_text(
            """stages:
  - name: train
    outputs:
      model: {type: directory}
"""
        )

        # Create modules dir and module
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text(
            """def run():
    pass
"""
        )

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        return PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

    @pytest.mark.asyncio
    async def test_review_fallback_to_null_provider(self, reviewer: PreRunReviewer) -> None:
        """Review falls back to NullProvider when no provider is available."""
        from goldfish.svs.agent import NullProvider, ReviewResult

        # Create a mock provider that returns a specific result
        mock_provider = NullProvider()
        mock_result = ReviewResult(
            decision="approved",
            findings=[],
            response_text="NullProvider: approved for pre_run review\n",
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.approved is True
            # NullProvider returns approved immediately
            assert "nullprovider" in review.full_review.lower()

    @pytest.mark.asyncio
    async def test_review_success_no_issues(self, reviewer: PreRunReviewer) -> None:
        """Successful review with no issues."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.approved is True
            assert len(review.issues) == 0
            assert "no issues" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_uses_review_result_response_text(self, reviewer: PreRunReviewer) -> None:
        """Review should use ReviewResult.response_text when provided."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.full_review.startswith("## train")

    @pytest.mark.asyncio
    async def test_review_with_errors_not_approved(self, reviewer: PreRunReviewer) -> None:
        """Review with errors is not approved."""
        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="blocked",
                response_text="## train\nERROR: train.py:10 - Missing import statement\nWARNING: train.py:20 - Unused variable",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.approved is False
            assert len(review.issues) == 2
            assert "error" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_with_warnings_approved(self, reviewer: PreRunReviewer) -> None:
        """Review with only warnings is approved."""
        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="warned",
                response_text="## train\nWARNING: train.py - Consider adding error handling\nNOTE: Could use type hints",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.approved is True
            assert len(review.issues) == 2
            assert "warning" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_includes_reason(self, reviewer: PreRunReviewer) -> None:
        """Review includes RunReason in context."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        reason = RunReason(
            description="Test run",
            hypothesis="Test hypothesis",
            approach="Test approach",
            min_result="Test min",
            goal="Test goal",
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            await reviewer.review(["train"], reason=reason)
            # Verify reason was included in the request context or prompt
            assert mock_provider.call_args is not None
            assert "Test hypothesis" in mock_provider.call_args.context["prompt"]

    @pytest.mark.asyncio
    async def test_review_includes_diff(self, reviewer: PreRunReviewer) -> None:
        """Review includes diff text."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        diff_text = "+def new_function():\n+    pass"
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            await reviewer.review(["train"], diff_text=diff_text)
            assert mock_provider.call_args is not None
            assert diff_text in mock_provider.call_args.context["prompt"]

    @pytest.mark.asyncio
    async def test_review_includes_input_resolution(self, reviewer: PreRunReviewer) -> None:
        """Review includes resolved input context."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        input_context = [
            {
                "input": "tokens",
                "source_type": "stage",
                "from_stage": "compute_state_features",
                "signal": "tokens",
                "selected_run_id": "stage-old",
                "selected_run_started_at": "2025-12-28T00:00:00+00:00",
                "latest_run_id": "stage-new",
                "latest_run_state": "running",  # State machine state (source of truth)
                "latest_run_started_at": "2025-12-29T00:00:00+00:00",
                "consumer_stage": "train",
            }
        ]
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            await reviewer.review(["train"], input_context=input_context)
            assert mock_provider.call_args is not None
            prompt = mock_provider.call_args.context["prompt"]
            assert "Input Resolution" in prompt
            assert "stage-old" in prompt
            assert "stage-new" in prompt

    @pytest.mark.asyncio
    async def test_review_blocks_stale_inputs(self, reviewer: PreRunReviewer) -> None:
        """Review blocks when newer upstream run is still running."""
        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="## train\nNo issues found."))
        input_context = [
            {
                "input": "tokens",
                "source_type": "stage",
                "from_stage": "compute_state_features",
                "signal": "tokens",
                "selected_run_id": "stage-old",
                "selected_run_started_at": "2025-12-28T00:00:00+00:00",
                "latest_run_id": "stage-new",
                "latest_run_state": "running",  # State machine state (source of truth)
                "latest_run_started_at": "2025-12-29T00:00:00+00:00",
                "consumer_stage": "train",
            }
        ]
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"], input_context=input_context)
            assert review.approved is False
            assert any(issue.severity == ReviewSeverity.ERROR for issue in review.issues)

    @pytest.mark.asyncio
    async def test_review_handles_exception(self, reviewer: PreRunReviewer) -> None:
        """Review handles Claude API exceptions gracefully."""
        mock_provider = MockAgentProvider(side_effect=RuntimeError("API error"))
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            # Should not block on failure
            assert review.approved is True
            assert "failed" in review.summary.lower()


class TestReviewBeforeRunFunction:
    """Tests for the convenience function."""

    @pytest.mark.asyncio
    async def test_convenience_function_creates_reviewer(self, tmp_path: Path) -> None:
        """review_before_run creates reviewer and calls it."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages: []")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        with patch.dict("os.environ", {}, clear=True):
            with patch("os.environ.get", return_value=None):
                review = await review_before_run(
                    config=config,
                    svs_config=svs_config,
                    workspace_path=workspace_path,
                    dev_repo_path=tmp_path / "dev",
                    stages=["train"],
                )
                assert review.approved is True  # Skipped due to no API key


class TestPreRunReviewerBuildStageSections:
    """Tests for _build_stage_sections method."""

    def test_build_stage_sections_with_module(self, tmp_path: Path) -> None:
        """Build stage sections includes module content."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("def run(): pass")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        sections = reviewer._build_stage_sections(["train"])
        assert "def run(): pass" in sections
        assert "train" in sections

    def test_build_stage_sections_with_config(self, tmp_path: Path) -> None:
        """Build stage sections includes config content."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("pass")
        (workspace_path / "configs").mkdir()
        (workspace_path / "configs" / "train.yaml").write_text("lr: 0.001")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        sections = reviewer._build_stage_sections(["train"])
        assert "lr: 0.001" in sections

    def test_build_stage_sections_missing_module(self, tmp_path: Path) -> None:
        """Build stage sections handles missing module."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        sections = reviewer._build_stage_sections(["missing"])
        assert "Module not found" in sections


class TestPreRunReviewerReadPipelineYaml:
    """Tests for _read_pipeline_yaml method."""

    def test_read_pipeline_yaml_exists(self, tmp_path: Path) -> None:
        """Read pipeline.yaml when it exists."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages:\n  - name: train")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_pipeline_yaml()
        assert "stages:" in content
        assert "train" in content

    def test_read_pipeline_yaml_missing(self, tmp_path: Path) -> None:
        """Handle missing pipeline.yaml."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_pipeline_yaml()
        assert "No pipeline.yaml found" in content


class TestSecurityFeatures:
    """Tests for security features."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir(exist_ok=True)
        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        return PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

    def test_symlink_blocked(self, tmp_path: Path, reviewer: PreRunReviewer) -> None:
        """Symlinks should be blocked for security."""
        workspace_path = reviewer.workspace_path
        (workspace_path / "modules").mkdir()
        (workspace_path / "configs").mkdir()

        # Create a file inside workspace
        target = workspace_path / "configs" / "secret.yaml"
        target.write_text("secret data")

        # Create a symlink within workspace (could still be dangerous)
        symlink = workspace_path / "modules" / "train.py"
        symlink.symlink_to(target)

        content = reviewer._read_file_safe(symlink, "default")
        assert "Symlink detected" in content
        assert "secret data" not in content

    def test_path_traversal_blocked(self, tmp_path: Path, reviewer: PreRunReviewer) -> None:
        """Path traversal attempts should be blocked."""
        workspace_path = reviewer.workspace_path

        # Create a file outside workspace
        outside = tmp_path / "outside.txt"
        outside.write_text("outside content")

        # Try to read outside workspace
        traversal_path = workspace_path / ".." / "outside.txt"
        content = reviewer._read_file_safe(traversal_path, "default")
        assert content == "default"
        assert "outside content" not in content

    def test_large_file_blocked(self, reviewer: PreRunReviewer) -> None:
        """Large files should be blocked to prevent DoS."""
        workspace_path = reviewer.workspace_path
        (workspace_path / "modules").mkdir(exist_ok=True)

        # Create a file larger than MAX_FILE_SIZE (100KB)
        large_file = workspace_path / "modules" / "train.py"
        large_file.write_text("x" * 150_000)  # 150KB

        content = reviewer._read_file_safe(large_file, "default")
        assert "too large" in content.lower()

    def test_unsafe_stage_name_blocked(self, reviewer: PreRunReviewer) -> None:
        """Unsafe stage names should be rejected."""
        # These should be rejected
        assert reviewer._is_safe_filename("../etc/passwd") is False
        assert reviewer._is_safe_filename("..") is False
        assert reviewer._is_safe_filename(".hidden") is False
        assert reviewer._is_safe_filename(".hidden") is False
        assert reviewer._is_safe_filename("path/traversal") is False
        assert reviewer._is_safe_filename("") is False

        # These should be accepted
        assert reviewer._is_safe_filename("train") is True
        assert reviewer._is_safe_filename("train_v2") is True
        assert reviewer._is_safe_filename("my-stage") is True

    def test_binary_file_encoding_error(self, tmp_path: Path) -> None:
        """Binary files should be handled gracefully."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()

        # Create a binary file
        binary_file = workspace_path / "modules" / "train.py"
        binary_file.write_bytes(b"\x00\x01\x02\xff\xfe")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_file_safe(binary_file, "default")
        assert "invalid UTF-8" in content

    @pytest.mark.asyncio
    async def test_total_context_size_enforced(self, reviewer: PreRunReviewer) -> None:
        """Total context size limit should be enforced by truncation."""
        # Create large prompt context by mocking a file read
        mock_large_yaml = "stages: " + ("A" * 600_000)

        mock_provider = MockAgentProvider(ReviewResult(decision="approved", response_text="OK"))

        with patch.object(reviewer, "_read_pipeline_yaml", return_value=mock_large_yaml):
            with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
                await reviewer.review(["train"])
                assert mock_provider.call_args is not None
                assert len(mock_provider.call_args.context["prompt"]) <= 500_000 + 1000  # Allow some overhead
                assert "truncated" in mock_provider.call_args.context["prompt"]


class TestParsingRobustness:
    """Tests for parsing robustness with various formats."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        return PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

    def test_parse_json_file_reference(self, reviewer: PreRunReviewer) -> None:
        """Parse issues referencing JSON files."""
        review_text = """## train
ERROR: config.json:5 - Invalid schema
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].file == "config.json"
        assert issues[0].line == 5

    def test_parse_sh_file_reference(self, reviewer: PreRunReviewer) -> None:
        """Parse issues referencing shell scripts."""
        review_text = """## train
WARNING: setup.sh - Missing shebang
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].file == "setup.sh"

    def test_parse_stage_with_variations(self, reviewer: PreRunReviewer) -> None:
        """Parse stage headers with variations."""
        # Test "### train" (3 hashes)
        review_text = """### train
ERROR: train.py:1 - Issue
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].stage == "train"

    def test_parse_stage_with_prefix(self, reviewer: PreRunReviewer) -> None:
        """Parse stage headers with 'Stage:' prefix."""
        review_text = """## Stage: train
ERROR: train.py:1 - Issue
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 1
        assert issues[0].stage == "train"

    def test_parse_case_insensitive_severity(self, reviewer: PreRunReviewer) -> None:
        """Parse severity markers case-insensitively."""
        review_text = """## train
error: train.py:1 - lowercase error
Error: train.py:2 - title case error
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 2
        assert all(i.severity == ReviewSeverity.ERROR for i in issues)

    def test_parse_bullet_point_format(self, reviewer: PreRunReviewer) -> None:
        """Parse bullet point format: - ERROR: ..."""
        review_text = """## train
- ERROR: train.py:1 - Issue 1
- WARNING: train.py:2 - Issue 2
- NOTE: Issue 3
"""
        issues = reviewer._parse_review(review_text, ["train"])
        assert len(issues) == 3
        assert issues[0].severity == ReviewSeverity.ERROR
        assert issues[1].severity == ReviewSeverity.WARNING
        assert issues[2].severity == ReviewSeverity.NOTE


class TestExplicitDecisionParsing:
    """Tests for explicit DECISION directive parsing."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        return PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

    def test_parse_explicit_approve(self, reviewer: PreRunReviewer) -> None:
        """Parse explicit DECISION: APPROVE directive."""
        review_text = """## train
No issues found.

DECISION: APPROVE
"""
        decision = reviewer._parse_explicit_decision(review_text)
        assert decision == "approve"

    def test_parse_explicit_block(self, reviewer: PreRunReviewer) -> None:
        """Parse explicit DECISION: BLOCK directive."""
        review_text = """## train
ERROR: train.py:10 - Missing import

DECISION: BLOCK
"""
        decision = reviewer._parse_explicit_decision(review_text)
        assert decision == "block"

    def test_parse_explicit_decision_case_insensitive(self, reviewer: PreRunReviewer) -> None:
        """Parse decision directive case-insensitively."""
        review_text = """## train
decision: approve
"""
        decision = reviewer._parse_explicit_decision(review_text)
        assert decision == "approve"

        review_text2 = """## train
Decision: Block
"""
        decision2 = reviewer._parse_explicit_decision(review_text2)
        assert decision2 == "block"

    def test_parse_explicit_decision_with_trailing_content(self, reviewer: PreRunReviewer) -> None:
        """Parse decision with trailing text or punctuation."""
        review_text = """## train
DECISION: APPROVE - no blocking issues found
"""
        decision = reviewer._parse_explicit_decision(review_text)
        assert decision == "approve"

    def test_parse_explicit_decision_none_when_missing(self, reviewer: PreRunReviewer) -> None:
        """Return None when no explicit decision directive present."""
        review_text = """## train
No issues found. Looks good!
"""
        decision = reviewer._parse_explicit_decision(review_text)
        assert decision is None

    def test_parse_explicit_decision_ignores_embedded_decision(self, reviewer: PreRunReviewer) -> None:
        """Ignore 'decision' word when not at start of line."""
        review_text = """## train
The decision should be to approve this code.
I recommend the decision: approve approach.
"""
        decision = reviewer._parse_explicit_decision(review_text)
        # Should still find the second line since it starts with "decision:"
        # Actually the second line starts with "I recommend" - let me re-check
        # The method looks for lines starting with "DECISION:" - the second line doesn't start with that
        assert decision is None

    @pytest.mark.asyncio
    async def test_explicit_approve_overrides_error_markers(self, tmp_path: Path) -> None:
        """Explicit APPROVE decision overrides ERROR markers in text."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages:\n  - name: train")
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("def run(): pass")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # Simulate AI discussing potential issues but ultimately approving
        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="approved",
                response_text="""## train
I analyzed the code and found some potential concerns:

ERROR: train.py:10 - This could be an issue, but upon closer inspection it's fine
because the variable is defined in the __init__ method.

No blocking issues found.

DECISION: APPROVE
""",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            # Explicit APPROVE should override the ERROR marker
            assert review.approved is True

    @pytest.mark.asyncio
    async def test_explicit_block_respected(self, tmp_path: Path) -> None:
        """Explicit BLOCK decision is respected."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages:\n  - name: train")
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("def run(): pass")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="blocked",
                response_text="""## train
ERROR: train.py:10 - Critical issue that will cause runtime failure

DECISION: BLOCK
""",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            assert review.approved is False

    @pytest.mark.asyncio
    async def test_fallback_to_error_markers_when_no_explicit_decision(self, tmp_path: Path) -> None:
        """Fall back to ERROR marker inference when no explicit decision."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages:\n  - name: train")
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("def run(): pass")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # No explicit DECISION directive - should fall back to ERROR markers
        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="blocked",
                response_text="""## train
ERROR: train.py:10 - Missing import statement
""",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            # Should be blocked because ERROR marker present and no explicit decision
            assert review.approved is False

    @pytest.mark.asyncio
    async def test_no_errors_and_no_explicit_decision_approves(self, tmp_path: Path) -> None:
        """Approve when no errors and no explicit decision."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "pipeline.yaml").write_text("stages:\n  - name: train")
        (workspace_path / "modules").mkdir()
        (workspace_path / "modules" / "train.py").write_text("def run(): pass")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(
            config=config,
            svs_config=svs_config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # No explicit DECISION and no ERROR markers
        mock_provider = MockAgentProvider(
            ReviewResult(
                decision="approved",
                response_text="""## train
WARNING: train.py - Consider adding type hints
NOTE: Code looks clean overall
""",
            )
        )
        with patch("goldfish.pre_run_review.get_agent_provider", return_value=mock_provider):
            review = await reviewer.review(["train"])
            # Should be approved - only warnings and notes
            assert review.approved is True
