"""Tests for pre-run review functionality."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from goldfish.config import PreRunReviewConfig
from goldfish.models import ReviewIssue, ReviewSeverity, RunReason, RunReview
from goldfish.pre_run_review import PreRunReviewer, review_before_run


class TestPreRunReviewerParseReview:
    """Tests for the _parse_review method."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        config = PreRunReviewConfig()
        return PreRunReviewer(
            config=config,
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
        return PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

    @pytest.mark.asyncio
    async def test_review_skipped_when_no_api_key(self, reviewer: PreRunReviewer) -> None:
        """Review is skipped when ANTHROPIC_API_KEY not set."""
        with patch.dict("os.environ", {}, clear=True):
            # Clear the API key
            with patch("os.environ.get", return_value=None):
                review = await reviewer.review(["train"])
                assert review.approved is True
                assert "skipped" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_success_no_issues(self, reviewer: PreRunReviewer) -> None:
        """Successful review with no issues."""
        mock_response = """## train
No issues found.
"""
        with patch.object(reviewer, "_call_claude", new_callable=AsyncMock, return_value=mock_response):
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
                review = await reviewer.review(["train"])
                assert review.approved is True
                assert len(review.issues) == 0
                assert "no issues" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_with_errors_not_approved(self, reviewer: PreRunReviewer) -> None:
        """Review with errors is not approved."""
        mock_response = """## train
ERROR: train.py:10 - Missing import statement
WARNING: train.py:20 - Unused variable
"""
        with patch.object(reviewer, "_call_claude", new_callable=AsyncMock, return_value=mock_response):
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
                review = await reviewer.review(["train"])
                assert review.approved is False
                assert len(review.issues) == 2
                assert "error" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_with_warnings_approved(self, reviewer: PreRunReviewer) -> None:
        """Review with only warnings is approved."""
        mock_response = """## train
WARNING: train.py - Consider adding error handling
NOTE: Could use type hints
"""
        with patch.object(reviewer, "_call_claude", new_callable=AsyncMock, return_value=mock_response):
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
                review = await reviewer.review(["train"])
                assert review.approved is True
                assert len(review.issues) == 2
                assert "warning" in review.summary.lower()

    @pytest.mark.asyncio
    async def test_review_includes_reason(self, reviewer: PreRunReviewer) -> None:
        """Review includes RunReason in context."""
        mock_response = "## train\nNo issues found."
        reason = RunReason(
            description="Test run",
            hypothesis="Test hypothesis",
            approach="Test approach",
            min_result="Test min",
            goal="Test goal",
        )
        with patch.object(reviewer, "_call_claude", new_callable=AsyncMock, return_value=mock_response) as mock_call:
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
                await reviewer.review(["train"], reason=reason)
                # Verify reason was included in the prompt
                call_args = mock_call.call_args[0][0]
                assert "Test hypothesis" in call_args

    @pytest.mark.asyncio
    async def test_review_includes_diff(self, reviewer: PreRunReviewer) -> None:
        """Review includes diff text."""
        mock_response = "## train\nNo issues found."
        diff_text = "+def new_function():\n+    pass"
        with patch.object(reviewer, "_call_claude", new_callable=AsyncMock, return_value=mock_response) as mock_call:
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
                await reviewer.review(["train"], diff_text=diff_text)
                call_args = mock_call.call_args[0][0]
                assert diff_text in call_args

    @pytest.mark.asyncio
    async def test_review_handles_exception(self, reviewer: PreRunReviewer) -> None:
        """Review handles Claude API exceptions gracefully."""
        with patch.object(
            reviewer,
            "_call_claude",
            new_callable=AsyncMock,
            side_effect=RuntimeError("API error"),
        ):
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
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
        with patch.dict("os.environ", {}, clear=True):
            with patch("os.environ.get", return_value=None):
                review = await review_before_run(
                    config=config,
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
        reviewer = PreRunReviewer(
            config=config,
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
        reviewer = PreRunReviewer(
            config=config,
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
        reviewer = PreRunReviewer(
            config=config,
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
        reviewer = PreRunReviewer(
            config=config,
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
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_pipeline_yaml()
        assert "No pipeline.yaml found" in content


class TestSecurityFeatures:
    """Tests for security features."""

    def test_symlink_blocked(self, tmp_path: Path) -> None:
        """Symlinks should be blocked for security."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()
        (workspace_path / "configs").mkdir()

        # Create a file inside workspace
        target = workspace_path / "configs" / "secret.yaml"
        target.write_text("secret data")

        # Create a symlink within workspace (could still be dangerous)
        symlink = workspace_path / "modules" / "train.py"
        symlink.symlink_to(target)

        config = PreRunReviewConfig()
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_file_safe(symlink, "default")
        assert "Symlink detected" in content
        assert "secret data" not in content

    def test_path_traversal_blocked(self, tmp_path: Path) -> None:
        """Path traversal attempts should be blocked."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()

        # Create a file outside workspace
        outside = tmp_path / "outside.txt"
        outside.write_text("outside content")

        config = PreRunReviewConfig()
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # Try to read outside workspace
        traversal_path = workspace_path / ".." / "outside.txt"
        content = reviewer._read_file_safe(traversal_path, "default")
        assert content == "default"
        assert "outside content" not in content

    def test_large_file_blocked(self, tmp_path: Path) -> None:
        """Large files should be blocked to prevent DoS."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()

        # Create a file larger than MAX_FILE_SIZE (100KB)
        large_file = workspace_path / "modules" / "train.py"
        large_file.write_text("x" * 150_000)  # 150KB

        config = PreRunReviewConfig()
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_file_safe(large_file, "default")
        assert "too large" in content.lower()

    def test_unsafe_stage_name_blocked(self, tmp_path: Path) -> None:
        """Unsafe stage names should be rejected."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()

        config = PreRunReviewConfig()
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # These should be rejected
        assert reviewer._is_safe_filename("../etc/passwd") is False
        assert reviewer._is_safe_filename("..") is False
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
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        content = reviewer._read_file_safe(binary_file, "default")
        assert "invalid UTF-8" in content

    @pytest.mark.asyncio
    async def test_total_context_size_enforced(self, tmp_path: Path) -> None:
        """Total context size should be truncated to prevent API cost/token issues."""
        from goldfish.pre_run_review import MAX_TOTAL_CONTEXT_SIZE

        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        (workspace_path / "modules").mkdir()

        # Create a large but valid file (under per-file limit, but will push total over)
        large_content = "# " + "x" * 90_000  # 90KB per stage
        for stage in ["stage1", "stage2", "stage3", "stage4", "stage5", "stage6"]:
            (workspace_path / "modules" / f"{stage}.py").write_text(large_content)

        (workspace_path / "pipeline.yaml").write_text("stages: []")

        config = PreRunReviewConfig()
        reviewer = PreRunReviewer(
            config=config,
            workspace_path=workspace_path,
            dev_repo_path=tmp_path / "dev",
        )

        # Capture the prompt that would be sent
        captured_prompt: list[str] = []

        async def mock_claude(prompt: str) -> str:
            captured_prompt.append(prompt)
            return "## stage1\nNo issues found."

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            with patch.object(reviewer, "_call_claude", mock_claude):
                await reviewer.review(["stage1", "stage2", "stage3", "stage4", "stage5", "stage6"])

        # Prompt should be truncated to MAX_TOTAL_CONTEXT_SIZE
        assert len(captured_prompt) == 1
        assert len(captured_prompt[0]) <= MAX_TOTAL_CONTEXT_SIZE
        assert "truncated" in captured_prompt[0].lower()


class TestParsingRobustness:
    """Tests for parsing robustness with various formats."""

    @pytest.fixture
    def reviewer(self, tmp_path: Path) -> PreRunReviewer:
        """Create a reviewer instance."""
        workspace_path = tmp_path / "workspace"
        workspace_path.mkdir()
        config = PreRunReviewConfig()
        return PreRunReviewer(
            config=config,
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
