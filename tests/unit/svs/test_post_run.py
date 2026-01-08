"""Unit tests for SVS post-run AI review.

Tests for post-run review functionality that analyzes stage outputs and statistics.
Following TDD: RED → GREEN → REFACTOR

Key functions:
1. run_post_run_review() - Main function that orchestrates post-run review
2. PostRunReview - Result dataclass with findings and metadata

Review behavior:
- Skips when ai_post_run_enabled=False
- Analyzes outputs directory and stage statistics
- Writes findings to .goldfish/svs_findings.json
- Handles errors gracefully (fails open)
- Respects rate limits
- Records timing information
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

from goldfish.svs.agent import AgentProvider, NullProvider, ReviewRequest, ReviewResult
from goldfish.svs.config import SVSConfig


class TestPostRunReviewDataclass:
    """Test PostRunReview dataclass structure."""

    def test_has_skipped_field(self):
        """PostRunReview should have skipped field (bool)."""
        from goldfish.svs.post_run import PostRunReview

        review = PostRunReview(
            skipped=False,
            decision="approved",
            findings=[],
            stats={},
            duration_ms=100,
        )
        assert review.skipped is False

    def test_has_decision_field(self):
        """PostRunReview should have decision field (str)."""
        from goldfish.svs.post_run import PostRunReview

        review = PostRunReview(
            skipped=False,
            decision="approved",
            findings=[],
            stats={},
            duration_ms=100,
        )
        assert review.decision == "approved"

    def test_has_findings_field(self):
        """PostRunReview should have findings field (list[str])."""
        from goldfish.svs.post_run import PostRunReview

        findings = ["WARNING: Output size exceeded threshold"]
        review = PostRunReview(
            skipped=False,
            decision="warned",
            findings=findings,
            stats={},
            duration_ms=100,
        )
        assert review.findings == findings

    def test_has_stats_field(self):
        """PostRunReview should have stats field (dict)."""
        from goldfish.svs.post_run import PostRunReview

        stats = {"file_count": 5, "total_size": 1000}
        review = PostRunReview(
            skipped=False,
            decision="approved",
            findings=[],
            stats=stats,
            duration_ms=100,
        )
        assert review.stats == stats

    def test_has_duration_ms_field(self):
        """PostRunReview should have duration_ms field (int)."""
        from goldfish.svs.post_run import PostRunReview

        review = PostRunReview(
            skipped=False,
            decision="approved",
            findings=[],
            stats={},
            duration_ms=250,
        )
        assert review.duration_ms == 250


class TestRunPostRunReviewSkipping:
    """Test review skipping behavior based on configuration."""

    def test_skips_when_disabled_in_config(self):
        """Should skip review when ai_post_run_enabled=False."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=False)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.skipped is True

    def test_returns_empty_findings_when_skipped(self):
        """Skipped review should have empty findings."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=False)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.findings == []

    def test_skipped_review_has_near_zero_duration(self):
        """Skipped review should have minimal duration (<10ms)."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=False)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.duration_ms < 10

    def test_skipped_review_includes_stats(self):
        """Skipped review should still include stats in result."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=False)
        agent = NullProvider()
        stats = {"file_count": 10, "total_size": 5000}

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats=stats,
            config=config,
            agent=agent,
        )

        assert result.stats == stats

    def test_runs_when_enabled_in_config(self):
        """Should run review when ai_post_run_enabled=True."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.skipped is False


class TestRunPostRunReviewExecution:
    """Test review execution with agent."""

    def test_calls_agent_with_post_run_review_type(self):
        """Should call agent with review_type='post_run'."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        agent.run.assert_called_once()
        call_args = agent.run.call_args[0][0]
        assert isinstance(call_args, ReviewRequest)
        assert call_args.review_type == "post_run"

    def test_includes_outputs_dir_in_context(self):
        """Should include outputs_dir in review context."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        outputs_dir = Path("/tmp/outputs")
        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        call_args = agent.run.call_args[0][0]
        assert "outputs_dir" in call_args.context
        assert call_args.context["outputs_dir"] == str(outputs_dir)

    def test_includes_stats_in_request(self):
        """Should pass stats to agent in ReviewRequest."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        stats = {"file_count": 10, "total_size": 5000, "avg_size": 500}
        run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats=stats,
            config=config,
            agent=agent,
        )

        call_args = agent.run.call_args[0][0]
        assert call_args.stats == stats

    def test_returns_agent_decision(self):
        """Should return agent's decision in result."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        agent.configure_response("blocked")

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.decision == "blocked"

    def test_returns_agent_findings(self):
        """Should return agent's findings in result."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        findings = ["ERROR: Output validation failed", "WARNING: Large file detected"]
        agent.configure_response("blocked", findings=findings)

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.findings == findings

    def test_records_duration_from_agent(self):
        """Should record duration from agent result."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=350,
        )

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.duration_ms == 350


class TestRunPostRunReviewFindingsFile:
    """Test writing findings to .goldfish/svs_findings.json."""

    def test_writes_findings_to_json_file(self, tmp_path: Path):
        """Should write findings to .goldfish/svs_findings.json."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        findings = ["WARNING: Output size larger than expected"]
        agent.configure_response("warned", findings=findings)

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        assert findings_file.exists()

    def test_findings_file_contains_decision(self, tmp_path: Path):
        """Findings file should contain decision field."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        agent.configure_response("blocked")

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert data["decision"] == "blocked"

    def test_findings_file_contains_findings_list(self, tmp_path: Path):
        """Findings file should contain findings array."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        findings = ["ERROR: Missing output file", "WARNING: Deprecated format"]
        agent.configure_response("blocked", findings=findings)

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert data["findings"] == findings

    def test_findings_file_contains_stats(self, tmp_path: Path):
        """Findings file should contain stats in metadata."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        stats = {"file_count": 10, "total_size": 5000}
        run_post_run_review(
            outputs_dir=outputs_dir,
            stats=stats,
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert "stats" in data
        assert data["stats"] == stats

    def test_findings_file_merges_existing_findings(self, tmp_path: Path):
        """Post-run review should merge with existing findings instead of overwriting."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        agent.configure_response("approved", findings=["NOTE: Post-run review ok"])

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        existing = {
            "version": 1,
            "decision": "warned",
            "findings": ["WARNING: NaN detected during training"],
            "stats": {"signal": {"mean": 1.0}},
        }
        (goldfish_dir / "svs_findings.json").write_text(json.dumps(existing))

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 3},
            config=config,
            agent=agent,
        )

        data = json.loads((goldfish_dir / "svs_findings.json").read_text())
        assert "WARNING: NaN detected during training" in data.get("findings", [])
        assert "NOTE: Post-run review ok" in data.get("findings", [])
        assert data.get("decision") == "warned"
        assert data.get("stats", {}).get("file_count") == 3

    def test_findings_file_contains_duration(self, tmp_path: Path):
        """Findings file should contain duration_ms."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=275,
        )

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert data["duration_ms"] == 275

    def test_skipped_review_does_not_write_findings_file(self, tmp_path: Path):
        """Skipped review should not write findings file."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=False)
        agent = NullProvider()

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        findings_file = goldfish_dir / "svs_findings.json"
        assert not findings_file.exists()


class TestRunPostRunReviewErrorHandling:
    """Test graceful error handling (fail open)."""

    def test_handles_agent_exception_gracefully(self):
        """Should handle agent exceptions and fail open (approve)."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.side_effect = RuntimeError("Agent API timeout")

        # Should not raise, fail open
        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.decision == "approved"
        assert result.skipped is False

    def test_agent_error_includes_error_in_findings(self):
        """Agent error should add error message to findings."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.side_effect = RuntimeError("Agent API timeout")

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert len(result.findings) > 0
        assert any("error" in finding.lower() for finding in result.findings)

    def test_handles_missing_outputs_dir_gracefully(self):
        """Should handle missing outputs_dir gracefully."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        # Non-existent directory
        result = run_post_run_review(
            outputs_dir=Path("/nonexistent/outputs"),
            stats={"file_count": 0},
            config=config,
            agent=agent,
        )

        # Should not raise, handle gracefully
        assert result is not None

    def test_handles_empty_stats_dict(self):
        """Should handle empty stats dict."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={},
            config=config,
            agent=agent,
        )

        assert result.stats == {}
        assert result.decision == "approved"

    def test_handles_none_stats_gracefully(self):
        """Should handle None stats by converting to empty dict."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats=None,  # type: ignore
            config=config,
            agent=agent,
        )

        assert result.stats == {}

    def test_handles_missing_goldfish_dir(self, tmp_path: Path):
        """Should handle missing .goldfish directory by creating it."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        # Don't create .goldfish directory

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        # Should create .goldfish directory
        goldfish_dir = outputs_dir / ".goldfish"
        assert goldfish_dir.exists()

    def test_handles_file_write_error_gracefully(self, tmp_path: Path):
        """Should handle file write errors gracefully."""
        from unittest.mock import patch

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        # Mock file write to raise error
        with patch("builtins.open", side_effect=OSError("Disk full")):
            # Should not raise
            result = run_post_run_review(
                outputs_dir=outputs_dir,
                stats={"file_count": 5},
                config=config,
                agent=agent,
            )

        assert result is not None


class TestRunPostRunReviewRateLimiting:
    """Test rate limiting behavior."""

    def test_respects_rate_limit_config(self):
        """Should respect rate_limit_per_hour from config."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True, rate_limit_per_hour=0)
        agent = NullProvider()

        # With rate limit 0, should skip
        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        # Should skip due to rate limit
        assert result.skipped is True

    def test_rate_limit_skip_has_empty_findings(self):
        """Rate limit skip should have empty findings."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True, rate_limit_per_hour=0)
        agent = NullProvider()

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        assert result.findings == []

    def test_rate_limit_skip_includes_stats(self):
        """Rate limit skip should still include stats."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True, rate_limit_per_hour=0)
        agent = NullProvider()
        stats = {"file_count": 10}

        result = run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats=stats,
            config=config,
            agent=agent,
        )

        assert result.stats == stats


class TestRunPostRunReviewRunContext:
    """Test run_context handling in post-run review."""

    def test_loads_run_context_from_file(self, tmp_path: Path):
        """Should load run_context from svs_context.json when not provided."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        # Write svs_context.json
        context_data = {
            "stage_name": "train",
            "workspace": "experiment_1",
            "config_override": {"epochs": 100},
        }
        (goldfish_dir / "svs_context.json").write_text(json.dumps(context_data))

        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
        )

        # Verify run_context was included in the request
        call_args = agent.run.call_args[0][0]
        assert "run_context" in call_args.context
        assert call_args.context["run_context"]["workspace"] == "experiment_1"
        assert call_args.context["run_context"]["config_override"] == {"epochs": 100}

    def test_uses_provided_run_context_over_file(self, tmp_path: Path):
        """Should use provided run_context instead of reading from file."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        # Write svs_context.json with different data
        (goldfish_dir / "svs_context.json").write_text(json.dumps({"workspace": "from_file"}))

        # Provide run_context directly
        provided_context = {"workspace": "provided_context", "config_override": {"lr": 0.001}}
        run_post_run_review(
            outputs_dir=outputs_dir,
            stats={"file_count": 5},
            config=config,
            agent=agent,
            run_context=provided_context,
        )

        # Verify provided context was used
        call_args = agent.run.call_args[0][0]
        assert call_args.context["run_context"]["workspace"] == "provided_context"

    def test_includes_run_context_in_request(self):
        """Should include run_context in agent ReviewRequest."""
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = Mock(spec=AgentProvider)
        agent.name = "test_agent"
        agent.run.return_value = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=100,
        )

        run_context = {
            "stage_name": "train",
            "workspace": "my_experiment",
            "config_override": {"batch_size": 32},
            "inputs_override": {"data": "test_source"},
            "run_reason": {"goal": "Test new architecture"},
        }

        run_post_run_review(
            outputs_dir=Path("/tmp/outputs"),
            stats={"file_count": 5},
            config=config,
            agent=agent,
            run_context=run_context,
        )

        call_args = agent.run.call_args[0][0]
        assert call_args.context["run_context"] == run_context


class TestRunPostRunReviewIntegration:
    """Integration tests combining multiple features."""

    def test_full_workflow_approved(self, tmp_path: Path):
        """Test full workflow with approved result."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        agent.configure_response("approved")

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        stats = {"file_count": 3, "total_size": 1500}
        result = run_post_run_review(
            outputs_dir=outputs_dir,
            stats=stats,
            config=config,
            agent=agent,
        )

        # Check result
        assert result.skipped is False
        assert result.decision == "approved"
        assert result.findings == []
        assert result.stats == stats
        assert result.duration_ms >= 0

        # Check findings file
        findings_file = goldfish_dir / "svs_findings.json"
        assert findings_file.exists()
        data = json.loads(findings_file.read_text())
        assert data["decision"] == "approved"
        assert data["findings"] == []
        assert data["stats"] == stats

    def test_full_workflow_blocked(self, tmp_path: Path):
        """Test full workflow with blocked result."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        findings = ["ERROR: Output validation failed", "ERROR: Missing required files"]
        agent.configure_response("blocked", findings=findings)

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        stats = {"file_count": 1, "total_size": 100}
        result = run_post_run_review(
            outputs_dir=outputs_dir,
            stats=stats,
            config=config,
            agent=agent,
        )

        # Check result
        assert result.skipped is False
        assert result.decision == "blocked"
        assert result.findings == findings
        assert result.stats == stats

        # Check findings file
        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert data["decision"] == "blocked"
        assert data["findings"] == findings

    def test_full_workflow_warned(self, tmp_path: Path):
        """Test full workflow with warned result."""
        import json

        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig(ai_post_run_enabled=True)
        agent = NullProvider()
        findings = ["WARNING: Output size larger than typical"]
        agent.configure_response("warned", findings=findings)

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir()

        stats = {"file_count": 5, "total_size": 10000}
        result = run_post_run_review(
            outputs_dir=outputs_dir,
            stats=stats,
            config=config,
            agent=agent,
        )

        # Check result
        assert result.skipped is False
        assert result.decision == "warned"
        assert result.findings == findings
        assert result.stats == stats

        # Check findings file
        findings_file = goldfish_dir / "svs_findings.json"
        data = json.loads(findings_file.read_text())
        assert data["decision"] == "warned"
        assert data["findings"] == findings
