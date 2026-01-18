"""Unit tests for AgentProvider protocol and NullProvider test double.

These tests define the expected behavior of the agent abstraction before implementation.
Following TDD: RED → GREEN → REFACTOR
"""

from goldfish.svs.agent import (
    AgentProvider,
    NullProvider,
    ReviewRequest,
    ReviewResult,
    get_agent_provider,
)


class TestNullProviderBasics:
    """Test fundamental behavior of NullProvider."""

    def test_has_null_name(self):
        """NullProvider should have name 'null'."""
        provider = NullProvider()
        assert provider.name == "null"

    def test_default_decision_is_approved(self):
        """NullProvider should default to 'approved' decision."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.decision == "approved"

    def test_returns_review_result(self):
        """NullProvider.run() should return ReviewResult instance."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert isinstance(result, ReviewResult)

    def test_run_accepts_review_request(self):
        """NullProvider.run() should accept ReviewRequest parameter."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
            stats={"file_count": 5},
        )
        # Should not raise
        result = provider.run(request)
        assert result is not None


class TestNullProviderConfiguration:
    """Test NullProvider's configurable response mechanism."""

    def test_configure_blocked_response(self):
        """configure_response() should allow setting blocked decision."""
        provider = NullProvider()
        provider.configure_response("blocked")
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.decision == "blocked"

    def test_configure_warned_response(self):
        """configure_response() should allow setting warned decision."""
        provider = NullProvider()
        provider.configure_response("warned")
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.decision == "warned"

    def test_configure_custom_findings(self):
        """configure_response() should allow setting custom findings."""
        provider = NullProvider()
        findings = ["ERROR: Missing import", "WARNING: Deprecated API"]
        provider.configure_response("blocked", findings=findings)
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.findings == findings

    def test_configure_persists_across_calls(self):
        """Configured response should persist across multiple run() calls."""
        provider = NullProvider()
        provider.configure_response("blocked", findings=["ERROR: Test error"])
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )

        result1 = provider.run(request)
        result2 = provider.run(request)

        assert result1.decision == "blocked"
        assert result2.decision == "blocked"
        assert result1.findings == result2.findings

    def test_configure_default_findings_empty(self):
        """configure_response() without findings should use empty list."""
        provider = NullProvider()
        provider.configure_response("approved")
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.findings == []

    def test_constructor_sets_default_decision(self):
        """NullProvider(default_decision) should set initial decision."""
        provider = NullProvider(default_decision="warned")
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert result.decision == "warned"


class TestNullProviderReviewRequest:
    """Test NullProvider handling of different ReviewRequest configurations."""

    def test_handles_pre_run_request(self):
        """NullProvider should handle pre_run review type."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test", "stage": "train"},
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)

    def test_handles_during_run_request(self):
        """NullProvider should handle during_run review type."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="during_run",
            context={"workspace": "test", "progress": 0.5},
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)

    def test_handles_post_run_request(self):
        """NullProvider should handle post_run review type."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="post_run",
            context={"workspace": "test", "status": "completed"},
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)

    def test_handles_request_with_stats(self):
        """NullProvider should handle ReviewRequest with stats."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
            stats={"file_count": 10, "total_size": 5000},
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)

    def test_handles_request_without_stats(self):
        """NullProvider should handle ReviewRequest without stats (None)."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
            stats=None,
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)

    def test_handles_empty_context(self):
        """NullProvider should handle ReviewRequest with empty context dict."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={},
        )
        result = provider.run(request)
        assert result is not None
        assert isinstance(result, ReviewResult)


class TestNullProviderReviewResult:
    """Test ReviewResult structure returned by NullProvider."""

    def test_result_has_decision(self):
        """ReviewResult should have decision field."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert hasattr(result, "decision")
        assert result.decision in ["approved", "blocked", "warned"]

    def test_result_has_findings_list(self):
        """ReviewResult should have findings field as list."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert hasattr(result, "findings")
        assert isinstance(result.findings, list)

    def test_result_has_response_text(self):
        """ReviewResult should have response_text field."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert hasattr(result, "response_text")
        assert isinstance(result.response_text, str)

    def test_result_has_duration_ms(self):
        """ReviewResult should have duration_ms field."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert hasattr(result, "duration_ms")
        assert isinstance(result.duration_ms, int)

    def test_result_duration_is_near_zero(self):
        """NullProvider should return near-zero duration (it's instant)."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        # NullProvider is instant, should be < 10ms
        assert result.duration_ms < 10

    def test_result_response_text_non_empty(self):
        """ReviewResult.response_text should be non-empty string."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert len(result.response_text) > 0

    def test_result_findings_contains_strings(self):
        """ReviewResult.findings should contain only strings."""
        provider = NullProvider()
        provider.configure_response("blocked", findings=["ERROR: Test", "WARNING: Test"])
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert all(isinstance(finding, str) for finding in result.findings)

    def test_during_run_returns_json_format(self):
        """Regression: NullProvider should return JSON for during_run reviews.

        Bug: DuringRunMonitor expects JSON response with 'findings' and 'request_stop'
        fields. NullProvider was returning plain text, causing JSON parse failures.
        """
        import json

        provider = NullProvider()
        request = ReviewRequest(
            review_type="during_run",
            context={"workspace": "test", "output_format": "json"},
        )
        result = provider.run(request)

        # Response should contain JSON in markdown fence
        assert "```json" in result.response_text

        # Extract and parse the JSON
        json_match = result.response_text.split("```json")[1].split("```")[0].strip()
        parsed = json.loads(json_match)

        # Should have the required fields for during-run monitor
        assert "findings" in parsed
        assert "request_stop" in parsed
        assert isinstance(parsed["findings"], list)
        assert isinstance(parsed["request_stop"], bool)

    def test_during_run_json_has_findings_structure(self):
        """Regression: during_run JSON findings should have check/severity/summary."""
        import json

        provider = NullProvider()
        request = ReviewRequest(
            review_type="during_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)

        # Extract and parse the JSON
        json_match = result.response_text.split("```json")[1].split("```")[0].strip()
        parsed = json.loads(json_match)

        # Findings should have proper structure
        assert len(parsed["findings"]) > 0
        for finding in parsed["findings"]:
            assert "check" in finding
            assert "severity" in finding
            assert "summary" in finding

    def test_pre_run_still_returns_plain_text(self):
        """Pre-run reviews should continue returning plain text format."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)

        # Should not contain JSON fence for pre-run
        assert "```json" not in result.response_text
        # Should contain the plain text format
        assert "NullProvider: approved for pre_run review" in result.response_text


class TestAgentProviderProtocol:
    """Test AgentProvider protocol compliance."""

    def test_null_provider_implements_protocol(self):
        """NullProvider should implement AgentProvider protocol."""
        provider = NullProvider()
        # Should be able to use provider where AgentProvider is expected
        assert isinstance(provider, AgentProvider)

    def test_protocol_requires_name(self):
        """AgentProvider protocol should require name attribute."""
        provider = NullProvider()
        assert hasattr(provider, "name")
        assert isinstance(provider.name, str)

    def test_protocol_requires_run_method(self):
        """AgentProvider protocol should require run() method."""
        provider = NullProvider()
        assert hasattr(provider, "run")
        assert callable(provider.run)

    def test_protocol_run_signature(self):
        """AgentProvider.run() should accept ReviewRequest and return ReviewResult."""
        provider = NullProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"workspace": "test"},
        )
        result = provider.run(request)
        assert isinstance(result, ReviewResult)


class TestGetAgentProviderFallback:
    """Test get_agent_provider fallback when CLI binary is missing."""

    def test_missing_claude_binary_falls_back_to_null(self, monkeypatch):
        """If Claude CLI binary is missing, get_agent_provider should return NullProvider."""
        import goldfish.svs.agent as agent_module

        monkeypatch.setattr(agent_module.shutil, "which", lambda _: None)

        provider = get_agent_provider("claude_code")
        assert isinstance(provider, NullProvider)


class TestReviewRequestDataclass:
    """Test ReviewRequest dataclass structure."""

    def test_review_request_has_review_type(self):
        """ReviewRequest should have review_type field."""
        request = ReviewRequest(
            review_type="pre_run",
            context={},
        )
        assert request.review_type == "pre_run"

    def test_review_request_has_context(self):
        """ReviewRequest should have context field."""
        context = {"workspace": "test", "stage": "train"}
        request = ReviewRequest(
            review_type="pre_run",
            context=context,
        )
        assert request.context == context

    def test_review_request_stats_optional(self):
        """ReviewRequest.stats should be optional (None by default)."""
        request = ReviewRequest(
            review_type="pre_run",
            context={},
        )
        assert request.stats is None

    def test_review_request_with_stats(self):
        """ReviewRequest should accept stats dict."""
        stats = {"file_count": 5, "total_size": 1000}
        request = ReviewRequest(
            review_type="pre_run",
            context={},
            stats=stats,
        )
        assert request.stats == stats


class TestReviewResultDataclass:
    """Test ReviewResult dataclass structure."""

    def test_review_result_requires_decision(self):
        """ReviewResult should require decision field."""
        result = ReviewResult(
            decision="approved",
            findings=[],
            response_text="All good",
            duration_ms=0,
        )
        assert result.decision == "approved"

    def test_review_result_requires_findings(self):
        """ReviewResult should require findings field."""
        result = ReviewResult(
            decision="blocked",
            findings=["ERROR: Test"],
            response_text="Blocked",
            duration_ms=0,
        )
        assert result.findings == ["ERROR: Test"]

    def test_review_result_requires_response_text(self):
        """ReviewResult should require response_text field."""
        result = ReviewResult(
            decision="approved",
            findings=[],
            response_text="Analysis complete",
            duration_ms=0,
        )
        assert result.response_text == "Analysis complete"

    def test_review_result_requires_duration_ms(self):
        """ReviewResult should require duration_ms field."""
        result = ReviewResult(
            decision="approved",
            findings=[],
            response_text="Done",
            duration_ms=150,
        )
        assert result.duration_ms == 150
