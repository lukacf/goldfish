"""Unit tests for SVS AI CLI providers.

Tests the mapping of AgentRequest fields to CLI flags for:
- CodexCLIProvider
- GeminiCLIProvider
"""

from unittest.mock import MagicMock, patch

from goldfish.svs.agent import (
    AgentRequest,
    CodexCLIProvider,
    GeminiCLIProvider,
    ToolPolicy,
)


class TestToolPolicyDefault:
    """Tests for ToolPolicy defaults."""

    def test_tool_policy_default_permission_mode_is_bypass(self):
        """Test that ToolPolicy defaults to bypassPermissions for internal tracking.

        Note: We now use --dangerously-skip-permissions flag on CLI instead of
        --permission-mode, but keep the internal field for documentation/tracking.
        """
        policy = ToolPolicy()
        assert policy.permission_mode == "bypassPermissions", (
            f"ToolPolicy.permission_mode should default to 'bypassPermissions', " f"got '{policy.permission_mode}'"
        )


class TestCodexCLIProvider:
    """Tests for CodexCLIProvider CLI mapping."""

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_run_maps_codex_fields(self, mock_run, mock_which):
        """Should map sandbox and extra args for Codex CLI."""
        mock_which.return_value = "codex"
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="", text=True)

        provider = CodexCLIProvider()
        request = AgentRequest(
            prompt="Fix bug",
            context={
                "sandbox": "read-only",
                "cli_args": ["--verbose", "--quiet"],
            },
        )

        provider.run(request)

        cmd = mock_run.call_args[0][0]
        assert "codex" in cmd
        assert "exec" in cmd
        assert "--full-auto" in cmd
        assert "--sandbox" in cmd
        assert "read-only" in cmd
        assert "--verbose" in cmd
        assert "--quiet" in cmd
        assert "Fix bug" in cmd


class TestGeminiCLIProvider:
    """Tests for GeminiCLIProvider CLI mapping."""

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_run_maps_gemini_fields(self, mock_run, mock_which):
        """Should map prompt argument for Gemini CLI."""
        mock_which.return_value = "gemini"
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="", text=True)

        provider = GeminiCLIProvider()
        request = AgentRequest(
            prompt="Analyze this",
            context={
                "prompt_arg": "--input",
                "cli_args": ["--v=2"],
            },
        )

        provider.run(request)

        cmd = mock_run.call_args[0][0]
        assert "gemini" in cmd
        assert "--v=2" in cmd
        assert "--input" in cmd
        assert "Analyze this" in cmd


class TestToolPolicyRequirement:
    """Regression tests: all SVS review callers must pass tool_policy.

    Without tool_policy, the Claude CLI may hang in interactive mode,
    causing reviews to time out or never complete. This was a critical
    bug where during-run, post-run, and pattern reviews silently failed.
    """

    def test_during_run_monitor_passes_tool_policy(self):
        """DuringRunMonitor._do_review must include tool_policy with bypassPermissions."""
        from pathlib import Path
        from unittest.mock import MagicMock, patch

        from goldfish.svs.config import SVSConfig
        from goldfish.svs.during_run_monitor import DuringRunMonitor

        config = SVSConfig()
        monitor = DuringRunMonitor(config, Path("/tmp/test_outputs"))

        # Mock the agent to capture the request
        captured_request = None

        def capture_run(request):
            nonlocal captured_request
            captured_request = request
            result = MagicMock()
            result.response_text = '{"findings": [], "request_stop": false}'
            return result

        # Patch where get_agent_provider is looked up (inside _do_review)
        with patch("goldfish.svs.agent.get_agent_provider") as mock_get:
            mock_agent = MagicMock()
            mock_agent.run = capture_run
            mock_get.return_value = mock_agent

            # Call _do_review directly
            monitor._do_review([], "test logs")

        assert captured_request is not None, "Request was not captured"
        tool_policy = captured_request.context.get("tool_policy")
        assert tool_policy is not None, "tool_policy must be passed in context"
        assert (
            tool_policy.permission_mode == "bypassPermissions"
        ), f"permission_mode must be 'bypassPermissions', got '{tool_policy.permission_mode}'"

    def test_post_run_review_passes_tool_policy(self):
        """run_post_run_review must include tool_policy with bypassPermissions."""
        from pathlib import Path
        from unittest.mock import MagicMock

        from goldfish.svs.config import SVSConfig
        from goldfish.svs.post_run import run_post_run_review

        config = SVSConfig()
        outputs_dir = Path("/tmp/test_outputs")

        captured_request = None

        def capture_run(request):
            nonlocal captured_request
            captured_request = request
            result = MagicMock()
            result.decision = "approved"
            result.findings = []
            result.duration_ms = 100
            result.response_text = "Test review response"
            return result

        mock_agent = MagicMock()
        mock_agent.run = capture_run

        # Call run_post_run_review with mock agent
        # Signature: run_post_run_review(outputs_dir, stats, config, agent)
        run_post_run_review(outputs_dir, None, config, mock_agent)

        assert captured_request is not None, "Request was not captured"
        tool_policy = captured_request.context.get("tool_policy")
        assert tool_policy is not None, "tool_policy must be passed in context"
        assert (
            tool_policy.permission_mode == "bypassPermissions"
        ), f"permission_mode must be 'bypassPermissions', got '{tool_policy.permission_mode}'"
