"""Unit tests for SVS AI CLI providers.

Tests the mapping of AgentRequest fields to CLI flags for:
- ClaudeCodeProvider
- CodexCLIProvider
- GeminiCLIProvider
"""

from unittest.mock import MagicMock, patch

from goldfish.svs.agent import (
    AgentRequest,
    ClaudeCodeProvider,
    CodexCLIProvider,
    GeminiCLIProvider,
    ToolPolicy,
)


class TestClaudeCodeProvider:
    """Tests for ClaudeCodeProvider CLI mapping."""

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_run_maps_basic_fields(self, mock_run, mock_which):
        """Should map prompt, model, and max_turns to CLI flags."""
        mock_which.return_value = "/usr/local/bin/claude"
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="", text=True)

        provider = ClaudeCodeProvider(binary="/usr/local/bin/claude")
        request = AgentRequest(
            prompt="Test prompt",
            model="opus",
            max_turns=5,
            timeout_seconds=60,
        )

        provider.run(request)

        # Verify command line
        args, kwargs = mock_run.call_args
        cmd = args[0]
        assert "/usr/local/bin/claude" in cmd
        assert "-p" in cmd
        assert "Test prompt" in cmd
        assert "--model" in cmd
        assert "opus" in cmd
        assert "--max-turns" in cmd
        assert "5" in cmd
        assert kwargs["timeout"] == 60

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_run_maps_tool_policy(self, mock_run, mock_which):
        """Should map ToolPolicy fields to CLI flags."""
        mock_which.return_value = "claude"
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="", text=True)

        provider = ClaudeCodeProvider()
        policy = ToolPolicy(
            permission_mode="bypassPermissions",
            allow_tools=["read_file", "list_directory"],
            deny_tools=["run_shell_command"],
            mcp_servers=["github", "google-search"],
        )
        request = AgentRequest(prompt="Test", tool_policy=policy)

        provider.run(request)

        cmd = mock_run.call_args[0][0]
        assert "--permission-mode" in cmd
        assert "bypassPermissions" in cmd
        assert "--allowed-tools" in cmd
        assert "read_file" in cmd
        assert "list_directory" in cmd
        assert "--disallowed-tools" in cmd
        assert "run_shell_command" in cmd
        assert "--mcp-server" in cmd
        assert "github" in cmd
        assert "google-search" in cmd

    def test_tool_policy_default_permission_mode_is_valid(self):
        """Regression test: permission_mode default must be a valid Claude CLI value.

        The Claude CLI only accepts specific permission modes:
        bypassPermissions, default, acceptEdits, dontAsk, plan, delegate.

        Previously the default was 'auto' which caused silent failures:
        'error: option --permission-mode argument auto is invalid'
        """
        policy = ToolPolicy()
        valid_modes = {"bypassPermissions", "default", "acceptEdits", "dontAsk", "plan", "delegate"}
        assert policy.permission_mode in valid_modes, (
            f"ToolPolicy.permission_mode default '{policy.permission_mode}' is not valid. "
            f"Valid modes: {valid_modes}"
        )

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_cli_error_nonzero_exit_code_logs_warning_and_returns_approved(self, mock_run, mock_which):
        """Regression test: CLI failures should log warning and return approved (fail-open).

        When Claude CLI exits with non-zero code (e.g., invalid args), we should:
        1. Log a warning (not silently swallow the error)
        2. Return 'approved' decision (fail-open, don't block user)
        3. Include a WARNING finding explaining the failure
        """
        mock_which.return_value = "claude"
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="error: option '--permission-mode <mode>' argument 'auto' is invalid",
            text=True,
        )

        provider = ClaudeCodeProvider()
        request = AgentRequest(prompt="Test")

        result = provider.run(request)

        # Should fail-open with approved decision
        assert result.decision == "approved"
        # Should include a warning finding about the failure
        assert len(result.findings) > 0
        assert any("AI review failed" in f for f in result.findings)

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_cli_error_output_starts_with_error_logs_warning(self, mock_run, mock_which):
        """Regression test: CLI error in output should be detected.

        When stdout starts with 'error:', we should detect this as a CLI failure
        even if exit code is 0.
        """
        mock_which.return_value = "claude"
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="error: invalid API key",
            stderr="",
            text=True,
        )

        provider = ClaudeCodeProvider()
        request = AgentRequest(prompt="Test")

        result = provider.run(request)

        # Should fail-open with approved decision
        assert result.decision == "approved"
        # Should include a warning finding
        assert len(result.findings) > 0
        assert any("AI review failed" in f for f in result.findings)

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_cli_retry_with_fallback_model_on_api_error(self, mock_run, mock_which):
        """Should retry with fallback model when API error is returned."""
        mock_which.return_value = "claude"
        mock_run.side_effect = [
            MagicMock(
                returncode=0,
                stdout="API Error: Repeated 529 Overloaded errors",
                stderr="",
                text=True,
            ),
            MagicMock(returncode=0, stdout="OK", stderr="", text=True),
        ]

        provider = ClaudeCodeProvider()
        request = AgentRequest(
            prompt="Test",
            context={"fallback_model": "sonnet-4.5"},
        )

        result = provider.run(request)

        assert result.decision == "approved"
        assert result.findings == []
        assert mock_run.call_count == 2
        second_cmd = mock_run.call_args_list[1][0][0]
        assert "--model" in second_cmd
        assert "sonnet-4.5" in second_cmd

    @patch("shutil.which")
    @patch("subprocess.run")
    def test_cli_api_error_detected_as_failure(self, mock_run, mock_which):
        """Should treat API-side error messages as CLI failure (fail-open with warning)."""
        mock_which.return_value = "claude"
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="API Error: Repeated 529 Overloaded errors",
            stderr="",
            text=True,
        )

        provider = ClaudeCodeProvider()
        request = AgentRequest(prompt="Test")

        result = provider.run(request)

        assert result.decision == "approved"
        assert len(result.findings) > 0
        assert any("AI review failed" in f for f in result.findings)


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
