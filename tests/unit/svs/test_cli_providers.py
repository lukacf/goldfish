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
            permission_mode="ask",
            allow_tools=["read_file", "list_directory"],
            deny_tools=["run_shell_command"],
            mcp_servers=["github", "google-search"],
        )
        request = AgentRequest(prompt="Test", tool_policy=policy)

        provider.run(request)

        cmd = mock_run.call_args[0][0]
        assert "--permission-mode" in cmd
        assert "ask" in cmd
        assert "--allow-tool" in cmd
        assert "read_file" in cmd
        assert "list_directory" in cmd
        assert "--deny-tool" in cmd
        assert "run_shell_command" in cmd
        assert "--mcp-server" in cmd
        assert "github" in cmd
        assert "google-search" in cmd


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
