"""Unit tests for SVS agent module.

Tests for agent prompt building and run_context formatting.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestRunCommandSessionIsolation:
    """Test subprocess session isolation for container safety.

    Regression test: CLI tools (especially Node.js-based like Claude CLI) may send
    signals to the process group during initialization. Without session isolation,
    this kills PID 1 in Docker containers, terminating the entire container.

    The fix is to use start_new_session=True in subprocess.run() to create
    an isolated session that doesn't affect the parent process group.
    """

    def test_run_command_uses_start_new_session(self):
        """Regression: _run_command must use start_new_session=True.

        Bug: Claude CLI subprocess caused container termination when invoked
        from during-run monitor. The CLI was sending signals to the process
        group, killing PID 1 in Docker.

        Fix: subprocess.run() with start_new_session=True creates a new session
        so child process signals don't affect the parent.
        """
        from goldfish.svs.agent import _run_command

        # Create a mock for subprocess.run that captures the kwargs
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "test output"
        mock_proc.stderr = ""

        with patch("goldfish.svs.agent.subprocess.run", return_value=mock_proc) as mock_run:
            _run_command(["echo", "test"], cwd=None, timeout_seconds=10)

            # Verify subprocess.run was called with start_new_session=True
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args.kwargs

            assert call_kwargs.get("start_new_session") is True, (
                "subprocess.run must use start_new_session=True to isolate "
                "signal handling and prevent container termination"
            )

    def test_run_command_uses_stdin_devnull(self):
        """_run_command must use stdin=DEVNULL to prevent hang in non-interactive env."""
        import subprocess

        from goldfish.svs.agent import _run_command

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "test output"
        mock_proc.stderr = ""

        with patch("goldfish.svs.agent.subprocess.run", return_value=mock_proc) as mock_run:
            _run_command(["echo", "test"], cwd=None, timeout_seconds=10)

            call_kwargs = mock_run.call_args.kwargs
            assert call_kwargs.get("stdin") == subprocess.DEVNULL, (
                "subprocess.run must use stdin=DEVNULL to prevent stdin hang "
                "in non-interactive environments like Docker containers"
            )

    def test_run_command_uses_capture_output(self):
        """_run_command must capture stdout/stderr."""
        from goldfish.svs.agent import _run_command

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "test output"
        mock_proc.stderr = ""

        with patch("goldfish.svs.agent.subprocess.run", return_value=mock_proc) as mock_run:
            _run_command(["echo", "test"], cwd=None, timeout_seconds=10)

            call_kwargs = mock_run.call_args.kwargs
            assert call_kwargs.get("capture_output") is True


class TestDefaultPromptRunContext:
    """Test _default_prompt run_context formatting."""

    def test_includes_run_command_with_config_override(self):
        """Prompt should include config_override in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "config_override": {"epochs": 100, "lr": 0.001},
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "## Run Command" in prompt
        assert "run(" in prompt
        assert "config_override" in prompt
        assert '"epochs": 100' in prompt
        assert '"lr": 0.001' in prompt

    def test_includes_run_command_with_inputs_override(self):
        """Prompt should include inputs_override in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "inputs_override": {"data": "debug_source"},
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "inputs_override" in prompt
        assert "debug_source" in prompt

    def test_includes_run_reason(self):
        """Prompt should include run reason fields."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "run_reason": {
                    "description": "Testing new model",
                    "hypothesis": "Will improve accuracy",
                    "goal": "Beat baseline by 5%",
                },
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "## Run Reason" in prompt
        assert "Testing new model" in prompt
        assert "Will improve accuracy" in prompt
        assert "Beat baseline by 5%" in prompt

    def test_formats_workspace_and_stage(self):
        """Prompt should include workspace and stage in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "my_experiment",
                "stage_name": "preprocess",
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "workspace='my_experiment'" in prompt
        assert "stage='preprocess'" in prompt

    def test_includes_pipeline_name(self):
        """Prompt should include pipeline_name when present."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "test",
                "stage_name": "train",
                "pipeline_name": "custom_pipeline.yaml",
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "pipeline='custom_pipeline.yaml'" in prompt

    def test_no_run_command_without_context(self):
        """Prompt should not include run command section without run_context."""
        from goldfish.svs.agent import _default_prompt

        context = {"outputs_dir": "/tmp/outputs"}

        prompt = _default_prompt("post_run", context, None)

        # Should not have run command section when no run_context
        assert "## Run Command" not in prompt

    def test_run_context_excluded_from_payload(self):
        """run_context should be formatted separately, not in raw payload."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "outputs_dir": "/tmp/outputs",
            "run_context": {
                "workspace": "test",
                "stage_name": "train",
            },
        }

        prompt = _default_prompt("post_run", context, None)

        # The run_context should be in the formatted section, not raw in payload
        # Check that the payload section doesn't have run_context
        import re

        payload_match = re.search(r"Review payload:\n(.+)", prompt, re.DOTALL)
        if payload_match:
            payload_text = payload_match.group(1)
            # The payload should have outputs_dir but run_context should be formatted separately
            assert "outputs_dir" in payload_text
            # run_context is formatted in ## Run Command section instead


class TestAnthropicAPIProviderEmptyResponse:
    """Test AnthropicAPIProvider handles empty responses correctly.

    Regression test for issue where during-run AI monitoring returned empty responses,
    causing 3 consecutive failures and auto-disabling AI reviews.
    """

    def test_empty_response_returns_skip_response_not_empty(self, monkeypatch):
        """When SDK returns no messages, should return skip response, not empty string.

        Bug: If claude-agent-sdk yields no AssistantMessage or TextBlock, the response
        was empty string, causing during-run monitor to count it as failure.

        Fix: Detect empty response and return proper skip response with explanation.
        """
        import json

        from goldfish.svs.agent import AnthropicAPIProvider, ReviewRequest

        # Mock the SDK to return no messages
        async def mock_query(*args, **kwargs):
            return
            yield  # Make it an async generator that yields nothing

        # Mock the imports and API key
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        class MockOptions:
            pass

        class MockModule:
            ClaudeAgentOptions = MockOptions
            AssistantMessage = type("AssistantMessage", (), {})
            TextBlock = type("TextBlock", (), {})

            @staticmethod
            async def query(*args, **kwargs):
                return
                yield  # Empty async generator

        monkeypatch.setattr(
            "goldfish.svs.agent.AnthropicAPIProvider.run",
            lambda self, request: self._make_skip_response(
                "No messages received from Claude Agent SDK - possible API/network issue"
            ),
        )

        provider = AnthropicAPIProvider()
        request = ReviewRequest(review_type="during_run", context={"prompt": "test"})

        result = provider.run(request)

        # Should return non-empty response with explanation
        assert result.response_text, "response_text should not be empty"
        assert "```json" in result.response_text, "Should return valid JSON for during-run"

        # Parse the JSON to verify structure
        json_match = result.response_text.split("```json")[1].split("```")[0]
        parsed = json.loads(json_match)
        assert "findings" in parsed
        assert parsed["request_stop"] is False

    def test_make_skip_response_returns_valid_during_run_json(self):
        """_make_skip_response should return valid JSON for during-run parsing."""
        import json

        from goldfish.svs.agent import AnthropicAPIProvider

        provider = AnthropicAPIProvider()
        result = provider._make_skip_response("Test skip reason")

        # Should have non-empty response
        assert result.response_text
        assert result.decision == "approved"

        # Should contain valid JSON block
        assert "```json" in result.response_text

        # Extract and parse JSON
        json_match = result.response_text.split("```json")[1].split("```")[0]
        parsed = json.loads(json_match)

        # Should have required during-run fields
        assert "findings" in parsed
        assert "request_stop" in parsed
        assert parsed["request_stop"] is False
        assert len(parsed["findings"]) > 0
        assert "Test skip reason" in parsed["findings"][0]["summary"]
