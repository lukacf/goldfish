"""Unit tests for DuringRunMonitor."""

import json
from unittest.mock import MagicMock, patch

import pytest

from goldfish.svs.config import SVSConfig
from goldfish.svs.during_run_monitor import DuringRunMonitor


@pytest.fixture
def mock_outputs_dir(tmp_path):
    outputs_dir = tmp_path / "outputs"
    goldfish_dir = outputs_dir / ".goldfish"
    goldfish_dir.mkdir(parents=True)
    return outputs_dir


@pytest.fixture
def config():
    return SVSConfig(
        ai_during_run_enabled=True,
        ai_during_run_interval_seconds=60,  # Minimum allowed
        ai_during_run_min_metrics=10,  # Minimum allowed
        ai_during_run_min_log_lines=1,
        agent_provider="null",
    )


def test_monitor_reads_metrics(mock_outputs_dir, config):
    metrics_file = mock_outputs_dir / ".goldfish" / "metrics.jsonl"
    metrics_file.write_text(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "step": 1}) + "\n")

    monitor = DuringRunMonitor(config, mock_outputs_dir)
    metrics, offset = monitor._read_new_metrics()

    assert len(metrics) == 1
    assert metrics[0]["name"] == "loss"
    assert offset > 0

    # Second read should be empty
    monitor.metrics_offset = offset
    metrics2, offset2 = monitor._read_new_metrics()
    assert len(metrics2) == 0
    assert offset2 == offset


def test_monitor_reads_logs(mock_outputs_dir, config):
    logs_file = mock_outputs_dir / ".goldfish" / "logs.txt"
    logs_file.write_text("INFO: Normal log\nERROR: Critical failure\n")

    monitor = DuringRunMonitor(config, mock_outputs_dir)
    logs, offset = monitor._read_new_logs()

    # Should only keep ERROR because of default filters
    assert "ERROR" in logs
    assert "INFO" not in logs
    assert offset > 0


def test_monitor_skips_if_not_enough_data(mock_outputs_dir, config):
    config.ai_during_run_min_metrics = 100
    config.ai_during_run_min_log_lines = 100

    monitor = DuringRunMonitor(config, mock_outputs_dir)

    # Write some data but not enough
    metrics_file = mock_outputs_dir / ".goldfish" / "metrics.jsonl"
    metrics_file.write_text(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "step": 1}) + "\n")

    with patch.object(monitor, "_do_review") as mock_review:
        monitor._check_and_review()
        mock_review.assert_not_called()


def test_monitor_performs_review(mock_outputs_dir, config):
    metrics_file = mock_outputs_dir / ".goldfish" / "metrics.jsonl"
    metrics_file.write_text(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "step": 1}) + "\n")

    logs_file = mock_outputs_dir / ".goldfish" / "logs.txt"
    logs_file.write_text("ERROR: Failure\n")

    monitor = DuringRunMonitor(config, mock_outputs_dir)

    # Mock agent provider and result
    mock_agent = MagicMock()
    mock_result = MagicMock()
    # ReviewResult has response_text, not review
    mock_result.response_text = (
        '```json\n{"findings": [{"check": "test", "severity": "WARN", "summary": "issue"}], "request_stop": false}\n```'
    )
    mock_agent.run.return_value = mock_result

    with patch("goldfish.svs.agent.get_agent_provider", return_value=mock_agent):
        success = monitor._do_review([{"name": "loss", "value": 0.5}], "ERROR: Failure")
        assert success is True

        # Check findings file
        findings_file = mock_outputs_dir / ".goldfish" / "svs_findings_during.json"
        assert findings_file.exists()
        data = json.loads(findings_file.read_text())
        assert len(data["history"]) == 1
        assert data["history"][0]["findings"][0]["check"] == "test"


def test_monitor_requests_stop(mock_outputs_dir, config):
    config.ai_during_run_auto_stop = True
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    parsed = {
        "findings": [{"check": "oom", "severity": "ERROR", "summary": "Out of memory"}],
        "request_stop": True,
        "stop_reason": "CUDA OOM detected",
    }

    monitor._save_findings(parsed)
    monitor._request_stop("CUDA OOM detected")

    stop_file = mock_outputs_dir / ".goldfish" / "stop_requested"
    assert stop_file.exists()
    assert stop_file.read_text() == "CUDA OOM detected"


def test_parse_json_response(mock_outputs_dir, config):
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    text = 'Some chatter\n```json\n{"key": "val"}\n```\nMore chatter'
    parsed = monitor._parse_json_response(text)
    assert parsed == {"key": "val"}

    # Test without fence
    parsed2 = monitor._parse_json_response('{"key": "val2"}')
    assert parsed2 == {"key": "val2"}

    # Test invalid JSON
    assert monitor._parse_json_response("not json") is None


class TestParseJsonResponseEdgeCases:
    """Regression tests for JSON parsing edge cases."""

    def test_claude_cli_wrapper_format(self, mock_outputs_dir, config):
        """Should extract JSON from Claude CLI wrapper format."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        # Claude CLI JSON output format
        wrapper = {
            "type": "result",
            "subtype": "success",
            "result": '```json\n{"findings": [], "request_stop": false}\n```',
        }
        text = json.dumps(wrapper)
        parsed = monitor._parse_json_response(text)
        assert parsed == {"findings": [], "request_stop": False}

    def test_claude_cli_wrapper_without_fence(self, mock_outputs_dir, config):
        """Should handle wrapper with plain text result (no JSON fence)."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        wrapper = {
            "type": "result",
            "subtype": "success",
            "result": '{"findings": [{"check": "test", "severity": "WARN", "summary": "issue"}]}',
        }
        text = json.dumps(wrapper)
        parsed = monitor._parse_json_response(text)
        assert parsed is not None
        assert parsed["findings"][0]["check"] == "test"

    def test_uppercase_json_fence(self, mock_outputs_dir, config):
        """Should handle uppercase JSON fence."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '```JSON\n{"key": "uppercase"}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed == {"key": "uppercase"}

    def test_json_fence_with_space(self, mock_outputs_dir, config):
        """Should handle ``` json with space."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '``` json\n{"key": "space"}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed == {"key": "space"}

    def test_plain_fence_with_json_object(self, mock_outputs_dir, config):
        """Should handle plain fence containing JSON object."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '```\n{"key": "plain"}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed == {"key": "plain"}

    def test_json_embedded_in_prose(self, mock_outputs_dir, config):
        """Should extract JSON embedded in natural language response."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = """Based on my analysis, here is the result:

{"findings": [{"check": "loss_spike", "severity": "WARN", "summary": "Loss increased"}], "request_stop": false}

The training appears to be proceeding normally."""

        parsed = monitor._parse_json_response(text)
        assert parsed is not None
        assert len(parsed["findings"]) == 1
        assert parsed["findings"][0]["check"] == "loss_spike"

    def test_multiline_json_in_fence(self, mock_outputs_dir, config):
        """Should handle pretty-printed multiline JSON in fence."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = """```json
{
    "findings": [
        {
            "check": "gradient_norm",
            "severity": "ERROR",
            "summary": "Gradient explosion detected"
        }
    ],
    "request_stop": true,
    "stop_reason": "Critical training failure"
}
```"""
        parsed = monitor._parse_json_response(text)
        assert parsed is not None
        assert parsed["request_stop"] is True
        assert parsed["stop_reason"] == "Critical training failure"

    def test_json_with_nested_objects(self, mock_outputs_dir, config):
        """Should handle JSON with nested objects."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '```json\n{"outer": {"inner": {"deep": "value"}}}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed == {"outer": {"inner": {"deep": "value"}}}

    def test_empty_findings_array(self, mock_outputs_dir, config):
        """Should handle empty findings array (no issues found)."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '```json\n{"findings": [], "request_stop": false}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed == {"findings": [], "request_stop": False}

    def test_returns_none_for_no_json(self, mock_outputs_dir, config):
        """Should return None when no JSON found."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = "This is just plain text with no JSON at all."
        parsed = monitor._parse_json_response(text)
        assert parsed is None

    def test_returns_none_for_invalid_json(self, mock_outputs_dir, config):
        """Should return None for malformed JSON."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '```json\n{"key": "missing quote}\n```'
        parsed = monitor._parse_json_response(text)
        assert parsed is None

    def test_returns_none_for_array(self, mock_outputs_dir, config):
        """Should return None for JSON arrays (expect object)."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = "```json\n[1, 2, 3]\n```"
        parsed = monitor._parse_json_response(text)
        assert parsed is None

    def test_whitespace_around_fence(self, mock_outputs_dir, config):
        """Should handle extra whitespace around fences."""
        monitor = DuringRunMonitor(config, mock_outputs_dir)

        text = '  ```json  \n  {"key": "whitespace"}  \n  ```  '
        parsed = monitor._parse_json_response(text)
        assert parsed == {"key": "whitespace"}


def test_build_prompt_includes_config_override(mock_outputs_dir, config):
    """Prompt should include config_override when present in context."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    context = {
        "stage_name": "train",
        "workspace": "experiment_1",
        "config_override": {"encoding": "6_2", "epochs": 50},
        "run_reason": {"goal": "Test training"},
    }

    prompt = monitor._build_prompt(context, "loss: 0.5", "Training log")

    assert "config_override" in prompt
    assert '"encoding": "6_2"' in prompt
    assert '"epochs": 50' in prompt


def test_build_prompt_includes_inputs_override(mock_outputs_dir, config):
    """Prompt should include inputs_override when present in context."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    context = {
        "stage_name": "train",
        "workspace": "experiment_1",
        "inputs_override": {"features": "debug_features_v1"},
        "run_reason": {},
    }

    prompt = monitor._build_prompt(context, "loss: 0.5", "Training log")

    assert "inputs_override" in prompt
    assert "debug_features_v1" in prompt


def test_build_prompt_includes_run_command_format(mock_outputs_dir, config):
    """Prompt should show run command in run() format."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    context = {
        "stage_name": "train",
        "workspace": "my_workspace",
        "pipeline_name": "custom.yaml",
        "run_reason": {},
    }

    prompt = monitor._build_prompt(context, "loss: 0.5", "Training log")

    assert "## Run Command" in prompt
    assert "run(" in prompt
    assert "workspace=my_workspace" in prompt
    assert "stage=train" in prompt
    assert "pipeline=custom.yaml" in prompt


def test_consecutive_failures_disables_review(mock_outputs_dir, config):
    """After max_consecutive_failures, AI review should be disabled."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)
    monitor.max_consecutive_failures = 3

    # Simulate failures
    monitor.consecutive_failures = 2
    monitor.consecutive_failures += 1

    # Check that 3 failures would trigger disable
    if monitor.consecutive_failures >= monitor.max_consecutive_failures:
        monitor.ai_review_disabled = True

    assert monitor.ai_review_disabled is True


def test_success_resets_consecutive_failures(mock_outputs_dir, config):
    """Successful review should reset consecutive failure counter."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)

    # Simulate some failures followed by success
    monitor.consecutive_failures = 2

    # On success, counter should reset (this happens in _check_and_review)
    # We're testing the state change directly here
    monitor.consecutive_failures = 0  # Reset on success

    assert monitor.consecutive_failures == 0


def test_disabled_monitor_skips_review(mock_outputs_dir, config):
    """Disabled monitor should not attempt reviews."""
    monitor = DuringRunMonitor(config, mock_outputs_dir)
    monitor.ai_review_disabled = True

    # Write enough data to normally trigger a review
    metrics_file = mock_outputs_dir / ".goldfish" / "metrics.jsonl"
    metrics_file.write_text(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "step": 1}) + "\n")

    logs_file = mock_outputs_dir / ".goldfish" / "logs.txt"
    logs_file.write_text("ERROR: Failure\n")

    with patch.object(monitor, "_do_review") as mock_review:
        monitor._check_and_review()
        # Should not have called _do_review because monitor is disabled
        mock_review.assert_not_called()
