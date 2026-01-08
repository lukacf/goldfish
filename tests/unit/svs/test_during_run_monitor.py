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
