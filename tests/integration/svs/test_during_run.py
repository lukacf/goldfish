"""Integration tests for during-run SVS monitoring."""

import json
import os
import time
from itertools import count
from unittest.mock import MagicMock, patch

import pytest

from goldfish.io import flush_metrics, log_metric, runtime_log, should_stop
from goldfish.io.bootstrap import run_stage_with_svs
from goldfish.svs.config import SVSConfig


@pytest.fixture
def svs_env(tmp_path):
    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir()

    config = SVSConfig(
        ai_during_run_enabled=True,
        ai_during_run_interval_seconds=60,
        ai_during_run_min_metrics=10,
        ai_during_run_min_log_lines=1,
        agent_provider="null",
    )

    env = {
        "GOLDFISH_OUTPUTS_DIR": str(outputs_dir),
        "GOLDFISH_SVS_CONFIG": config.model_dump_json(),
        "GOLDFISH_SVS_STATS_ENABLED": "true",
        "GOLDFISH_SVS_CONTEXT": json.dumps({"stage_name": "test_stage"}),
    }

    with patch.dict(os.environ, env):
        yield outputs_dir


def test_full_during_run_flow(svs_env):
    outputs_dir = svs_env

    # Mock AI agent to request stop
    mock_agent = MagicMock()
    mock_result = MagicMock()
    # ReviewResult has response_text
    mock_result.response_text = '```json\n{"findings": [{"check": "test", "severity": "ERROR", "summary": "stop now"}], "request_stop": true, "stop_reason": "critical anomaly"}\n```'
    mock_agent.run.return_value = mock_result

    config = SVSConfig(
        ai_during_run_enabled=True,
        ai_during_run_interval_seconds=60,
        ai_during_run_min_metrics=10,
        ai_during_run_min_log_lines=1,
        ai_during_run_auto_stop=True,
        agent_provider="null",
    )

    time_gen = count(start=1000)

    with patch.dict(os.environ, {"GOLDFISH_SVS_CONFIG": config.model_dump_json()}):
        with patch("goldfish.svs.agent.get_agent_provider", return_value=mock_agent):
            # We want the monitor to run at least once
            # First 5 calls to wait() return False, then True to stop
            wait_results = [False] * 5 + [True]
            wait_iter = iter(wait_results)
            with patch("threading.Event.wait", side_effect=lambda timeout=None: next(wait_iter, True)):
                with patch("time.time", side_effect=lambda: float(next(time_gen))):

                    def stage_main_with_stop():
                        # Log enough metrics to trigger review
                        for i in range(10):
                            # The public log_metric adds type: metric automatically
                            log_metric("loss", 0.5, step=i)
                        runtime_log("ERROR: Something bad")
                        flush_metrics()

                        # Wait for monitor to write stop file
                        for _ in range(50):
                            if should_stop():
                                return 10
                            time.sleep(0.01)
                        return 0

                    exit_code = run_stage_with_svs(stage_main_with_stop)

                    assert exit_code == 10
                    assert should_stop() is True

                    # Verify findings file
                    findings_file = outputs_dir / ".goldfish" / "svs_findings_during.json"
                    assert findings_file.exists()
                    data = json.loads(findings_file.read_text())
                    assert data["history"][0]["request_stop"] is True
