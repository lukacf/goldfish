"""During-Run AI Monitoring for SVS.

This module provides a background thread that periodically reviews metrics
and logs using an AI agent to detect training anomalies early.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from goldfish.svs.config import SVSConfig

logger = logging.getLogger(__name__)


class DuringRunMonitor(threading.Thread):
    """Background monitor that calls AI review during stage execution.

    Reviews metrics and logs periodically (e.g., every 5 minutes) and
    can request early termination if anomalies are detected.
    """

    def __init__(self, config: SVSConfig, outputs_dir: Path):
        super().__init__(name="DuringRunMonitor", daemon=True)
        self.config = config
        self.outputs_dir = outputs_dir
        self.goldfish_dir = outputs_dir / ".goldfish"
        self.metrics_file = self.goldfish_dir / "metrics.jsonl"
        self.logs_file = self.goldfish_dir / "logs.txt"
        self.findings_file = self.goldfish_dir / "svs_findings_during.json"
        self.stop_requested_file = self.goldfish_dir / "stop_requested"
        self.state_file = self.goldfish_dir / "svs_monitor_state.json"
        self.context_file = self.goldfish_dir / "svs_context.json"

        self._stop_event = threading.Event()

        # Incremental state
        self.metrics_offset = 0
        self.logs_offset = 0
        self.last_review_time = 0.0
        self.reviews_this_hour = 0
        self.hour_start = time.time()

        # Failure tracking - back off after consecutive failures
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3  # After this, disable for the run
        self.ai_review_disabled = False

        # Load existing state if available
        self._load_state()

    def _load_state(self) -> None:
        """Load monitor state from disk."""
        if not self.state_file.exists():
            return
        try:
            state = json.loads(self.state_file.read_text())
            if not isinstance(state, dict):
                logger.warning("Monitor state is not a dict, ignoring")
                return
            self.metrics_offset = state.get("metrics_offset", 0)
            self.logs_offset = state.get("logs_offset", 0)
            self.last_review_time = state.get("last_review_time", 0.0)
            self.reviews_this_hour = state.get("reviews_this_hour", 0)
            self.hour_start = state.get("hour_start", time.time())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load monitor state: {e}")

    def _save_state(self) -> None:
        """Save monitor state atomically."""
        state = {
            "metrics_offset": self.metrics_offset,
            "logs_offset": self.logs_offset,
            "last_review_time": self.last_review_time,
            "reviews_this_hour": self.reviews_this_hour,
            "hour_start": self.hour_start,
        }
        try:
            temp_path = self.state_file.with_suffix(".tmp")
            temp_path.write_text(json.dumps(state, indent=2))
            temp_path.rename(self.state_file)
        except OSError as e:
            logger.error(f"Failed to save monitor state: {e}")

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the monitor to stop and wait for it."""
        self._stop_event.set()
        self.join(timeout=timeout)

    def run(self) -> None:
        """Main loop of the monitor."""
        logger.info("During-run SVS monitor started")
        interval = self.config.ai_during_run_interval_seconds

        while not self._stop_event.is_set():
            try:
                self._check_and_review()
            except Exception as e:  # Intentionally broad - monitor must not crash
                logger.error(f"Error in during-run monitor: {e}", exc_info=True)

            # Wait for interval or stop event
            self._stop_event.wait(timeout=min(interval, 60.0))

    def _check_and_review(self) -> None:
        """Check if we should perform a review and do it if so."""
        if not self.config.ai_during_run_enabled:
            return

        # Check if AI review was disabled due to repeated failures
        if self.ai_review_disabled:
            return

        # 1. Rate limiting check (Shared budget + Specific budget)
        now = time.time()
        if now - self.hour_start >= 3600:
            self.reviews_this_hour = 0
            self.hour_start = now

        # Specific during-run limit
        if self.reviews_this_hour >= self.config.ai_during_run_max_runs_per_hour:
            logger.debug(f"During-run review skipped: hourly limit reached ({self.reviews_this_hour})")
            return

        # Shared SVS budget (overall per-hour cap)
        if self.reviews_this_hour >= self.config.rate_limit_per_hour:
            logger.debug(f"During-run review skipped: rate limit reached ({self.reviews_this_hour})")
            return

        # 2. Minimum interval check
        time_since_last = now - self.last_review_time
        if time_since_last < self.config.ai_during_run_interval_seconds:
            logger.debug(
                f"During-run review skipped: interval not reached "
                f"({time_since_last:.0f}s < {self.config.ai_during_run_interval_seconds}s)"
            )
            return

        # 3. Data sufficiency check
        metrics, new_metrics_offset = self._read_new_metrics()
        logs, new_logs_offset = self._read_new_logs()
        num_log_lines = len(logs.splitlines())

        if (
            len(metrics) < self.config.ai_during_run_min_metrics
            and num_log_lines < self.config.ai_during_run_min_log_lines
        ):
            logger.info(
                f"During-run review skipped: insufficient data "
                f"(metrics={len(metrics)}/{self.config.ai_during_run_min_metrics}, "
                f"logs={num_log_lines}/{self.config.ai_during_run_min_log_lines})"
            )
            return

        # 4. Perform review
        logger.info(f"Performing during-run AI review ({len(metrics)} metrics, {num_log_lines} log lines)")
        success = self._do_review(metrics, logs)

        if success:
            self.last_review_time = now
            self.reviews_this_hour += 1
            self.metrics_offset = new_metrics_offset
            self.logs_offset = new_logs_offset
            self.consecutive_failures = 0  # Reset on success
        else:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                logger.warning(
                    f"During-run AI review disabled after {self.consecutive_failures} consecutive failures. "
                    "Check Claude CLI configuration or API availability."
                )
                self.ai_review_disabled = True
            self._save_state()

    def _read_new_metrics(self) -> tuple[list[dict], int]:
        """Read new metrics from JSONL file."""
        if not self.metrics_file.exists():
            return [], 0

        metrics: list[dict[str, Any]] = []
        offset = self.metrics_offset
        try:
            with open(self.metrics_file) as f:
                f.seek(offset)
                for line in f:
                    try:
                        data = json.loads(line)
                        if isinstance(data, dict) and data.get("type") == "metric":
                            metrics.append(data)
                    except json.JSONDecodeError:
                        continue
                offset = f.tell()
        except OSError as e:
            logger.error(f"Failed to read metrics: {e}")

        return metrics, offset

    def _read_new_logs(self) -> tuple[str, int]:
        """Read and filter new logs from text file."""
        if not self.logs_file.exists():
            return "", 0

        lines = []
        offset = self.logs_offset
        try:
            with open(self.logs_file) as f:
                # Handle truncation atomically: check size AFTER opening file
                # This avoids TOCTOU race between stat() and open()
                f.seek(0, 2)  # Seek to end
                file_size = f.tell()
                if file_size < offset:
                    logger.info(f"Log file truncated ({file_size} < {offset}), resetting offset")
                    offset = 0
                f.seek(offset)
                for line in f:
                    if self._should_keep_log_line(line):
                        lines.append(line)
                offset = f.tell()
        except OSError as e:
            logger.error(f"Failed to read logs: {e}")

        # Enforce max lines (before bytes)
        max_lines = self.config.ai_during_run_log_max_lines
        if len(lines) > max_lines:
            lines = lines[-max_lines:]

        # Truncate if too large (bytes)
        result = "".join(lines)
        if len(result) > self.config.ai_during_run_log_max_bytes:
            result = result[-self.config.ai_during_run_log_max_bytes :]

        return result, offset

    def _should_keep_log_line(self, line: str) -> bool:
        """Check if log line matches any filter patterns."""
        for pattern in self.config.ai_during_run_log_filters:
            if re.search(pattern, line):
                return True
        return False

    def _do_review(self, metrics: list[dict], logs: str) -> bool:
        """Call AI agent for review and save findings."""
        from goldfish.svs.agent import ReviewRequest, ToolPolicy, get_agent_provider

        agent = get_agent_provider(self.config.agent_provider)

        # Prepare context
        context = self._get_context()
        metrics_summary = self._summarize_metrics(metrics)

        prompt = self._build_prompt(context, metrics_summary, logs)

        # During-run reviews must bypass permission prompts (non-interactive)
        tool_policy = ToolPolicy(
            permission_mode="bypassPermissions",
            allow_tools=["Read", "Glob", "Grep"],  # Read-only tools for reviewing
        )

        request = ReviewRequest(
            review_type="during_run",
            context={
                "prompt": prompt,
                "output_format": "json",
                "model": self.config.agent_model,
                "max_turns": self.config.agent_max_turns,
                "timeout_seconds": self.config.agent_timeout,
                "tool_policy": tool_policy,
            },
            stats=None,  # We pass stats in the prompt
        )

        try:
            result = agent.run(request)
            # ReviewResult has response_text, not review/success
            if not result.response_text:
                # Only warn on first failure, then debug to reduce spam
                log_fn = logger.warning if self.consecutive_failures == 0 else logger.debug
                log_fn("During-run AI review returned empty response")
                return False

            # Parse findings
            parsed = self._parse_json_response(result.response_text)
            if not parsed:
                # Only warn on first failure, then debug to reduce spam
                log_fn = logger.warning if self.consecutive_failures == 0 else logger.debug
                log_fn("Failed to parse JSON from AI review")
                return False

            self._save_findings(parsed)

            if parsed.get("request_stop") and self.config.ai_during_run_auto_stop:
                self._request_stop(parsed.get("stop_reason", "AI detected critical anomaly"))

            return True
        except (OSError, TimeoutError, RuntimeError) as e:
            log_fn = logger.warning if self.consecutive_failures == 0 else logger.debug
            log_fn(f"Error calling AI agent: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in during-run review: {e}")
            return False

    def _get_context(self) -> dict[str, Any]:
        """Load stage context from file."""
        if not self.context_file.exists():
            return {}
        try:
            data = json.loads(self.context_file.read_text())
            if not isinstance(data, dict):
                logger.warning("Stage context is not a dict, ignoring")
                return {}
            return data
        except (json.JSONDecodeError, OSError):
            return {}

    def _summarize_metrics(self, metrics: list[dict]) -> str:
        """Create a compact summary of metrics for the AI prompt."""
        if not metrics:
            return "No new metrics recorded."

        # Group by name and get min/max/last
        stats: dict[str, dict] = {}
        for m in metrics:
            name = m["name"]
            val = m["value"]
            if name not in stats:
                stats[name] = {"min": val, "max": val, "last": val, "count": 1}
            else:
                s = stats[name]
                s["min"] = min(s["min"], val)
                s["max"] = max(s["max"], val)
                s["last"] = val
                s["count"] += 1

        lines = ["Metric Summaries (new data since last review):"]
        for name, s in stats.items():
            lines.append(
                f"- {name}: min={s['min']:.4f}, max={s['max']:.4f}, last={s['last']:.4f} ({s['count']} samples)"
            )

        return "\n".join(lines)

    def _build_prompt(self, context: dict, metrics_summary: str, logs: str) -> str:
        """Build the prompt for the AI agent."""
        stage_name = context.get("stage_name", "unknown")
        workspace = context.get("workspace", "unknown")
        pipeline_name = context.get("pipeline_name")
        config_override = context.get("config_override")
        inputs_override = context.get("inputs_override")
        run_reason = context.get("run_reason", {})

        # Test mode instructions
        test_mode_section = ""
        if self.config.test_mode:
            test_mode_section = """
## TEST MODE ENABLED
This is a test run to verify the AI review system works. You MUST:
1. Always provide at least one finding, even if just a NOTE-level observation
2. Look for [SVS-TEST] markers in logs - these indicate intentional test triggers
3. Comment on metrics trends (improving/degrading) even if not anomalous
4. Be verbose - this is testing the review pipeline, not production monitoring
"""

        # Build run command section
        run_command_parts = [f"workspace={workspace}", f"stage={stage_name}"]
        if pipeline_name:
            run_command_parts.append(f"pipeline={pipeline_name}")
        if config_override:
            run_command_parts.append(f"config_override={json.dumps(config_override)}")
        if inputs_override:
            run_command_parts.append(f"inputs_override={json.dumps(inputs_override)}")
        run_command_str = ", ".join(run_command_parts)

        prompt = f"""You are an expert ML monitoring agent. You are reviewing a training run for stage '{stage_name}'.
{test_mode_section}
## Run Command
run({run_command_str})

## Context
Goal: {run_reason.get('goal', 'Unknown')}
Hypothesis: {run_reason.get('hypothesis', 'Unknown')}

## New Metrics (Summary)
{metrics_summary}

## Filtered Logs (New Entries)
{logs}

## Task
Analyze the metrics and logs for anomalies, such as:
1. Diverging loss or exploding gradients (NaN/Inf)
2. Sudden performance drops
3. Error messages related to CUDA, OOM, or hardware
4. Lack of progress (stalling)

## Output Format
Respond ONLY with a JSON block fenced with ```json:
{{
  "findings": [
    {{
      "check": "check_name",
      "severity": "WARN|ERROR",
      "summary": "Detailed explanation of the issue"
    }}
  ],
  "request_stop": true/false,
  "stop_reason": "Explanation if request_stop is true"
}}
"""
        return prompt

    def _parse_json_response(self, text: str) -> dict[str, Any] | None:
        """Extract and parse JSON from fenced blocks or Claude CLI wrapper."""
        # First, check if this is a Claude CLI wrapper response
        # Format: {"type":"result","subtype":"success","result":"```json\n{...}\n```"}
        try:
            wrapper = json.loads(text)
            if isinstance(wrapper, dict) and wrapper.get("type") == "result":
                # Extract the result field which contains the actual response
                result_text = wrapper.get("result", "")
                if result_text:
                    text = result_text
        except json.JSONDecodeError:
            pass  # Not a wrapper, continue with original text

        # Look for JSON in markdown fences
        match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass

        # Try raw JSON if no fence
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass

        return None

    def _save_findings(self, parsed: dict[str, Any]) -> None:
        """Save AI findings to svs_findings_during.json."""
        findings = parsed.get("findings", [])
        if not findings:
            return

        history_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "findings": findings,
            "request_stop": parsed.get("request_stop", False),
            "stop_reason": parsed.get("stop_reason"),
        }

        data: dict[str, Any] = {"version": 1, "history": []}
        if self.findings_file.exists():
            try:
                loaded = json.loads(self.findings_file.read_text())
                if isinstance(loaded, dict):
                    data = loaded
            except (json.JSONDecodeError, OSError):
                pass

        history = data.setdefault("history", [])
        if isinstance(history, list):
            history.append(history_entry)

        try:
            temp_path = self.findings_file.with_suffix(".tmp")
            temp_path.write_text(json.dumps(data, indent=2))
            temp_path.rename(self.findings_file)
        except OSError as e:
            logger.error(f"Failed to save findings: {e}")

    def _request_stop(self, reason: str) -> None:
        """Request early termination by writing the stop_requested file."""
        logger.warning(f"SVS requesting stop: {reason}")
        try:
            temp_path = self.stop_requested_file.with_suffix(".tmp")
            temp_path.write_text(reason)
            temp_path.rename(self.stop_requested_file)
        except OSError as e:
            logger.error(f"Failed to request stop: {e}")
