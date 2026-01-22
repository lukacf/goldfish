"""SVS post-run AI review.

This module provides post-run review functionality that analyzes stage outputs
and statistics after a stage completes. It writes findings to
.goldfish/svs_findings.json for later aggregation.

Key behaviors:
- Skips when ai_post_run_enabled=False in config
- Skips when rate_limit_per_hour=0
- Handles errors gracefully (fails open - approves on error)
- Creates .goldfish directory if missing
- Records timing information
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.svs.agent import ReviewRequest, ToolPolicy

if TYPE_CHECKING:
    from goldfish.svs.agent import AgentProvider
    from goldfish.svs.config import SVSConfig

logger = logging.getLogger("goldfish.svs.post_run")


@dataclass
class PostRunReview:
    """Result from post-run AI review.

    Attributes:
        skipped: Whether the review was skipped (disabled or rate limited)
        decision: Review decision - "approved", "blocked", or "warned"
        findings: List of issues found (ERROR/WARNING/NOTE messages)
        stats: Stage output statistics that were reviewed
        duration_ms: Time taken for review in milliseconds
        response_text: Full response text from the AI agent (for audit trail)
        ml_outcome: ML assessment result (success/partial/miss/unknown) or None
        ml_metric_value: The actual metric value found by the AI, or None
    """

    skipped: bool
    decision: str
    findings: list[str]
    stats: dict
    duration_ms: int
    response_text: str = ""
    ml_outcome: str | None = None
    ml_metric_value: float | None = None


def _parse_ml_outcome(response_text: str) -> tuple[str | None, float | None]:
    """Parse ML_OUTCOME line from AI response.

    Expected format: ML_OUTCOME: metric_name=VALUE, outcome=OUTCOME
    Example: ML_OUTCOME: val_accuracy=0.85, outcome=success

    Args:
        response_text: Full response text from the AI agent

    Returns:
        Tuple of (ml_outcome, ml_metric_value) or (None, None) if not found
    """
    if not response_text:
        return None, None

    # Pattern matches: ML_OUTCOME: metric=value, outcome=success/partial/miss/unknown
    pattern = r"ML_OUTCOME:\s*\w+\s*=\s*([\d.]+)\s*,\s*outcome\s*=\s*(success|partial|miss|unknown)"
    match = re.search(pattern, response_text, re.IGNORECASE)

    if not match:
        return None, None

    try:
        metric_value = float(match.group(1))
        outcome = match.group(2).lower()
        return outcome, metric_value
    except (ValueError, IndexError):
        return None, None


def run_post_run_review(
    outputs_dir: Path,
    stats: dict | None,
    config: SVSConfig,
    agent: AgentProvider,
    run_context: dict | None = None,
) -> PostRunReview:
    """Run post-run AI review on stage outputs.

    Analyzes the outputs directory and statistics using the configured agent
    provider. Writes findings to .goldfish/svs_findings.json.

    Args:
        outputs_dir: Path to stage outputs directory
        stats: Statistics about the stage outputs (can be None)
        config: SVS configuration
        agent: Agent provider to use for review
        run_context: Optional run context with workspace, config_override, etc.
                     If not provided, attempts to read from svs_context.json.

    Returns:
        PostRunReview with decision, findings, and timing information

    Note:
        This function fails open - if an error occurs during review,
        it returns an approved decision with an error message in findings.
    """
    start_time = time.time()

    # Normalize stats to empty dict if None
    if stats is None:
        stats = {}

    # Load run context from file if not provided (inside container case)
    if run_context is None:
        context_file = outputs_dir / ".goldfish" / "svs_context.json"
        if context_file.exists():
            try:
                run_context = json.loads(context_file.read_text())
            except (json.JSONDecodeError, OSError):
                run_context = {}

    # Check if review is disabled
    if not config.ai_post_run_enabled:
        duration_ms = int((time.time() - start_time) * 1000)
        return PostRunReview(
            skipped=True,
            decision="approved",
            findings=[],
            stats=stats,
            duration_ms=duration_ms,
        )

    # Check rate limit
    if config.rate_limit_per_hour == 0:
        duration_ms = int((time.time() - start_time) * 1000)
        return PostRunReview(
            skipped=True,
            decision="approved",
            findings=[],
            stats=stats,
            duration_ms=duration_ms,
        )

    # Build review request
    # Post-run reviews must bypass permission prompts (non-interactive)
    tool_policy = ToolPolicy(
        permission_mode="bypassPermissions",
        allow_tools=["Read", "Glob", "Grep"],  # Read-only tools for reviewing
    )

    request = ReviewRequest(
        review_type="post_run",
        context={
            "outputs_dir": str(outputs_dir),
            "model": config.agent_model,
            "fallback_model": config.agent_fallback_model,
            "max_turns": config.agent_max_turns,
            "timeout_seconds": config.agent_timeout,
            "tool_policy": tool_policy,
            "test_mode": config.test_mode,
            "run_context": run_context or {},
        },
        stats=stats,
    )

    # Run agent
    response_text = ""
    try:
        result = agent.run(request)
        decision = result.decision
        findings = result.findings
        duration_ms = result.duration_ms
        response_text = result.response_text
    except Exception as e:
        # Fail open - approve on error but record the error
        logger.warning("Post-run review agent error: %s", e)
        decision = "approved"
        findings = [f"SVS post-run review error: {e}"]
        duration_ms = int((time.time() - start_time) * 1000)
        response_text = f"Error during review: {e}"

    # Parse ML outcome from response (if present)
    ml_outcome, ml_metric_value = _parse_ml_outcome(response_text)

    # Write findings to file
    _write_findings_file(
        outputs_dir, decision, findings, stats, duration_ms, response_text, ml_outcome, ml_metric_value
    )

    return PostRunReview(
        skipped=False,
        decision=decision,
        findings=findings,
        stats=stats,
        duration_ms=duration_ms,
        response_text=response_text,
        ml_outcome=ml_outcome,
        ml_metric_value=ml_metric_value,
    )


def _write_findings_file(
    outputs_dir: Path,
    decision: str,
    findings: list[str],
    stats: dict,
    duration_ms: int,
    response_text: str = "",
    ml_outcome: str | None = None,
    ml_metric_value: float | None = None,
) -> None:
    """Write findings to .goldfish/svs_findings.json.

    Creates .goldfish directory if it doesn't exist.
    Handles file write errors gracefully.

    Args:
        outputs_dir: Path to stage outputs directory
        decision: Review decision
        findings: List of finding messages
        stats: Stage output statistics
        duration_ms: Review duration in milliseconds
        response_text: Full AI response text for audit trail
        ml_outcome: ML assessment result (success/partial/miss/unknown)
        ml_metric_value: The actual metric value found by the AI
    """
    try:
        goldfish_dir = outputs_dir / ".goldfish"
        goldfish_dir.mkdir(parents=True, exist_ok=True)

        findings_path = goldfish_dir / "svs_findings.json"
        findings_data: dict = {"version": 1, "findings": [], "stats": {}}
        if findings_path.exists():
            try:
                findings_data = json.loads(findings_path.read_text())
            except Exception:
                findings_data = {"version": 1, "findings": [], "stats": {}}

        # Merge findings list
        existing_findings = findings_data.get("findings")
        if not isinstance(existing_findings, list):
            existing_findings = []
        merged_findings = existing_findings + [f for f in findings if f not in existing_findings]

        # Escalate decision only (blocked > warned > approved)
        severity_rank = {"approved": 0, "warned": 1, "blocked": 2}
        existing_decision = findings_data.get("decision") or "approved"
        new_decision = decision or "approved"
        merged_decision = (
            existing_decision
            if severity_rank.get(existing_decision, 0) >= severity_rank.get(new_decision, 0)
            else new_decision
        )

        # Merge stats (post-run overrides existing)
        merged_stats = findings_data.get("stats")
        if not isinstance(merged_stats, dict):
            merged_stats = {}
        merged_stats.update(stats or {})

        findings_data.update(
            {
                "version": 1,
                "decision": merged_decision,
                "findings": merged_findings,
                "stats": merged_stats,
                "duration_ms": duration_ms,
                "response_text": response_text,
                "ml_outcome": ml_outcome,
                "ml_metric_value": ml_metric_value,
            }
        )

        findings_path.write_text(json.dumps(findings_data, indent=2))
    except OSError as e:
        logger.warning("Failed to write svs_findings.json: %s", e)
