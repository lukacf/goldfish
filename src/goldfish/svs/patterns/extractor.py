"""Failure pattern extraction using AI analysis.

This module extracts structured failure patterns from stage run failures
to enable self-learning failure detection and prevention.
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from goldfish.errors import GoldfishError
from goldfish.svs.agent import ReviewRequest, ToolPolicy

if TYPE_CHECKING:
    from goldfish.db.database import Database
    from goldfish.db.types import FailurePatternRow
    from goldfish.svs.agent import AgentProvider

logger = logging.getLogger(__name__)


# Rate limiting configuration
MAX_PATTERNS_PER_HOUR = 10

# Log truncation - preserve end of logs (most recent)
MAX_LOG_SIZE = 50_000  # 50KB


class PatternExtractionError(GoldfishError):
    """Error during pattern extraction."""

    pass


class RateLimitExceededError(GoldfishError):
    """Rate limit exceeded for pattern extraction."""

    pass


@dataclass
class FailurePattern:
    """Structured failure pattern extracted from a failure.

    Attributes:
        id: Unique pattern ID (based on symptom hash)
        symptom: What went wrong (observable error)
        root_cause: Why it happened (underlying cause)
        detection_heuristic: How to detect it (pattern matching logic)
        prevention: How to prevent it (remediation steps)
        severity: CRITICAL, HIGH, MEDIUM, or LOW
        confidence: HIGH, MEDIUM, or LOW (confidence in analysis)
        source_run_id: Stage run that triggered extraction
        source_workspace: Workspace where failure occurred
        stage_type: Stage type (train, preprocess, etc.) or None for generic
    """

    id: str
    symptom: str
    root_cause: str
    detection_heuristic: str
    prevention: str
    severity: str  # CRITICAL | HIGH | MEDIUM | LOW
    confidence: str  # HIGH | MEDIUM | LOW
    source_run_id: str | None
    source_workspace: str | None
    stage_type: str | None = None


def _compute_symptom_hash(symptom: str) -> str:
    """Compute hash for symptom deduplication.

    Uses lowercase normalized symptom to handle case variations.
    """
    normalized = symptom.lower()
    return hashlib.sha256(normalized.encode()).hexdigest()


def _truncate_logs(logs: str | None) -> str:
    """Truncate logs to prevent token overflow, preserving end (most recent)."""
    if not logs:
        return ""
    if len(logs) <= MAX_LOG_SIZE:
        return logs
    # Preserve the END of logs (most recent)
    return "[... logs truncated ...]\n" + logs[-MAX_LOG_SIZE:]


def _check_rate_limit(db: Database) -> None:
    """Check if rate limit has been exceeded.

    Raises:
        RateLimitExceededError: If more than MAX_PATTERNS_PER_HOUR created recently
    """
    one_hour_ago = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

    with db._conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as count FROM failure_patterns WHERE created_at > ?",
            (one_hour_ago,),
        ).fetchone()

    if row and row["count"] >= MAX_PATTERNS_PER_HOUR:
        raise RateLimitExceededError(
            f"Rate limit exceeded: {row['count']} patterns created in the last hour " f"(max: {MAX_PATTERNS_PER_HOUR})"
        )


def _get_stage_info(db: Database, stage_run_id: str) -> tuple[str, str]:
    """Get workspace name and stage type from stage run.

    Returns:
        Tuple of (workspace_name, stage_name)
    """
    with db._conn() as conn:
        row = conn.execute(
            "SELECT workspace_name, stage_name FROM stage_runs WHERE id = ?",
            (stage_run_id,),
        ).fetchone()

    if not row:
        raise PatternExtractionError(f"Stage run not found: {stage_run_id}")

    return row["workspace_name"], row["stage_name"]


def _parse_findings(findings: list[str]) -> dict[str, str]:
    """Parse structured findings from agent response.

    Expected format:
    - SYMPTOM: description
    - ROOT_CAUSE: description
    - DETECTION: description
    - PREVENTION: description
    - SEVERITY: CRITICAL|HIGH|MEDIUM|LOW
    - CONFIDENCE: HIGH|MEDIUM|LOW
    """
    result: dict[str, str] = {}
    patterns = {
        "SYMPTOM": r"SYMPTOM:\s*(.+)",
        "ROOT_CAUSE": r"ROOT_CAUSE:\s*(.+)",
        "DETECTION": r"DETECTION:\s*(.+)",
        "PREVENTION": r"PREVENTION:\s*(.+)",
        "SEVERITY": r"SEVERITY:\s*(CRITICAL|HIGH|MEDIUM|LOW)",
        "CONFIDENCE": r"CONFIDENCE:\s*(HIGH|MEDIUM|LOW)",
    }

    for finding in findings:
        for key, pattern in patterns.items():
            match = re.match(pattern, finding, re.IGNORECASE)
            if match:
                result[key] = match.group(1).strip()
                break

    return result


def _find_duplicate(db: Database, symptom: str) -> FailurePatternRow | None:
    """Find existing pattern with same symptom hash.

    Returns:
        Existing pattern row if found, None otherwise
    """
    symptom_hash = _compute_symptom_hash(symptom)

    # Get all non-archived patterns
    patterns = db.list_failure_patterns(limit=1000)

    for pattern in patterns:
        if pattern["status"] == "archived":
            continue
        if _compute_symptom_hash(pattern["symptom"]) == symptom_hash:
            return pattern

    return None


def extract_failure_pattern(
    db: Database,
    stage_run_id: str,
    error: str,
    logs: str | None,
    agent: AgentProvider,
) -> FailurePattern | None:
    """Extract failure pattern from a stage run failure using AI analysis.

    This function analyzes the error message and logs from a failed stage run
    to extract a structured failure pattern. It uses an AI agent to perform
    the analysis and extract symptom, root cause, detection heuristic, and
    prevention steps.

    The function implements:
    - Deduplication: Same symptom hash updates occurrence_count
    - Rate limiting: Max patterns per hour
    - Graceful timeout handling: Returns None on agent timeout
    - Log truncation: Prevents token overflow for large logs

    Args:
        db: Database instance for storing patterns
        stage_run_id: ID of the failed stage run
        error: Error message from the failure
        logs: Log output from the failure (may be None or empty)
        agent: AI agent provider for pattern analysis

    Returns:
        FailurePattern if extraction succeeds, None if agent fails/times out

    Raises:
        RateLimitExceededError: If rate limit is exceeded
        PatternExtractionError: If extraction fails for other reasons
    """
    logger.info(f"Starting failure pattern extraction for {stage_run_id}")
    logger.debug(f"Error preview: {error[:200] if error else 'none'}")

    # 1. Check rate limit
    _check_rate_limit(db)

    # 2. Get stage info
    workspace_name, stage_name = _get_stage_info(db, stage_run_id)

    # 3. Truncate logs
    truncated_logs = _truncate_logs(logs)

    # 4. Call agent
    try:
        prompt = (
            "You are analyzing a failed ML stage run. Return ONLY the following lines:\n"
            "SYMPTOM: <what went wrong>\n"
            "ROOT_CAUSE: <why it happened>\n"
            "DETECTION: <how to detect it>\n"
            "PREVENTION: <how to prevent it>\n"
            "SEVERITY: CRITICAL|HIGH|MEDIUM|LOW\n"
            "CONFIDENCE: HIGH|MEDIUM|LOW\n"
            "\n"
            "Context:\n"
            f"Stage: {stage_name}\n"
            f"Error: {error}\n"
            f"Logs:\n{truncated_logs}\n"
        )
        # Pattern extraction must bypass permission prompts (non-interactive)
        tool_policy = ToolPolicy(
            permission_mode="bypassPermissions",
            allow_tools=["Read", "Glob", "Grep"],
        )

        request = ReviewRequest(
            review_type="pattern_extraction",
            context={
                "prompt": prompt,
                "output_format": "text",
                "error": error,
                "logs": truncated_logs,
                "stage_run_id": stage_run_id,
                "workspace": workspace_name,
                "stage_type": stage_name,
                "timeout_seconds": 120,  # 2-minute timeout for pattern extraction
                "max_turns": 1,  # Single turn - just analyze and respond
                "tool_policy": tool_policy,
            },
        )
        result = agent.run(request)
    except (TimeoutError, Exception) as e:
        logger.warning(f"Agent failed during pattern extraction: {e}")
        return None

    # 5. Parse findings from raw response text (not result.findings which only has ERROR/WARNING/NOTE)
    # Split response text into lines for parsing
    logger.info(
        f"Agent returned for {stage_run_id}: decision={result.decision}, response_len={len(result.response_text) if result.response_text else 0}"
    )
    response_lines = result.response_text.splitlines() if result.response_text else []
    parsed = _parse_findings(response_lines)
    logger.debug(f"Parsed fields: {list(parsed.keys())}")

    if not parsed.get("SYMPTOM"):
        logger.warning(
            f"Agent did not return valid SYMPTOM in response. Raw output: {result.response_text[:500] if result.response_text else 'empty'}"
        )
        return None

    symptom = parsed["SYMPTOM"]
    root_cause = parsed.get("ROOT_CAUSE", "Unknown")
    detection = parsed.get("DETECTION", "Unknown")
    prevention = parsed.get("PREVENTION", "Unknown")
    severity = parsed.get("SEVERITY", "MEDIUM")
    confidence = parsed.get("CONFIDENCE", "MEDIUM")

    # 6. Check for duplicate
    existing = _find_duplicate(db, symptom)
    if existing:
        # Update occurrence count
        logger.info(f"Found existing pattern {existing['id']}, incrementing occurrence count")
        db.increment_pattern_occurrence(
            existing["id"],
            datetime.now(UTC).isoformat(),
        )
        return FailurePattern(
            id=existing["id"],
            symptom=existing["symptom"],
            root_cause=existing["root_cause"],
            detection_heuristic=existing["detection_heuristic"],
            prevention=existing["prevention"],
            severity=existing["severity"] or severity,
            confidence=existing["confidence"] or confidence,
            source_run_id=existing["source_run_id"],
            source_workspace=existing["source_workspace"],
            stage_type=existing["stage_type"],
        )

    # 7. Create new pattern
    pattern_id = f"fp-{str(uuid.uuid4())[:8]}"
    created_at = datetime.now(UTC).isoformat()
    logger.info(f"Creating new failure pattern {pattern_id}: {symptom[:50]}...")

    db.create_failure_pattern(
        pattern_id=pattern_id,
        symptom=symptom,
        root_cause=root_cause,
        detection_heuristic=detection,
        prevention=prevention,
        created_at=created_at,
        severity=severity,
        stage_type=stage_name,
        source_run_id=stage_run_id,
        source_workspace=workspace_name,
        confidence=confidence,
    )

    return FailurePattern(
        id=pattern_id,
        symptom=symptom,
        root_cause=root_cause,
        detection_heuristic=detection,
        prevention=prevention,
        severity=severity,
        confidence=confidence,
        source_run_id=stage_run_id,
        source_workspace=workspace_name,
        stage_type=stage_name,
    )
