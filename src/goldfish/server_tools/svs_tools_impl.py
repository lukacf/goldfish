"""Implementation helpers for SVS tools to avoid circular imports."""

import json
import logging
from typing import TYPE_CHECKING, Any

from goldfish.db.database import Database
from goldfish.errors import GoldfishError

if TYPE_CHECKING:
    from goldfish.svs.agent import AgentProvider
    from goldfish.svs.config import SVSConfig

logger = logging.getLogger("goldfish.server")


def _review_to_dict(review: Any) -> dict:
    return {
        "id": review["id"],
        "stage_run_id": review["stage_run_id"],
        "review_type": review["review_type"],
        "decision": review["decision"],
        "reviewed_at": review["reviewed_at"],
        "parsed_findings": review.get("parsed_findings"),
    }


def _pattern_to_dict(pattern: Any) -> dict:
    return {
        "id": pattern["id"],
        "symptom": pattern["symptom"],
        "root_cause": pattern["root_cause"],
        "status": pattern.get("status", "pending"),
        "severity": pattern.get("severity"),
        "created_at": pattern.get("created_at"),
    }


def get_run_svs_findings(db: Database, stage_run_id: str) -> dict:
    run = db.get_stage_run(stage_run_id)
    if run is None:
        return {"success": False, "error": f"Stage run not found: {stage_run_id}"}

    reviews = db.get_svs_reviews(stage_run_id=stage_run_id)
    all_patterns = db.list_failure_patterns(limit=1000)
    run_patterns = [p for p in all_patterns if p.get("source_run_id") == stage_run_id]

    return {
        "success": True,
        "stage_run_id": stage_run_id,
        "reviews": [_review_to_dict(r) for r in reviews],
        "failure_patterns": [_pattern_to_dict(p) for p in run_patterns],
    }


def review_pending_patterns(db: Database, dry_run: bool = True) -> dict:
    from goldfish.svs.patterns.manager import FailurePatternManager

    manager = FailurePatternManager(db)

    pending = db.list_failure_patterns(status="pending")

    if not pending:
        return {"success": True, "dry_run": dry_run, "actions": [], "message": "No pending patterns"}

    # Use the shared librarian logic

    recommendations = librarian_review_patterns([_pattern_to_dict(p) for p in pending])

    actions = []

    for pattern_id, rec in recommendations.items():
        actions.append(
            {
                "pattern_id": pattern_id,
                "action": rec.get("action", "skip"),
                "confidence": rec.get("confidence", "low"),
                "reason": rec.get("reason"),
            }
        )

    if not dry_run:
        for action in actions:
            if action["action"] == "approve":
                manager.approve_pattern(action["pattern_id"], approved_by="ai_librarian")

            elif action["action"] == "reject":
                manager.reject_pattern(action["pattern_id"], reason=action["reason"] or "Rejected by AI librarian")

    return {"success": True, "dry_run": dry_run, "actions": actions}


def librarian_review_patterns(
    patterns: list[dict],
    *,
    agent: "AgentProvider | None" = None,
    config: "SVSConfig | None" = None,
) -> dict[str, dict]:
    """AI librarian review of patterns.





    Args:


        patterns: List of pattern dicts to review


        agent: Optional AgentProvider override


        config: Optional SVSConfig override





    Returns:


        Dict mapping pattern_id to action recommendation


    """

    if not patterns:
        return {}

    if config is None:
        from goldfish.server_core import _get_config

        config = _get_config().svs

    if agent is None:
        from goldfish.svs.agent import get_agent_provider

        agent = get_agent_provider(config.agent_provider)

    prompt = (
        "You are an AI librarian reviewing failure patterns. For each pattern, decide:\n"
        "- action: approve | reject | skip\n"
        "- confidence: high | medium | low\n"
        "- reason: short justification\n\n"
        "Return ONLY valid JSON mapping pattern_id -> {action, confidence, reason}.\n\n"
        f"Patterns:\n{json.dumps(patterns, indent=2)}\n"
    )

    from goldfish.svs.agent import ReviewRequest

    request = ReviewRequest(
        review_type="pattern_review",
        context={
            "prompt": prompt,
            "output_format": "json",
            "model": config.agent_model,
            "timeout_seconds": config.agent_timeout,
            "max_turns": config.agent_max_turns,
        },
    )

    try:
        result = agent.run(request)
    except Exception as exc:
        logger.error("Librarian agent failed: %s", exc)
        raise GoldfishError(f"Librarian agent failed: {exc}") from exc

    raw = getattr(result, "response_text", None) or getattr(result, "raw_output", "")
    raw_text = raw if isinstance(raw, str) else str(raw)

    # Attempt to extract JSON from markdown blocks
    if "```json" in raw_text:
        raw_text = raw_text.split("```json")[1].split("```")[0].strip()
    elif "```" in raw_text:
        raw_text = raw_text.split("```")[1].split("```")[0].strip()

    parsed = None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        # Fallback: try to find the first outer brace pair
        try:
            start = raw_text.index("{")
            end = raw_text.rindex("}") + 1
            parsed = json.loads(raw_text[start:end])
        except (ValueError, json.JSONDecodeError):
            parsed = None

    if not isinstance(parsed, dict):
        logger.error(f"Librarian agent returned unparseable recommendations. Raw output: {raw_text[:500]}...")
        return {}

    valid_ids = {p.get("id") for p in patterns if isinstance(p, dict)}

    recommendations: dict[str, dict] = {}

    for pattern_id, rec in parsed.items():
        if pattern_id not in valid_ids or not isinstance(rec, dict):
            continue

        action = str(rec.get("action", "skip")).lower()

        if action not in {"approve", "reject", "skip"}:
            action = "skip"

        confidence = str(rec.get("confidence", "low")).lower()

        reason = rec.get("reason")

        recommendations[str(pattern_id)] = {
            "action": action,
            "confidence": confidence,
            "reason": reason,
        }

    return recommendations
