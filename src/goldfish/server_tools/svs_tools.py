"""Goldfish MCP tools - SVS (Semantic Validation System) Tools.

Consolidated toolset for managing failure patterns and AI reviews.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from goldfish.errors import GoldfishError
from goldfish.server_core import _get_db, mcp
from goldfish.svs.patterns.manager import FailurePatternManager

logger = logging.getLogger("goldfish.server")

if TYPE_CHECKING:
    from goldfish.db.database import Database


def list_failure_patterns(
    db: Database,
    *,
    status: str | None = None,
    stage_type: str | None = None,
    severity: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List patterns with filtering and pagination."""
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    patterns = db.list_failure_patterns(
        status=status,
        stage_type=stage_type,
        severity=severity,
        limit=limit,
        offset=offset,
    )
    total = db.count_failure_patterns(
        status=status,
        stage_type=stage_type,
        severity=severity,
    )
    rendered = [_pattern_to_dict(p) for p in patterns]

    return {
        "success": True,
        "patterns": rendered,
        "total_count": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rendered) < total,
    }


def get_failure_pattern(db: Database, *, pattern_id: str) -> dict:
    """Get a single pattern by ID."""
    pattern = db.get_failure_pattern(pattern_id)
    if pattern is None:
        return {"success": False, "error": f"Pattern not found: {pattern_id}"}
    return {"success": True, "pattern": _pattern_to_dict(pattern)}


def approve_pattern(db: Database, *, pattern_id: str) -> dict:
    """Approve a pattern (idempotent)."""
    if db.get_failure_pattern(pattern_id) is None:
        return {"success": False, "error": f"Pattern not found: {pattern_id}"}
    manager = FailurePatternManager(db)
    manager.approve_pattern(pattern_id)
    return {"success": True, "pattern_id": pattern_id, "status": "approved"}


def reject_pattern(db: Database, *, pattern_id: str, reason: str) -> dict:
    """Reject a pattern with a reason."""
    if not reason:
        raise GoldfishError("reason is required for rejection")
    if db.get_failure_pattern(pattern_id) is None:
        return {"success": False, "error": f"Pattern not found: {pattern_id}"}
    manager = FailurePatternManager(db)
    manager.reject_pattern(pattern_id, reason=reason)
    return {"success": True, "pattern_id": pattern_id, "status": "rejected"}


_ALLOWED_PATTERN_UPDATE_FIELDS = {
    "symptom",
    "root_cause",
    "detection_heuristic",
    "prevention",
    "severity",
    "stage_type",
    "confidence",
    "status",
    "rejection_reason",
    "approved_at",
    "approved_by",
    "manually_edited",
    "enabled",
    "source_run_id",
    "source_workspace",
    "last_seen_at",
    "occurrence_count",
}


def update_pattern(db: Database, pattern_id: str, **updates: Any) -> dict:
    """Update editable pattern fields."""
    if db.get_failure_pattern(pattern_id) is None:
        return {"success": False, "error": f"Pattern not found: {pattern_id}"}

    invalid = [k for k in updates if k not in _ALLOWED_PATTERN_UPDATE_FIELDS]
    if invalid:
        raise GoldfishError(f"Invalid field: {invalid[0]}")

    if "manually_edited" not in updates:
        updates["manually_edited"] = True

    ok = db.update_failure_pattern(pattern_id, **updates)
    if not ok:
        return {"success": False, "error": f"Pattern not found: {pattern_id}"}
    return {"success": True, "pattern_id": pattern_id}


def get_svs_reviews(
    db: Database,
    *,
    stage_run_id: str,
    review_type: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List SVS reviews for a stage run."""
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    with db._conn() as conn:
        query = "SELECT * FROM svs_reviews WHERE stage_run_id = ?"
        params: list[Any] = [stage_run_id]

        if review_type is not None:
            query += " AND review_type = ?"
            params.append(review_type)

        query += " ORDER BY reviewed_at DESC LIMIT ? OFFSET ?"
        params.extend([limit + 1, offset])

        rows = conn.execute(query, params).fetchall()

    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "success": True,
        "stage_run_id": stage_run_id,
        "reviews": [_review_to_dict(r) for r in rows],
        "has_more": has_more,
    }


def get_run_svs_findings(db: Database, *, stage_run_id: str) -> dict:
    """Get aggregated SVS findings for a stage run."""
    from goldfish.server_tools.svs_tools_impl import get_run_svs_findings as get_findings

    return get_findings(db, stage_run_id)


def review_pending_patterns(db: Database, *, dry_run: bool = True) -> dict:
    """Review pending patterns using the librarian agent."""
    pending = db.list_failure_patterns(status="pending")
    if not pending:
        return {"success": True, "dry_run": dry_run, "actions": [], "message": "No pending patterns"}

    try:
        recommendations = librarian_review_patterns([_pattern_to_dict(p) for p in pending])
    except Exception as exc:
        return {"success": False, "error": str(exc)}

    manager = FailurePatternManager(db)
    actions: list[dict[str, Any]] = []

    for pattern in pending:
        pattern_id = pattern["id"]
        rec = recommendations.get(pattern_id, {}) if isinstance(recommendations, dict) else {}
        action = str(rec.get("action", "skip")).lower()
        if action not in {"approve", "reject", "skip"}:
            action = "skip"

        actions.append(
            {
                "pattern_id": pattern_id,
                "action": action,
                "confidence": rec.get("confidence", "low"),
                "reason": rec.get("reason"),
            }
        )

        if dry_run:
            continue

        if action == "approve":
            manager.approve_pattern(pattern_id, approved_by="ai_librarian")
        elif action == "reject":
            manager.reject_pattern(pattern_id, reason=rec.get("reason") or "Rejected by AI librarian")

    return {"success": True, "dry_run": dry_run, "actions": actions}


def librarian_review_patterns(patterns: list[dict], **kwargs: Any) -> dict[str, dict]:
    """AI librarian review of patterns (thin wrapper for easier patching in tests)."""
    from goldfish.server_tools.svs_tools_impl import librarian_review_patterns as impl

    return impl(patterns, **kwargs)


@mcp.tool()
def manage_patterns(
    action: str,
    pattern_id: str | None = None,
    stage_type: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    reason: str | None = None,
    dry_run: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Unified tool for managing SVS failure patterns.

    Args:
        action: "list", "get", "approve", "reject", "review"
        pattern_id: Identifier for get/approve/reject actions
        status: Filter for 'list' action (pending, approved, rejected)
        stage_type: Filter for 'list' action (e.g., 'train', 'preprocess')
        severity: Filter for 'list' action (CRITICAL, HIGH, MEDIUM, LOW)
        reason: Required for 'reject' action
        dry_run: For 'review' action (AI librarian)
        limit/offset: Pagination for 'list'
    """
    db = _get_db()
    manager = FailurePatternManager(db)

    if action == "list":
        patterns = db.list_failure_patterns(
            status=status,
            stage_type=stage_type,
            severity=severity,
            limit=limit,
            offset=offset,
        )
        total = db.count_failure_patterns(status=status, stage_type=stage_type, severity=severity)
        return {"patterns": [_pattern_to_dict(p) for p in patterns], "total": total}

    if action == "review":
        return review_pending_patterns(db, dry_run=dry_run)

    if not pattern_id:
        raise GoldfishError("pattern_id is required for this action")

    if action == "get":
        p = db.get_failure_pattern(pattern_id)

        if not p:
            raise GoldfishError(f"Pattern not found: {pattern_id}")

        return _pattern_to_dict(p)

    if action == "approve":
        manager.approve_pattern(pattern_id)
        return {"success": True, "status": "approved"}

    if action == "reject":
        if not reason:
            raise GoldfishError("reason is required for rejection")
        manager.reject_pattern(pattern_id, reason=reason)
        return {"success": True, "status": "rejected"}

    raise GoldfishError(f"Unknown action: {action}")


@mcp.tool(name="get_run_svs_findings")
def _get_run_svs_findings_tool(stage_run_id: str) -> dict:
    """Get all SVS findings (AI reviews and patterns) for a specific run."""
    db = _get_db()
    return get_run_svs_findings(db, stage_run_id=stage_run_id)


def _review_to_dict(review: Any) -> dict:
    return {
        "id": review["id"],
        "stage_run_id": review["stage_run_id"],
        "signal_name": review["signal_name"],
        "review_type": review["review_type"],
        "model_used": review["model_used"],
        "prompt_hash": review["prompt_hash"],
        "decision": review["decision"],
        "reviewed_at": review["reviewed_at"],
        "parsed_findings": review["parsed_findings"],
    }


def _pattern_to_dict(pattern: Any) -> dict:
    return {
        "id": pattern["id"],
        "symptom": pattern["symptom"],
        "root_cause": pattern["root_cause"],
        "detection_heuristic": pattern.get("detection_heuristic"),
        "prevention": pattern.get("prevention"),
        "severity": pattern.get("severity"),
        "stage_type": pattern.get("stage_type"),
        "confidence": pattern.get("confidence"),
        "status": pattern.get("status", "pending"),
        "created_at": pattern.get("created_at"),
        "approved_at": pattern.get("approved_at"),
        "approved_by": pattern.get("approved_by"),
        "rejection_reason": pattern.get("rejection_reason"),
        "manually_edited": pattern.get("manually_edited"),
        "enabled": pattern.get("enabled"),
        "source_run_id": pattern.get("source_run_id"),
        "source_workspace": pattern.get("source_workspace"),
    }
