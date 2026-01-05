"""Goldfish MCP tools - SVS (Semantic Validation System) Tools.

Consolidated toolset for managing failure patterns and AI reviews.
"""

from __future__ import annotations

import logging
from typing import Any

from goldfish.errors import GoldfishError
from goldfish.server_core import _get_db, mcp
from goldfish.svs.patterns.manager import FailurePatternManager

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def manage_patterns(
    action: str,
    pattern_id: str | None = None,
    status: str | None = None,
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
        reason: Required for 'reject' action
        dry_run: For 'review' action (AI librarian)
        limit/offset: Pagination for 'list'
    """
    db = _get_db()
    manager = FailurePatternManager(db)

    if action == "list":
        patterns = db.list_failure_patterns(status=status, limit=limit, offset=offset)
        total = db.count_failure_patterns(status=status)
        return {"patterns": [_pattern_to_dict(p) for p in patterns], "total": total}

    if action == "review":
        from goldfish.server_tools.svs_tools_impl import review_pending_patterns

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


@mcp.tool()
def get_run_svs_findings(stage_run_id: str) -> dict:
    """Get all SVS findings (AI reviews and patterns) for a specific run."""
    db = _get_db()
    from goldfish.server_tools.svs_tools_impl import get_run_svs_findings as get_findings

    return get_findings(db, stage_run_id)


def _pattern_to_dict(pattern: Any) -> dict:
    return {
        "id": pattern["id"],
        "symptom": pattern["symptom"],
        "root_cause": pattern["root_cause"],
        "status": pattern.get("status", "pending"),
        "severity": pattern.get("severity"),
        "created_at": pattern.get("created_at"),
    }
