"""Goldfish MCP tools - SVS (Semantic Validation System) Tools.

Provides MCP tools for managing failure patterns and SVS reviews.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from goldfish.errors import GoldfishError
from goldfish.svs.patterns.manager import FailurePatternManager

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger("goldfish.server")


# Valid fields for pattern updates
ALLOWED_PATTERN_UPDATE_FIELDS = {
    "symptom",
    "root_cause",
    "detection_heuristic",
    "prevention",
    "severity",
    "stage_type",
    "confidence",
    "enabled",
    "manually_edited",
}


def _validate_limit_offset(limit: int | None, offset: int | None) -> tuple[int, int]:
    """Validate and normalize limit/offset parameters.

    Args:
        limit: Page size (1-200)
        offset: Number of items to skip (>= 0)

    Returns:
        Tuple of (limit, offset)

    Raises:
        GoldfishError: If parameters are invalid
    """
    if limit is None:
        limit = 50
    if offset is None:
        offset = 0

    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    return limit, offset


def librarian_review_patterns(patterns: list[dict]) -> dict[str, dict]:
    """AI librarian review of patterns (stub for testing).

    In production, this calls the AI agent to review patterns.
    Tests will mock this function.

    Args:
        patterns: List of pattern dicts to review

    Returns:
        Dict mapping pattern_id to action recommendation
    """
    # Stub - should be mocked in tests
    return {}


def list_failure_patterns(
    db: Database,
    status: str | None = None,
    stage_type: str | None = None,
    severity: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> dict:
    """List failure patterns with optional filtering.

    Returns patterns ordered by creation time (newest first).

    Args:
        db: Database instance
        status: Filter by status ('pending', 'approved', 'rejected', 'archived')
        stage_type: Filter by stage type ('train', 'preprocess', etc.)
        severity: Filter by severity ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')
        limit: Max results to return (1-200, default 50)
        offset: Number of results to skip (default 0)

    Returns:
        Dict with:
        - success: True
        - patterns: List of pattern dicts
        - total_count: Total matching patterns (for pagination)
        - limit: Limit used
        - offset: Offset used
        - has_more: Whether more results exist
    """
    limit, offset = _validate_limit_offset(limit, offset)

    # Get accurate total count matching filters
    total_count = db.count_failure_patterns(
        status=status,
        stage_type=stage_type,
        severity=severity,
    )

    # Get the requested page
    patterns = db.list_failure_patterns(
        status=status,
        stage_type=stage_type,
        severity=severity,
        limit=limit,
        offset=offset,
    )

    return {
        "success": True,
        "patterns": [_pattern_to_dict(p) for p in patterns],
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + len(patterns)) < total_count,
    }


def get_failure_pattern(db: Database, pattern_id: str) -> dict:
    """Get a single failure pattern by ID.

    Args:
        db: Database instance
        pattern_id: Pattern identifier

    Returns:
        Dict with:
        - success: True if found
        - pattern: Pattern details (or None if not found)
        - error: Error message if not found
    """
    pattern = db.get_failure_pattern(pattern_id)

    if pattern is None:
        return {
            "success": False,
            "error": f"Pattern not found: {pattern_id}",
        }

    return {
        "success": True,
        "pattern": _pattern_to_dict(pattern),
    }


def approve_pattern(db: Database, pattern_id: str, approved_by: str | None = None) -> dict:
    """Approve a pending failure pattern.

    Marks pattern as approved and active for failure detection.

    Args:
        db: Database instance
        pattern_id: Pattern identifier
        approved_by: Optional identifier of who approved

    Returns:
        Dict with:
        - success: True if approved
        - pattern_id: Pattern identifier
        - status: 'approved'
        - error: Error message if failed
    """
    manager = FailurePatternManager(db)

    # Check if pattern exists
    pattern = db.get_failure_pattern(pattern_id)
    if pattern is None:
        return {
            "success": False,
            "error": f"Pattern not found: {pattern_id}",
        }

    # Approve (idempotent - handles already approved)
    manager.approve_pattern(pattern_id, approved_by=approved_by)

    # Audit log
    db.log_audit(
        operation="approve_pattern",
        reason=f"Approved failure pattern {pattern_id}",
        details={
            "pattern_id": pattern_id,
            "approved_by": approved_by,
            "symptom": pattern.get("symptom", "")[:100],
        },
    )

    return {
        "success": True,
        "pattern_id": pattern_id,
        "status": "approved",
    }


def reject_pattern(db: Database, pattern_id: str, reason: str) -> dict:
    """Reject a failure pattern with reason.

    Marks pattern as rejected. Rejection reason is required.

    Args:
        db: Database instance
        pattern_id: Pattern identifier
        reason: Why the pattern was rejected (required)

    Returns:
        Dict with:
        - success: True if rejected
        - pattern_id: Pattern identifier
        - status: 'rejected'
        - error: Error message if failed
    """
    manager = FailurePatternManager(db)

    # Validate reason is provided
    if not reason or not reason.strip():
        raise GoldfishError("Rejection reason is required")

    # Check if pattern exists
    pattern = db.get_failure_pattern(pattern_id)
    if pattern is None:
        return {
            "success": False,
            "error": f"Pattern not found: {pattern_id}",
        }

    manager.reject_pattern(pattern_id, reason=reason)

    # Audit log
    db.log_audit(
        operation="reject_pattern",
        reason=f"Rejected failure pattern {pattern_id}: {reason[:100]}",
        details={
            "pattern_id": pattern_id,
            "rejection_reason": reason,
            "symptom": pattern.get("symptom", "")[:100],
        },
    )

    return {
        "success": True,
        "pattern_id": pattern_id,
        "status": "rejected",
    }


def update_pattern(db: Database, pattern_id: str, **kwargs: Any) -> dict:
    """Update fields on a failure pattern.

    Allowed fields: symptom, root_cause, detection_heuristic, prevention,
    severity, stage_type, confidence, enabled, manually_edited.

    Args:
        db: Database instance
        pattern_id: Pattern identifier
        **kwargs: Fields to update

    Returns:
        Dict with:
        - success: True if updated
        - pattern_id: Pattern identifier
        - error: Error message if failed
    """
    manager = FailurePatternManager(db)

    # Validate fields
    invalid_fields = set(kwargs.keys()) - ALLOWED_PATTERN_UPDATE_FIELDS
    if invalid_fields:
        raise GoldfishError(f"Invalid field(s): {', '.join(invalid_fields)}")

    # Check if pattern exists
    pattern = db.get_failure_pattern(pattern_id)
    if pattern is None:
        return {
            "success": False,
            "error": f"Pattern not found: {pattern_id}",
        }

    # Update
    manager.update_pattern(pattern_id, **kwargs)

    # Audit log
    db.log_audit(
        operation="update_pattern",
        reason=f"Updated failure pattern {pattern_id}",
        details={
            "pattern_id": pattern_id,
            "updated_fields": list(kwargs.keys()),
        },
    )

    return {
        "success": True,
        "pattern_id": pattern_id,
    }


def get_svs_reviews(
    db: Database,
    stage_run_id: str,
    review_type: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> dict:
    """Get SVS reviews for a stage run.

    Args:
        db: Database instance
        stage_run_id: Stage run identifier
        review_type: Filter by type ('pre_run', 'during_run', 'post_run')
        limit: Max results (1-200, default 50)
        offset: Number to skip (default 0)

    Returns:
        Dict with:
        - success: True
        - stage_run_id: Stage run identifier
        - reviews: List of review dicts
        - has_more: Whether more results exist
    """
    limit, offset = _validate_limit_offset(limit, offset)

    # Get reviews with extra to check has_more
    reviews = db.get_svs_reviews(
        stage_run_id=stage_run_id,
        review_type=review_type,
        limit=limit + 1,
        offset=offset,
    )

    has_more = len(reviews) > limit
    if has_more:
        reviews = reviews[:limit]

    return {
        "success": True,
        "stage_run_id": stage_run_id,
        "reviews": [_review_to_dict(r) for r in reviews],
        "has_more": has_more,
    }


def get_run_svs_findings(db: Database, stage_run_id: str) -> dict:
    """Get SVS findings summary for a stage run.

    Aggregates reviews and failure patterns for a run.

    Args:
        db: Database instance
        stage_run_id: Stage run identifier

    Returns:
        Dict with:
        - success: True if run exists
        - stage_run_id: Stage run identifier
        - reviews: List of reviews
        - failure_patterns: List of patterns from this run
        - error: Error message if run not found
    """
    # Check run exists
    run = db.get_stage_run(stage_run_id)
    if run is None:
        return {
            "success": False,
            "error": f"Stage run not found: {stage_run_id}",
        }

    # Get reviews
    reviews = db.get_svs_reviews(stage_run_id=stage_run_id)

    # Get patterns that originated from this run
    all_patterns = db.list_failure_patterns(limit=1000)
    run_patterns = [p for p in all_patterns if p.get("source_run_id") == stage_run_id]

    return {
        "success": True,
        "stage_run_id": stage_run_id,
        "reviews": [_review_to_dict(r) for r in reviews],
        "failure_patterns": [_pattern_to_dict(p) for p in run_patterns],
    }


def review_pending_patterns(db: Database, dry_run: bool = True) -> dict:
    """Batch review pending patterns using AI librarian.

    Analyzes all pending patterns and recommends approve/reject.

    Args:
        db: Database instance
        dry_run: If True, preview actions without applying (default True)

    Returns:
        Dict with:
        - success: True if review completed
        - dry_run: Whether this was a dry run
        - actions: List of recommended actions
        - message: Summary message
        - error: Error message if failed
    """
    manager = FailurePatternManager(db)

    # Get pending patterns
    pending = db.list_failure_patterns(status="pending")

    if not pending:
        return {
            "success": True,
            "dry_run": dry_run,
            "actions": [],
            "message": "No pending patterns to review",
        }

    # Call AI librarian
    try:
        recommendations = librarian_review_patterns([_pattern_to_dict(p) for p in pending])
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }

    # Build actions list
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

    # Apply if not dry run
    if not dry_run:
        approved_count = 0
        rejected_count = 0
        for pattern_id, rec in recommendations.items():
            action = rec.get("action")
            if action == "approve":
                manager.approve_pattern(pattern_id, approved_by="ai_librarian")
                approved_count += 1
            elif action == "reject":
                reason = rec.get("reason", "Rejected by AI librarian")
                manager.reject_pattern(pattern_id, reason=reason)
                rejected_count += 1

        # Audit log for batch review
        db.log_audit(
            operation="review_pending_patterns",
            reason=f"AI librarian reviewed {len(pending)} patterns",
            details={
                "patterns_reviewed": len(pending),
                "approved_count": approved_count,
                "rejected_count": rejected_count,
                "actions": actions,
            },
        )

    return {
        "success": True,
        "dry_run": dry_run,
        "actions": actions,
        "message": f"Reviewed {len(pending)} pending pattern(s)",
    }


def _pattern_to_dict(pattern: Any) -> dict:
    """Convert pattern row to API dict."""
    return {
        "id": pattern["id"],
        "symptom": pattern["symptom"],
        "root_cause": pattern["root_cause"],
        "detection_heuristic": pattern["detection_heuristic"],
        "prevention": pattern["prevention"],
        "severity": pattern.get("severity"),
        "stage_type": pattern.get("stage_type"),
        "confidence": pattern.get("confidence"),
        "status": pattern.get("status", "pending"),
        "created_at": pattern.get("created_at"),
        "approved_at": pattern.get("approved_at"),
        "approved_by": pattern.get("approved_by"),
        "rejection_reason": pattern.get("rejection_reason"),
        "occurrence_count": pattern.get("occurrence_count", 1),
        "last_seen_at": pattern.get("last_seen_at"),
        "source_run_id": pattern.get("source_run_id"),
        "source_workspace": pattern.get("source_workspace"),
        "enabled": pattern.get("enabled"),
        "manually_edited": pattern.get("manually_edited"),
    }


def _review_to_dict(review: Any) -> dict:
    """Convert review row to API dict."""
    return {
        "id": review["id"],
        "stage_run_id": review["stage_run_id"],
        "signal_name": review.get("signal_name"),
        "review_type": review["review_type"],
        "model_used": review["model_used"],
        "decision": review["decision"],
        "reviewed_at": review["reviewed_at"],
        "duration_ms": review.get("duration_ms"),
        "parsed_findings": review.get("parsed_findings"),
    }


# =============================================================================
# MCP Tool Wrappers (only loaded when server imports us)
# These are thin wrappers that get database from server context
# =============================================================================


def _register_mcp_tools() -> None:
    """Register MCP tools with server. Called when server imports this module."""
    # Import from server only when called (avoid circular import at module level)
    from goldfish.server import _get_db, mcp

    @mcp.tool()
    def list_failure_patterns_tool(
        status: str | None = None,
        stage_type: str | None = None,
        severity: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict:
        """List failure patterns with optional filtering."""
        return list_failure_patterns(_get_db(), status, stage_type, severity, limit, offset)

    @mcp.tool()
    def get_failure_pattern_tool(pattern_id: str) -> dict:
        """Get a single failure pattern by ID."""
        return get_failure_pattern(_get_db(), pattern_id)

    @mcp.tool()
    def approve_pattern_tool(pattern_id: str, approved_by: str | None = None) -> dict:
        """Approve a pending failure pattern."""
        return approve_pattern(_get_db(), pattern_id, approved_by)

    @mcp.tool()
    def reject_pattern_tool(pattern_id: str, reason: str) -> dict:
        """Reject a failure pattern with reason."""
        return reject_pattern(_get_db(), pattern_id, reason)

    @mcp.tool()
    def update_pattern_tool(
        pattern_id: str,
        symptom: str | None = None,
        root_cause: str | None = None,
        detection_heuristic: str | None = None,
        prevention: str | None = None,
        severity: str | None = None,
        stage_type: str | None = None,
        confidence: str | None = None,
        enabled: bool | None = None,
        manually_edited: bool | None = None,
    ) -> dict:
        """Update fields on a failure pattern."""
        updates = {
            k: v
            for k, v in {
                "symptom": symptom,
                "root_cause": root_cause,
                "detection_heuristic": detection_heuristic,
                "prevention": prevention,
                "severity": severity,
                "stage_type": stage_type,
                "confidence": confidence,
                "enabled": enabled,
                "manually_edited": manually_edited,
            }.items()
            if v is not None
        }
        return update_pattern(_get_db(), pattern_id, **updates)

    @mcp.tool()
    def get_svs_reviews_tool(
        stage_run_id: str,
        review_type: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict:
        """Get SVS reviews for a stage run."""
        return get_svs_reviews(_get_db(), stage_run_id, review_type, limit, offset)

    @mcp.tool()
    def get_run_svs_findings_tool(stage_run_id: str) -> dict:
        """Get SVS findings summary for a stage run."""
        return get_run_svs_findings(_get_db(), stage_run_id)

    @mcp.tool()
    def review_pending_patterns_tool(dry_run: bool = True) -> dict:
        """Batch review pending patterns using AI librarian."""
        return review_pending_patterns(_get_db(), dry_run)


# Register tools on import
_register_mcp_tools()
