"""Failure Pattern Manager for self-learning failure detection.

Manages the lifecycle of failure patterns: creation, deduplication,
approval workflow, and querying.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from goldfish.db.database import Database
    from goldfish.db.types import FailurePatternRow


class FailurePatternManager:
    """Manages failure patterns with deduplication and approval workflow.

    Patterns go through a lifecycle:
    1. pending - Newly created, awaiting review
    2. approved - Approved for active use
    3. rejected - Rejected with reason
    4. archived - No longer active but preserved for history

    Example:
        manager = FailurePatternManager(db)
        pattern_id = manager.record_pattern(
            symptom="Training loss NaN after epoch 5",
            root_cause="Learning rate too high",
            detection_heuristic="loss == NaN OR loss > 1e6",
            prevention="Use learning rate scheduler with warmup",
        )
        manager.approve_pattern(pattern_id)
    """

    def __init__(self, db: Database) -> None:
        """Initialize the pattern manager.

        Args:
            db: Database instance for persistence
        """
        self.db = db

    def record_pattern(
        self,
        symptom: str,
        root_cause: str,
        detection_heuristic: str,
        prevention: str,
        severity: str | None = None,
        stage_type: str | None = None,
        source_run_id: str | None = None,
        source_workspace: str | None = None,
        confidence: str | None = None,
    ) -> str:
        """Record a new failure pattern.

        Creates a pattern with status='pending', enabled=True, occurrence_count=1.

        Args:
            symptom: What went wrong (error message, behavior)
            root_cause: Why it happened (underlying cause)
            detection_heuristic: How to detect it programmatically
            prevention: How to prevent it (fix or workaround)
            severity: CRITICAL | HIGH | MEDIUM | LOW
            stage_type: Stage type filter (e.g., 'train', 'preprocess')
            source_run_id: Stage run that triggered extraction
            source_workspace: Workspace where pattern was discovered
            confidence: HIGH | MEDIUM | LOW

        Returns:
            Pattern ID (UUID string)
        """
        pattern_id = str(uuid.uuid4())
        created_at = datetime.now(UTC).isoformat()

        self.db.create_failure_pattern(
            pattern_id=pattern_id,
            symptom=symptom,
            root_cause=root_cause,
            detection_heuristic=detection_heuristic,
            prevention=prevention,
            created_at=created_at,
            severity=severity,
            stage_type=stage_type,
            source_run_id=source_run_id,
            source_workspace=source_workspace,
            confidence=confidence,
        )

        return pattern_id

    def get_pattern(self, pattern_id: str) -> FailurePatternRow | None:
        """Get a single pattern by ID.

        Args:
            pattern_id: Pattern UUID

        Returns:
            FailurePatternRow or None if not found
        """
        return self.db.get_failure_pattern(pattern_id)

    def get_patterns_for_stage(self, stage_type: str) -> list[FailurePatternRow]:
        """Get active patterns applicable to a stage type.

        Returns patterns that are:
        - status='approved' (reviewed and accepted)
        - enabled=True (not disabled)
        - stage_type matches OR stage_type is NULL (universal patterns)

        Args:
            stage_type: Stage type to filter for (e.g., 'train')

        Returns:
            List of matching patterns
        """
        # Get approved, enabled patterns for this stage type
        patterns = self.db.list_failure_patterns(
            status="approved",
            enabled=True,
        )

        # Filter by stage_type: matches OR NULL (universal)
        return [p for p in patterns if p["stage_type"] is None or p["stage_type"] == stage_type]

    def get_pending_patterns(self) -> list[FailurePatternRow]:
        """Get all patterns awaiting review.

        Returns:
            List of patterns with status='pending'
        """
        return self.db.list_failure_patterns(status="pending")

    def approve_pattern(
        self,
        pattern_id: str,
        approved_by: str | None = None,
    ) -> bool:
        """Approve a pattern for active use.

        Sets status='approved' and records approval timestamp.

        Args:
            pattern_id: Pattern UUID
            approved_by: Optional identifier of who approved

        Returns:
            True if updated, False if pattern not found
        """
        approved_at = datetime.now(UTC).isoformat()
        updates: dict = {
            "status": "approved",
            "approved_at": approved_at,
        }
        if approved_by is not None:
            updates["approved_by"] = approved_by

        return self.db.update_failure_pattern(pattern_id, **updates)

    def reject_pattern(self, pattern_id: str, reason: str) -> bool:
        """Reject a pattern with reason.

        Sets status='rejected' and stores rejection reason.

        Args:
            pattern_id: Pattern UUID
            reason: Why the pattern was rejected

        Returns:
            True if updated, False if pattern not found
        """
        return self.db.update_failure_pattern(
            pattern_id,
            status="rejected",
            rejection_reason=reason,
        )

    def update_pattern(self, pattern_id: str, **updates) -> bool:
        """Update pattern fields.

        Automatically sets manually_edited=True to indicate human modification.

        Args:
            pattern_id: Pattern UUID
            **updates: Fields to update (symptom, root_cause, severity, etc.)

        Returns:
            True if updated, False if pattern not found
        """
        # Mark as manually edited when fields are updated
        updates["manually_edited"] = True
        return self.db.update_failure_pattern(pattern_id, **updates)

    def increment_occurrence(self, pattern_id: str) -> bool:
        """Increment occurrence count and update last_seen_at.

        Called when a pattern is matched again.

        Args:
            pattern_id: Pattern UUID

        Returns:
            True if updated, False if pattern not found
        """
        last_seen_at = datetime.now(UTC).isoformat()
        return self.db.increment_pattern_occurrence(pattern_id, last_seen_at)

    def archive_pattern(self, pattern_id: str) -> bool:
        """Archive a pattern.

        Archived patterns are no longer active but preserved for history.
        Does NOT set manually_edited flag.

        Args:
            pattern_id: Pattern UUID

        Returns:
            True if updated, False if pattern not found
        """
        return self.db.update_failure_pattern(pattern_id, status="archived")

    def compute_symptom_hash(self, symptom: str) -> str:
        """Compute normalized hash of symptom for deduplication.

        Normalizes by:
        - Converting to lowercase
        - Collapsing whitespace to single spaces

        Args:
            symptom: Symptom text to hash

        Returns:
            SHA256 hex digest of normalized symptom
        """
        # Normalize: lowercase and collapse whitespace
        normalized = symptom.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return hashlib.sha256(normalized.encode()).hexdigest()

    def dedup_check(self, symptom: str) -> FailurePatternRow | None:
        """Check if a similar pattern already exists.

        Searches for non-archived patterns with matching symptom hash.

        Args:
            symptom: Symptom text to check for duplicates

        Returns:
            Existing pattern if found, None otherwise
        """
        symptom_hash = self.compute_symptom_hash(symptom)

        # Get all non-archived patterns and check hash
        # We need to compute hash for each pattern since we don't store it
        patterns = self.db.list_failure_patterns(limit=1000)

        for pattern in patterns:
            # Skip archived patterns
            if pattern["status"] == "archived":
                continue

            # Check if symptoms match (after normalization)
            if self.compute_symptom_hash(pattern["symptom"]) == symptom_hash:
                return pattern

        return None
