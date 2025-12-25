"""Unit tests for FailurePatternManager.

These tests define expected behavior of the pattern manager before implementation.
Following TDD: RED → GREEN → REFACTOR
"""

import hashlib
import uuid
from datetime import UTC, datetime

from goldfish.db.database import Database


class TestFailurePatternManagerBasics:
    """Test fundamental CRUD operations."""

    def test_record_pattern_creates_with_uuid_id(self, test_db: Database):
        """record_pattern() should create pattern with UUID id."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        # Create workspace and stage run for FK constraint
        test_db.create_workspace_lineage("baseline", description="Test")
        test_db.create_version("baseline", "v1", "baseline-v1", "sha123", "manual")
        test_db.create_stage_run(
            stage_run_id="stage-abc123",
            workspace_name="baseline",
            version="v1",
            stage_name="train",
        )

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Training loss NaN after epoch 5",
            root_cause="Learning rate too high (0.1)",
            detection_heuristic="loss == NaN OR loss > 1e6",
            prevention="Use learning rate scheduler with warmup",
            severity="HIGH",
            stage_type="train",
            source_run_id="stage-abc123",
            source_workspace="baseline",
            confidence="HIGH",
        )

        # Should be valid UUID format
        assert isinstance(pattern_id, str)
        uuid.UUID(pattern_id)  # Raises if not valid UUID

    def test_record_pattern_stores_all_fields(self, test_db: Database):
        """record_pattern() should store all provided fields correctly."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        # Create workspace and stage run for FK constraint
        test_db.create_workspace_lineage("baseline", description="Test")
        test_db.create_version("baseline", "v1", "baseline-v1", "sha123", "manual")
        test_db.create_stage_run(
            stage_run_id="stage-abc123",
            workspace_name="baseline",
            version="v1",
            stage_name="train",
        )

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Training loss NaN",
            root_cause="Learning rate too high",
            detection_heuristic="loss == NaN",
            prevention="Use learning rate scheduler",
            severity="HIGH",
            stage_type="train",
            source_run_id="stage-abc123",
            source_workspace="baseline",
            confidence="HIGH",
        )

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["symptom"] == "Training loss NaN"
        assert pattern["root_cause"] == "Learning rate too high"
        assert pattern["detection_heuristic"] == "loss == NaN"
        assert pattern["prevention"] == "Use learning rate scheduler"
        assert pattern["severity"] == "HIGH"
        assert pattern["stage_type"] == "train"
        assert pattern["source_run_id"] == "stage-abc123"
        assert pattern["source_workspace"] == "baseline"
        assert pattern["confidence"] == "HIGH"

    def test_record_pattern_sets_defaults(self, test_db: Database):
        """record_pattern() should set default values for status, enabled, occurrence_count."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error message",
            root_cause="Bug in code",
            detection_heuristic="error contains 'X'",
            prevention="Fix the bug",
        )

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["status"] == "pending"
        assert pattern["enabled"] == 1 or pattern["enabled"] is True  # SQLite returns int
        assert pattern["occurrence_count"] == 1
        assert pattern["manually_edited"] == 0 or pattern["manually_edited"] is False  # SQLite returns int

    def test_record_pattern_sets_created_at_timestamp(self, test_db: Database):
        """record_pattern() should set created_at to current timestamp."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        before = datetime.now(UTC).isoformat()
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )
        after = datetime.now(UTC).isoformat()

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert before <= pattern["created_at"] <= after

    def test_get_pattern_returns_none_for_missing_id(self, test_db: Database):
        """get_pattern() should return None for non-existent pattern."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern = manager.get_pattern("nonexistent-uuid")
        assert pattern is None

    def test_get_pattern_returns_full_row(self, test_db: Database):
        """get_pattern() should return FailurePatternRow TypedDict."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        # Verify it has all expected fields
        assert "id" in pattern
        assert "symptom" in pattern
        assert "root_cause" in pattern
        assert "detection_heuristic" in pattern
        assert "prevention" in pattern
        assert "status" in pattern
        assert "created_at" in pattern


class TestFailurePatternFiltering:
    """Test pattern filtering and querying."""

    def test_get_patterns_for_stage_filters_by_stage_type(self, test_db: Database):
        """get_patterns_for_stage() should return only patterns for specified stage."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        train_id = manager.record_pattern(
            symptom="Train error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        preprocess_id = manager.record_pattern(
            symptom="Preprocess error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="preprocess",
        )
        # Approve both patterns so they appear in queries
        manager.approve_pattern(train_id)
        manager.approve_pattern(preprocess_id)

        train_patterns = manager.get_patterns_for_stage("train")
        assert len(train_patterns) == 1
        assert train_patterns[0]["id"] == train_id

    def test_get_patterns_for_stage_includes_null_stage_types(self, test_db: Database):
        """get_patterns_for_stage() should include patterns with stage_type=NULL (universal)."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        universal_id = manager.record_pattern(
            symptom="Universal error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type=None,  # Applies to all stages
        )
        train_id = manager.record_pattern(
            symptom="Train error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        # Approve both patterns so they appear in queries
        manager.approve_pattern(universal_id)
        manager.approve_pattern(train_id)

        train_patterns = manager.get_patterns_for_stage("train")
        assert len(train_patterns) == 2
        pattern_ids = {p["id"] for p in train_patterns}
        assert universal_id in pattern_ids
        assert train_id in pattern_ids

    def test_get_patterns_for_stage_only_returns_enabled(self, test_db: Database):
        """get_patterns_for_stage() should only return enabled=True patterns."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        enabled_id = manager.record_pattern(
            symptom="Enabled error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        disabled_id = manager.record_pattern(
            symptom="Disabled error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        # Approve both patterns, then disable one
        manager.approve_pattern(enabled_id)
        manager.approve_pattern(disabled_id)
        manager.update_pattern(disabled_id, enabled=False)

        train_patterns = manager.get_patterns_for_stage("train")
        assert len(train_patterns) == 1
        assert train_patterns[0]["id"] == enabled_id

    def test_get_patterns_for_stage_only_returns_approved(self, test_db: Database):
        """get_patterns_for_stage() should only return status='approved' patterns."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        approved_id = manager.record_pattern(
            symptom="Approved error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        manager.approve_pattern(approved_id)

        pending_id = manager.record_pattern(
            symptom="Pending error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )

        train_patterns = manager.get_patterns_for_stage("train")
        assert len(train_patterns) == 1
        assert train_patterns[0]["id"] == approved_id

    def test_get_pending_patterns_filters_by_status(self, test_db: Database):
        """get_pending_patterns() should return only patterns with status='pending'."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pending_id = manager.record_pattern(
            symptom="Pending",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )
        approved_id = manager.record_pattern(
            symptom="Approved",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )
        manager.approve_pattern(approved_id)

        pending = manager.get_pending_patterns()
        assert len(pending) == 1
        assert pending[0]["id"] == pending_id
        assert pending[0]["status"] == "pending"


class TestFailurePatternApproval:
    """Test approval workflow operations."""

    def test_approve_pattern_sets_status_to_approved(self, test_db: Database):
        """approve_pattern() should set status='approved'."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.approve_pattern(pattern_id)

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["status"] == "approved"

    def test_approve_pattern_sets_approved_at_timestamp(self, test_db: Database):
        """approve_pattern() should set approved_at to current timestamp."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        before = datetime.now(UTC).isoformat()
        manager.approve_pattern(pattern_id)
        after = datetime.now(UTC).isoformat()

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["approved_at"] is not None
        assert before <= pattern["approved_at"] <= after

    def test_approve_pattern_accepts_optional_approved_by(self, test_db: Database):
        """approve_pattern() should accept optional approved_by parameter."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.approve_pattern(pattern_id, approved_by="user@example.com")

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["approved_by"] == "user@example.com"

    def test_approve_pattern_returns_true_on_success(self, test_db: Database):
        """approve_pattern() should return True when pattern is updated."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        result = manager.approve_pattern(pattern_id)
        assert result is True

    def test_approve_pattern_returns_false_for_missing_pattern(self, test_db: Database):
        """approve_pattern() should return False if pattern not found."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        result = manager.approve_pattern("nonexistent-uuid")
        assert result is False

    def test_reject_pattern_sets_status_to_rejected(self, test_db: Database):
        """reject_pattern() should set status='rejected'."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.reject_pattern(pattern_id, reason="Too generic")

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["status"] == "rejected"

    def test_reject_pattern_stores_rejection_reason(self, test_db: Database):
        """reject_pattern() should store rejection reason."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.reject_pattern(pattern_id, reason="Too generic, needs more context")

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["rejection_reason"] == "Too generic, needs more context"

    def test_reject_pattern_returns_true_on_success(self, test_db: Database):
        """reject_pattern() should return True when pattern is updated."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        result = manager.reject_pattern(pattern_id, reason="Too generic")
        assert result is True

    def test_reject_pattern_returns_false_for_missing_pattern(self, test_db: Database):
        """reject_pattern() should return False if pattern not found."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        result = manager.reject_pattern("nonexistent-uuid", reason="Test")
        assert result is False


class TestFailurePatternUpdates:
    """Test pattern update operations."""

    def test_update_pattern_modifies_fields(self, test_db: Database):
        """update_pattern() should modify specified fields."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Original symptom",
            root_cause="Original cause",
            detection_heuristic="Original heuristic",
            prevention="Original prevention",
        )

        manager.update_pattern(
            pattern_id,
            symptom="Updated symptom",
            root_cause="Updated cause",
        )

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["symptom"] == "Updated symptom"
        assert pattern["root_cause"] == "Updated cause"
        assert pattern["detection_heuristic"] == "Original heuristic"  # Unchanged

    def test_update_pattern_sets_manually_edited_flag(self, test_db: Database):
        """update_pattern() should set manually_edited=True."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.update_pattern(pattern_id, symptom="Updated")

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["manually_edited"] == 1 or pattern["manually_edited"] is True  # SQLite returns int

    def test_update_pattern_returns_true_on_success(self, test_db: Database):
        """update_pattern() should return True when pattern is updated."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        result = manager.update_pattern(pattern_id, severity="CRITICAL")
        assert result is True

    def test_update_pattern_returns_false_for_missing_pattern(self, test_db: Database):
        """update_pattern() should return False if pattern not found."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        result = manager.update_pattern("nonexistent-uuid", symptom="Updated")
        assert result is False

    def test_increment_occurrence_increases_count(self, test_db: Database):
        """increment_occurrence() should increment occurrence_count."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        initial_pattern = manager.get_pattern(pattern_id)
        assert initial_pattern is not None
        initial_count = initial_pattern["occurrence_count"]

        manager.increment_occurrence(pattern_id)

        updated_pattern = manager.get_pattern(pattern_id)
        assert updated_pattern is not None
        assert updated_pattern["occurrence_count"] == initial_count + 1

    def test_increment_occurrence_updates_last_seen_at(self, test_db: Database):
        """increment_occurrence() should update last_seen_at to current timestamp."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        before = datetime.now(UTC).isoformat()
        manager.increment_occurrence(pattern_id)
        after = datetime.now(UTC).isoformat()

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["last_seen_at"] is not None
        assert before <= pattern["last_seen_at"] <= after

    def test_increment_occurrence_returns_true_on_success(self, test_db: Database):
        """increment_occurrence() should return True when pattern is updated."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        result = manager.increment_occurrence(pattern_id)
        assert result is True

    def test_increment_occurrence_returns_false_for_missing_pattern(self, test_db: Database):
        """increment_occurrence() should return False if pattern not found."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        result = manager.increment_occurrence("nonexistent-uuid")
        assert result is False


class TestFailurePatternDeduplication:
    """Test deduplication logic for patterns."""

    def test_compute_symptom_hash_generates_sha256(self, test_db: Database):
        """compute_symptom_hash() should generate SHA256 hash of symptom."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        symptom = "Training loss NaN after epoch 5"
        hash_value = manager.compute_symptom_hash(symptom)

        # Hash is computed on normalized symptom (lowercase, collapsed whitespace)
        normalized = symptom.lower()
        expected = hashlib.sha256(normalized.encode()).hexdigest()
        assert hash_value == expected

    def test_compute_symptom_hash_normalizes_whitespace(self, test_db: Database):
        """compute_symptom_hash() should normalize whitespace in symptom."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        hash1 = manager.compute_symptom_hash("Training loss  NaN\n  after epoch 5")
        hash2 = manager.compute_symptom_hash("Training loss NaN after epoch 5")

        assert hash1 == hash2

    def test_compute_symptom_hash_is_case_insensitive(self, test_db: Database):
        """compute_symptom_hash() should be case-insensitive."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        hash1 = manager.compute_symptom_hash("Training Loss NaN")
        hash2 = manager.compute_symptom_hash("training loss nan")

        assert hash1 == hash2

    def test_dedup_check_returns_none_for_new_pattern(self, test_db: Database):
        """dedup_check() should return None if no duplicate exists."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        existing = manager.dedup_check("Unique error message")
        assert existing is None

    def test_dedup_check_finds_existing_pattern(self, test_db: Database):
        """dedup_check() should return existing pattern if symptom matches."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        symptom = "Training loss NaN"
        pattern_id = manager.record_pattern(
            symptom=symptom,
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        existing = manager.dedup_check(symptom)
        assert existing is not None
        assert existing["id"] == pattern_id

    def test_dedup_check_normalizes_symptom(self, test_db: Database):
        """dedup_check() should find pattern with normalized symptom."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Training Loss NaN",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        # Different whitespace and case
        existing = manager.dedup_check("training  loss   nan")
        assert existing is not None
        assert existing["id"] == pattern_id

    def test_dedup_check_ignores_archived_patterns(self, test_db: Database):
        """dedup_check() should ignore patterns with status='archived'."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        symptom = "Archived error"
        pattern_id = manager.record_pattern(
            symptom=symptom,
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )
        manager.update_pattern(pattern_id, status="archived")

        existing = manager.dedup_check(symptom)
        assert existing is None


class TestFailurePatternArchiving:
    """Test pattern archiving operations."""

    def test_archive_pattern_sets_status_to_archived(self, test_db: Database):
        """archive_pattern() should set status='archived'."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        manager.archive_pattern(pattern_id)

        pattern = manager.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern["status"] == "archived"

    def test_archive_pattern_returns_true_on_success(self, test_db: Database):
        """archive_pattern() should return True when pattern is updated."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
        )

        result = manager.archive_pattern(pattern_id)
        assert result is True

    def test_archive_pattern_returns_false_for_missing_pattern(self, test_db: Database):
        """archive_pattern() should return False if pattern not found."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        result = manager.archive_pattern("nonexistent-uuid")
        assert result is False

    def test_archived_patterns_excluded_from_active_queries(self, test_db: Database):
        """Archived patterns should not appear in get_patterns_for_stage()."""
        from goldfish.svs.patterns.manager import FailurePatternManager

        manager = FailurePatternManager(test_db)
        pattern_id = manager.record_pattern(
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            stage_type="train",
        )
        manager.approve_pattern(pattern_id)
        manager.archive_pattern(pattern_id)

        patterns = manager.get_patterns_for_stage("train")
        assert len(patterns) == 0
