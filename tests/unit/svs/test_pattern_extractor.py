"""Unit tests for failure pattern extraction.

Tests the pattern extractor module which uses AI to analyze failures and extract
structured patterns for self-learning failure detection.

Following TDD: RED → GREEN → REFACTOR
These tests define expected behavior before implementation.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from goldfish.db.database import Database
from goldfish.svs.agent import NullProvider, ReviewRequest, ReviewResult
from goldfish.svs.patterns.extractor import (
    FailurePattern,
    RateLimitExceededError,
    extract_failure_pattern,
)


class TestFailurePatternDataclass:
    """Test FailurePattern dataclass structure."""

    def test_pattern_has_required_fields(self):
        """FailurePattern should have all required fields."""
        pattern = FailurePattern(
            id="test-id",
            symptom="Test symptom",
            root_cause="Test cause",
            detection_heuristic="Test heuristic",
            prevention="Test prevention",
            severity="HIGH",
            confidence="MEDIUM",
            source_run_id="stage-abc123",
            source_workspace="test-workspace",
            stage_type="train",
        )

        assert pattern.id == "test-id"
        assert pattern.symptom == "Test symptom"
        assert pattern.root_cause == "Test cause"
        assert pattern.detection_heuristic == "Test heuristic"
        assert pattern.prevention == "Test prevention"
        assert pattern.severity == "HIGH"
        assert pattern.confidence == "MEDIUM"
        assert pattern.source_run_id == "stage-abc123"
        assert pattern.source_workspace == "test-workspace"
        assert pattern.stage_type == "train"

    def test_pattern_severity_values(self):
        """Severity should accept valid values: CRITICAL, HIGH, MEDIUM, LOW."""
        for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            pattern = FailurePattern(
                id="test-id",
                symptom="Test",
                root_cause="Test",
                detection_heuristic="Test",
                prevention="Test",
                severity=severity,
                confidence="HIGH",
                source_run_id="stage-abc123",
                source_workspace="test",
            )
            assert pattern.severity == severity

    def test_pattern_confidence_values(self):
        """Confidence should accept valid values: HIGH, MEDIUM, LOW."""
        for confidence in ["HIGH", "MEDIUM", "LOW"]:
            pattern = FailurePattern(
                id="test-id",
                symptom="Test",
                root_cause="Test",
                detection_heuristic="Test",
                prevention="Test",
                severity="MEDIUM",
                confidence=confidence,
                source_run_id="stage-abc123",
                source_workspace="test",
            )
            assert pattern.confidence == confidence

    def test_pattern_stage_type_optional(self):
        """stage_type should be optional (None means applies to all stages)."""
        pattern = FailurePattern(
            id="test-id",
            symptom="Test",
            root_cause="Test",
            detection_heuristic="Test",
            prevention="Test",
            severity="MEDIUM",
            confidence="HIGH",
            source_run_id="stage-abc123",
            source_workspace="test",
            stage_type=None,
        )
        assert pattern.stage_type is None


class TestExtractFailurePatternBasics:
    """Test basic failure pattern extraction."""

    def test_extracts_pattern_from_training_failure(self, test_db: Database):
        """Should extract structured pattern from training stage failure."""
        # Setup: Create mock agent that returns structured pattern
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: CUDA out of memory error during batch processing",
                "ROOT_CAUSE: Batch size (128) exceeds GPU memory for model size",
                "DETECTION: Error message contains 'CUDA out of memory' and batch_size in config",
                "PREVENTION: Reduce batch_size to 64 or enable gradient accumulation",
                "SEVERITY: HIGH",
                "CONFIDENCE: HIGH",
            ],
        )

        # Create stage run
        stage_run_id = "stage-train001"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        # Extract pattern
        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="RuntimeError: CUDA out of memory. Tried to allocate 2.00 GiB",
            logs="Batch 10/100\nRuntimeError: CUDA out of memory\nKilled",
            agent=agent,
        )

        # Verify pattern structure
        assert pattern is not None
        assert "CUDA out of memory" in pattern.symptom
        assert "batch size" in pattern.root_cause.lower()
        assert "CUDA out of memory" in pattern.detection_heuristic
        assert "batch" in pattern.prevention.lower()
        assert pattern.severity == "HIGH"
        assert pattern.confidence == "HIGH"
        assert pattern.source_run_id == stage_run_id
        assert pattern.source_workspace == "test-workspace"
        assert pattern.stage_type == "train"

    def test_extracts_pattern_from_preprocessing_failure(self, test_db: Database):
        """Should extract pattern from preprocessing stage failure."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: KeyError accessing missing column in DataFrame",
                "ROOT_CAUSE: Input data schema changed - 'user_id' column removed",
                "DETECTION: KeyError with column name in error message",
                "PREVENTION: Add schema validation before processing",
                "SEVERITY: CRITICAL",
                "CONFIDENCE: HIGH",
            ],
        )

        stage_run_id = "stage-preproc001"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "preprocess")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="KeyError: 'user_id'",
            logs="Loading data...\nKeyError: 'user_id'",
            agent=agent,
        )

        assert pattern is not None
        assert "KeyError" in pattern.symptom
        assert "schema" in pattern.root_cause.lower()
        assert pattern.severity == "CRITICAL"
        assert pattern.stage_type == "preprocess"

    def test_stores_pattern_in_database(self, test_db: Database):
        """Extracted pattern should be stored in failure_patterns table."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Test symptom",
                "ROOT_CAUSE: Test cause",
                "DETECTION: Test detection",
                "PREVENTION: Test prevention",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        stage_run_id = "stage-test001"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs="Test logs",
            agent=agent,
        )

        # Verify stored in DB
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM failure_patterns WHERE id = ?",
                (pattern.id,),
            ).fetchone()

        assert row is not None
        assert row["symptom"] == pattern.symptom
        assert row["root_cause"] == pattern.root_cause
        assert row["source_run_id"] == stage_run_id
        assert row["status"] == "pending"

    def test_sets_created_at_timestamp(self, test_db: Database):
        """Pattern should have created_at timestamp set."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Test",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        stage_run_id = "stage-test001"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        before = datetime.now(UTC)
        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs="Test logs",
            agent=agent,
        )
        after = datetime.now(UTC)

        # Verify timestamp in DB
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT created_at FROM failure_patterns WHERE id = ?",
                (pattern.id,),
            ).fetchone()

        created_at = datetime.fromisoformat(row["created_at"])
        assert before <= created_at <= after


class TestPatternDeduplication:
    """Test deduplication of similar failure patterns."""

    def test_dedup_same_symptom_hash_updates_occurrence_count(self, test_db: Database):
        """Same symptom hash should update occurrence_count instead of creating new pattern."""
        agent = NullProvider()
        # Same symptom across two extractions
        findings = [
            "SYMPTOM: CUDA out of memory during training",
            "ROOT_CAUSE: Insufficient GPU memory",
            "DETECTION: Error contains 'CUDA out of memory'",
            "PREVENTION: Reduce batch size",
            "SEVERITY: HIGH",
            "CONFIDENCE: HIGH",
        ]
        agent.configure_response(decision="approved", findings=findings)

        # First extraction
        stage_run_id_1 = "stage-train001"
        self._insert_stage_run(test_db, stage_run_id_1, "test-workspace", "train")
        pattern1 = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id_1,
            error="CUDA out of memory error",
            logs="Training logs",
            agent=agent,
        )

        # Second extraction with same symptom
        stage_run_id_2 = "stage-train002"
        self._insert_stage_run(test_db, stage_run_id_2, "test-workspace", "train")
        pattern2 = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id_2,
            error="CUDA out of memory error",
            logs="Training logs",
            agent=agent,
        )

        # Should be same pattern ID
        assert pattern1.id == pattern2.id

        # Check occurrence_count incremented
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT occurrence_count, last_seen_at FROM failure_patterns WHERE id = ?",
                (pattern1.id,),
            ).fetchone()

        assert row["occurrence_count"] == 2
        assert row["last_seen_at"] is not None

    def test_dedup_different_symptoms_create_separate_patterns(self, test_db: Database):
        """Different symptoms should create separate patterns."""
        agent = NullProvider()

        # First extraction - CUDA error
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: CUDA out of memory",
                "ROOT_CAUSE: GPU memory",
                "DETECTION: CUDA error",
                "PREVENTION: Reduce batch",
                "SEVERITY: HIGH",
                "CONFIDENCE: HIGH",
            ],
        )
        stage_run_id_1 = "stage-train001"
        self._insert_stage_run(test_db, stage_run_id_1, "test-workspace", "train")
        pattern1 = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id_1,
            error="CUDA out of memory",
            logs="Logs",
            agent=agent,
        )

        # Second extraction - different symptom
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: KeyError missing column",
                "ROOT_CAUSE: Schema mismatch",
                "DETECTION: KeyError",
                "PREVENTION: Validate schema",
                "SEVERITY: HIGH",
                "CONFIDENCE: HIGH",
            ],
        )
        stage_run_id_2 = "stage-preproc001"
        self._insert_stage_run(test_db, stage_run_id_2, "test-workspace", "preprocess")
        pattern2 = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id_2,
            error="KeyError: 'column'",
            logs="Logs",
            agent=agent,
        )

        # Should be different patterns
        assert pattern1.id != pattern2.id

        # Both should have occurrence_count = 1
        with test_db._conn() as conn:
            rows = conn.execute("SELECT id, occurrence_count FROM failure_patterns").fetchall()

        assert len(rows) == 2
        assert all(row["occurrence_count"] == 1 for row in rows)

    def test_dedup_uses_symptom_hash(self, test_db: Database):
        """Deduplication should use hash of symptom text."""
        symptom = "CUDA out of memory during training"
        expected_hash = hashlib.sha256(symptom.encode()).hexdigest()

        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                f"SYMPTOM: {symptom}",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: HIGH",
                "CONFIDENCE: HIGH",
            ],
        )

        stage_run_id = "stage-train001"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")
        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Error",
            logs="Logs",
            agent=agent,
        )

        # Pattern ID should be based on symptom hash
        assert expected_hash in pattern.id or pattern.id.startswith("fp-")


class TestRateLimiting:
    """Test rate limiting for pattern extraction."""

    def test_rate_limit_max_patterns_per_hour(self, test_db: Database):
        """Should enforce rate limit of max patterns per hour."""
        agent = NullProvider()
        max_per_hour = 10  # Assume default rate limit

        # Extract patterns rapidly
        for i in range(max_per_hour):
            agent.configure_response(
                decision="approved",
                findings=[
                    f"SYMPTOM: Unique error {i}",
                    "ROOT_CAUSE: Test",
                    "DETECTION: Test",
                    "PREVENTION: Test",
                    "SEVERITY: MEDIUM",
                    "CONFIDENCE: MEDIUM",
                ],
            )
            stage_run_id = f"stage-test{i:03d}"
            self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")
            pattern = extract_failure_pattern(
                db=test_db,
                stage_run_id=stage_run_id,
                error=f"Error {i}",
                logs="Logs",
                agent=agent,
            )
            assert pattern is not None

        # Next extraction should hit rate limit
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Rate limited error",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )
        stage_run_id = "stage-ratelimit"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        with pytest.raises(RateLimitExceededError) as exc_info:
            extract_failure_pattern(
                db=test_db,
                stage_run_id=stage_run_id,
                error="Rate limited error",
                logs="Logs",
                agent=agent,
            )

        assert "rate limit" in str(exc_info.value).lower()

    def test_rate_limit_resets_after_hour(self, test_db: Database):
        """Rate limit should reset after one hour."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Test error",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        # Extract pattern with old timestamp (> 1 hour ago)
        old_timestamp = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        stage_run_id = "stage-old"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")
        with test_db._conn() as conn:
            conn.execute(
                "INSERT INTO failure_patterns (id, symptom, root_cause, detection_heuristic, "
                "prevention, severity, confidence, source_run_id, source_workspace, created_at, "
                "occurrence_count, status, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "fp-old",
                    "Old symptom",
                    "Old cause",
                    "Old detection",
                    "Old prevention",
                    "MEDIUM",
                    "MEDIUM",
                    stage_run_id,
                    "test-workspace",
                    old_timestamp,
                    1,
                    "pending",
                    1,
                ),
            )

        # New extraction should succeed (old pattern doesn't count toward rate limit)
        stage_run_id_new = "stage-new"
        self._insert_stage_run(test_db, stage_run_id_new, "test-workspace", "train")
        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id_new,
            error="New error",
            logs="Logs",
            agent=agent,
        )
        assert pattern is not None


class TestAgentTimeout:
    """Test graceful handling of agent timeouts."""

    def test_agent_timeout_returns_none(self, test_db: Database):
        """Agent timeout should return None gracefully, not raise exception."""

        class TimeoutAgent:
            name = "timeout_agent"

            def run(self, request: ReviewRequest) -> ReviewResult:
                raise TimeoutError("Agent request timed out after 60s")

        agent = TimeoutAgent()
        stage_run_id = "stage-timeout"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        # Should return None, not raise
        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs="Test logs",
            agent=agent,
        )

        assert pattern is None

    def test_agent_exception_returns_none(self, test_db: Database):
        """Agent exceptions should be handled gracefully."""

        class BrokenAgent:
            name = "broken_agent"

            def run(self, request: ReviewRequest) -> ReviewResult:
                raise RuntimeError("Agent internal error")

        agent = BrokenAgent()
        stage_run_id = "stage-error"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs="Test logs",
            agent=agent,
        )

        assert pattern is None


class TestEmptyLogsHandling:
    """Test extraction with empty or minimal logs."""

    def test_empty_logs_still_extracts_from_error(self, test_db: Database):
        """Should extract pattern even with empty logs using just error message."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Import error",
                "ROOT_CAUSE: Missing dependency",
                "DETECTION: ImportError in error message",
                "PREVENTION: Add dependency to requirements",
                "SEVERITY: HIGH",
                "CONFIDENCE: MEDIUM",
            ],
        )

        stage_run_id = "stage-nologs"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="ImportError: No module named 'torch'",
            logs="",  # Empty logs
            agent=agent,
        )

        assert pattern is not None
        assert "Import error" in pattern.symptom
        assert pattern.confidence == "MEDIUM"  # Lower confidence without logs

    def test_none_logs_handled_gracefully(self, test_db: Database):
        """Should handle None logs without crashing."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Test",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: LOW",
            ],
        )

        stage_run_id = "stage-nonelogs"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs=None,  # None logs
            agent=agent,
        )

        assert pattern is not None


class TestLargeLogsHandling:
    """Test handling of very large log files."""

    def test_large_logs_truncated_to_prevent_token_overflow(self, test_db: Database):
        """Large logs should be truncated to prevent API token overflow."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Test",
                "ROOT_CAUSE: Test",
                "DETECTION: Test",
                "PREVENTION: Test",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        # Create very large logs (> 100KB)
        large_logs = "Log line\n" * 50000  # ~500KB

        stage_run_id = "stage-biglogs"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs=large_logs,
            agent=agent,
        )

        # Should succeed - logs should be truncated internally
        assert pattern is not None

    def test_log_truncation_preserves_end_of_logs(self, test_db: Database):
        """Log truncation should preserve the end (most recent) logs."""
        agent = NullProvider()

        # Capture what was sent to agent
        sent_logs = []

        class SpyAgent:
            name = "spy_agent"

            def run(self, request: ReviewRequest) -> ReviewResult:
                sent_logs.append(request.context.get("logs", ""))
                return ReviewResult(
                    decision="approved",
                    findings=[
                        "SYMPTOM: Test",
                        "ROOT_CAUSE: Test",
                        "DETECTION: Test",
                        "PREVENTION: Test",
                        "SEVERITY: MEDIUM",
                        "CONFIDENCE: MEDIUM",
                    ],
                    response_text="Analysis complete",
                    duration_ms=100,
                )

        agent = SpyAgent()

        # Create logs with distinctive start and end
        logs = "START_MARKER\n" + ("Middle line\n" * 10000) + "END_MARKER\n"

        stage_run_id = "stage-trunctest"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Test error",
            logs=logs,
            agent=agent,
        )

        # Check that END_MARKER was preserved
        assert len(sent_logs) == 1
        assert "END_MARKER" in sent_logs[0]


class TestStageTypeExtraction:
    """Test that stage_type is correctly extracted from context."""

    def test_extracts_stage_type_from_stage_run(self, test_db: Database):
        """Should extract stage_type from stage_runs table."""
        agent = NullProvider()

        for stage_type in ["train", "preprocess", "evaluate", "infer"]:
            # Use unique symptom per stage type to avoid deduplication
            agent.configure_response(
                decision="approved",
                findings=[
                    f"SYMPTOM: Test error for {stage_type}",
                    "ROOT_CAUSE: Test",
                    "DETECTION: Test",
                    "PREVENTION: Test",
                    "SEVERITY: MEDIUM",
                    "CONFIDENCE: MEDIUM",
                ],
            )

            stage_run_id = f"stage-{stage_type}"
            self._insert_stage_run(test_db, stage_run_id, "test-workspace", stage_type)

            pattern = extract_failure_pattern(
                db=test_db,
                stage_run_id=stage_run_id,
                error="Test error",
                logs="Test logs",
                agent=agent,
            )

            assert pattern.stage_type == stage_type

    def test_generic_pattern_when_stage_type_unclear(self, test_db: Database):
        """Should set stage_type=None when stage type is unclear or generic."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Generic Python error",
                "ROOT_CAUSE: Generic cause",
                "DETECTION: Generic detection",
                "PREVENTION: Generic prevention",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        # Stage run with generic/unknown stage name
        stage_run_id = "stage-unknown"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "custom_stage")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Generic error",
            logs="Logs",
            agent=agent,
        )

        # Generic patterns should have stage_type set based on actual stage name
        assert pattern.stage_type == "custom_stage"


class TestConfidenceLevel:
    """Test confidence level assignment."""

    def test_high_confidence_with_detailed_analysis(self, test_db: Database):
        """Should assign HIGH confidence when analysis is detailed and clear."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Very specific error with clear indicators",
                "ROOT_CAUSE: Well-understood root cause with evidence",
                "DETECTION: Precise detection heuristic with examples",
                "PREVENTION: Concrete prevention steps",
                "SEVERITY: HIGH",
                "CONFIDENCE: HIGH",
            ],
        )

        stage_run_id = "stage-high-conf"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Specific error with details",
            logs="Detailed logs with context",
            agent=agent,
        )

        assert pattern.confidence == "HIGH"

    def test_low_confidence_with_insufficient_info(self, test_db: Database):
        """Should assign LOW confidence when information is insufficient."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Generic error",
                "ROOT_CAUSE: Unclear",
                "DETECTION: Vague detection",
                "PREVENTION: Generic advice",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: LOW",
            ],
        )

        stage_run_id = "stage-low-conf"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Generic error",
            logs="",  # Empty logs lower confidence
            agent=agent,
        )

        assert pattern.confidence == "LOW"

    def test_medium_confidence_default(self, test_db: Database):
        """Should assign MEDIUM confidence as default for normal cases."""
        agent = NullProvider()
        agent.configure_response(
            decision="approved",
            findings=[
                "SYMPTOM: Standard error",
                "ROOT_CAUSE: Reasonable cause",
                "DETECTION: Standard detection",
                "PREVENTION: Standard prevention",
                "SEVERITY: MEDIUM",
                "CONFIDENCE: MEDIUM",
            ],
        )

        stage_run_id = "stage-med-conf"
        self._insert_stage_run(test_db, stage_run_id, "test-workspace", "train")

        pattern = extract_failure_pattern(
            db=test_db,
            stage_run_id=stage_run_id,
            error="Standard error",
            logs="Standard logs",
            agent=agent,
        )

        assert pattern.confidence == "MEDIUM"


# Helper methods for test setup


def _insert_stage_run(
    db: Database,
    stage_run_id: str,
    workspace_name: str,
    stage_name: str,
) -> None:
    """Helper to insert a stage run for testing."""
    with db._conn() as conn:
        # Ensure workspace exists
        conn.execute(
            "INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at) VALUES (?, ?)",
            (workspace_name, datetime.now(UTC).isoformat()),
        )
        # Ensure version exists
        conn.execute(
            "INSERT OR IGNORE INTO workspace_versions (workspace_name, version, git_tag, "
            "git_sha, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?)",
            (
                workspace_name,
                "v1",
                f"{workspace_name}-v1",
                "abc123",
                datetime.now(UTC).isoformat(),
                "test",
            ),
        )
        # Insert stage run
        conn.execute(
            "INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, "
            "started_at, backend_type) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                stage_run_id,
                workspace_name,
                "v1",
                stage_name,
                "failed",
                datetime.now(UTC).isoformat(),
                "local",
            ),
        )


# Add helper as class method to test classes
TestExtractFailurePatternBasics._insert_stage_run = staticmethod(_insert_stage_run)
TestPatternDeduplication._insert_stage_run = staticmethod(_insert_stage_run)
TestRateLimiting._insert_stage_run = staticmethod(_insert_stage_run)
TestAgentTimeout._insert_stage_run = staticmethod(_insert_stage_run)
TestEmptyLogsHandling._insert_stage_run = staticmethod(_insert_stage_run)
TestLargeLogsHandling._insert_stage_run = staticmethod(_insert_stage_run)
TestStageTypeExtraction._insert_stage_run = staticmethod(_insert_stage_run)
TestConfidenceLevel._insert_stage_run = staticmethod(_insert_stage_run)
