"""Integration tests for SVS MCP tools.

TDD: These tests are written BEFORE implementation. They will fail until
server_tools/svs_tools.py is created with the required MCP tool functions.

Tests cover:
1. list_failure_patterns - List patterns with filtering and pagination
2. get_failure_pattern - Get single pattern details
3. approve_pattern - Approve a pending pattern
4. reject_pattern - Reject pattern with reason
5. update_pattern - Update pattern fields
6. get_svs_reviews - List reviews for a stage run
7. get_run_svs_findings - Get findings summary for a run
8. review_pending_patterns - Batch review patterns with AI (librarian)
"""

# Import SVS tools functions directly by loading the module without triggering __init__.py
import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from goldfish.db.database import Database
from goldfish.errors import GoldfishError

svs_tools_available = False

# Load svs_tools.py directly without going through the package
svs_tools_path = Path(__file__).parent.parent.parent.parent / "src" / "goldfish" / "server_tools" / "svs_tools.py"
if svs_tools_path.exists():
    try:
        spec = importlib.util.spec_from_file_location("svs_tools_standalone", svs_tools_path)
        if spec and spec.loader:
            svs_module = importlib.util.module_from_spec(spec)
            sys.modules["svs_tools_standalone"] = svs_module
            spec.loader.exec_module(svs_module)

            # Extract functions
            list_failure_patterns = svs_module.list_failure_patterns
            get_failure_pattern = svs_module.get_failure_pattern
            approve_pattern = svs_module.approve_pattern
            reject_pattern = svs_module.reject_pattern
            update_pattern = svs_module.update_pattern
            get_svs_reviews = svs_module.get_svs_reviews
            get_run_svs_findings = svs_module.get_run_svs_findings
            review_pending_patterns = svs_module.review_pending_patterns
            librarian_review_patterns = svs_module.librarian_review_patterns
            svs_tools_available = True
    except Exception as e:
        print(f"Failed to load svs_tools: {e}")


pytestmark = pytest.mark.skipif(
    not svs_tools_available,
    reason="svs_tools.py not yet implemented",
)


# Create a mock svs_tools namespace that wraps functions with test_db
class _SVSToolsWrapper:
    """Wrapper that provides svs_tools.function() interface using test_db."""

    def __init__(self, db: Database):
        self.db = db

    def list_failure_patterns(self, **kwargs):
        return list_failure_patterns(self.db, **kwargs)

    def get_failure_pattern(self, **kwargs):
        return get_failure_pattern(self.db, **kwargs)

    def approve_pattern(self, **kwargs):
        return approve_pattern(self.db, **kwargs)

    def reject_pattern(self, **kwargs):
        return reject_pattern(self.db, **kwargs)

    def update_pattern(self, pattern_id: str, **kwargs):
        return update_pattern(self.db, pattern_id, **kwargs)

    def get_svs_reviews(self, **kwargs):
        return get_svs_reviews(self.db, **kwargs)

    def get_run_svs_findings(self, **kwargs):
        return get_run_svs_findings(self.db, **kwargs)

    def review_pending_patterns(self, **kwargs):
        return review_pending_patterns(self.db, **kwargs)


@pytest.fixture
def svs_tools(test_db: Database):
    """Provide svs_tools wrapper bound to test database."""
    return _SVSToolsWrapper(test_db)


def _setup_stage_run(test_db: Database, stage_run_id: str, workspace_name: str, version: str, stage_name: str):
    """Helper to create stage run with required FK dependencies."""
    # Create workspace lineage first (FK requirement)
    try:
        test_db.create_workspace_lineage(workspace_name, description="Test workspace")
    except Exception:
        pass  # May already exist

    # Create version (FK requirement)
    try:
        test_db.create_version(workspace_name, version, f"{workspace_name}-{version}", "sha123", "manual")
    except Exception:
        pass  # May already exist

    # Now create stage run
    test_db.create_stage_run(
        stage_run_id=stage_run_id,
        workspace_name=workspace_name,
        version=version,
        stage_name=stage_name,
    )


class TestListFailurePatterns:
    """Tests for list_failure_patterns MCP tool."""

    def test_list_all_patterns_no_filters(self, test_db: Database, svs_tools):
        """list_failure_patterns should return all patterns when no filters provided."""
        # Create test patterns
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM error",
            root_cause="Batch size too large",
            detection_heuristic="Error message contains 'out of memory'",
            prevention="Reduce batch size in config",
            created_at=datetime.now(UTC).isoformat(),
            severity="HIGH",
            status="pending",
        )
        test_db.create_failure_pattern(
            pattern_id="pat-002",
            symptom="Slow training",
            root_cause="No GPU acceleration",
            detection_heuristic="Training speed < 10 samples/sec",
            prevention="Check CUDA installation",
            created_at=datetime.now(UTC).isoformat(),
            severity="MEDIUM",
            status="approved",
        )

        result = svs_tools.list_failure_patterns()

        assert result["success"] is True
        assert len(result["patterns"]) == 2
        assert result["total_count"] == 2
        assert result["limit"] == 50  # Default
        assert result["offset"] == 0

    def test_list_patterns_filter_by_status(self, test_db: Database, svs_tools):
        """list_failure_patterns should filter by status."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM",
            root_cause="Memory",
            detection_heuristic="Error",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )
        test_db.create_failure_pattern(
            pattern_id="pat-002",
            symptom="Slow",
            root_cause="GPU",
            detection_heuristic="Speed",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="approved",
        )

        result = svs_tools.list_failure_patterns(status="pending")

        assert result["success"] is True
        assert len(result["patterns"]) == 1
        assert result["patterns"][0]["id"] == "pat-001"
        assert result["patterns"][0]["status"] == "pending"

    def test_list_patterns_filter_by_stage_type(self, test_db: Database, svs_tools):
        """list_failure_patterns should filter by stage_type."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM",
            root_cause="Memory",
            detection_heuristic="Error",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            stage_type="train",
        )
        test_db.create_failure_pattern(
            pattern_id="pat-002",
            symptom="Parse error",
            root_cause="Bad data",
            detection_heuristic="Exception",
            prevention="Validate",
            created_at=datetime.now(UTC).isoformat(),
            stage_type="preprocess",
        )

        result = svs_tools.list_failure_patterns(stage_type="train")

        assert result["success"] is True
        assert len(result["patterns"]) == 1
        assert result["patterns"][0]["stage_type"] == "train"

    def test_list_patterns_filter_by_severity(self, test_db: Database, svs_tools):
        """list_failure_patterns should filter by severity."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Critical failure",
            root_cause="System crash",
            detection_heuristic="SIGKILL",
            prevention="Fix bug",
            created_at=datetime.now(UTC).isoformat(),
            severity="CRITICAL",
        )
        test_db.create_failure_pattern(
            pattern_id="pat-002",
            symptom="Warning",
            root_cause="Minor issue",
            detection_heuristic="Log warning",
            prevention="Ignore",
            created_at=datetime.now(UTC).isoformat(),
            severity="LOW",
        )

        result = svs_tools.list_failure_patterns(severity="CRITICAL")

        assert result["success"] is True
        assert len(result["patterns"]) == 1
        assert result["patterns"][0]["severity"] == "CRITICAL"

    def test_list_patterns_pagination(self, test_db: Database, svs_tools):
        """list_failure_patterns should support pagination."""
        # Create 5 patterns
        for i in range(5):
            test_db.create_failure_pattern(
                pattern_id=f"pat-{i:03d}",
                symptom=f"Issue {i}",
                root_cause=f"Cause {i}",
                detection_heuristic=f"Detect {i}",
                prevention=f"Prevent {i}",
                created_at=datetime.now(UTC).isoformat(),
            )

        # First page
        result = svs_tools.list_failure_patterns(limit=2, offset=0)
        assert len(result["patterns"]) == 2
        assert result["total_count"] == 5
        assert result["has_more"] is True

        # Second page
        result = svs_tools.list_failure_patterns(limit=2, offset=2)
        assert len(result["patterns"]) == 2
        assert result["has_more"] is True

        # Last page
        result = svs_tools.list_failure_patterns(limit=2, offset=4)
        assert len(result["patterns"]) == 1
        assert result["has_more"] is False

    def test_list_patterns_invalid_limit(self, test_db: Database, svs_tools):
        """list_failure_patterns should reject invalid limit."""
        with pytest.raises(GoldfishError, match="limit must be between 1 and 200"):
            svs_tools.list_failure_patterns(limit=0)

        with pytest.raises(GoldfishError, match="limit must be between 1 and 200"):
            svs_tools.list_failure_patterns(limit=201)

    def test_list_patterns_invalid_offset(self, test_db: Database, svs_tools):
        """list_failure_patterns should reject invalid offset."""
        with pytest.raises(GoldfishError, match="offset must be >= 0"):
            svs_tools.list_failure_patterns(offset=-1)


class TestGetFailurePattern:
    """Tests for get_failure_pattern MCP tool."""

    def test_get_existing_pattern(self, test_db: Database, svs_tools):
        """get_failure_pattern should return pattern details."""
        test_db.create_failure_pattern(
            pattern_id="pat-123",
            symptom="OOM error",
            root_cause="Batch size too large",
            detection_heuristic="Error message contains 'out of memory'",
            prevention="Reduce batch_size to 32 or lower",
            created_at="2025-12-25T10:00:00Z",
            severity="HIGH",
            stage_type="train",
            confidence="HIGH",
        )

        result = svs_tools.get_failure_pattern(pattern_id="pat-123")

        assert result["success"] is True
        assert result["pattern"]["id"] == "pat-123"
        assert result["pattern"]["symptom"] == "OOM error"
        assert result["pattern"]["root_cause"] == "Batch size too large"
        assert result["pattern"]["severity"] == "HIGH"
        assert result["pattern"]["stage_type"] == "train"

    def test_get_nonexistent_pattern(self, test_db: Database, svs_tools):
        """get_failure_pattern should return error for nonexistent pattern."""
        result = svs_tools.get_failure_pattern(pattern_id="nonexistent")

        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestApprovePattern:
    """Tests for approve_pattern MCP tool."""

    def test_approve_pending_pattern(self, test_db: Database, svs_tools):
        """approve_pattern should approve pending pattern."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM",
            root_cause="Memory",
            detection_heuristic="Error",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        result = svs_tools.approve_pattern(pattern_id="pat-001")

        assert result["success"] is True
        assert result["pattern_id"] == "pat-001"
        assert result["status"] == "approved"

        # Verify in database
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["status"] == "approved"
        assert pattern["approved_at"] is not None

    def test_approve_nonexistent_pattern(self, test_db: Database, svs_tools):
        """approve_pattern should return error for nonexistent pattern."""
        result = svs_tools.approve_pattern(pattern_id="nonexistent")

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_approve_already_approved_pattern(self, test_db: Database, svs_tools):
        """approve_pattern should handle already-approved pattern gracefully."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM",
            root_cause="Memory",
            detection_heuristic="Error",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        # Approve once
        svs_tools.approve_pattern(pattern_id="pat-001")

        # Approve again - should succeed (idempotent)
        result = svs_tools.approve_pattern(pattern_id="pat-001")
        assert result["success"] is True


class TestRejectPattern:
    """Tests for reject_pattern MCP tool."""

    def test_reject_pending_pattern(self, test_db: Database, svs_tools):
        """reject_pattern should reject pending pattern with reason."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Spurious error",
            root_cause="Flaky test",
            detection_heuristic="Random",
            prevention="None",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        result = svs_tools.reject_pattern(
            pattern_id="pat-001",
            reason="This is a false positive - error is environmental",
        )

        assert result["success"] is True
        assert result["pattern_id"] == "pat-001"
        assert result["status"] == "rejected"

        # Verify in database
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["status"] == "rejected"
        assert pattern["rejection_reason"] == "This is a false positive - error is environmental"

    def test_reject_pattern_requires_reason(self, test_db: Database, svs_tools):
        """reject_pattern should require rejection reason."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        with pytest.raises(GoldfishError, match="reason.*required"):
            svs_tools.reject_pattern(pattern_id="pat-001", reason="")

    def test_reject_nonexistent_pattern(self, test_db: Database, svs_tools):
        """reject_pattern should return error for nonexistent pattern."""
        result = svs_tools.reject_pattern(
            pattern_id="nonexistent",
            reason="Does not exist",
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestUpdatePattern:
    """Tests for update_pattern MCP tool."""

    def test_update_pattern_severity(self, test_db: Database, svs_tools):
        """update_pattern should update severity field."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            created_at=datetime.now(UTC).isoformat(),
            severity="MEDIUM",
        )

        result = svs_tools.update_pattern(pattern_id="pat-001", severity="HIGH")

        assert result["success"] is True
        assert result["pattern_id"] == "pat-001"

        # Verify in database
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["severity"] == "HIGH"

    def test_update_pattern_multiple_fields(self, test_db: Database, svs_tools):
        """update_pattern should update multiple fields at once."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            created_at=datetime.now(UTC).isoformat(),
            severity="LOW",
            confidence="LOW",
        )

        result = svs_tools.update_pattern(
            pattern_id="pat-001",
            severity="CRITICAL",
            confidence="HIGH",
            manually_edited=True,
        )

        assert result["success"] is True

        # Verify all updates
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["severity"] == "CRITICAL"
        assert pattern["confidence"] == "HIGH"
        assert pattern["manually_edited"] == 1 or pattern["manually_edited"] is True  # SQLite returns int

    def test_update_pattern_invalid_field(self, test_db: Database, svs_tools):
        """update_pattern should reject invalid field names."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            created_at=datetime.now(UTC).isoformat(),
        )

        with pytest.raises(GoldfishError, match="Invalid field"):
            svs_tools.update_pattern(pattern_id="pat-001", invalid_field="value")

    def test_update_nonexistent_pattern(self, test_db: Database, svs_tools):
        """update_pattern should return error for nonexistent pattern."""
        result = svs_tools.update_pattern(pattern_id="nonexistent", severity="HIGH")

        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestGetSVSReviews:
    """Tests for get_svs_reviews MCP tool."""

    def test_get_reviews_for_stage_run(self, test_db: Database, svs_tools):
        """get_svs_reviews should return all reviews for a stage run."""
        # Create stage run with FK dependencies
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        # Create reviews
        test_db.create_svs_review(
            stage_run_id="stage-001",
            review_type="pre_run",
            model_used="claude-opus-4-5-20251101",
            prompt_hash="abc123",
            decision="approved",
            reviewed_at=datetime.now(UTC).isoformat(),
        )
        test_db.create_svs_review(
            stage_run_id="stage-001",
            review_type="post_run",
            model_used="claude-opus-4-5-20251101",
            prompt_hash="def456",
            decision="warned",
            reviewed_at=datetime.now(UTC).isoformat(),
            signal_name="model_output",
        )

        result = svs_tools.get_svs_reviews(stage_run_id="stage-001")

        assert result["success"] is True
        assert len(result["reviews"]) == 2
        assert result["stage_run_id"] == "stage-001"

    def test_get_reviews_filter_by_type(self, test_db: Database, svs_tools):
        """get_svs_reviews should filter by review_type."""
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        test_db.create_svs_review(
            stage_run_id="stage-001",
            review_type="pre_run",
            model_used="claude-opus-4-5-20251101",
            prompt_hash="abc123",
            decision="approved",
            reviewed_at=datetime.now(UTC).isoformat(),
        )
        test_db.create_svs_review(
            stage_run_id="stage-001",
            review_type="post_run",
            model_used="claude-opus-4-5-20251101",
            prompt_hash="def456",
            decision="warned",
            reviewed_at=datetime.now(UTC).isoformat(),
        )

        result = svs_tools.get_svs_reviews(
            stage_run_id="stage-001",
            review_type="pre_run",
        )

        assert result["success"] is True
        assert len(result["reviews"]) == 1
        assert result["reviews"][0]["review_type"] == "pre_run"

    def test_get_reviews_pagination(self, test_db: Database, svs_tools):
        """get_svs_reviews should support pagination."""
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        # Create 5 reviews
        for i in range(5):
            test_db.create_svs_review(
                stage_run_id="stage-001",
                review_type="post_run",
                model_used="claude-opus-4-5-20251101",
                prompt_hash=f"hash-{i}",
                decision="approved",
                reviewed_at=datetime.now(UTC).isoformat(),
            )

        result = svs_tools.get_svs_reviews(
            stage_run_id="stage-001",
            limit=2,
            offset=0,
        )

        assert len(result["reviews"]) == 2
        assert result["has_more"] is True

    def test_get_reviews_empty_result(self, test_db: Database, svs_tools):
        """get_svs_reviews should handle stage run with no reviews."""
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        result = svs_tools.get_svs_reviews(stage_run_id="stage-001")

        assert result["success"] is True
        assert len(result["reviews"]) == 0


class TestGetRunSVSFindings:
    """Tests for get_run_svs_findings MCP tool."""

    def test_get_findings_with_reviews_and_patterns(self, test_db: Database, svs_tools):
        """get_run_svs_findings should aggregate findings from reviews and patterns."""
        # Create stage run with FK dependencies
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        # Create SVS review with findings
        findings = {
            "errors": ["Memory allocation failed"],
            "warnings": ["Slow convergence detected"],
        }
        test_db.create_svs_review(
            stage_run_id="stage-001",
            review_type="post_run",
            model_used="claude-opus-4-5-20251101",
            prompt_hash="abc123",
            decision="blocked",
            reviewed_at=datetime.now(UTC).isoformat(),
            parsed_findings=json.dumps(findings),
        )

        # Create failure pattern
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Memory allocation failed",
            root_cause="OOM",
            detection_heuristic="Error message",
            prevention="Reduce batch size",
            created_at=datetime.now(UTC).isoformat(),
            source_run_id="stage-001",
            severity="CRITICAL",
        )

        result = svs_tools.get_run_svs_findings(stage_run_id="stage-001")

        assert result["success"] is True
        assert result["stage_run_id"] == "stage-001"
        assert "reviews" in result
        assert "failure_patterns" in result
        assert len(result["reviews"]) == 1
        assert len(result["failure_patterns"]) == 1

    def test_get_findings_no_data(self, test_db: Database, svs_tools):
        """get_run_svs_findings should handle run with no findings."""
        _setup_stage_run(test_db, "stage-001", "test", "v1", "train")

        result = svs_tools.get_run_svs_findings(stage_run_id="stage-001")

        assert result["success"] is True
        assert len(result["reviews"]) == 0
        assert len(result["failure_patterns"]) == 0

    def test_get_findings_nonexistent_run(self, test_db: Database, svs_tools):
        """get_run_svs_findings should return error for nonexistent run."""
        result = svs_tools.get_run_svs_findings(stage_run_id="nonexistent")

        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestReviewPendingPatterns:
    """Tests for review_pending_patterns MCP tool (batch AI review)."""

    @patch("svs_tools_standalone.librarian_review_patterns")
    def test_review_patterns_dry_run(self, mock_librarian, test_db: Database, svs_tools):
        """review_pending_patterns should preview actions in dry_run mode."""
        # Create pending patterns
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="OOM",
            root_cause="Memory",
            detection_heuristic="Error",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        # Mock AI response
        mock_librarian.return_value = {
            "pat-001": {"action": "approve", "confidence": "high"},
        }

        result = svs_tools.review_pending_patterns(dry_run=True)

        assert result["success"] is True
        assert result["dry_run"] is True
        assert len(result["actions"]) == 1
        assert result["actions"][0]["pattern_id"] == "pat-001"
        assert result["actions"][0]["action"] == "approve"

        # Verify no changes in database
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["status"] == "pending"

    @patch("svs_tools_standalone.librarian_review_patterns")
    def test_review_patterns_apply_changes(self, mock_librarian, test_db: Database, svs_tools):
        """review_pending_patterns should apply AI recommendations when dry_run=False."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Valid error",
            root_cause="Real issue",
            detection_heuristic="Solid",
            prevention="Fix",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )
        test_db.create_failure_pattern(
            pattern_id="pat-002",
            symptom="False positive",
            root_cause="Flaky",
            detection_heuristic="Random",
            prevention="None",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        # Mock AI response
        mock_librarian.return_value = {
            "pat-001": {"action": "approve", "confidence": "high"},
            "pat-002": {"action": "reject", "reason": "False positive", "confidence": "medium"},
        }

        result = svs_tools.review_pending_patterns(dry_run=False)

        assert result["success"] is True
        assert result["dry_run"] is False
        assert len(result["actions"]) == 2

        # Verify changes applied
        pattern1 = test_db.get_failure_pattern("pat-001")
        assert pattern1 is not None
        assert pattern1["status"] == "approved"

        pattern2 = test_db.get_failure_pattern("pat-002")
        assert pattern2 is not None
        assert pattern2["status"] == "rejected"

    def test_review_patterns_no_pending(self, test_db: Database, svs_tools):
        """review_pending_patterns should handle no pending patterns gracefully."""
        result = svs_tools.review_pending_patterns(dry_run=False)

        assert result["success"] is True
        assert len(result["actions"]) == 0
        assert "No pending patterns" in result.get("message", "")

    @patch("svs_tools_standalone.librarian_review_patterns")
    def test_review_patterns_ai_error_handling(self, mock_librarian, test_db: Database, svs_tools):
        """review_pending_patterns should handle AI errors gracefully."""
        test_db.create_failure_pattern(
            pattern_id="pat-001",
            symptom="Error",
            root_cause="Cause",
            detection_heuristic="Detect",
            prevention="Prevent",
            created_at=datetime.now(UTC).isoformat(),
            status="pending",
        )

        # Mock AI failure
        mock_librarian.side_effect = Exception("AI service unavailable")

        result = svs_tools.review_pending_patterns(dry_run=False)

        assert result["success"] is False
        assert "AI service unavailable" in result["error"]

        # Verify pattern unchanged
        pattern = test_db.get_failure_pattern("pat-001")
        assert pattern is not None
        assert pattern["status"] == "pending"
