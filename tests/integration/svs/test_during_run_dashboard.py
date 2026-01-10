"""Integration tests for during-run SVS findings appearing in dashboard.

Tests that during-run findings get synced to svs_reviews table during live sync,
so they show up in new_svs_reviews in the dashboard.
"""

import hashlib
import json

import pytest

from goldfish.db.database import Database


class FakeConfig:
    """Minimal config for testing _sync_during_run_to_svs_reviews."""

    class SVS:
        agent_model = "test-model"

    svs = SVS()


class SyncTester:
    """Wrapper to test _sync_during_run_to_svs_reviews in isolation."""

    def __init__(self, db: Database):
        self.db = db
        self.config = FakeConfig()

    def _sync_during_run_to_svs_reviews(self, stage_run_id: str, during_run: dict) -> None:
        """Copy of the method from StageExecutor for testing."""
        history = during_run.get("history", [])
        if not history:
            return

        # Get existing during-run review timestamps for this run
        existing_reviews = self.db.get_svs_reviews(stage_run_id=stage_run_id, review_type="during_run")
        existing_hashes = set()
        for r in existing_reviews:
            if r.get("prompt_hash"):
                existing_hashes.add(r["prompt_hash"])

        # Insert new findings
        decision = during_run.get("decision", "approved")
        for entry in history:
            if not isinstance(entry, dict):
                continue

            # Create a unique hash for this entry
            timestamp = entry.get("timestamp", "")
            check_name = entry.get("check", "")
            summary = entry.get("summary", "")[:100]
            unique_key = f"during_run_{stage_run_id}_{timestamp}_{check_name}_{summary}"
            entry_hash = hashlib.sha256(unique_key.encode()).hexdigest()[:16]

            if entry_hash in existing_hashes:
                continue

            try:
                from datetime import datetime

                self.db.create_svs_review(
                    stage_run_id=stage_run_id,
                    review_type="during_run",
                    model_used=self.config.svs.agent_model or "unknown",
                    prompt_hash=entry_hash,
                    decision=decision,
                    parsed_findings=json.dumps([entry]),
                    response_text=entry.get("summary"),
                    reviewed_at=timestamp or datetime.now().isoformat(),
                    duration_ms=0,
                )
            except Exception:
                pass


@pytest.fixture
def sync_tester(test_db: Database):
    """Create a SyncTester for testing the sync function."""
    return SyncTester(test_db)


class TestDuringRunSyncToSVSReviews:
    """Test that during-run findings sync to svs_reviews table."""

    def _setup_workspace(self, db: Database, workspace: str = "test-ws") -> None:
        """Helper to create workspace lineage and versions."""
        db.create_workspace_lineage(workspace, None, None, "Test workspace")
        for i in range(1, 3):
            db.create_version(workspace, f"v{i}", f"{workspace}-v{i}", f"sha{i}", "run")

    def test_sync_during_run_inserts_to_svs_reviews(self, test_db: Database, sync_tester: SyncTester):
        """During-run findings should be inserted into svs_reviews table."""
        # Arrange: Create workspace and a running stage
        self._setup_workspace(test_db)
        test_db.create_stage_run(
            stage_run_id="stage-test123",
            workspace_name="test-ws",
            stage_name="train",
            version="v1",
        )

        # Simulate during-run findings from the AI monitor
        during_run = {
            "decision": "blocked",
            "history": [
                {
                    "phase": "during_run",
                    "severity": "ERROR",
                    "check": "AI: loss_divergence",
                    "summary": "Loss is diverging badly",
                    "timestamp": "2026-01-09T10:00:00+00:00",
                    "step": 100,
                },
                {
                    "phase": "during_run",
                    "severity": "WARN",
                    "check": "AI: accuracy_stall",
                    "summary": "Accuracy not improving",
                    "timestamp": "2026-01-09T10:05:00+00:00",
                    "step": 200,
                },
            ],
        }

        # Act: Call the sync function
        sync_tester._sync_during_run_to_svs_reviews("stage-test123", during_run)

        # Assert: Check that findings were inserted into svs_reviews
        reviews = test_db.get_svs_reviews(stage_run_id="stage-test123", review_type="during_run")
        assert len(reviews) == 2

        # Extract checks from all reviews (order independent)
        all_checks = set()
        for r in reviews:
            assert r["review_type"] == "during_run"
            assert r["decision"] == "blocked"
            assert r["notified"] == 0  # Should be unnotified for dashboard
            parsed = json.loads(r["parsed_findings"])
            assert len(parsed) == 1
            all_checks.add(parsed[0]["check"])

        # Verify both findings were inserted
        assert all_checks == {"AI: loss_divergence", "AI: accuracy_stall"}

    def test_sync_during_run_avoids_duplicates(self, test_db: Database, sync_tester):
        """Calling sync multiple times should not create duplicate entries."""
        # Arrange
        self._setup_workspace(test_db)
        test_db.create_stage_run(
            stage_run_id="stage-dup123",
            workspace_name="test-ws",
            stage_name="train",
            version="v1",
        )

        during_run = {
            "decision": "approved",
            "history": [
                {
                    "phase": "during_run",
                    "severity": "NOTE",
                    "check": "AI: progress",
                    "summary": "Training progressing normally",
                    "timestamp": "2026-01-09T10:00:00+00:00",
                    "step": 100,
                },
            ],
        }

        # Act: Call sync twice
        sync_tester._sync_during_run_to_svs_reviews("stage-dup123", during_run)
        sync_tester._sync_during_run_to_svs_reviews("stage-dup123", during_run)

        # Assert: Should still only have 1 entry
        reviews = test_db.get_svs_reviews(stage_run_id="stage-dup123", review_type="during_run")
        assert len(reviews) == 1

    def test_sync_during_run_incremental(self, test_db: Database, sync_tester):
        """New findings should be added on subsequent syncs."""
        # Arrange
        self._setup_workspace(test_db)
        test_db.create_stage_run(
            stage_run_id="stage-inc123",
            workspace_name="test-ws",
            stage_name="train",
            version="v1",
        )

        # First sync with 1 finding
        during_run_v1 = {
            "decision": "approved",
            "history": [
                {
                    "phase": "during_run",
                    "severity": "NOTE",
                    "check": "AI: epoch1",
                    "summary": "First epoch complete",
                    "timestamp": "2026-01-09T10:00:00+00:00",
                    "step": 100,
                },
            ],
        }
        sync_tester._sync_during_run_to_svs_reviews("stage-inc123", during_run_v1)

        # Second sync with 2 findings (original + new)
        during_run_v2 = {
            "decision": "warned",
            "history": [
                {
                    "phase": "during_run",
                    "severity": "NOTE",
                    "check": "AI: epoch1",
                    "summary": "First epoch complete",
                    "timestamp": "2026-01-09T10:00:00+00:00",
                    "step": 100,
                },
                {
                    "phase": "during_run",
                    "severity": "WARN",
                    "check": "AI: epoch2",
                    "summary": "Second epoch has issues",
                    "timestamp": "2026-01-09T10:10:00+00:00",
                    "step": 200,
                },
            ],
        }
        sync_tester._sync_during_run_to_svs_reviews("stage-inc123", during_run_v2)

        # Assert: Should have 2 entries now
        reviews = test_db.get_svs_reviews(stage_run_id="stage-inc123", review_type="during_run")
        assert len(reviews) == 2

    def test_unnotified_reviews_appear_in_dashboard(self, test_db: Database, sync_tester):
        """Unnotified during-run reviews should be returned by get_unnotified_svs_reviews."""
        # Arrange
        self._setup_workspace(test_db)
        test_db.create_stage_run(
            stage_run_id="stage-dash123",
            workspace_name="test-ws",
            stage_name="train",
            version="v1",
        )

        during_run = {
            "decision": "blocked",
            "history": [
                {
                    "phase": "during_run",
                    "severity": "ERROR",
                    "check": "AI: critical_issue",
                    "summary": "Critical training issue detected",
                    "timestamp": "2026-01-09T10:00:00+00:00",
                    "step": 100,
                },
            ],
        }

        # Act: Sync the findings
        sync_tester._sync_during_run_to_svs_reviews("stage-dash123", during_run)

        # Assert: Should appear in unnotified reviews
        unnotified = test_db.get_unnotified_svs_reviews(limit=10)
        assert len(unnotified) >= 1

        # Find our review
        our_review = next((r for r in unnotified if r["stage_run_id"] == "stage-dash123"), None)
        assert our_review is not None
        assert our_review["review_type"] == "during_run"
        assert our_review["notified"] == 0

        # Act: Mark as notified
        test_db.mark_svs_reviews_notified([our_review["id"]])

        # Assert: Should no longer appear
        unnotified_after = test_db.get_unnotified_svs_reviews(limit=10)
        our_review_after = next((r for r in unnotified_after if r["stage_run_id"] == "stage-dash123"), None)
        assert our_review_after is None
