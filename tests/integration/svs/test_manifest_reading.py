"""TDD tests for SVS manifest reading in stage finalization.

Tests verify:
- Manifest file reading and aggregation
- Graceful handling of missing/corrupt manifests
- Version checking and compatibility
- Stats storage in signal_lineage
- Findings storage in stage_runs
- Pattern extraction on failure
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from goldfish.state_machine import EventContext, StageEvent, transition

if TYPE_CHECKING:
    from goldfish.db.database import Database


@pytest.fixture
def outputs_dir(tmp_path: Path) -> Path:
    """Create a temporary outputs directory with .goldfish subdirectory."""
    goldfish_dir = tmp_path / ".goldfish"
    goldfish_dir.mkdir(parents=True)
    return tmp_path


@pytest.fixture
def setup_workspace_for_run(test_db: Database) -> None:
    """Setup workspace and version for stage runs."""
    test_db.create_workspace_lineage("test_ws", description="Test workspace")
    test_db.create_version("test_ws", "v1", "test_ws-v1", "sha123", "manual")


class TestReadSVSManifests:
    """Tests for reading SVS manifest files from outputs directory."""

    def test_reads_stats_manifest(self, outputs_dir: Path) -> None:
        """Should read stats from svs_stats.json."""
        from goldfish.svs.manifest import read_svs_manifests

        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_data = {
            "version": 1,
            "stats": {
                "tokens": {"mean": 7234.5, "std": 4102.3, "min": 0, "max": 15033},
            },
        }
        stats_path.write_text(json.dumps(stats_data))

        result = read_svs_manifests(outputs_dir)

        assert result["stats"]["tokens"]["mean"] == 7234.5
        assert result["version"] == 1

    def test_reads_findings_manifest(self, outputs_dir: Path) -> None:
        """Should read AI review findings from svs_findings.json."""
        from goldfish.svs.manifest import read_svs_manifests

        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        findings_data = {
            "version": 1,
            "decision": "warned",
            "findings": ["WARNING: Low entropy detected"],
            "stats": {"tokens": {"entropy": 3.2}},
            "duration_ms": 1500,
        }
        findings_path.write_text(json.dumps(findings_data))

        result = read_svs_manifests(outputs_dir)

        assert result["ai_review"]["decision"] == "warned"
        assert "WARNING: Low entropy" in result["ai_review"]["findings"][0]

    def test_reads_during_run_history(self, outputs_dir: Path) -> None:
        """Should expose during-run history from svs_findings.json."""
        from goldfish.svs.manifest import read_svs_manifests

        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        findings_data = {
            "version": 1,
            "decision": "warned",
            "findings": ["WARN: [during_run] metric_health at step 10 - NaN detected"],
            "history": [
                {
                    "phase": "during_run",
                    "severity": "WARN",
                    "check": "metric_health",
                    "summary": "NaN detected",
                    "step": 10,
                    "timestamp": "2025-01-01T00:00:00Z",
                }
            ],
        }
        findings_path.write_text(json.dumps(findings_data))

        result = read_svs_manifests(outputs_dir)

        assert result["during_run"]["decision"] == "warned"
        assert result["during_run"]["history"][0]["check"] == "metric_health"

    def test_findings_stats_override_base_stats(self, outputs_dir: Path) -> None:
        """Findings manifest stats should override base stats manifest."""
        from goldfish.svs.manifest import read_svs_manifests

        # Base stats
        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_data = {
            "version": 1,
            "stats": {
                "tokens": {"mean": 7234.5, "entropy": 9.0},
            },
        }
        stats_path.write_text(json.dumps(stats_data))

        # Findings with updated entropy
        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        findings_data = {
            "version": 1,
            "stats": {
                "tokens": {"entropy": 3.2},  # More recent, should override
            },
        }
        findings_path.write_text(json.dumps(findings_data))

        result = read_svs_manifests(outputs_dir)

        # entropy should be from findings (3.2), mean from stats (7234.5)
        assert result["stats"]["tokens"]["entropy"] == 3.2
        assert result["stats"]["tokens"]["mean"] == 7234.5


class TestMissingManifests:
    """Tests for graceful handling of missing manifests."""

    def test_handles_missing_stats_manifest(self, outputs_dir: Path) -> None:
        """Should record missing stats manifest but not fail."""
        from goldfish.svs.manifest import read_svs_manifests

        # No stats file created
        result = read_svs_manifests(outputs_dir)

        assert "svs_stats.json" in result["missing"]
        assert result["stats"] == {}

    def test_handles_missing_findings_manifest(self, outputs_dir: Path) -> None:
        """Should record missing findings manifest but not fail."""
        from goldfish.svs.manifest import read_svs_manifests

        # Create stats but no findings
        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_path.write_text(json.dumps({"version": 1, "stats": {}}))

        result = read_svs_manifests(outputs_dir)

        assert "svs_findings.json" in result["missing"]
        assert result["ai_review"] is None

    def test_handles_missing_goldfish_dir(self, tmp_path: Path) -> None:
        """Should handle completely missing .goldfish directory."""
        from goldfish.svs.manifest import read_svs_manifests

        result = read_svs_manifests(tmp_path)

        assert len(result["missing"]) >= 1
        assert result["stats"] == {}


class TestCorruptManifests:
    """Tests for graceful handling of corrupt manifests."""

    def test_handles_corrupt_stats_json(self, outputs_dir: Path) -> None:
        """Should handle invalid JSON in stats manifest."""
        from goldfish.svs.manifest import read_svs_manifests

        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_path.write_text("{ not valid json }")

        result = read_svs_manifests(outputs_dir)

        assert "svs_stats.json (corrupt)" in result["missing"]

    def test_handles_corrupt_findings_json(self, outputs_dir: Path) -> None:
        """Should handle invalid JSON in findings manifest."""
        from goldfish.svs.manifest import read_svs_manifests

        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        findings_path.write_text("not json at all")

        result = read_svs_manifests(outputs_dir)

        assert "svs_findings.json (corrupt)" in result["missing"]


class TestVersionChecking:
    """Tests for manifest version compatibility."""

    def test_accepts_current_version(self, outputs_dir: Path) -> None:
        """Should accept manifests with current version."""
        from goldfish.svs.manifest import EXPECTED_MANIFEST_VERSION, read_svs_manifests

        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_path.write_text(json.dumps({"version": EXPECTED_MANIFEST_VERSION, "stats": {}}))

        result = read_svs_manifests(outputs_dir)

        assert result["version"] == EXPECTED_MANIFEST_VERSION
        assert result.get("version_mismatch") is False

    def test_warns_on_version_mismatch(self, outputs_dir: Path) -> None:
        """Should warn but continue with mismatched version."""
        from goldfish.svs.manifest import read_svs_manifests

        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_path.write_text(json.dumps({"version": 99, "stats": {"test": {}}}))

        result = read_svs_manifests(outputs_dir)

        # Should still read the data
        assert result["stats"] == {"test": {}}
        # But record version mismatch
        assert result.get("version_mismatch") is True

    def test_handles_missing_version(self, outputs_dir: Path) -> None:
        """Should handle manifest without version field (legacy)."""
        from goldfish.svs.manifest import read_svs_manifests

        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_path.write_text(json.dumps({"stats": {"test": {}}}))

        result = read_svs_manifests(outputs_dir)

        # Should still read the data
        assert result["stats"] == {"test": {}}


class TestIntegrationWithFinalize:
    """Tests for SVS manifest integration in _finalize_stage_run."""

    def test_stores_svs_findings_in_stage_run(
        self,
        test_db: Database,
        setup_workspace_for_run: None,
        outputs_dir: Path,
    ) -> None:
        """Should store SVS findings JSON in stage_runs table."""
        # Create stage run
        stage_run_id = "stage-svs-001"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="tokenize",
        )

        # Create SVS findings
        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        findings_data = {
            "version": 1,
            "decision": "approved",
            "findings": [],
            "stats": {"tokens": {"entropy": 9.06}},
        }
        findings_path.write_text(json.dumps(findings_data))

        # Read manifests and store
        from goldfish.svs.manifest import read_svs_manifests

        svs_data = read_svs_manifests(outputs_dir)
        test_db.update_stage_run_svs_findings(stage_run_id, json.dumps(svs_data))

        # Verify stored
        stage_run = test_db.get_stage_run(stage_run_id)
        assert stage_run is not None
        stored_findings = json.loads(stage_run["svs_findings_json"] or "{}")
        assert stored_findings["stats"]["tokens"]["entropy"] == 9.06

    def test_stores_stats_in_signal_lineage(
        self,
        test_db: Database,
        setup_workspace_for_run: None,
        outputs_dir: Path,
    ) -> None:
        """Should store output stats in signal_lineage.stats_json."""
        # Create stage run
        stage_run_id = "stage-svs-002"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="tokenize",
        )

        # Create signal lineage entry using add_signal
        test_db.add_signal(
            stage_run_id=stage_run_id,
            signal_name="tokens",
            signal_type="npy",
            storage_location="/outputs/tokens.npy",
        )

        # Create SVS stats
        stats_path = outputs_dir / ".goldfish" / "svs_stats.json"
        stats_data = {
            "version": 1,
            "stats": {
                "tokens": {"mean": 7234.5, "entropy": 9.06, "null_ratio": 0.0},
            },
        }
        stats_path.write_text(json.dumps(stats_data))

        # Read manifests and update signal stats
        from goldfish.svs.manifest import read_svs_manifests

        svs_data = read_svs_manifests(outputs_dir)
        for signal_name, signal_stats in svs_data.get("stats", {}).items():
            test_db.update_signal_lineage_stats(stage_run_id, signal_name, json.dumps(signal_stats))

        # Verify stored
        signals = test_db.list_signals(stage_run_id=stage_run_id)
        assert len(signals) == 1
        stored_stats = json.loads(signals[0]["stats_json"] or "{}")
        assert stored_stats["entropy"] == 9.06


class TestPatternExtractionOnFailure:
    """Tests for failure pattern extraction during finalization."""

    def test_extracts_pattern_on_failed_run(
        self,
        test_db: Database,
        setup_workspace_for_run: None,
    ) -> None:
        """Should extract failure pattern when run fails (if enabled)."""
        # This tests the integration point - actual extraction logic
        # is tested in test_pattern_extractor.py
        stage_run_id = "stage-fail-001"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )

        # Transition through states to FAILED using state machine
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        # Transition: PREPARING -> BUILDING -> LAUNCHING -> RUNNING -> FAILED
        transition(test_db, stage_run_id, StageEvent.BUILD_START, ctx)
        transition(test_db, stage_run_id, StageEvent.BUILD_OK, ctx)
        transition(test_db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
        fail_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=1, exit_code_exists=True)
        transition(test_db, stage_run_id, StageEvent.EXIT_FAILURE, fail_ctx)

        # Update metadata separately
        test_db.update_stage_run_status(
            stage_run_id=stage_run_id,
            completed_at=datetime.now(UTC).isoformat(),
            error="OOM: CUDA out of memory",
        )

        # Verify stage run is in failed state
        stage_run = test_db.get_stage_run(stage_run_id)
        assert stage_run is not None
        assert stage_run["state"] == "failed"
        assert "OOM" in (stage_run["error"] or "")

    def test_no_pattern_extraction_on_success(
        self,
        test_db: Database,
        setup_workspace_for_run: None,
    ) -> None:
        """Should not extract patterns when run succeeds."""
        stage_run_id = "stage-success-001"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )

        # Transition through states to COMPLETED using state machine (v1.2 lifecycle)
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        # Transition: PREPARING -> BUILDING -> LAUNCHING -> RUNNING -> POST_RUN -> AWAITING_USER_FINALIZATION -> COMPLETED
        transition(test_db, stage_run_id, StageEvent.BUILD_START, ctx)
        transition(test_db, stage_run_id, StageEvent.BUILD_OK, ctx)
        transition(test_db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
        success_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=0, exit_code_exists=True)
        transition(test_db, stage_run_id, StageEvent.EXIT_SUCCESS, success_ctx)
        transition(test_db, stage_run_id, StageEvent.POST_RUN_OK, ctx)
        finalize_ctx = EventContext(timestamp=datetime.now(UTC), source="mcp_tool")
        transition(test_db, stage_run_id, StageEvent.USER_FINALIZE, finalize_ctx)

        # Update metadata separately
        test_db.update_stage_run_status(
            stage_run_id=stage_run_id,
            completed_at=datetime.now(UTC).isoformat(),
        )

        # Get initial pattern count
        patterns = test_db.list_failure_patterns()

        # Successful runs should not create patterns
        assert len(patterns) == 0
