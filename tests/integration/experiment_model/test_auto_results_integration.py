"""Integration tests for auto results extraction.

TDD: Tests for extracting and storing results_auto in database.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestAutoResultsIntegration:
    """Integration tests for auto results with real database."""

    def test_extract_and_update_auto_results(self, test_db: Database) -> None:
        """Can extract auto results and update run_results."""
        # Setup
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-auto-test", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)

        # Create run record
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-auto-test",
        )

        # Store results spec
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing accuracy for auto extraction.",
        }
        manager.save_results_spec("stage-auto-test", record_id, spec)

        # Add some metrics
        _add_metric(test_db, "stage-auto-test", "accuracy", 0.75)

        # Extract auto results
        auto_results = manager.extract_auto_results("stage-auto-test")
        assert auto_results is not None
        assert auto_results["value"] == 0.75

        # Update run_results
        manager.update_auto_results("stage-auto-test", auto_results, "completed")

        # Verify run_results was updated
        run_results = manager.get_run_results("stage-auto-test")
        assert run_results is not None
        assert run_results["results_status"] == "auto"
        assert run_results["infra_outcome"] == "completed"
        assert run_results["results_auto"] is not None

        parsed_auto = json.loads(run_results["results_auto"])
        assert parsed_auto["value"] == 0.75

    def test_extract_auto_results_with_secondary(self, test_db: Database) -> None:
        """Auto results include secondary metrics."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-secondary", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-secondary",
        )

        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "secondary_metrics": ["loss", "f1"],
            "context": "Testing with secondary metrics.",
        }
        manager.save_results_spec("stage-secondary", record_id, spec)

        _add_metric(test_db, "stage-secondary", "accuracy", 0.75)
        _add_metric(test_db, "stage-secondary", "loss", 0.25)
        _add_metric(test_db, "stage-secondary", "f1", 0.70)

        auto_results = manager.extract_auto_results("stage-secondary")

        assert auto_results is not None
        assert auto_results["value"] == 0.75
        assert "secondary" in auto_results
        assert auto_results["secondary"]["loss"] == 0.25
        assert auto_results["secondary"]["f1"] == 0.70

    def test_derive_infra_outcome_from_status(self, test_db: Database) -> None:
        """Infra outcome is derived from run status."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        # Test various status mappings
        assert manager.derive_infra_outcome("completed") == "completed"
        assert manager.derive_infra_outcome("failed") == "crashed"
        assert manager.derive_infra_outcome("preempted") == "preempted"
        assert manager.derive_infra_outcome("canceled") == "canceled"
        assert manager.derive_infra_outcome("running") == "unknown"

    def test_auto_results_no_spec_returns_none(self, test_db: Database) -> None:
        """Returns None when no spec exists."""
        manager = ExperimentRecordManager(test_db)
        result = manager.extract_auto_results("nonexistent-stage")
        assert result is None

    def test_auto_results_missing_primary_metric(self, test_db: Database) -> None:
        """Auto results handle missing primary metric gracefully."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-missing", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-missing",
        )

        spec = {
            "primary_metric": "accuracy",  # But no accuracy metric logged
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing missing metric handling.",
        }
        manager.save_results_spec("stage-missing", record_id, spec)

        # Don't add any metrics
        auto_results = manager.extract_auto_results("stage-missing")

        assert auto_results is not None
        assert auto_results["primary_metric"] == "accuracy"
        assert auto_results["value"] is None  # No metric found


# Helper functions


def _setup_workspace_and_version(db: Database, workspace: str, version: str) -> None:
    """Set up workspace lineage and version for foreign key constraints."""
    with db._conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM workspace_lineage WHERE workspace_name = ?",
            (workspace,),
        ).fetchone()

        if not existing:
            conn.execute(
                """
                INSERT INTO workspace_lineage (workspace_name, created_at)
                VALUES (?, ?)
                """,
                (workspace, datetime.now(UTC).isoformat()),
            )

        existing_version = conn.execute(
            "SELECT 1 FROM workspace_versions WHERE workspace_name = ? AND version = ?",
            (workspace, version),
        ).fetchone()

        if not existing_version:
            conn.execute(
                """
                INSERT INTO workspace_versions
                (workspace_name, version, git_tag, git_sha, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace,
                    version,
                    f"{workspace}-{version}",
                    "abc123def456",
                    datetime.now(UTC).isoformat(),
                    "test",
                ),
            )


def _setup_stage_run(db: Database, run_id: str, workspace: str, version: str, status: str = "completed") -> None:
    """Set up a stage run for foreign key constraints."""
    with db._conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM stage_runs WHERE id = ?",
            (run_id,),
        ).fetchone()

        if not existing:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, workspace, version, "test_stage", status, datetime.now(UTC).isoformat()),
            )


def _add_metric(db: Database, stage_run_id: str, name: str, value: float) -> None:
    """Add a metric to run_metrics_summary."""
    with db._conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO run_metrics_summary
            (stage_run_id, name, last_value, min_value, max_value, count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (stage_run_id, name, value, value, value, 1),
        )
