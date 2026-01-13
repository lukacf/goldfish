"""Integration tests for finalize_run functionality.

TDD: Tests for finalizing ML results with real database.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import InvalidFinalizeResultsError


class TestFinalizeRunIntegration:
    """Integration tests for finalizing run results."""

    def test_finalize_run_integration(self, test_db: Database) -> None:
        """Can finalize a run and verify in database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-finalize", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-finalize",
        )

        # Verify initial state
        run_results = manager.get_run_results("stage-finalize")
        assert run_results is not None
        assert run_results["results_status"] == "missing"
        assert run_results["ml_outcome"] == "unknown"

        # Finalize
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Good results with stable training process.",
        }
        manager.finalize_run("stage-finalize", results)

        # Verify finalized state
        run_results = manager.get_run_results("stage-finalize")
        assert run_results is not None
        assert run_results["results_status"] == "finalized"
        assert run_results["ml_outcome"] == "success"
        assert run_results["results_final"] is not None

        parsed = json.loads(run_results["results_final"])
        assert parsed["value"] == 0.75

    def test_finalize_run_preserves_auto_results(self, test_db: Database) -> None:
        """Finalize preserves results_auto unchanged."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-preserve", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-preserve",
        )

        # Store spec and add metrics for auto extraction
        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing preservation of auto results.",
        }
        manager.store_results_spec("stage-preserve", record_id, spec)
        _add_metric(test_db, "stage-preserve", "accuracy", 0.70)

        # Extract and update auto results
        auto = manager.extract_auto_results("stage-preserve")
        manager.update_auto_results("stage-preserve", auto, "completed")

        # Verify auto results
        run_results = manager.get_run_results("stage-preserve")
        assert run_results["results_status"] == "auto"
        auto_before = run_results["results_auto"]

        # Now finalize with different value
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,  # Different from auto 0.70
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Final value differs from auto.",
        }
        manager.finalize_run("stage-preserve", results)

        # Verify auto is preserved
        run_results = manager.get_run_results("stage-preserve")
        assert run_results["results_auto"] == auto_before
        assert run_results["results_status"] == "finalized"

    def test_finalize_run_validates_results(self, test_db: Database) -> None:
        """Invalid results are rejected."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-invalid", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-invalid",
        )

        invalid_results = {
            "primary_metric": "accuracy",
            # Missing required fields
        }

        with pytest.raises(InvalidFinalizeResultsError):
            manager.finalize_run("stage-invalid", invalid_results)

        # Verify state unchanged
        run_results = manager.get_run_results("stage-invalid")
        assert run_results["results_status"] == "missing"

    def test_finalize_run_by_record_id(self, test_db: Database) -> None:
        """Can finalize using record_id instead of stage_run_id."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-by-record", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-by-record",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Finalized via record ID lookup.",
        }

        # Use record_id instead of stage_run_id
        manager.finalize_run(record_id, results)

        # Verify
        run_results = manager.get_run_results("stage-by-record")
        assert run_results["results_status"] == "finalized"

    def test_finalize_run_sets_timestamps(self, test_db: Database) -> None:
        """Finalize sets finalized_at and finalized_by."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-timestamps", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-timestamps",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing finalization timestamps.",
        }
        manager.finalize_run("stage-timestamps", results, finalized_by="test_user")

        run_results = manager.get_run_results("stage-timestamps")
        assert run_results["finalized_by"] == "test_user"
        assert run_results["finalized_at"] is not None

    def test_get_finalized_results_integration(self, test_db: Database) -> None:
        """Can retrieve finalized results as dict."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-get-final", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-get-final",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing get finalized results.",
        }
        manager.finalize_run("stage-get-final", results)

        parsed = manager.get_finalized_results("stage-get-final")
        assert parsed is not None
        assert parsed["value"] == 0.75
        assert parsed["ml_outcome"] == "success"


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
