"""Integration tests for comparison block computation.

TDD: Tests for computing and storing comparison blocks in database.
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestComparisonIntegration:
    """Integration tests for comparison with real database."""

    def test_compute_comparison_vs_previous(self, test_db: Database) -> None:
        """Can compute vs_previous comparison with database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-prev", "test_ws", "v1", stage_name="train")
        _setup_stage_run(test_db, "stage-current", "test_ws", "v1", stage_name="train")

        manager = ExperimentRecordManager(test_db)

        # Create and finalize previous run
        prev_record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-prev",
        )
        prev_results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.70,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Previous run results.",
        }
        manager.finalize_run("stage-prev", prev_results)

        # Create current run record
        current_record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-current",
        )

        # Compute comparison for current run
        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }
        comparison = manager.compute_comparison(
            stage_run_id="stage-current",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert comparison is not None
        assert comparison["vs_previous"] is not None
        assert comparison["vs_previous"]["record"] == prev_record_id
        assert abs(comparison["vs_previous"]["delta"] - 0.05) < 0.001

    def test_store_and_retrieve_comparison(self, test_db: Database) -> None:
        """Can store and retrieve comparison from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-test", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-test",
        )

        comparison = {
            "vs_previous": {"record": "rec-prev", "delta": 0.05},
            "vs_best": None,
        }

        manager.store_comparison("stage-test", comparison)

        retrieved = manager.get_comparison("stage-test")
        assert retrieved is not None
        assert retrieved["vs_previous"]["record"] == "rec-prev"
        assert retrieved["vs_previous"]["delta"] == 0.05

    def test_comparison_with_no_previous_runs(self, test_db: Database) -> None:
        """vs_previous is None when no previous finalized runs exist."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-first", "test_ws", "v1", stage_name="train")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-first",
        )

        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }
        comparison = manager.compute_comparison(
            stage_run_id="stage-first",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        assert comparison["vs_previous"] is None

    def test_comparison_excludes_current_run(self, test_db: Database) -> None:
        """vs_previous excludes the current run being compared."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-only", "test_ws", "v1", stage_name="train")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-only",
        )

        # Finalize the run
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Only run in workspace.",
        }
        manager.finalize_run("stage-only", results)

        # Now compute comparison - should not compare to itself
        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }
        comparison = manager.compute_comparison(
            stage_run_id="stage-only",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        # Should be None since there's no OTHER finalized run
        assert comparison["vs_previous"] is None

    def test_comparison_filters_by_stage(self, test_db: Database) -> None:
        """vs_previous only considers runs from the same stage."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-preprocess", "test_ws", "v1", stage_name="preprocess")
        _setup_stage_run(test_db, "stage-train", "test_ws", "v1", stage_name="train")

        manager = ExperimentRecordManager(test_db)

        # Finalize a preprocess run
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-preprocess",
        )
        manager.finalize_run(
            "stage-preprocess",
            {
                "primary_metric": "accuracy",
                "direction": "maximize",
                "value": 0.90,
                "dataset_split": "val",
                "ml_outcome": "success",
                "notes": "Preprocess stage run.",
            },
        )

        # Create train run
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-train",
        )

        # Compare - should not find preprocess run
        current_results = {
            "value": 0.75,
            "primary_metric": "accuracy",
            "direction": "maximize",
        }
        comparison = manager.compute_comparison(
            stage_run_id="stage-train",
            workspace_name="test_ws",
            stage_name="train",
            results=current_results,
        )

        # No previous train runs
        assert comparison["vs_previous"] is None


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


def _setup_stage_run(
    db: Database,
    run_id: str,
    workspace: str,
    version: str,
    status: str = "completed",
    stage_name: str = "test_stage",
) -> None:
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
                (run_id, workspace, version, stage_name, status, datetime.now(UTC).isoformat()),
            )
