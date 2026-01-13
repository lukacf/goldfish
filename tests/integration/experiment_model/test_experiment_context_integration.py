"""Integration tests for experiment context retrieval.

TDD: Tests for getting experiment context with real database.
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestExperimentContextIntegration:
    """Integration tests for experiment context with real database."""

    def test_get_experiment_context_integration(self, test_db: Database) -> None:
        """Can get experiment context from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        context = manager.get_experiment_context("test_ws")

        assert context is not None
        assert "current_best" in context
        assert "awaiting_finalization" in context
        assert "recent_trend" in context

    def test_context_includes_unfinalized_runs(self, test_db: Database) -> None:
        """Context shows runs awaiting finalization."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-ctx", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-ctx",
        )

        # Mark as completed (terminal) but don't finalize
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-ctx",),
            )

        context = manager.get_experiment_context("test_ws")

        assert len(context["awaiting_finalization"]) == 1
        assert context["awaiting_finalization"][0]["record_id"] == record_id

    def test_context_includes_recent_trend(self, test_db: Database) -> None:
        """Context shows recent finalized values."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-trend1", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-trend2", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-trend1")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-trend2")

        # Finalize both
        results1 = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.70,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "First trend point.",
        }
        results2 = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Second trend point.",
        }
        manager.finalize_run("stage-trend1", results1)
        manager.finalize_run("stage-trend2", results2)

        context = manager.get_experiment_context("test_ws")

        assert len(context["recent_trend"]) >= 2

    def test_get_recent_trend_integration(self, test_db: Database) -> None:
        """Can get recent trend from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-trend", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-trend")

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Trend test run.",
        }
        manager.finalize_run("stage-trend", results)

        trend = manager.get_recent_trend("test_ws", limit=10)

        assert len(trend) == 1
        assert trend[0]["value"] == 0.75

    def test_get_current_best_integration(self, test_db: Database) -> None:
        """Can get current best from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-best", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-best")

        # Finalize and tag as best
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.85,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Best run for test.",
        }
        manager.finalize_run("stage-best", results)
        manager.tag_record(record_id, "best-accuracy")

        best = manager.get_current_best("test_ws", tag_prefix="best-")

        assert best is not None
        assert best["record_id"] == record_id
        assert best["value"] == 0.85


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
                (run_id, workspace, version, "test_stage", status, datetime.now(UTC).isoformat()),
            )
