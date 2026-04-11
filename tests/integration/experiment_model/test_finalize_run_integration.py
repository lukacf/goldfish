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
        manager.save_results_spec("stage-preserve", record_id, spec)
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


def _setup_stage_run(
    db: Database,
    run_id: str,
    workspace: str,
    version: str,
    status: str = "completed",
    state: str | None = None,
    completed_with_warnings: int = 0,
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
                (id, workspace_name, version, stage_name, status, started_at, state, completed_with_warnings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    workspace,
                    version,
                    "test_stage",
                    status,
                    datetime.now(UTC).isoformat(),
                    state,
                    completed_with_warnings,
                ),
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


class TestFinalizeRunStateMachine:
    """Tests for finalize_run state machine integration (v1.2 lifecycle)."""

    def test_finalize_emits_user_finalize_transition(self, test_db: Database) -> None:
        """finalize_run should emit USER_FINALIZE event when state is AWAITING_USER_FINALIZATION."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-sm-finalize",
            "test_ws",
            "v1",
            status="completed",
            state="awaiting_user_finalization",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-sm-finalize",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing state machine transition.",
        }
        manager.finalize_run("stage-sm-finalize", results)

        # Verify state transitioned to COMPLETED
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state FROM stage_runs WHERE id = ?",
                ("stage-sm-finalize",),
            ).fetchone()
            assert row is not None
            assert row["state"] == "completed"

    def test_finalize_preserves_completed_with_warnings(self, test_db: Database) -> None:
        """finalize_run should preserve completed_with_warnings flag when transitioning."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-warnings",
            "test_ws",
            "v1",
            status="completed",
            state="awaiting_user_finalization",
            completed_with_warnings=1,  # Stage had warnings
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-warnings",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Testing warnings preservation.",
        }
        manager.finalize_run("stage-warnings", results)

        # Verify completed_with_warnings is preserved
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, completed_with_warnings FROM stage_runs WHERE id = ?",
                ("stage-warnings",),
            ).fetchone()
            assert row is not None
            assert row["state"] == "completed"
            assert row["completed_with_warnings"] == 1  # Should be preserved

    def test_finalize_transitions_running_to_completed(self, test_db: Database) -> None:
        """finalize_run on a RUNNING run should transition state to COMPLETED.

        This covers the orphaned-run scenario: executor is gone, run is stuck
        in 'running' state, but user has results and wants to finalize.
        """
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-orphan-running",
            "test_ws",
            "v1",
            status="running",
            state="running",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-orphan-running",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.72,
            "dataset_split": "val",
            "ml_outcome": "partial",
            "notes": "Run was orphaned but produced usable results.",
        }
        manager.finalize_run("stage-orphan-running", results)

        # State must transition — no more zombies
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state FROM stage_runs WHERE id = ?",
                ("stage-orphan-running",),
            ).fetchone()
            assert row is not None
            assert row["state"] == "completed"

    def test_finalize_transitions_post_run_to_completed(self, test_db: Database) -> None:
        """finalize_run on a POST_RUN run should transition state to COMPLETED."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-orphan-postrun",
            "test_ws",
            "v1",
            status="running",
            state="post_run",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-orphan-postrun",
        )

        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.68,
            "dataset_split": "val",
            "ml_outcome": "miss",
            "notes": "Post-run got stuck, finalizing manually.",
        }
        manager.finalize_run("stage-orphan-postrun", results)

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state FROM stage_runs WHERE id = ?",
                ("stage-orphan-postrun",),
            ).fetchone()
            assert row is not None
            assert row["state"] == "completed"


class TestFinalizationGate:
    """Tests for finalization gate behavior."""

    def test_gate_blocks_on_failed_runs(self, test_db: Database) -> None:
        """Gate should block when there are FAILED runs without finalization."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-failed",
            "test_ws",
            "v1",
            status="failed",
            state="failed",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-failed",
        )

        # Update infra_outcome to crashed (FAILED state maps to crashed infra_outcome)
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'crashed' WHERE stage_run_id = ?",
                ("stage-failed",),
            )

        result = manager.check_finalization_gate("test_ws")
        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 1

    def test_gate_blocks_on_terminated_runs(self, test_db: Database) -> None:
        """Gate should block when there are TERMINATED runs without finalization."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-terminated",
            "test_ws",
            "v1",
            status="terminated",
            state="terminated",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-terminated",
        )

        # Update infra_outcome to preempted (a common cause of TERMINATED)
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'preempted' WHERE stage_run_id = ?",
                ("stage-terminated",),
            )

        result = manager.check_finalization_gate("test_ws")
        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 1

    def test_gate_does_not_block_on_canceled_runs(self, test_db: Database) -> None:
        """Gate should NOT block on CANCELED runs - cancel already has reason."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-canceled",
            "test_ws",
            "v1",
            status="canceled",
            state="canceled",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-canceled",
        )

        # Update infra_outcome to canceled
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'canceled' WHERE stage_run_id = ?",
                ("stage-canceled",),
            )

        result = manager.check_finalization_gate("test_ws")
        # CANCELED should NOT require finalization
        assert result["blocked"] is False
        assert len(result["unfinalized"]) == 0

    def test_gate_clears_after_finalization(self, test_db: Database) -> None:
        """Gate should clear after run is finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(
            test_db,
            "stage-gate-clear",
            "test_ws",
            "v1",
            status="completed",
            state="awaiting_user_finalization",
        )

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-gate-clear",
        )

        # Set infra_outcome to completed (simulating post-run processing)
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-gate-clear",),
            )

        # Initially should be blocked (awaiting finalization)
        result = manager.check_finalization_gate("test_ws")
        assert result["blocked"] is True

        # Finalize the run
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Finalizing to clear gate.",
        }
        manager.finalize_run("stage-gate-clear", results)

        # Gate should now be clear
        result = manager.check_finalization_gate("test_ws")
        assert result["blocked"] is False
        assert len(result["unfinalized"]) == 0
