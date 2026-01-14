"""Integration tests for strict finalization gate.

TDD: Tests for blocking runs when unfinalized terminal runs exist.
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestFinalizationGateIntegration:
    """Integration tests for finalization gate with real database."""

    def test_gate_blocks_when_unfinalized_completed_run(self, test_db: Database) -> None:
        """Gate blocks when a completed run is not finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-unfinalized", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-unfinalized",
        )

        # Update to completed infra outcome without finalizing
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-unfinalized",),
            )

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 1

    def test_gate_allows_when_all_finalized(self, test_db: Database) -> None:
        """Gate allows when all terminal runs are finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-finalized", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-finalized",
        )

        # Finalize the run
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Finalized run for gate test.",
        }
        manager.finalize_run("stage-finalized", results)

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is False
        assert result["unfinalized"] == []

    def test_gate_ignores_running_runs(self, test_db: Database) -> None:
        """Gate does not consider running/pending runs."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-running", "test_ws", "v1", status="running")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-running",
        )

        # Leave infra_outcome as 'unknown' (default for running)
        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is False

    def test_gate_blocks_on_preempted_unfinalized(self, test_db: Database) -> None:
        """Gate blocks on preempted runs that are not finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-preempted", "test_ws", "v1", status="preempted")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-preempted",
        )

        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'preempted' WHERE stage_run_id = ?",
                ("stage-preempted",),
            )

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True

    def test_gate_blocks_on_crashed_unfinalized(self, test_db: Database) -> None:
        """Gate blocks on crashed runs that are not finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-crashed", "test_ws", "v1", status="failed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-crashed",
        )

        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'crashed' WHERE stage_run_id = ?",
                ("stage-crashed",),
            )

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True

    def test_list_unfinalized_runs_integration(self, test_db: Database) -> None:
        """Can list all unfinalized terminal runs."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-done1", "test_ws", "v1", status="completed")
        _setup_stage_run(test_db, "stage-done2", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        rec1 = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-done1")
        rec2 = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-done2")

        # Set both to completed
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id IN (?, ?)",
                ("stage-done1", "stage-done2"),
            )

        unfinalized = manager.list_unfinalized_runs("test_ws")

        assert len(unfinalized) == 2
        record_ids = [r["record_id"] for r in unfinalized]
        assert rec1 in record_ids
        assert rec2 in record_ids

    def test_gate_scoped_to_workspace(self, test_db: Database) -> None:
        """Gate only considers runs in the specified workspace."""
        _setup_workspace_and_version(test_db, "ws1", "v1")
        _setup_workspace_and_version(test_db, "ws2", "v1")
        _setup_stage_run(test_db, "stage-ws1", "ws1", "v1", status="completed")
        _setup_stage_run(test_db, "stage-ws2", "ws2", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="ws1", version="v1", stage_run_id="stage-ws1")
        manager.create_run_record(workspace_name="ws2", version="v1", stage_run_id="stage-ws2")

        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-ws1",),
            )

        # ws1 should be blocked, ws2 should not
        result_ws1 = manager.check_finalization_gate("ws1")
        result_ws2 = manager.check_finalization_gate("ws2")

        assert result_ws1["blocked"] is True
        assert result_ws2["blocked"] is False


class TestParallelRunsFinalizationGate:
    """Tests for parallel runs behavior with finalization gate.

    Per spec: "Block run() only if there exists a terminal infra run in the same
    workspace with results_status != finalized. Running/pending runs do not block.
    Parallel runs are allowed."
    """

    def test_gate_allows_multiple_parallel_running_runs(self, test_db: Database) -> None:
        """Multiple parallel running runs don't block each other."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-train-1", "test_ws", "v1", status="running")
        _setup_stage_run(test_db, "stage-train-2", "test_ws", "v1", status="running")
        _setup_stage_run(test_db, "stage-eval", "test_ws", "v1", status="pending")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-train-1")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-train-2")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-eval")

        # All have infra_outcome='unknown' (default for running/pending)
        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is False
        assert result["unfinalized"] == []

    def test_gate_allows_parallel_runs_in_different_stages(self, test_db: Database) -> None:
        """Parallel runs in different stages are allowed simultaneously."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-preprocess", "test_ws", "v1", status="running", stage="preprocess")
        _setup_stage_run(test_db, "stage-train", "test_ws", "v1", status="running", stage="train")
        _setup_stage_run(test_db, "stage-eval", "test_ws", "v1", status="running", stage="evaluate")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-preprocess")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-train")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-eval")

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is False

    def test_gate_blocks_when_one_terminal_among_parallel_runs(self, test_db: Database) -> None:
        """Gate blocks when at least one terminal run is unfinalized, even with parallel running."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-running", "test_ws", "v1", status="running")
        _setup_stage_run(test_db, "stage-completed", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-running")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-completed")

        # Mark one as completed (terminal)
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-completed",),
            )

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 1

    def test_gate_unblocks_after_finalizing_terminal_run(self, test_db: Database) -> None:
        """Gate unblocks once the terminal run is finalized, even with parallel running."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-running", "test_ws", "v1", status="running")
        _setup_stage_run(test_db, "stage-completed", "test_ws", "v1", status="completed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-running")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-completed")

        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-completed",),
            )

        # Initially blocked
        assert manager.check_finalization_gate("test_ws")["blocked"] is True

        # Finalize the completed run
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.85,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Finalized to unblock gate.",
        }
        manager.finalize_run("stage-completed", results)

        # Now should be unblocked (running run doesn't count)
        result = manager.check_finalization_gate("test_ws")
        assert result["blocked"] is False

    def test_gate_handles_mixed_terminal_statuses(self, test_db: Database) -> None:
        """Gate correctly handles mix of terminal statuses (completed, preempted, crashed)."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-completed", "test_ws", "v1", status="completed")
        _setup_stage_run(test_db, "stage-preempted", "test_ws", "v1", status="preempted")
        _setup_stage_run(test_db, "stage-crashed", "test_ws", "v1", status="failed")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-completed")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-preempted")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-crashed")

        with test_db._conn() as conn:
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'completed' WHERE stage_run_id = ?",
                ("stage-completed",),
            )
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'preempted' WHERE stage_run_id = ?",
                ("stage-preempted",),
            )
            conn.execute(
                "UPDATE run_results SET infra_outcome = 'crashed' WHERE stage_run_id = ?",
                ("stage-crashed",),
            )

        result = manager.check_finalization_gate("test_ws")

        assert result["blocked"] is True
        assert len(result["unfinalized"]) == 3


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
    stage: str = "test_stage",
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
                (run_id, workspace, version, stage, status, datetime.now(UTC).isoformat()),
            )
