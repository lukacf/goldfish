"""Integration tests for experiment model database schema.

TDD: These tests define the expected behavior for the new database tables.
Tests should fail initially (RED), then pass after schema is added (GREEN).
"""

import json
import sqlite3
from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database


class TestExperimentRecordsTable:
    """Tests for the experiment_records table."""

    def test_create_experiment_record_for_run(self, test_db: Database) -> None:
        """Can create an experiment record for a run."""
        # First create the workspace and version it depends on
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")

        record_id = "01HXYZ1234567890ABCDEF"
        created_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO experiment_records
                (record_id, workspace_name, type, stage_run_id, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, "test_ws", "run", "stage-abc123", "v1", created_at),
            )

            row = conn.execute(
                "SELECT * FROM experiment_records WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        assert row is not None
        assert row["record_id"] == record_id
        assert row["workspace_name"] == "test_ws"
        assert row["type"] == "run"
        assert row["stage_run_id"] == "stage-abc123"
        assert row["version"] == "v1"

    def test_create_experiment_record_for_checkpoint(self, test_db: Database) -> None:
        """Can create an experiment record for a checkpoint (no stage_run_id)."""
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        record_id = "01HXYZ1234567890ABCDEG"
        created_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO experiment_records
                (record_id, workspace_name, type, stage_run_id, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, "test_ws", "checkpoint", None, "v2", created_at),
            )

            row = conn.execute(
                "SELECT * FROM experiment_records WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        assert row is not None
        assert row["type"] == "checkpoint"
        assert row["stage_run_id"] is None

    def test_experiment_records_workspace_index(self, test_db: Database) -> None:
        """Index on workspace_name enables fast queries."""
        _setup_workspace_and_version(test_db, "ws1", "v1")
        _setup_workspace_and_version(test_db, "ws2", "v1")

        with test_db._conn() as conn:
            # Insert records for different workspaces
            for i, ws in enumerate(["ws1", "ws1", "ws2"]):
                conn.execute(
                    """
                    INSERT INTO experiment_records
                    (record_id, workspace_name, type, version, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (f"rec-{i}", ws, "checkpoint", "v1", datetime.now(UTC).isoformat()),
                )

            # Query by workspace
            rows = conn.execute(
                "SELECT * FROM experiment_records WHERE workspace_name = ?",
                ("ws1",),
            ).fetchall()

        assert len(rows) == 2


class TestRunResultsTable:
    """Tests for the run_results table."""

    def test_create_run_results(self, test_db: Database) -> None:
        """Can create run_results with all fields."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")
        _setup_experiment_record(test_db, "rec-001", "test_ws", "v1", "run", "stage-abc123")

        results_auto = json.dumps({"accuracy": 0.62, "loss": 2.3})
        results_final = json.dumps({"primary_metric": "accuracy", "value": 0.63})
        comparison = json.dumps({"vs_best": {"delta": 0.01}})
        finalized_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_results
                (stage_run_id, record_id, results_status, infra_outcome, ml_outcome,
                 results_auto, results_final, comparison, finalized_by, finalized_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-abc123",
                    "rec-001",
                    "finalized",
                    "completed",
                    "success",
                    results_auto,
                    results_final,
                    comparison,
                    "ml_claude",
                    finalized_at,
                ),
            )

            row = conn.execute(
                "SELECT * FROM run_results WHERE stage_run_id = ?",
                ("stage-abc123",),
            ).fetchone()

        assert row is not None
        assert row["results_status"] == "finalized"
        assert row["infra_outcome"] == "completed"
        assert row["ml_outcome"] == "success"
        assert json.loads(row["results_auto"])["accuracy"] == 0.62
        assert json.loads(row["results_final"])["value"] == 0.63

    def test_run_results_status_values(self, test_db: Database) -> None:
        """results_status can be: missing, auto, finalized."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        for i, status in enumerate(["missing", "auto", "finalized"]):
            run_id = f"stage-{i:08x}"
            rec_id = f"rec-{i:03d}"
            _setup_stage_run(test_db, run_id, "test_ws", "v1")
            _setup_experiment_record(test_db, rec_id, "test_ws", "v1", "run", run_id)

            with test_db._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO run_results
                    (stage_run_id, record_id, results_status, infra_outcome, ml_outcome)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (run_id, rec_id, status, "completed", "unknown"),
                )

                row = conn.execute(
                    "SELECT results_status FROM run_results WHERE stage_run_id = ?",
                    (run_id,),
                ).fetchone()

            assert row["results_status"] == status

    def test_run_results_infra_outcome_values(self, test_db: Database) -> None:
        """infra_outcome can be: completed, preempted, crashed, canceled, unknown."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        outcomes = ["completed", "preempted", "crashed", "canceled", "unknown"]
        for i, outcome in enumerate(outcomes):
            run_id = f"stage-inf{i:06x}"
            rec_id = f"rec-inf{i:03d}"
            _setup_stage_run(test_db, run_id, "test_ws", "v1")
            _setup_experiment_record(test_db, rec_id, "test_ws", "v1", "run", run_id)

            with test_db._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO run_results
                    (stage_run_id, record_id, results_status, infra_outcome, ml_outcome)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (run_id, rec_id, "missing", outcome, "unknown"),
                )

                row = conn.execute(
                    "SELECT infra_outcome FROM run_results WHERE stage_run_id = ?",
                    (run_id,),
                ).fetchone()

            assert row["infra_outcome"] == outcome

    def test_run_results_ml_outcome_values(self, test_db: Database) -> None:
        """ml_outcome can be: success, partial, miss, unknown."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        outcomes = ["success", "partial", "miss", "unknown"]
        for i, outcome in enumerate(outcomes):
            run_id = f"stage-ml{i:07x}"
            rec_id = f"rec-ml{i:03d}"
            _setup_stage_run(test_db, run_id, "test_ws", "v1")
            _setup_experiment_record(test_db, rec_id, "test_ws", "v1", "run", run_id)

            with test_db._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO run_results
                    (stage_run_id, record_id, results_status, infra_outcome, ml_outcome)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (run_id, rec_id, "missing", "completed", outcome),
                )

                row = conn.execute(
                    "SELECT ml_outcome FROM run_results WHERE stage_run_id = ?",
                    (run_id,),
                ).fetchone()

            assert row["ml_outcome"] == outcome


class TestRunResultsSpecTable:
    """Tests for the run_results_spec table."""

    def test_create_run_results_spec(self, test_db: Database) -> None:
        """Can create run_results_spec entry."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-spec123", "test_ws", "v1")
        _setup_experiment_record(test_db, "rec-spec001", "test_ws", "v1", "run", "stage-spec123")

        spec_json = json.dumps(
            {
                "primary_metric": "accuracy",
                "direction": "maximize",
                "min_value": 0.6,
                "goal_value": 0.8,
                "dataset_split": "val",
                "tolerance": 0.01,
                "context": "Testing results spec persistence in database.",
            }
        )
        created_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_results_spec
                (stage_run_id, record_id, spec_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                ("stage-spec123", "rec-spec001", spec_json, created_at),
            )

            row = conn.execute(
                "SELECT * FROM run_results_spec WHERE stage_run_id = ?",
                ("stage-spec123",),
            ).fetchone()

        assert row is not None
        spec = json.loads(row["spec_json"])
        assert spec["primary_metric"] == "accuracy"
        assert spec["direction"] == "maximize"


class TestRunTagsTable:
    """Tests for the run_tags table."""

    def test_create_run_tag(self, test_db: Database) -> None:
        """Can create a run tag."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tag123", "test_ws", "v1")
        _setup_experiment_record(test_db, "rec-tag001", "test_ws", "v1", "run", "stage-tag123")

        created_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_tags
                (workspace_name, record_id, tag_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                ("test_ws", "rec-tag001", "best-25m-63pct", created_at),
            )

            row = conn.execute(
                "SELECT * FROM run_tags WHERE tag_name = ?",
                ("best-25m-63pct",),
            ).fetchone()

        assert row is not None
        assert row["workspace_name"] == "test_ws"
        assert row["record_id"] == "rec-tag001"

    def test_run_tags_unique_per_workspace(self, test_db: Database) -> None:
        """Tag names must be unique within a workspace."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tag1", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tag2", "test_ws", "v1")
        _setup_experiment_record(test_db, "rec-tag1", "test_ws", "v1", "run", "stage-tag1")
        _setup_experiment_record(test_db, "rec-tag2", "test_ws", "v1", "run", "stage-tag2")

        created_at = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_tags
                (workspace_name, record_id, tag_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                ("test_ws", "rec-tag1", "best-model", created_at),
            )

            # Same tag name in same workspace should fail
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO run_tags
                    (workspace_name, record_id, tag_name, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    ("test_ws", "rec-tag2", "best-model", created_at),
                )


# Helper functions to set up test data


def _setup_workspace_and_version(db: Database, workspace: str, version: str) -> None:
    """Set up workspace lineage and version for foreign key constraints."""
    with db._conn() as conn:
        # Check if workspace already exists
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

        # Check if version already exists
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


def _setup_stage_run(db: Database, run_id: str, workspace: str, version: str) -> None:
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
                (run_id, workspace, version, "test_stage", "completed", datetime.now(UTC).isoformat()),
            )


def _setup_experiment_record(
    db: Database,
    record_id: str,
    workspace: str,
    version: str,
    record_type: str,
    stage_run_id: str | None = None,
) -> None:
    """Set up an experiment record."""
    with db._conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM experiment_records WHERE record_id = ?",
            (record_id,),
        ).fetchone()

        if not existing:
            conn.execute(
                """
                INSERT INTO experiment_records
                (record_id, workspace_name, type, stage_run_id, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, workspace, record_type, stage_run_id, version, datetime.now(UTC).isoformat()),
            )
