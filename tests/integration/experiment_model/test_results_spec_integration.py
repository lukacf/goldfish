"""Integration tests for run_results_spec persistence.

TDD: These tests verify actual database operations for results specs.
Tests should fail initially (RED), then pass after implementation (GREEN).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.experiment_model.schemas import InvalidResultsSpecError


class TestResultsSpecIntegration:
    """Integration tests for results spec persistence with real database."""

    def test_store_and_retrieve_results_spec(self, test_db: Database) -> None:
        """Can store and retrieve a results spec from database."""
        # Setup workspace and version
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-spec-test", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        # Create a run record first
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-spec-test",
        )

        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing model accuracy on validation set for spec test.",
        }

        # Store the spec
        manager.store_results_spec(
            stage_run_id="stage-spec-test",
            record_id=record_id,
            spec=spec,
        )

        # Retrieve the spec
        result = manager.get_results_spec("stage-spec-test")

        assert result is not None
        assert result["stage_run_id"] == "stage-spec-test"
        assert result["record_id"] == record_id

        # Verify JSON content
        parsed = json.loads(result["spec_json"])
        assert parsed["primary_metric"] == "accuracy"
        assert parsed["direction"] == "maximize"

    def test_store_results_spec_validates_before_insert(self, test_db: Database) -> None:
        """Invalid specs are rejected before database insert."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-invalid", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-invalid",
        )

        invalid_spec = {
            "primary_metric": "accuracy",
            # Missing required fields
        }

        with pytest.raises(InvalidResultsSpecError):
            manager.store_results_spec(
                stage_run_id="stage-invalid",
                record_id=record_id,
                spec=invalid_spec,
            )

        # Verify nothing was inserted
        result = manager.get_results_spec("stage-invalid")
        assert result is None

    def test_get_results_spec_by_record_integration(self, test_db: Database) -> None:
        """Can retrieve results spec by record_id."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-by-record", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-by-record",
        )

        spec = {
            "primary_metric": "loss",
            "direction": "minimize",
            "min_value": 0.1,
            "goal_value": 0.05,
            "dataset_split": "test",
            "tolerance": 0.005,
            "context": "Testing loss minimization on test set.",
        }

        manager.store_results_spec(
            stage_run_id="stage-by-record",
            record_id=record_id,
            spec=spec,
        )

        # Retrieve by record_id
        result = manager.get_results_spec_by_record(record_id)

        assert result is not None
        assert result["record_id"] == record_id
        parsed = json.loads(result["spec_json"])
        assert parsed["direction"] == "minimize"

    def test_get_results_spec_not_found_integration(self, test_db: Database) -> None:
        """Returns None when results spec not found."""
        manager = ExperimentRecordManager(test_db)
        result = manager.get_results_spec("nonexistent-stage")
        assert result is None

    def test_get_results_spec_by_record_not_found_integration(self, test_db: Database) -> None:
        """Returns None when results spec not found by record_id."""
        manager = ExperimentRecordManager(test_db)
        result = manager.get_results_spec_by_record("nonexistent-record")
        assert result is None

    def test_get_results_spec_parsed_integration(self, test_db: Database) -> None:
        """Can retrieve parsed results spec from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-parsed", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-parsed",
        )

        spec = {
            "primary_metric": "f1_score",
            "direction": "maximize",
            "min_value": 0.70,
            "goal_value": 0.85,
            "dataset_split": "val",
            "tolerance": 0.02,
            "secondary_metrics": ["precision", "recall"],
            "context": "Testing F1 score on validation set with secondary metrics.",
        }

        manager.store_results_spec(
            stage_run_id="stage-parsed",
            record_id=record_id,
            spec=spec,
        )

        # Get parsed result
        parsed = manager.get_results_spec_parsed("stage-parsed")

        assert parsed is not None
        assert parsed["primary_metric"] == "f1_score"
        assert parsed["secondary_metrics"] == ["precision", "recall"]

    def test_results_spec_with_all_optional_fields(self, test_db: Database) -> None:
        """Can store and retrieve spec with all optional fields."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-full-spec", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-full-spec",
        )

        spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "secondary_metrics": ["loss", "auc"],
            "baseline_run": "@best-baseline",
            "failure_threshold": 0.50,
            "known_caveats": ["Small dataset", "High variance"],
            "context": "Full spec test with all optional fields populated.",
        }

        manager.store_results_spec(
            stage_run_id="stage-full-spec",
            record_id=record_id,
            spec=spec,
        )

        parsed = manager.get_results_spec_parsed("stage-full-spec")

        assert parsed is not None
        assert parsed["baseline_run"] == "@best-baseline"
        assert parsed["failure_threshold"] == 0.50
        assert parsed["known_caveats"] == ["Small dataset", "High variance"]


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
