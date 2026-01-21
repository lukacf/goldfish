"""Integration tests for list_history and inspect_record.

TDD: Tests for listing and inspecting experiment records with real database.
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestListHistoryIntegration:
    """Integration tests for list_history with real database."""

    def test_list_history_basic(self, test_db: Database) -> None:
        """Can list experiment history from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-hist1", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-hist2", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-hist1")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-hist2")

        result = manager.list_history("test_ws")

        assert result is not None
        assert len(result["records"]) == 2

    def test_list_history_filter_by_type(self, test_db: Database) -> None:
        """Can filter by run vs checkpoint."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-run", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-run")
        manager.create_checkpoint_record(workspace_name="test_ws", version="v1")

        runs_only = manager.list_history("test_ws", record_type="run")
        checkpoints_only = manager.list_history("test_ws", record_type="checkpoint")

        assert len(runs_only["records"]) == 1
        assert len(checkpoints_only["records"]) == 1

    def test_list_history_filter_by_stage(self, test_db: Database) -> None:
        """Can filter by stage name."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-train", "test_ws", "v1", stage_name="train")
        _setup_stage_run(test_db, "stage-eval", "test_ws", "v1", stage_name="eval")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-train")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-eval")

        train_only = manager.list_history("test_ws", stage="train")

        assert len(train_only["records"]) == 1

    def test_list_history_filter_by_tagged(self, test_db: Database) -> None:
        """Can filter to only tagged records."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tagged", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-untagged", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        tagged_record = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-tagged")
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-untagged")

        manager.tag_record(tagged_record, "best-run")

        tagged_only = manager.list_history("test_ws", tagged=True)

        assert len(tagged_only["records"]) == 1
        assert tagged_only["records"][0]["record_id"] == tagged_record

    def test_list_history_filter_by_specific_tag(self, test_db: Database) -> None:
        """Can filter by specific tag name."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-best", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-prod", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        best_record = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-best")
        prod_record = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-prod")

        manager.tag_record(best_record, "best-run")
        manager.tag_record(prod_record, "production")

        best_only = manager.list_history("test_ws", tagged="best-run")

        assert len(best_only["records"]) == 1
        assert best_only["records"][0]["record_id"] == best_record

    def test_list_history_limit_offset(self, test_db: Database) -> None:
        """Supports pagination via limit and offset."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        # Create 5 records
        for i in range(5):
            _setup_stage_run(test_db, f"stage-page{i}", "test_ws", "v1")
            manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id=f"stage-page{i}")

        # Get first 2
        first_page = manager.list_history("test_ws", limit=2, offset=0)
        assert len(first_page["records"]) == 2

        # Get next 2
        second_page = manager.list_history("test_ws", limit=2, offset=2)
        assert len(second_page["records"]) == 2

        # Verify no overlap
        first_ids = {r["record_id"] for r in first_page["records"]}
        second_ids = {r["record_id"] for r in second_page["records"]}
        assert first_ids.isdisjoint(second_ids)


class TestListHistorySemanticFields:
    """Integration tests for list_history semantic enrichment fields."""

    def test_list_history_includes_age(self, test_db: Database) -> None:
        """list_history includes age field computed from ULID."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-age", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-age")

        result = manager.list_history("test_ws")

        assert len(result["records"]) == 1
        assert "age" in result["records"][0]
        # Age should be a string like "0m ago", "1h ago", etc.
        assert result["records"][0]["age"].endswith("ago")

    def test_list_history_includes_reason(self, test_db: Database) -> None:
        """list_history includes reason from stage_runs.reason_json."""
        import json

        _setup_workspace_and_version(test_db, "test_ws", "v1")
        reason_data = {"description": "Testing new architecture"}
        _setup_stage_run(test_db, "stage-reason", "test_ws", "v1", reason_json=json.dumps(reason_data))

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-reason")

        result = manager.list_history("test_ws")

        assert len(result["records"]) == 1
        assert "reason" in result["records"][0]
        assert result["records"][0]["reason"] == "Testing new architecture"

    def test_list_history_includes_primary_metric(self, test_db: Database) -> None:
        """list_history includes primary_metric from run_results."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-metric", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-metric")

        # Finalize with results
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.85,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Great accuracy results.",
        }
        manager.finalize_run("stage-metric", results)

        result = manager.list_history("test_ws")

        assert len(result["records"]) == 1
        assert "primary_metric" in result["records"][0]
        assert result["records"][0]["primary_metric"]["name"] == "accuracy"
        assert result["records"][0]["primary_metric"]["value"] == 0.85

    def test_list_history_includes_stage(self, test_db: Database) -> None:
        """list_history includes stage name from stage_runs."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-with-name", "test_ws", "v1", stage_name="train_model")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-with-name")

        result = manager.list_history("test_ws")

        assert len(result["records"]) == 1
        assert "stage" in result["records"][0]
        assert result["records"][0]["stage"] == "train_model"


class TestInspectRecordIntegration:
    """Integration tests for inspect_record with real database."""

    def test_inspect_record_basic(self, test_db: Database) -> None:
        """Can inspect a record."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-inspect", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-inspect")

        result = manager.inspect_record(record_id)

        assert result is not None
        assert result["record_id"] == record_id
        assert result["type"] == "run"

    def test_inspect_record_with_results(self, test_db: Database) -> None:
        """Can include results in inspection."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-results", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-results")

        # Finalize to have results
        results = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "value": 0.75,
            "dataset_split": "val",
            "ml_outcome": "success",
            "notes": "Test finalized results.",
        }
        manager.finalize_run("stage-results", results)

        inspected = manager.inspect_record(record_id, include=["results"])

        assert "results" in inspected
        assert inspected["results"]["results_status"] == "finalized"

    def test_inspect_record_with_tags(self, test_db: Database) -> None:
        """Can include tags in inspection."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tags", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-tags")

        manager.tag_record(record_id, "best-run")
        manager.tag_record(record_id, "production")

        inspected = manager.inspect_record(record_id, include=["tags"])

        assert "tags" in inspected
        assert "best-run" in inspected["tags"]
        assert "production" in inspected["tags"]

    def test_inspect_record_with_comparison(self, test_db: Database) -> None:
        """Can include comparison in inspection."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-comp", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-comp")

        comparison = {"vs_previous": {"record": "prev-rec", "delta": 0.05}}
        manager.store_comparison("stage-comp", comparison)

        inspected = manager.inspect_record(record_id, include=["comparison"])

        assert "comparison" in inspected
        assert inspected["comparison"]["vs_previous"]["delta"] == 0.05

    def test_inspect_record_by_tag(self, test_db: Database) -> None:
        """Can inspect record using @tag reference."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tag-lookup", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(workspace_name="test_ws", version="v1", stage_run_id="stage-tag-lookup")
        manager.tag_record(record_id, "best-run")

        # Lookup by @tag
        inspected = manager.inspect_record("@best-run", workspace_name="test_ws")

        assert inspected is not None
        assert inspected["record_id"] == record_id


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
    reason_json: str | None = None,
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
                (id, workspace_name, version, stage_name, status, started_at, reason_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, workspace, version, stage_name, status, datetime.now(UTC).isoformat(), reason_json),
            )
