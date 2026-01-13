"""Integration tests for experiment record creation on run and save_version.

TDD: These tests define the expected integration behavior.
Tests should fail initially (RED), then pass after integration is complete (GREEN).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestExperimentRecordManagerIntegration:
    """Integration tests for ExperimentRecordManager with real database."""

    def test_create_run_record_integration(self, test_db: Database) -> None:
        """Can create a run record in the database."""
        # Setup workspace and version
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        # Verify record was created
        assert record_id is not None
        assert len(record_id) == 26

        # Verify in database
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM experiment_records WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        assert row is not None
        assert row["workspace_name"] == "test_ws"
        assert row["type"] == "run"
        assert row["stage_run_id"] == "stage-abc123"
        assert row["version"] == "v1"

    def test_create_run_record_creates_run_results(self, test_db: Database) -> None:
        """Creating a run record also creates run_results entry."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        # Verify run_results was created
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results WHERE stage_run_id = ?",
                ("stage-abc123",),
            ).fetchone()

        assert row is not None
        assert row["record_id"] == record_id
        assert row["results_status"] == "missing"
        assert row["infra_outcome"] == "unknown"
        assert row["ml_outcome"] == "unknown"

    def test_create_checkpoint_record_integration(self, test_db: Database) -> None:
        """Can create a checkpoint record in the database."""
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        # Verify record was created
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM experiment_records WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        assert row is not None
        assert row["workspace_name"] == "test_ws"
        assert row["type"] == "checkpoint"
        assert row["stage_run_id"] is None
        assert row["version"] == "v2"

    def test_checkpoint_record_no_run_results(self, test_db: Database) -> None:
        """Checkpoint records do not create run_results."""
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        # Verify NO run_results was created
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        assert row is None

    def test_get_record_by_id_integration(self, test_db: Database) -> None:
        """Can retrieve a record by ID from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        record = manager.get_record(record_id)

        assert record is not None
        assert record["record_id"] == record_id
        assert record["workspace_name"] == "test_ws"

    def test_get_record_by_stage_run_integration(self, test_db: Database) -> None:
        """Can retrieve a record by stage_run_id from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-abc123", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        record = manager.get_record_by_stage_run("stage-abc123")

        assert record is not None
        assert record["record_id"] == record_id
        assert record["stage_run_id"] == "stage-abc123"

    def test_list_records_integration(self, test_db: Database) -> None:
        """Can list records by workspace from database."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_workspace_and_version(test_db, "test_ws", "v2")
        _setup_stage_run(test_db, "stage-001", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-001",
        )
        manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        records = manager.list_records("test_ws")

        assert len(records) == 2

    def test_list_records_filters_by_type(self, test_db: Database) -> None:
        """Can filter records by type."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_workspace_and_version(test_db, "test_ws", "v2")
        _setup_stage_run(test_db, "stage-001", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-001",
        )
        manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        run_records = manager.list_records("test_ws", record_type="run")
        checkpoint_records = manager.list_records("test_ws", record_type="checkpoint")

        assert len(run_records) == 1
        assert run_records[0]["type"] == "run"
        assert len(checkpoint_records) == 1
        assert checkpoint_records[0]["type"] == "checkpoint"

    def test_list_records_ordered_newest_first(self, test_db: Database) -> None:
        """Records are returned newest first."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        manager = ExperimentRecordManager(test_db)

        id1 = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v1",
        )
        time.sleep(0.01)  # Small delay to ensure different timestamps
        id2 = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        records = manager.list_records("test_ws")

        # Newest (id2) should be first
        assert records[0]["record_id"] == id2
        assert records[1]["record_id"] == id1

    def test_record_ids_are_ulid_sortable(self, test_db: Database) -> None:
        """Record IDs are lexicographically sortable (newer > older)."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        manager = ExperimentRecordManager(test_db)

        id1 = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v1",
        )
        time.sleep(0.01)
        id2 = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        # Newer ID should be lexicographically greater
        assert id2 > id1

    def test_list_records_filters_by_version(self, test_db: Database) -> None:
        """Can filter records by version."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_workspace_and_version(test_db, "test_ws", "v2")

        manager = ExperimentRecordManager(test_db)

        manager.create_checkpoint_record(workspace_name="test_ws", version="v1")
        manager.create_checkpoint_record(workspace_name="test_ws", version="v2")
        manager.create_checkpoint_record(workspace_name="test_ws", version="v1")

        v1_records = manager.list_records("test_ws", version="v1")
        v2_records = manager.list_records("test_ws", version="v2")

        assert len(v1_records) == 2
        assert all(r["version"] == "v1" for r in v1_records)
        assert len(v2_records) == 1
        assert v2_records[0]["version"] == "v2"

    def test_list_records_pagination(self, test_db: Database) -> None:
        """Pagination works correctly with limit and offset."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)

        # Create 5 records
        record_ids = []
        for _ in range(5):
            record_ids.append(manager.create_checkpoint_record(workspace_name="test_ws", version="v1"))

        # Test limit
        limited = manager.list_records("test_ws", limit=2)
        assert len(limited) == 2

        # Test offset
        offset_records = manager.list_records("test_ws", limit=2, offset=2)
        assert len(offset_records) == 2

        # Verify offset works correctly (should get different records)
        limited_ids = {r["record_id"] for r in limited}
        offset_ids = {r["record_id"] for r in offset_records}
        assert limited_ids.isdisjoint(offset_ids)

    def test_get_run_results_integration(self, test_db: Database) -> None:
        """Can retrieve run results by stage_run_id."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-results-test", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-results-test",
        )

        results = manager.get_run_results("stage-results-test")

        assert results is not None
        assert results["stage_run_id"] == "stage-results-test"
        assert results["record_id"] == record_id
        assert results["results_status"] == "missing"
        assert results["infra_outcome"] == "unknown"
        assert results["ml_outcome"] == "unknown"

    def test_get_run_results_by_record_integration(self, test_db: Database) -> None:
        """Can retrieve run results by record_id."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-record-test", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-record-test",
        )

        results = manager.get_run_results_by_record(record_id)

        assert results is not None
        assert results["record_id"] == record_id

    def test_get_run_results_not_found(self, test_db: Database) -> None:
        """Returns None when run results not found."""
        manager = ExperimentRecordManager(test_db)
        results = manager.get_run_results("nonexistent-stage")
        assert results is None

    def test_get_record_by_stage_run_not_found_integration(self, test_db: Database) -> None:
        """Returns None when record not found by stage_run_id."""
        manager = ExperimentRecordManager(test_db)
        record = manager.get_record_by_stage_run("nonexistent-stage-run")
        assert record is None

    def test_get_run_results_by_record_not_found_integration(self, test_db: Database) -> None:
        """Returns None when run results not found by record_id."""
        manager = ExperimentRecordManager(test_db)
        results = manager.get_run_results_by_record("nonexistent-record-id")
        assert results is None


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
