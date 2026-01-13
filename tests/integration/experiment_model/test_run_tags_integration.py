"""Integration tests for run tags functionality.

TDD: Tests for tagging experiment records with real database.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.experiment_model.records import ExperimentRecordManager


class TestRunTagsIntegration:
    """Integration tests for run tags with real database."""

    def test_tag_run_record_integration(self, test_db: Database) -> None:
        """Can tag a run record and retrieve it."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-tag-test", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-tag-test",
        )

        manager.tag_record(record_id, "best-run")

        tags = manager.get_record_tags(record_id)
        assert "best-run" in tags

    def test_tag_checkpoint_record_integration(self, test_db: Database) -> None:
        """Can tag a checkpoint record."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v1",
        )

        manager.tag_record(record_id, "stable-checkpoint")

        tags = manager.get_record_tags(record_id)
        assert "stable-checkpoint" in tags

    def test_multiple_tags_on_record(self, test_db: Database) -> None:
        """Can add multiple tags to same record."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-multi-tag", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-multi-tag",
        )

        manager.tag_record(record_id, "best-run")
        manager.tag_record(record_id, "v1-final")
        manager.tag_record(record_id, "production")

        tags = manager.get_record_tags(record_id)
        assert len(tags) == 3
        assert "best-run" in tags
        assert "v1-final" in tags
        assert "production" in tags

    def test_get_record_by_tag_integration(self, test_db: Database) -> None:
        """Can look up a record by its tag."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-lookup", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-lookup",
        )
        manager.tag_record(record_id, "best-run")

        record = manager.get_record_by_tag("test_ws", "best-run")
        assert record is not None
        assert record["record_id"] == record_id

    def test_tag_uniqueness_per_workspace(self, test_db: Database) -> None:
        """Same tag cannot be used twice in same workspace."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-dup1", "test_ws", "v1")
        _setup_stage_run(test_db, "stage-dup2", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id1 = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-dup1",
        )
        record_id2 = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-dup2",
        )

        manager.tag_record(record_id1, "unique-tag")

        with pytest.raises(ValueError, match="already exists"):
            manager.tag_record(record_id2, "unique-tag")

    def test_tags_isolated_across_workspaces(self, test_db: Database) -> None:
        """Same tag can be used in different workspaces."""
        _setup_workspace_and_version(test_db, "ws1", "v1")
        _setup_workspace_and_version(test_db, "ws2", "v1")
        _setup_stage_run(test_db, "stage-ws1", "ws1", "v1")
        _setup_stage_run(test_db, "stage-ws2", "ws2", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id1 = manager.create_run_record(
            workspace_name="ws1",
            version="v1",
            stage_run_id="stage-ws1",
        )
        record_id2 = manager.create_run_record(
            workspace_name="ws2",
            version="v1",
            stage_run_id="stage-ws2",
        )

        # Same tag name in different workspaces should work
        manager.tag_record(record_id1, "best-run")
        manager.tag_record(record_id2, "best-run")

        # Each workspace should resolve to its own record
        ws1_record = manager.get_record_by_tag("ws1", "best-run")
        ws2_record = manager.get_record_by_tag("ws2", "best-run")

        assert ws1_record["record_id"] == record_id1
        assert ws2_record["record_id"] == record_id2

    def test_remove_tag_integration(self, test_db: Database) -> None:
        """Can remove a tag from a record."""
        _setup_workspace_and_version(test_db, "test_ws", "v1")
        _setup_stage_run(test_db, "stage-remove", "test_ws", "v1")

        manager = ExperimentRecordManager(test_db)
        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-remove",
        )

        manager.tag_record(record_id, "temp-tag")
        tags_before = manager.get_record_tags(record_id)
        assert "temp-tag" in tags_before

        manager.remove_tag("test_ws", "temp-tag")

        tags_after = manager.get_record_tags(record_id)
        assert "temp-tag" not in tags_after


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
