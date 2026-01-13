"""Integration tests for experiment model database migrations.

TDD: Tests that verify migrations create the necessary tables for existing databases.
Tests should fail initially (RED), then pass after migration is added (GREEN).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from goldfish.db.database import Database


def _create_minimal_old_database(db_path: Path) -> None:
    """Create a minimal database that simulates an 'old' version without experiment_records.

    This creates just enough tables and columns to satisfy foreign key constraints
    and index creation in schema.sql.
    """
    conn = sqlite3.connect(db_path)

    # Create schema_version at version 4 (before experiment model)
    conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    conn.execute("INSERT INTO schema_version (version) VALUES (4)")

    # Create all required tables with all columns that schema.sql references
    conn.executescript(
        """
        CREATE TABLE audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            operation TEXT NOT NULL,
            slot TEXT,
            workspace TEXT,
            reason TEXT NOT NULL,
            details TEXT
        );

        CREATE TABLE sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            gcs_location TEXT NOT NULL,
            size_bytes INTEGER,
            status TEXT NOT NULL DEFAULT 'available',
            metadata TEXT
        );

        CREATE TABLE source_lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            parent_source_id TEXT,
            job_id TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            details TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            error TEXT,
            snapshot_id TEXT
        );

        CREATE TABLE workspace_lineage (
            workspace_name TEXT PRIMARY KEY,
            parent_workspace TEXT,
            forked_from_version TEXT,
            created_at TEXT NOT NULL,
            goal TEXT
        );

        CREATE TABLE workspace_versions (
            workspace_name TEXT NOT NULL,
            version TEXT NOT NULL,
            git_tag TEXT NOT NULL,
            git_sha TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            job_id TEXT,
            description TEXT,
            pruned_at TEXT,
            prune_reason TEXT,
            PRIMARY KEY (workspace_name, version)
        );

        CREATE TABLE stage_runs (
            id TEXT PRIMARY KEY,
            job_id TEXT,
            pipeline_run_id TEXT,
            workspace_name TEXT NOT NULL,
            pipeline_name TEXT,
            version TEXT,
            stage_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error TEXT,
            progress TEXT,
            profile TEXT,
            hints_json TEXT,
            outputs_json TEXT,
            config_json TEXT,
            inputs_json TEXT,
            reason_json TEXT,
            preflight_errors_json TEXT,
            preflight_warnings_json TEXT,
            backend_type TEXT,
            backend_handle TEXT,
            artifact_uri TEXT,
            stage_version_id INTEGER,
            outcome TEXT,
            attempt_num INTEGER,
            svs_findings_json TEXT
        );

        CREATE TABLE signal_lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_run_id TEXT NOT NULL,
            signal_name TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            storage_location TEXT,
            created_at TEXT NOT NULL,
            source_stage_run_id TEXT,
            source_stage_version_id INTEGER,
            stats_json TEXT
        );

        CREATE TABLE run_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_run_id TEXT NOT NULL,
            name TEXT NOT NULL,
            value REAL NOT NULL,
            step INTEGER,
            timestamp TEXT NOT NULL
        );

        CREATE TABLE run_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_run_id TEXT NOT NULL,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            backend_url TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE backups (
            id TEXT PRIMARY KEY,
            workspace_name TEXT NOT NULL,
            version TEXT,
            git_sha TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            gcs_path TEXT NOT NULL,
            size_bytes INTEGER,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            error TEXT
        );

        CREATE TABLE failure_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_name TEXT NOT NULL,
            pattern TEXT NOT NULL,
            action TEXT NOT NULL DEFAULT 'retry',
            max_retries INTEGER DEFAULT 3,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE svs_reviews (
            id TEXT PRIMARY KEY,
            stage_run_id TEXT NOT NULL,
            workspace_name TEXT NOT NULL,
            version TEXT,
            stage_name TEXT NOT NULL,
            review_type TEXT NOT NULL,
            outcome TEXT NOT NULL,
            confidence TEXT,
            findings_json TEXT,
            duration_ms INTEGER,
            created_at TEXT NOT NULL,
            notified INTEGER DEFAULT 0
        );

        CREATE TABLE run_metrics_summary (
            stage_run_id TEXT NOT NULL,
            name TEXT NOT NULL,
            min_value REAL,
            max_value REAL,
            last_value REAL,
            last_timestamp TEXT,
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (stage_run_id, name)
        );

        CREATE TABLE pipeline_runs (
            id TEXT PRIMARY KEY,
            workspace_name TEXT NOT NULL,
            pipeline_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error TEXT,
            reason_json TEXT
        );

        CREATE TABLE pipeline_stage_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_run_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            deps TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            stage_run_id TEXT,
            claimed_at TEXT
        );

        CREATE TABLE stage_versions (
            id INTEGER PRIMARY KEY,
            workspace_name TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            version_num INTEGER NOT NULL,
            git_sha TEXT NOT NULL,
            config_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE workspace_version_tags (
            workspace_name TEXT NOT NULL,
            version TEXT NOT NULL,
            tag_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (workspace_name, tag_name)
        );

        CREATE TABLE docker_builds (
            id TEXT PRIMARY KEY,
            image_type TEXT NOT NULL,
            base_image_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            image_tag TEXT,
            gcs_log_path TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error TEXT,
            workspace_name TEXT,
            version TEXT
        );
        """
    )
    conn.commit()
    conn.close()


class TestExperimentModelMigration:
    """Tests for migrating existing databases to include experiment model tables."""

    def test_migration_is_idempotent(self, tmp_path: Path) -> None:
        """Running migration multiple times doesn't cause errors."""
        db_path = tmp_path / "goldfish.db"

        # Create database first time
        db1 = Database(db_path)

        # Verify tables exist
        with db1._conn() as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='experiment_records'"
            ).fetchone()
            assert table is not None

        # Create new Database instance (re-runs migrations)
        db2 = Database(db_path)

        # Should still work fine
        with db2._conn() as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='experiment_records'"
            ).fetchone()
            assert table is not None

    def test_fresh_database_has_experiment_tables(self, tmp_path: Path) -> None:
        """A fresh database gets experiment model tables from schema.sql."""
        db_path = tmp_path / "fresh.db"

        # Create fresh database
        db = Database(db_path)

        with db._conn() as conn:
            tables = {
                row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }

            expected = ["experiment_records", "run_results", "run_results_spec", "run_tags"]
            for table in expected:
                assert table in tables, f"Fresh database should have {table} table"

    def test_fresh_database_has_experiment_indexes(self, tmp_path: Path) -> None:
        """A fresh database gets experiment model indexes from schema.sql."""
        db_path = tmp_path / "fresh.db"

        db = Database(db_path)

        with db._conn() as conn:
            indexes = {
                row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
            }

            expected_indexes = [
                "idx_experiment_records_workspace",
                "idx_experiment_records_version",
                "idx_experiment_records_run",
                "idx_run_results_record",
                "idx_run_results_status",
                "idx_run_results_ml_outcome",
                "idx_run_results_spec_record",
                "idx_run_tags_record",
            ]

            for idx in expected_indexes:
                assert idx in indexes, f"Index {idx} should exist"

    def test_experiment_records_table_schema(self, tmp_path: Path) -> None:
        """experiment_records table has correct columns."""
        db_path = tmp_path / "fresh.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(experiment_records)").fetchall()}

            expected = ["record_id", "workspace_name", "type", "stage_run_id", "version", "created_at"]
            for col in expected:
                assert col in columns, f"Column {col} should exist in experiment_records"

    def test_run_results_table_schema(self, tmp_path: Path) -> None:
        """run_results table has correct columns."""
        db_path = tmp_path / "fresh.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(run_results)").fetchall()}

            expected = [
                "stage_run_id",
                "record_id",
                "results_status",
                "infra_outcome",
                "ml_outcome",
                "results_auto",
                "results_final",
                "comparison",
                "finalized_by",
                "finalized_at",
            ]
            for col in expected:
                assert col in columns, f"Column {col} should exist in run_results"

    def test_run_results_spec_table_schema(self, tmp_path: Path) -> None:
        """run_results_spec table has correct columns."""
        db_path = tmp_path / "fresh.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(run_results_spec)").fetchall()}

            expected = ["stage_run_id", "record_id", "spec_json", "created_at"]
            for col in expected:
                assert col in columns, f"Column {col} should exist in run_results_spec"

    def test_run_tags_table_schema(self, tmp_path: Path) -> None:
        """run_tags table has correct columns."""
        db_path = tmp_path / "fresh.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(run_tags)").fetchall()}

            expected = ["workspace_name", "record_id", "tag_name", "created_at"]
            for col in expected:
                assert col in columns, f"Column {col} should exist in run_tags"
