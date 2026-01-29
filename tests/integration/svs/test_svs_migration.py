"""TDD tests for SVS database migration.

These tests verify that the SVS tables and columns are correctly added to both
fresh and existing databases through the migration system.

TDD: Write failing tests first, then implement.
"""

import sqlite3

from goldfish.db.database import Database


class TestFreshDatabaseSVSSchema:
    """Tests that fresh databases get all SVS tables and columns."""

    def test_svs_reviews_table_exists(self, temp_dir):
        """Fresh database should have svs_reviews table."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='svs_reviews'").fetchone()

        assert result is not None
        assert result[0] == "svs_reviews"

    def test_svs_reviews_has_required_columns(self, temp_dir):
        """svs_reviews table should have all required columns."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(svs_reviews)").fetchall()}

        required_columns = {
            "id",
            "stage_run_id",
            "signal_name",
            "review_type",
            "model_used",
            "prompt_hash",
            "stats_json",
            "response_text",
            "parsed_findings",
            "decision",
            "policy_overrides",
            "reviewed_at",
            "duration_ms",
        }

        assert required_columns.issubset(columns)

    def test_failure_patterns_table_exists(self, temp_dir):
        """Fresh database should have failure_patterns table."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='failure_patterns'"
            ).fetchone()

        assert result is not None
        assert result[0] == "failure_patterns"

    def test_failure_patterns_has_required_columns(self, temp_dir):
        """failure_patterns table should have all required columns."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(failure_patterns)").fetchall()}

        required_columns = {
            "id",
            "symptom",
            "root_cause",
            "detection_heuristic",
            "prevention",
            "severity",
            "stage_type",
            "source_run_id",
            "source_workspace",
            "created_at",
            "last_seen_at",
            "occurrence_count",
            "status",
            "confidence",
            "approved_at",
            "approved_by",
            "rejection_reason",
            "manually_edited",
            "enabled",
        }

        assert required_columns.issubset(columns)

    def test_stage_runs_has_svs_findings_json(self, temp_dir):
        """stage_runs table should have SVS-related columns."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(stage_runs)").fetchall()}

        assert "svs_findings_json" in columns
        assert "preflight_errors_json" in columns
        assert "preflight_warnings_json" in columns

    def test_signal_lineage_has_stats_json(self, temp_dir):
        """signal_lineage table should have stats_json column."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(signal_lineage)").fetchall()}

        assert "stats_json" in columns


class TestMigrationIdempotency:
    """Tests that migrations are idempotent and can run multiple times safely."""

    def test_double_migration_succeeds(self, temp_dir):
        """Running migration twice should not fail."""
        db_path = temp_dir / "test.db"

        # First initialization
        db1 = Database(db_path)

        # Verify tables exist
        with db1._conn() as conn:
            tables_before = {
                row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }

        # Second initialization (should be idempotent)
        db2 = Database(db_path)

        # Verify tables still exist and no errors occurred
        with db2._conn() as conn:
            tables_after = {
                row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }

        assert tables_before == tables_after
        assert "svs_reviews" in tables_after
        assert "failure_patterns" in tables_after

    def test_reopening_database_preserves_data(self, temp_dir):
        """Data should persist when database is reopened."""
        db_path = temp_dir / "test.db"

        # Create database and insert data
        db1 = Database(db_path)
        with db1._conn() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('test-source', 'Test', '2024-01-01', 'test', 'gs://bucket', 'available')
                """
            )

        # Reopen database (triggers migration check again)
        db2 = Database(db_path)

        # Verify data persists
        with db2._conn() as conn:
            result = conn.execute("SELECT name FROM sources WHERE id='test-source'").fetchone()

        assert result is not None
        assert result[0] == "Test"


class TestExistingDatabaseMigration:
    """Tests that SVS columns are added to existing databases."""

    @staticmethod
    def _create_minimal_old_schema(conn):
        """Create minimal old schema that migration expects.

        Includes tables that the migration script references for creating
        indexes or performing data migrations.
        """
        conn.executescript(
            """
            CREATE TABLE workspace_lineage (
                workspace_name TEXT PRIMARY KEY,
                parent_workspace TEXT,
                parent_version TEXT,
                created_at TEXT NOT NULL,
                description TEXT
            );

            CREATE TABLE workspace_versions (
                workspace_name TEXT,
                version TEXT,
                git_tag TEXT NOT NULL,
                git_sha TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                PRIMARY KEY (workspace_name, version)
            );

            CREATE TABLE stage_runs (
                id TEXT PRIMARY KEY,
                workspace_name TEXT NOT NULL,
                version TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                started_at TEXT NOT NULL
            );

            CREATE TABLE signal_lineage (
                stage_run_id TEXT,
                signal_name TEXT,
                signal_type TEXT,
                storage_location TEXT,
                PRIMARY KEY (stage_run_id, signal_name)
            );

            -- Migration expects run_metrics for cascade rebuild
            CREATE TABLE run_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                step INTEGER,
                timestamp TEXT
            );

            -- Migration expects run_metrics_summary for cascade rebuild
            CREATE TABLE run_metrics_summary (
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                min_value REAL,
                max_value REAL,
                last_value REAL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (stage_run_id, name)
            );

            -- Migration expects run_artifacts for cascade rebuild
            CREATE TABLE run_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL,
                backend_url TEXT,
                created_at TEXT NOT NULL DEFAULT '2024-01-01T00:00:00Z'
            );

            -- Migration expects pipeline_runs for index creation
            CREATE TABLE pipeline_runs (
                id TEXT PRIMARY KEY,
                workspace_name TEXT,
                status TEXT DEFAULT 'pending'
            );
            """
        )

    def test_old_database_gets_svs_columns(self, temp_dir):
        """Existing database without SVS columns should get them after migration."""
        db_path = temp_dir / "test.db"

        # Create old-style database manually (without SVS tables)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        # Create minimal old schema
        self._create_minimal_old_schema(conn)
        conn.close()

        # Initialize with migration
        db = Database(db_path)

        # Verify SVS columns were added
        with db._conn() as conn:
            stage_runs_cols = {row[1] for row in conn.execute("PRAGMA table_info(stage_runs)").fetchall()}
            signal_lineage_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_lineage)").fetchall()}

        assert "svs_findings_json" in stage_runs_cols
        assert "stats_json" in signal_lineage_cols

    def test_old_database_gets_svs_tables(self, temp_dir):
        """Existing database should get new SVS tables."""
        db_path = temp_dir / "test.db"

        # Create old-style database manually
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        # Create minimal old schema
        self._create_minimal_old_schema(conn)
        conn.close()

        # Initialize with migration
        db = Database(db_path)

        # Verify SVS tables exist
        with db._conn() as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        assert "svs_reviews" in tables
        assert "failure_patterns" in tables

    def test_migration_preserves_existing_data(self, temp_dir):
        """Migration should not affect existing data in tables."""
        db_path = temp_dir / "test.db"

        # Create old-style database with data
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        conn.executescript(
            """
            CREATE TABLE workspace_lineage (
                workspace_name TEXT PRIMARY KEY,
                parent_workspace TEXT,
                parent_version TEXT,
                created_at TEXT NOT NULL,
                description TEXT
            );

            CREATE TABLE workspace_versions (
                workspace_name TEXT,
                version TEXT,
                git_tag TEXT NOT NULL,
                git_sha TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                PRIMARY KEY (workspace_name, version)
            );

            CREATE TABLE stage_runs (
                id TEXT PRIMARY KEY,
                workspace_name TEXT NOT NULL,
                version TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                started_at TEXT NOT NULL
            );

            CREATE TABLE signal_lineage (
                stage_run_id TEXT,
                signal_name TEXT,
                signal_type TEXT,
                storage_location TEXT,
                PRIMARY KEY (stage_run_id, signal_name)
            );

            -- Migration expects run_metrics for cascade rebuild
            CREATE TABLE run_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                step INTEGER,
                timestamp TEXT
            );

            -- Migration expects run_metrics_summary for cascade rebuild
            CREATE TABLE run_metrics_summary (
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                min_value REAL,
                max_value REAL,
                last_value REAL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (stage_run_id, name)
            );

            -- Migration expects run_artifacts for cascade rebuild
            CREATE TABLE run_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage_run_id TEXT NOT NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL,
                backend_url TEXT,
                created_at TEXT NOT NULL DEFAULT '2024-01-01T00:00:00Z'
            );

            -- Migration expects pipeline_runs for index creation
            CREATE TABLE pipeline_runs (
                id TEXT PRIMARY KEY,
                workspace_name TEXT,
                status TEXT DEFAULT 'pending'
            );

            INSERT INTO workspace_lineage (workspace_name, created_at)
            VALUES ('test_ws', '2024-01-01');

            INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES ('test_ws', 'v1', 'test_ws-v1', 'sha123', '2024-01-01', 'run');

            INSERT INTO stage_runs (id, workspace_name, version, stage_name, started_at)
            VALUES ('stage-123', 'test_ws', 'v1', 'train', '2024-01-01T12:00:00');
            """
        )
        conn.close()

        # Run migration
        db = Database(db_path)

        # Verify data still exists
        with db._conn() as conn:
            workspace = conn.execute(
                "SELECT workspace_name FROM workspace_lineage WHERE workspace_name='test_ws'"
            ).fetchone()
            version = conn.execute("SELECT version FROM workspace_versions WHERE workspace_name='test_ws'").fetchone()
            stage_run = conn.execute("SELECT id FROM stage_runs WHERE id='stage-123'").fetchone()

        assert workspace is not None
        assert workspace[0] == "test_ws"
        assert version is not None
        assert version[0] == "v1"
        assert stage_run is not None
        assert stage_run[0] == "stage-123"

    def test_migration_adds_indexes(self, temp_dir):
        """Migration should create appropriate indexes for SVS tables."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db._conn() as conn:
            # Query all indexes
            indexes = conn.execute(
                """
                SELECT name, tbl_name
                FROM sqlite_master
                WHERE type='index'
                """
            ).fetchall()

            index_dict = {row[0]: row[1] for row in indexes}

        # Check key SVS indexes exist
        assert "idx_svs_reviews_stage_run" in index_dict
        assert index_dict["idx_svs_reviews_stage_run"] == "svs_reviews"

        assert "idx_svs_reviews_type" in index_dict
        assert index_dict["idx_svs_reviews_type"] == "svs_reviews"

        assert "idx_svs_reviews_decision" in index_dict
        assert index_dict["idx_svs_reviews_decision"] == "svs_reviews"

        assert "idx_failure_patterns_status" in index_dict
        assert index_dict["idx_failure_patterns_status"] == "failure_patterns"

        assert "idx_failure_patterns_severity" in index_dict
        assert index_dict["idx_failure_patterns_severity"] == "failure_patterns"


class TestSVSDataIntegrity:
    """Tests that SVS columns accept valid data."""

    def test_stage_runs_accepts_svs_findings_json(self, temp_dir):
        """stage_runs should accept JSON in svs_findings_json column."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Create minimal dependencies
        with db._conn() as conn:
            conn.executescript(
                """
                INSERT INTO workspace_lineage (workspace_name, created_at)
                VALUES ('test_ws', '2024-01-01');

                INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by)
                VALUES ('test_ws', 'v1', 'test_ws-v1', 'sha123', '2024-01-01', 'run');

                INSERT INTO stage_runs (
                    id, workspace_name, version, stage_name, started_at, svs_findings_json
                )
                VALUES (
                    'stage-123',
                    'test_ws',
                    'v1',
                    'train',
                    '2024-01-01T12:00:00',
                    '{"errors": [], "warnings": ["Warning 1"], "notes": ["Note 1"]}'
                );
                """
            )

        # Verify data was stored
        with db._conn() as conn:
            result = conn.execute("SELECT svs_findings_json FROM stage_runs WHERE id='stage-123'").fetchone()

        assert result is not None
        assert "warnings" in result[0]

    def test_signal_lineage_accepts_stats_json(self, temp_dir):
        """signal_lineage should accept JSON in stats_json column."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Create minimal dependencies
        with db._conn() as conn:
            conn.executescript(
                """
                INSERT INTO workspace_lineage (workspace_name, created_at)
                VALUES ('test_ws', '2024-01-01');

                INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by)
                VALUES ('test_ws', 'v1', 'test_ws-v1', 'sha123', '2024-01-01', 'run');

                INSERT INTO stage_runs (
                    id, workspace_name, version, stage_name, started_at
                )
                VALUES (
                    'stage-123',
                    'test_ws',
                    'v1',
                    'train',
                    '2024-01-01T12:00:00'
                );

                INSERT INTO signal_lineage (
                    stage_run_id, signal_name, signal_type, storage_location, stats_json
                )
                VALUES (
                    'stage-123',
                    'output',
                    'npy',
                    '/path/to/output.npy',
                    '{"entropy": 3.5, "null_ratio": 0.01, "size_bytes": 1024}'
                );
                """
            )

        # Verify data was stored
        with db._conn() as conn:
            result = conn.execute("SELECT stats_json FROM signal_lineage WHERE stage_run_id='stage-123'").fetchone()

        assert result is not None
        assert "entropy" in result[0]

    def test_svs_reviews_accepts_review_data(self, temp_dir):
        """svs_reviews table should accept complete review records."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Create minimal dependencies
        with db._conn() as conn:
            conn.executescript(
                """
                INSERT INTO workspace_lineage (workspace_name, created_at)
                VALUES ('test_ws', '2024-01-01');

                INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by)
                VALUES ('test_ws', 'v1', 'test_ws-v1', 'sha123', '2024-01-01', 'run');

                INSERT INTO stage_runs (
                    id, workspace_name, version, stage_name, started_at
                )
                VALUES (
                    'stage-123',
                    'test_ws',
                    'v1',
                    'train',
                    '2024-01-01T12:00:00'
                );

                INSERT INTO svs_reviews (
                    stage_run_id,
                    signal_name,
                    review_type,
                    model_used,
                    prompt_hash,
                    response_text,
                    parsed_findings,
                    decision,
                    reviewed_at
                )
                VALUES (
                    'stage-123',
                    NULL,
                    'pre_run',
                    'claude-opus-4-5-20251101',
                    'abc123hash',
                    'Code looks good',
                    '{"errors": [], "warnings": [], "notes": ["Approved"]}',
                    'approved',
                    '2024-01-01T12:00:00'
                );
                """
            )

        # Verify review was stored
        with db._conn() as conn:
            result = conn.execute(
                "SELECT review_type, decision FROM svs_reviews WHERE stage_run_id='stage-123'"
            ).fetchone()

        assert result is not None
        assert result[0] == "pre_run"
        assert result[1] == "approved"
