"""SQLite database connection and schema initialization."""

import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

from goldfish.db.types import (
    ArtifactRow,
    AuditRow,
    BackupRow,
    DockerBuildRow,
    FailurePatternRow,
    JobInputWithSource,
    JobRow,
    LineageRow,
    MetricRow,
    MetricsSummaryRow,
    PrunedVersionRow,
    SourceRow,
    StageVersionRow,
    SVSReviewRow,
    VersionTagRow,
)
from goldfish.errors import DatabaseError
from goldfish.models import JobStatus, PipelineStatus

# Load schema from file
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    """SQLite database manager for Goldfish."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise DatabaseError(
                f"Cannot create database directory at '{db_path.parent}': {e}",
                path=str(db_path),
                operation="init",
            ) from e
        # For existing databases, run migrations FIRST to add missing columns,
        # then schema (which creates indexes that may reference new columns).
        # For fresh databases, schema runs first (creates tables), then migrations (no-op).
        if self._has_existing_tables():
            self._migrate_schema()
            self._init_schema()
        else:
            self._init_schema()
            self._migrate_schema()

    def _has_existing_tables(self) -> bool:
        """Check if database already has tables (existing vs fresh)."""
        try:
            with self._conn() as conn:
                result = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='stage_runs'"
                ).fetchone()
                return result is not None
        except sqlite3.Error:
            return False

    def _init_schema(self) -> None:
        """Initialize database schema."""
        try:
            schema = SCHEMA_PATH.read_text()
        except (OSError, FileNotFoundError) as e:
            raise DatabaseError(
                f"Cannot read database schema file: {e}",
                path=str(SCHEMA_PATH),
                operation="init_schema",
            ) from e

        try:
            with self._conn() as conn:
                conn.executescript(schema)
        except sqlite3.Error as e:
            raise DatabaseError(
                f"Cannot initialize database schema: {e}",
                path=str(self.db_path),
                operation="init_schema",
            ) from e

    def _migrate_schema(self) -> None:
        """Lightweight, idempotent migrations for existing databases."""
        required_columns = {
            "stage_runs": [
                ("pipeline_run_id", "TEXT"),
                ("pipeline_name", "TEXT"),
                ("progress", "TEXT"),
                ("profile", "TEXT"),
                ("hints_json", "TEXT"),
                ("outputs_json", "TEXT"),
                ("config_json", "TEXT"),
                ("inputs_json", "TEXT"),
                ("reason_json", "TEXT"),  # Structured RunReason
                ("preflight_errors_json", "TEXT"),  # Preflight validation errors
                ("preflight_warnings_json", "TEXT"),  # Preflight validation warnings
                ("backend_type", "TEXT"),
                ("backend_handle", "TEXT"),
                ("artifact_uri", "TEXT"),
                ("stage_version_id", "INTEGER"),  # Links to stage_versions
                ("outcome", "TEXT"),  # NULL, 'success', 'bad_results' - semantic result quality
                ("attempt_num", "INTEGER"),  # Groups consecutive runs per stage
                ("svs_findings_json", "TEXT"),  # SVS post-run findings
                # State machine columns (Phase 3)
                ("state", "TEXT"),  # State machine state
                ("phase", "TEXT"),  # Sub-phase within state
                ("termination_cause", "TEXT"),  # Why run terminated
                ("state_entered_at", "TEXT"),  # When current state was entered
                ("phase_updated_at", "TEXT"),  # When phase was last updated
                ("completed_with_warnings", "INTEGER DEFAULT 0"),  # Completed with non-critical failures
                ("output_sync_done", "INTEGER DEFAULT 0"),  # Output sync completed
                ("output_recording_done", "INTEGER DEFAULT 0"),  # Output recording completed
                ("gcs_outage_started", "TEXT"),  # When GCS outage was first detected
            ],
            "signal_lineage": [
                ("source_stage_run_id", "TEXT"),  # Upstream stage run
                ("source_stage_version_id", "INTEGER"),  # Upstream stage version
                ("stats_json", "TEXT"),  # SVS output statistics
            ],
            "run_metrics_summary": [
                ("last_timestamp", "TEXT"),
            ],
            "pipeline_runs": [
                ("reason_json", "TEXT"),  # Structured RunReason for async pipelines
                ("results_spec_json", "TEXT"),  # Results spec for async runs
                ("experiment_group", "TEXT"),  # Experiment group for async runs
            ],
            "workspace_versions": [
                ("pruned_at", "TEXT"),  # When version was pruned
                ("prune_reason", "TEXT"),  # Why version was pruned
            ],
            "svs_reviews": [
                ("notified", "INTEGER DEFAULT 0"),  # 0 = not shown in dashboard, 1 = shown
            ],
            "docker_builds": [
                ("workspace_name", "TEXT"),  # Workspace name (for workspace builds)
                ("version", "TEXT"),  # Workspace version (for workspace builds)
                ("content_hash", "TEXT"),  # SHA256 of build context (for cache hit detection)
            ],
            "experiment_records": [
                ("experiment_group", "TEXT"),  # Optional grouping for filtering
            ],
            "stage_state_transitions": [
                ("svs_review_id", "TEXT"),  # FK to svs_reviews.id for SVS_BLOCK and AI_STOP events
            ],
        }

        with self._conn() as conn:
            # Schema version tracking
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                );
                """
            )
            version_row = conn.execute("SELECT version FROM schema_version").fetchone()
            current_version = version_row["version"] if version_row else 0

            # Add missing columns
            for table, cols in required_columns.items():
                table_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not table_exists:
                    continue
                existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
                for col, col_type in cols:
                    if col not in existing:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

            # Ensure pipeline tables exist (safe to run multiple times)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id TEXT PRIMARY KEY,
                    workspace_name TEXT NOT NULL,
                    pipeline_name TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_workspace ON pipeline_runs(workspace_name);
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

                -- run_metrics_summary table and indexes (for older DBs)
                CREATE TABLE IF NOT EXISTS run_metrics_summary (
                    stage_run_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    min_value REAL,
                    max_value REAL,
                    last_value REAL,
                    last_timestamp TEXT,
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (stage_run_id, name),
                    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_run_metrics_summary_name ON run_metrics_summary(name);
                CREATE INDEX IF NOT EXISTS idx_run_metrics_summary_stage_name
                    ON run_metrics_summary(stage_run_id, name);

                CREATE TABLE IF NOT EXISTS pipeline_stage_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_run_id TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    deps TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    stage_run_id TEXT,
                    claimed_at TEXT,
                    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_pipeline_stage_queue_run ON pipeline_stage_queue(pipeline_run_id);
                CREATE INDEX IF NOT EXISTS idx_pipeline_stage_queue_status ON pipeline_stage_queue(status);

                CREATE INDEX IF NOT EXISTS idx_stage_runs_ws_stage_status ON stage_runs(workspace_name, stage_name, status);

                -- Stage versions table (tracks unique code + config per stage)
                CREATE TABLE IF NOT EXISTS stage_versions (
                    id INTEGER PRIMARY KEY,
                    workspace_name TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    version_num INTEGER NOT NULL,
                    git_sha TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(workspace_name, stage_name, version_num),
                    UNIQUE(workspace_name, stage_name, git_sha, config_hash),
                    FOREIGN KEY (workspace_name) REFERENCES workspace_lineage(workspace_name)
                );
                CREATE INDEX IF NOT EXISTS idx_stage_versions_lookup
                    ON stage_versions(workspace_name, stage_name, git_sha, config_hash);
                CREATE INDEX IF NOT EXISTS idx_stage_versions_workspace_stage
                    ON stage_versions(workspace_name, stage_name);

                -- Version tags (user-defined names for significant versions)
                CREATE TABLE IF NOT EXISTS workspace_version_tags (
                    workspace_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    tag_name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (workspace_name, tag_name),
                    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version)
                );
                CREATE INDEX IF NOT EXISTS idx_version_tags_version
                    ON workspace_version_tags(workspace_name, version);

                -- Experiment Records (user-facing entity representing runs or checkpoints)
                CREATE TABLE IF NOT EXISTS experiment_records (
                    record_id TEXT PRIMARY KEY,
                    workspace_name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    stage_run_id TEXT,
                    version TEXT NOT NULL,
                    experiment_group TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
                    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version)
                );
                CREATE INDEX IF NOT EXISTS idx_experiment_records_workspace
                    ON experiment_records(workspace_name);
                CREATE INDEX IF NOT EXISTS idx_experiment_records_version
                    ON experiment_records(workspace_name, version);
                CREATE INDEX IF NOT EXISTS idx_experiment_records_run
                    ON experiment_records(stage_run_id);
                CREATE INDEX IF NOT EXISTS idx_experiment_records_group
                    ON experiment_records(workspace_name, experiment_group);

                -- Run Results (auto + final results with ML/infra outcome separation)
                CREATE TABLE IF NOT EXISTS run_results (
                    stage_run_id TEXT PRIMARY KEY,
                    record_id TEXT NOT NULL,
                    results_status TEXT NOT NULL,
                    infra_outcome TEXT NOT NULL,
                    ml_outcome TEXT NOT NULL,
                    results_auto TEXT,
                    results_final TEXT,
                    comparison TEXT,
                    finalized_by TEXT,
                    finalized_at TEXT,
                    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
                    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
                );
                CREATE INDEX IF NOT EXISTS idx_run_results_record
                    ON run_results(record_id);
                CREATE INDEX IF NOT EXISTS idx_run_results_status
                    ON run_results(results_status);
                CREATE INDEX IF NOT EXISTS idx_run_results_ml_outcome
                    ON run_results(ml_outcome);

                -- Run Results Spec (required at run time to specify expected results)
                CREATE TABLE IF NOT EXISTS run_results_spec (
                    stage_run_id TEXT PRIMARY KEY,
                    record_id TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
                    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
                );
                CREATE INDEX IF NOT EXISTS idx_run_results_spec_record
                    ON run_results_spec(record_id);

                -- Run Tags (user-defined names for significant runs)
                CREATE TABLE IF NOT EXISTS run_tags (
                    workspace_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    tag_name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (workspace_name, tag_name),
                    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
                );
                CREATE INDEX IF NOT EXISTS idx_run_tags_record
                    ON run_tags(record_id);

                -- Stage state transitions (audit trail for state machine)
                CREATE TABLE IF NOT EXISTS stage_state_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stage_run_id TEXT NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    event TEXT NOT NULL,
                    phase TEXT,
                    termination_cause TEXT CHECK(termination_cause IS NULL OR termination_cause IN ('preempted', 'crashed', 'orphaned', 'timeout', 'ai_stopped', 'manual')),
                    exit_code INTEGER,
                    exit_code_exists INTEGER,
                    error_message TEXT,
                    svs_review_id TEXT,
                    source TEXT NOT NULL CHECK(source IN ('mcp_tool', 'executor', 'daemon', 'container', 'migration', 'admin')),
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE,
                    FOREIGN KEY (svs_review_id) REFERENCES svs_reviews(id)
                );
                CREATE INDEX IF NOT EXISTS idx_state_transitions_stage_run
                    ON stage_state_transitions(stage_run_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_state_transitions_created_at
                    ON stage_state_transitions(created_at);

                -- Partial index for active states (used by daemon polling)
                CREATE INDEX IF NOT EXISTS idx_stage_runs_active_state
                    ON stage_runs(state)
                    WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing', 'unknown');
                """
            )

            # Track latest schema version applied
            new_version = current_version

            # Version 3: Add CASCADE DELETE to metrics tables
            # Required because SQLite can't modify foreign key constraints
            if current_version < 3:
                # Recovery: handle failed migration where run_metrics was dropped but _new wasn't renamed
                run_metrics_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='run_metrics'"
                ).fetchone()
                run_metrics_new_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='run_metrics_new'"
                ).fetchone()

                if not run_metrics_exists and run_metrics_new_exists:
                    # Complete the failed migration
                    conn.executescript(
                        """
                        ALTER TABLE run_metrics_new RENAME TO run_metrics;
                        CREATE INDEX IF NOT EXISTS idx_run_metrics_stage_run ON run_metrics(stage_run_id);
                        CREATE INDEX IF NOT EXISTS idx_run_metrics_name ON run_metrics(stage_run_id, name);
                        """
                    )

                # Check if run_metrics already has CASCADE DELETE
                fk_info = conn.execute("PRAGMA foreign_key_list(run_metrics)").fetchall()
                has_cascade = any("CASCADE" in str(fk["on_delete"] if fk["on_delete"] else "") for fk in fk_info)

                if not has_cascade:
                    # Rebuild run_metrics with CASCADE DELETE
                    conn.executescript(
                        """
                        -- Temporarily disable foreign keys
                        PRAGMA foreign_keys = OFF;

                        -- Drop any leftover temp tables from failed migrations
                        DROP TABLE IF EXISTS run_metrics_new;
                        DROP TABLE IF EXISTS run_metrics_summary_new;

                        -- Create new table with CASCADE DELETE
                        CREATE TABLE run_metrics_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            stage_run_id TEXT NOT NULL,
                            name TEXT NOT NULL,
                            value REAL NOT NULL,
                            step INTEGER,
                            timestamp TEXT NOT NULL,
                            FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
                        );

                        -- Copy data
                        INSERT INTO run_metrics_new SELECT * FROM run_metrics;

                        -- Drop old table
                        DROP TABLE run_metrics;

                        -- Rename new table
                        ALTER TABLE run_metrics_new RENAME TO run_metrics;

                        -- Recreate indexes
                        CREATE INDEX idx_run_metrics_stage_run ON run_metrics(stage_run_id);
                        CREATE INDEX idx_run_metrics_name ON run_metrics(stage_run_id, name);

                        -- Rebuild run_metrics_summary with CASCADE DELETE
                        CREATE TABLE run_metrics_summary_new (
                            stage_run_id TEXT NOT NULL,
                            name TEXT NOT NULL,
                            min_value REAL,
                            max_value REAL,
                            last_value REAL,
                            last_timestamp TEXT,
                            count INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY (stage_run_id, name),
                            FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
                        );

                        INSERT INTO run_metrics_summary_new (stage_run_id, name, min_value, max_value, last_value, count)
                        SELECT stage_run_id, name, min_value, max_value, last_value, count FROM run_metrics_summary;
                        DROP TABLE run_metrics_summary;
                        ALTER TABLE run_metrics_summary_new RENAME TO run_metrics_summary;
                        CREATE INDEX idx_run_metrics_summary_stage_run ON run_metrics_summary(stage_run_id);
                        CREATE INDEX idx_run_metrics_summary_name ON run_metrics_summary(name);
                        CREATE INDEX idx_run_metrics_summary_stage_name ON run_metrics_summary(stage_run_id, name);

                        -- Rebuild run_artifacts with CASCADE DELETE
                        CREATE TABLE run_artifacts_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            stage_run_id TEXT NOT NULL,
                            name TEXT NOT NULL,
                            path TEXT NOT NULL,
                            backend_url TEXT,
                            created_at TEXT NOT NULL,
                            FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
                        );

                        INSERT INTO run_artifacts_new SELECT * FROM run_artifacts;
                        DROP TABLE run_artifacts;
                        ALTER TABLE run_artifacts_new RENAME TO run_artifacts;
                        CREATE INDEX idx_run_artifacts_stage_run ON run_artifacts(stage_run_id);

                        -- Re-enable foreign keys
                        PRAGMA foreign_keys = ON;
                        """
                    )

                new_version = 3

            # Version 4: Enforce idempotency for NULL steps using COALESCE
            if current_version < 4:
                conn.execute("DROP INDEX IF EXISTS idx_run_metrics_unique")
                # Deduplicate existing rows that would violate the new unique index
                conn.execute(
                    """
                    DELETE FROM run_metrics
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM run_metrics
                        GROUP BY stage_run_id, name, COALESCE(step, -1), timestamp
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_run_metrics_unique
                        ON run_metrics(stage_run_id, name, COALESCE(step, -1), timestamp)
                    """
                )
                new_version = 4

            # Version 5: Backfill experiment_records and run_results for existing stage_runs
            # Required for DBs created before the experiment model was added
            if current_version < 5:
                from goldfish.experiment_model.records import generate_record_id

                # Find stage_runs without experiment_records
                # IMPORTANT: Only include runs where workspace_version exists (FK constraint)
                # NOTE: Select both state and status - legacy DBs may have state=NULL
                # but status populated. state is the new state machine column, status
                # is the legacy column. Use COALESCE to prefer state over status.
                orphaned = conn.execute(
                    """
                    SELECT sr.id, sr.workspace_name, sr.version, sr.state, sr.status
                    FROM stage_runs sr
                    LEFT JOIN experiment_records er ON er.stage_run_id = sr.id
                    INNER JOIN workspace_versions wv
                        ON sr.workspace_name = wv.workspace_name
                        AND sr.version = wv.version
                    WHERE er.record_id IS NULL
                    """
                ).fetchall()

                for row in orphaned:
                    stage_run_id = row["id"]
                    workspace_name = row["workspace_name"]
                    version = row["version"]
                    # Use state if available, otherwise fall back to status (legacy column)
                    state = row["state"] or row["status"]

                    # Generate ULID for record_id
                    record_id = generate_record_id()

                    # Determine infra_outcome from state (state machine is source of truth)
                    # Only terminal states get mapped; non-terminal get "unknown"
                    # to satisfy CHECK constraint (completed/preempted/crashed/canceled/unknown)
                    infra_outcome_map = {
                        "completed": "completed",
                        "failed": "crashed",
                        "terminated": "crashed",
                        "canceled": "canceled",
                    }
                    # Active states -> unknown (valid CHECK value, will be updated later)
                    infra_outcome = infra_outcome_map.get(state, "unknown") if state else "unknown"

                    # Create experiment_record
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO experiment_records
                        (record_id, workspace_name, type, stage_run_id, version, created_at)
                        VALUES (?, ?, 'run', ?, ?, datetime('now'))
                        """,
                        (record_id, workspace_name, stage_run_id, version),
                    )

                    # Create run_results with status=missing (needs finalization)
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO run_results
                        (stage_run_id, record_id, results_status, infra_outcome, ml_outcome)
                        VALUES (?, ?, 'missing', ?, 'unknown')
                        """,
                        (stage_run_id, record_id, infra_outcome),
                    )

                new_version = 5

            # Version 6: Normalize stage_state_transitions schema (remove context_json)
            if current_version < 6:
                # stage_state_transitions may exist in older DBs with:
                #   (stage_run_id, from_state, to_state, event, context_json, timestamp)
                # New schema stores normalized columns (phase, termination_cause, exit_code, etc.)
                table_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='stage_state_transitions'"
                ).fetchone()
                if table_exists is not None:
                    transition_cols: set[str] = {
                        row["name"] for row in conn.execute("PRAGMA table_info(stage_state_transitions)")
                    }
                    if "context_json" in transition_cols:
                        legacy_rows = conn.execute(
                            """
                            SELECT id, stage_run_id, from_state, to_state, event, context_json, timestamp
                            FROM stage_state_transitions
                            ORDER BY id
                            """
                        ).fetchall()

                        # Rebuild via table recreation (portable across SQLite versions)
                        # NOTE: PRAGMA foreign_keys is per-connection; restore it after.
                        conn.execute("PRAGMA foreign_keys = OFF")
                        conn.execute("DROP TABLE IF EXISTS stage_state_transitions_new")
                        conn.execute(
                            """
                            CREATE TABLE stage_state_transitions_new (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                stage_run_id TEXT NOT NULL,
                                from_state TEXT NOT NULL,
                                to_state TEXT NOT NULL,
                                event TEXT NOT NULL,
                                phase TEXT,
                                termination_cause TEXT CHECK(
                                    termination_cause IS NULL OR termination_cause IN
                                    ('preempted', 'crashed', 'orphaned', 'timeout', 'ai_stopped', 'manual')
                                ),
                                exit_code INTEGER,
                                exit_code_exists INTEGER,
                                error_message TEXT,
                                svs_review_id TEXT,
                                source TEXT NOT NULL CHECK(
                                    source IN ('mcp_tool', 'executor', 'daemon', 'container', 'migration', 'admin')
                                ),
                                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                                FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE,
                                FOREIGN KEY (svs_review_id) REFERENCES svs_reviews(id)
                            )
                            """
                        )

                        for r in legacy_rows:
                            raw = r["context_json"] or "{}"
                            try:
                                ctx = json.loads(raw)
                            except Exception:
                                ctx = {}

                            phase = ctx.get("phase")
                            termination_cause = ctx.get("termination_cause")
                            exit_code = ctx.get("exit_code")
                            exit_code_exists = 1 if ctx.get("exit_code_exists") else 0
                            error_message = ctx.get("error_message")
                            source = ctx.get("source") or "migration"
                            created_at = r["timestamp"] or datetime.now(UTC).isoformat()

                            svs_review_id = ctx.get("svs_review_id")

                            conn.execute(
                                """
                                INSERT INTO stage_state_transitions_new
                                (id, stage_run_id, from_state, to_state, event,
                                 phase, termination_cause, exit_code, exit_code_exists,
                                 error_message, svs_review_id, source, created_at)
                                VALUES (?, ?, ?, ?, ?,
                                        ?, ?, ?, ?,
                                        ?, ?, ?, ?)
                                """,
                                (
                                    r["id"],
                                    r["stage_run_id"],
                                    r["from_state"],
                                    r["to_state"],
                                    r["event"],
                                    phase,
                                    termination_cause,
                                    exit_code,
                                    exit_code_exists,
                                    error_message,
                                    svs_review_id,
                                    source,
                                    created_at,
                                ),
                            )

                        conn.execute("DROP TABLE stage_state_transitions")
                        conn.execute("ALTER TABLE stage_state_transitions_new RENAME TO stage_state_transitions")
                        conn.execute(
                            "CREATE INDEX IF NOT EXISTS idx_state_transitions_stage_run ON stage_state_transitions(stage_run_id, created_at)"
                        )
                        conn.execute(
                            "CREATE INDEX IF NOT EXISTS idx_state_transitions_created_at ON stage_state_transitions(created_at)"
                        )
                        conn.execute("PRAGMA foreign_keys = ON")

                new_version = 6

            if current_version < 7:
                new_version = 7

            # Bump schema version if needed
            if new_version != current_version:
                if version_row:
                    conn.execute("UPDATE schema_version SET version = ?", (new_version,))
                else:
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (new_version,))

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a connection for multi-operation transactions.

        All operations within the context will be committed together
        on successful completion, or rolled back on exception.

        Example:
            with db.transaction() as conn:
                conn.execute("INSERT INTO ...")
                conn.execute("INSERT INTO ...")
            # Both committed, or neither

        Yields:
            Connection with autocommit disabled
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _get_raw_conn(self) -> sqlite3.Connection:
        """Get a raw database connection for manual transaction control.

        This is used for leader election with BEGIN IMMEDIATE.
        The caller is responsible for committing/rolling back and closing.

        Returns:
            Raw SQLite connection (caller must close).
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    # --- Audit operations ---

    def log_audit(
        self,
        operation: str,
        reason: str,
        slot: str | None = None,
        workspace: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> int:
        """Log an operation to the audit trail. Returns row ID."""
        timestamp = datetime.now(UTC).isoformat()
        details_json = json.dumps(details) if details else None

        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO audit (timestamp, operation, slot, workspace, reason, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (timestamp, operation, slot, workspace, reason, details_json),
            )
            return cursor.lastrowid or 0

    def get_recent_audit(self, limit: int = 20) -> list[AuditRow]:
        """Get recent audit entries."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [cast(AuditRow, dict(row)) for row in rows]

    def get_run_thoughts(self, run_id: str) -> list[AuditRow]:
        """Get all thoughts associated with a specific run."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM audit
                WHERE operation = 'thought'
                AND json_extract(details, '$.run_id') = ?
                ORDER BY timestamp ASC
                """,
                (run_id,),
            ).fetchall()
            return [cast(AuditRow, dict(row)) for row in rows]

    def get_workspace_thoughts(self, workspace: str, limit: int = 50, offset: int = 0) -> list[AuditRow]:
        """Get all thoughts associated with a workspace.

        Args:
            workspace: Workspace name to filter by
            limit: Maximum number of thoughts to return
            offset: Number of thoughts to skip (for pagination)

        Returns:
            List of audit rows containing thoughts for this workspace
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM audit
                WHERE operation = 'thought'
                AND workspace = ?
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
                """,
                (workspace, limit, offset),
            ).fetchall()
            return [cast(AuditRow, dict(row)) for row in rows]

    def count_workspace_thoughts(self, workspace: str) -> int:
        """Count total thoughts for a workspace."""
        with self._conn() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) FROM audit
                WHERE operation = 'thought'
                AND workspace = ?
                """,
                (workspace,),
            ).fetchone()
            count: int = result[0] if result else 0
            return count

    # --- Source operations ---

    def create_source(
        self,
        source_id: str,
        name: str,
        gcs_location: str,
        created_by: str,
        description: str | None = None,
        size_bytes: int | None = None,
        status: str = "available",
        metadata: dict | None = None,
    ) -> None:
        """Create a new source entry."""
        timestamp = datetime.now(UTC).isoformat()
        metadata_json = json.dumps(metadata) if metadata else None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, description, created_at, created_by,
                                     gcs_location, size_bytes, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    name,
                    description,
                    timestamp,
                    created_by,
                    gcs_location,
                    size_bytes,
                    status,
                    metadata_json,
                ),
            )

    def get_source(self, source_id_or_name: str) -> SourceRow | None:
        """Get a source by ID or name.

        Looks up by ID first, then by name as fallback. This allows users
        to reference sources by either their ID (e.g., "src-123") or their
        human-readable name (e.g., "v37-tokens").
        """
        with self._conn() as conn:
            # Try by ID first
            row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id_or_name,)).fetchone()
            if row:
                return cast(SourceRow, dict(row))
            # Fallback to name lookup
            row = conn.execute("SELECT * FROM sources WHERE name = ?", (source_id_or_name,)).fetchone()
            return cast(SourceRow, dict(row)) if row else None

    def update_source_metadata(
        self,
        source_id_or_name: str,
        metadata: dict[str, Any],
        description: str | None = None,
        size_bytes: int | None = None,
    ) -> None:
        """Update metadata (and related fields) for a source."""
        metadata_json = json.dumps(metadata)

        with self._conn() as conn:
            conn.execute(
                """
                UPDATE sources
                SET metadata = ?, description = ?, size_bytes = ?
                WHERE id = ? OR name = ?
                """,
                (metadata_json, description, size_bytes, source_id_or_name, source_id_or_name),
            )

    def count_sources(
        self,
        status: str | None = None,
        created_by: str | None = None,
    ) -> int:
        """Count sources matching filters.

        Args:
            status: Filter by status (available, processing, error)
            created_by: Filter by creator (external, internal, etc.)

        Returns:
            Number of sources matching filters
        """
        with self._conn() as conn:
            # Build query with filters
            query = "SELECT COUNT(*) FROM sources WHERE 1=1"
            params = []

            if status:
                query += " AND status = ?"
                params.append(status)
            if created_by:
                query += " AND created_by = ?"
                params.append(created_by)

            row = conn.execute(query, tuple(params)).fetchone()
            count: int = row[0] if row else 0
            return count

    def list_sources(
        self,
        status: str | None = None,
        created_by: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[SourceRow]:
        """List sources with optional filters and pagination.

        Args:
            status: Filter by status (available, processing, error)
            created_by: Filter by creator (external, internal, etc.)
            limit: Maximum number of sources to return (1-200, default 50)
            offset: Number of sources to skip (default 0)

        Returns:
            List of source dictionaries

        Raises:
            GoldfishError: If limit or offset are out of bounds
        """
        from goldfish.errors import GoldfishError

        # Validate bounds
        if limit < 1 or limit > 200:
            raise GoldfishError("limit must be between 1 and 200")
        if offset < 0:
            raise GoldfishError("offset must be >= 0")

        with self._conn() as conn:
            # Build query with filters
            query = "SELECT * FROM sources WHERE 1=1"
            params = []

            if status:
                query += " AND status = ?"
                params.append(status)
            if created_by:
                query += " AND created_by = ?"
                params.append(created_by)

            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([str(limit), str(offset)])

            rows = conn.execute(query, tuple(params)).fetchall()
            return [cast(SourceRow, dict(row)) for row in rows]

    def source_exists(self, source_id: str) -> bool:
        """Check if a source exists."""
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM sources WHERE id = ?", (source_id,)).fetchone()
            return row is not None

    # --- Lineage operations ---

    def add_lineage(
        self,
        source_id: str,
        parent_source_id: str | None = None,
        job_id: str | None = None,
    ) -> None:
        """Add a lineage record for a source."""
        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO source_lineage (source_id, parent_source_id, job_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (source_id, parent_source_id, job_id, timestamp),
            )

    def get_lineage(self, source_id: str) -> list[LineageRow]:
        """Get lineage records for a source."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM source_lineage WHERE source_id = ?", (source_id,)).fetchall()
            return [cast(LineageRow, dict(row)) for row in rows]

    # --- Job operations ---

    def create_job(
        self,
        job_id: str,
        workspace: str,
        snapshot_id: str,
        script: str,
        experiment_dir: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Create a new job entry."""
        timestamp = datetime.now(UTC).isoformat()
        metadata_json = json.dumps(metadata) if metadata else None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO jobs (id, workspace, snapshot_id, script, experiment_dir,
                                  status, started_at, metadata)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (job_id, workspace, snapshot_id, script, experiment_dir, timestamp, metadata_json),
            )

    def update_job_status(
        self,
        job_id: str,
        status: str,
        completed_at: str | None = None,
        log_uri: str | None = None,
        artifact_uri: str | None = None,
        error: str | None = None,
    ) -> None:
        """Update job status."""
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, completed_at = ?, log_uri = ?, artifact_uri = ?, error = ?
                WHERE id = ?
                """,
                (status, completed_at, log_uri, artifact_uri, error, job_id),
            )

    def get_job(self, job_id: str) -> JobRow | None:
        """Get a job by ID."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return cast(JobRow, dict(row)) if row else None

    def delete_job(self, job_id: str) -> tuple[bool, int]:
        """Delete a job and its inputs atomically.

        Args:
            job_id: Job ID to delete

        Returns:
            Tuple of (deleted, inputs_count) - whether job was deleted and count of inputs removed

        Raises:
            JobNotFoundError: If job doesn't exist
            GoldfishError: If job is not in a terminal state (pending/running)
        """
        from goldfish.errors import GoldfishError, JobNotFoundError

        # Check job exists and get its status
        job = self.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        # Only allow deleting completed, failed, or cancelled jobs
        if job["status"] in (JobStatus.PENDING, JobStatus.RUNNING):
            raise GoldfishError(
                f"Cannot delete {job['status']} job. Only completed, failed, or cancelled jobs can be deleted."
            )

        # Delete atomically with transaction
        with self.transaction() as conn:
            # Count and delete job_inputs
            inputs_result = conn.execute("SELECT COUNT(*) FROM job_inputs WHERE job_id = ?", (job_id,)).fetchone()
            inputs_count = inputs_result[0] if inputs_result else 0

            conn.execute("DELETE FROM job_inputs WHERE job_id = ?", (job_id,))

            # Delete job
            conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))

        return (True, inputs_count)

    def list_jobs(
        self,
        status: str | None = None,
        workspace: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[JobRow]:
        """List jobs with optional filters and pagination."""
        query = "SELECT * FROM jobs WHERE 1=1"
        params: list = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if workspace:
            query += " AND workspace = ?"
            params.append(workspace)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.append(limit)
        params.append(offset)

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [cast(JobRow, dict(row)) for row in rows]

    def count_jobs(
        self,
        status: str | None = None,
        workspace: str | None = None,
    ) -> int:
        """Count jobs matching filters (for pagination)."""
        query = "SELECT COUNT(*) FROM jobs WHERE 1=1"
        params: list = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if workspace:
            query += " AND workspace = ?"
            params.append(workspace)

        with self._conn() as conn:
            result = conn.execute(query, params).fetchone()
            count: int = result[0] if result else 0
            return count

    def get_active_jobs(self) -> list[JobRow]:
        """Get all running/pending jobs."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('pending', 'running') ORDER BY started_at DESC"
            ).fetchall()
            return [cast(JobRow, dict(row)) for row in rows]

    # --- Job inputs ---

    def add_job_input(self, job_id: str, source_id: str, input_name: str) -> None:
        """Record that a job used a source as input."""
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO job_inputs (job_id, source_id, input_name)
                VALUES (?, ?, ?)
                """,
                (job_id, source_id, input_name),
            )

    def get_job_inputs(self, job_id: str) -> list[JobInputWithSource]:
        """Get all input sources for a job."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT ji.*, s.name as source_name, s.gcs_location
                FROM job_inputs ji
                JOIN sources s ON ji.source_id = s.id
                WHERE ji.job_id = ?
                """,
                (job_id,),
            ).fetchall()
            return [cast(JobInputWithSource, dict(row)) for row in rows]

    def create_job_with_inputs(
        self,
        job_id: str,
        workspace: str,
        snapshot_id: str,
        script: str,
        experiment_dir: str | None = None,
        inputs: dict[str, str] | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Create a job and record its inputs atomically.

        If adding inputs fails, the job creation is rolled back.

        Args:
            job_id: Unique job identifier
            workspace: Workspace name
            snapshot_id: Snapshot the job runs against
            script: Script to execute
            experiment_dir: Path to experiment directory
            inputs: Map of input_name -> source_id
            metadata: Additional metadata to store
        """
        timestamp = datetime.now(UTC).isoformat()
        metadata_json = json.dumps(metadata) if metadata else None

        with self.transaction() as conn:
            # Create the job
            conn.execute(
                """
                INSERT INTO jobs (id, workspace, snapshot_id, script, experiment_dir,
                                  status, started_at, metadata)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (job_id, workspace, snapshot_id, script, experiment_dir, timestamp, metadata_json),
            )

            # Add inputs within same transaction
            if inputs:
                for input_name, source_id in inputs.items():
                    conn.execute(
                        """
                        INSERT INTO job_inputs (job_id, source_id, input_name)
                        VALUES (?, ?, ?)
                        """,
                        (job_id, source_id, input_name),
                    )

    # --- Workspace goal operations ---

    def set_workspace_goal(self, workspace: str, goal: str) -> None:
        """Set or update a workspace goal.

        Args:
            workspace: Workspace name
            goal: Goal description
        """
        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO workspace_goals (workspace, goal, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(workspace) DO UPDATE SET
                    goal = excluded.goal,
                    updated_at = excluded.updated_at
                """,
                (workspace, goal, timestamp, timestamp),
            )

    def get_workspace_goal(self, workspace: str) -> str | None:
        """Get the goal for a workspace.

        Args:
            workspace: Workspace name

        Returns:
            Goal string if set, None otherwise
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT goal FROM workspace_goals WHERE workspace = ?",
                (workspace,),
            ).fetchone()
            return row["goal"] if row else None

    def delete_workspace_goal(self, workspace: str) -> bool:
        """Delete a workspace goal.

        Args:
            workspace: Workspace name

        Returns:
            True if deleted, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM workspace_goals WHERE workspace = ?",
                (workspace,),
            )
            return cursor.rowcount > 0

    # --- Workspace mount operations (copy-based mounting) ---

    def record_mount(
        self,
        slot: str,
        workspace_name: str,
        branch: str,
        mounted_sha: str,
        status: str = "active",
    ) -> None:
        """Record a workspace mount operation.

        Args:
            slot: Slot name (e.g., "w1")
            workspace_name: Workspace name
            branch: Git branch name
            mounted_sha: SHA at time of mount
            status: Mount status (default "active")
        """
        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO workspace_mounts
                (slot, workspace_name, branch, mounted_sha, mounted_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (slot, workspace_name, branch, mounted_sha, timestamp, status),
            )

    def get_mount(self, slot: str) -> dict | None:
        """Get mount information for a slot.

        Args:
            slot: Slot name

        Returns:
            Mount dict or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_mounts WHERE slot = ?",
                (slot,),
            ).fetchone()
            return dict(row) if row else None

    def get_mount_by_workspace(self, workspace_name: str) -> dict | None:
        """Get mount information for a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            Mount dict or None if not mounted
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_mounts WHERE workspace_name = ? AND status = 'active'",
                (workspace_name,),
            ).fetchone()
            return dict(row) if row else None

    def update_mount_status(self, slot: str, status: str) -> bool:
        """Update mount status.

        Args:
            slot: Slot name
            status: New status

        Returns:
            True if updated, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                "UPDATE workspace_mounts SET status = ? WHERE slot = ?",
                (status, slot),
            )
            return cursor.rowcount > 0

    def update_mount_sha(self, slot: str, mounted_sha: str) -> bool:
        """Update mounted SHA after sync.

        Args:
            slot: Slot name
            mounted_sha: New SHA

        Returns:
            True if updated, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                "UPDATE workspace_mounts SET mounted_sha = ? WHERE slot = ?",
                (mounted_sha, slot),
            )
            return cursor.rowcount > 0

    def delete_mount(self, slot: str) -> bool:
        """Delete a mount record.

        Args:
            slot: Slot name

        Returns:
            True if deleted, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM workspace_mounts WHERE slot = ?",
                (slot,),
            )
            return cursor.rowcount > 0

    def get_mounts(self, status: str | None = None) -> list[dict]:
        """Get all mount records.

        Args:
            status: Optional status filter

        Returns:
            List of mount dicts
        """
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM workspace_mounts WHERE status = ?",
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM workspace_mounts").fetchall()
            return [dict(row) for row in rows]

    def delete_source(self, source_id: str) -> bool:
        """Delete a source and its lineage records.

        Args:
            source_id: Source ID to delete

        Returns:
            True if deleted, False if not found
        """
        with self.transaction() as conn:
            # First delete lineage records
            conn.execute(
                "DELETE FROM source_lineage WHERE source_id = ? OR parent_source_id = ?",
                (source_id, source_id),
            )
            # Then delete the source
            cursor = conn.execute(
                "DELETE FROM sources WHERE id = ?",
                (source_id,),
            )
            return cursor.rowcount > 0

    # --- Workspace lineage operations ---

    def create_workspace_lineage(
        self,
        workspace_name: str,
        parent_workspace: str | None = None,
        parent_version: str | None = None,
        description: str | None = None,
    ) -> None:
        """Record workspace creation in lineage.

        Args:
            workspace_name: Workspace name
            parent_workspace: Parent workspace if branched
            parent_version: Version branched from
            description: Workspace description
        """
        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO workspace_lineage
                (workspace_name, parent_workspace, parent_version, created_at, description)
                VALUES (?, ?, ?, ?, ?)
                """,
                (workspace_name, parent_workspace, parent_version, timestamp, description),
            )

    def get_workspace_lineage(self, workspace_name: str) -> dict | None:
        """Get workspace lineage information.

        Args:
            workspace_name: Workspace name

        Returns:
            Lineage dict or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_lineage WHERE workspace_name = ?",
                (workspace_name,),
            ).fetchone()
            return dict(row) if row else None

    def workspace_exists(self, workspace_name: str) -> bool:
        """Check if a workspace exists in lineage.

        Args:
            workspace_name: Workspace name

        Returns:
            True if exists, False otherwise
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM workspace_lineage WHERE workspace_name = ?",
                (workspace_name,),
            ).fetchone()
            return row is not None

    def list_workspace_lineages(self) -> list[dict]:
        """List all workspace lineages.

        Returns:
            List of workspace lineage dicts
        """
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM workspace_lineage ORDER BY created_at DESC").fetchall()
            return [dict(row) for row in rows]

    def get_workspace_branches(self, parent_workspace: str) -> list[dict]:
        """Get all workspaces branched from a parent.

        Args:
            parent_workspace: Parent workspace name

        Returns:
            List of child workspace lineages
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM workspace_lineage WHERE parent_workspace = ?",
                (parent_workspace,),
            ).fetchall()
            return [dict(row) for row in rows]

    # --- Workspace version operations ---

    def create_version(
        self,
        workspace_name: str,
        version: str,
        git_tag: str,
        git_sha: str,
        created_by: str,
        job_id: str | None = None,
        description: str | None = None,
    ) -> None:
        """Create a new workspace version.

        Args:
            workspace_name: Workspace name
            version: Version string (e.g., "v1", "v2")
            git_tag: Git tag name
            git_sha: Git commit SHA
            created_by: 'run', 'checkpoint', 'manual'
            job_id: Job that triggered version (if created_by='run')
            description: Version description
        """
        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO workspace_versions
                (workspace_name, version, git_tag, git_sha, created_at, created_by, job_id, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (workspace_name, version, git_tag, git_sha, timestamp, created_by, job_id, description),
            )

    def get_version(self, workspace_name: str, version: str) -> dict | None:
        """Get a specific workspace version.

        Args:
            workspace_name: Workspace name
            version: Version string

        Returns:
            Version dict or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ? AND version = ?
                """,
                (workspace_name, version),
            ).fetchone()
            return dict(row) if row else None

    def get_version_by_sha(self, workspace_name: str, git_sha: str) -> dict | None:
        """Get a version by its git SHA.

        Used to check if a version with this SHA already exists (idempotent retry).

        Args:
            workspace_name: Workspace name
            git_sha: Git commit SHA

        Returns:
            Version dict with version, git_tag, git_sha, etc. if found
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ? AND git_sha = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (workspace_name, git_sha),
            ).fetchone()
            return dict(row) if row else None

    def list_versions(
        self,
        workspace_name: str,
        include_pruned: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        """List versions for a workspace with pagination support.

        Args:
            workspace_name: Workspace name
            include_pruned: If True, include pruned versions in the list
            limit: Maximum number of versions to return
            offset: Number of versions to skip

        Returns:
            List of version dicts, ordered by creation time
        """
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        offset_clause = f"OFFSET {offset}" if offset is not None else ""

        with self._conn() as conn:
            if include_pruned:
                rows = conn.execute(
                    f"""
                    SELECT * FROM workspace_versions
                    WHERE workspace_name = ?
                    ORDER BY created_at ASC
                    {limit_clause} {offset_clause}
                    """,
                    (workspace_name,),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT * FROM workspace_versions
                    WHERE workspace_name = ?
                    AND pruned_at IS NULL
                    ORDER BY created_at ASC
                    {limit_clause} {offset_clause}
                    """,
                    (workspace_name,),
                ).fetchall()
            return [dict(row) for row in rows]

    def get_latest_version(self, workspace_name: str) -> dict | None:
        """Get the most recent non-pruned version for a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            Latest non-pruned version dict or None if no versions
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
                AND pruned_at IS NULL
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (workspace_name,),
            ).fetchone()
            return dict(row) if row else None

    def get_latest_explicit_version(self, workspace_name: str) -> dict | None:
        """Get the most recent explicit version for a workspace.

        Explicit versions are those created by save_version/checkpoint, NOT by run().
        This is used for diff() to show changes since last user-initiated save.

        Args:
            workspace_name: Workspace name

        Returns:
            Latest explicit version dict or None if no explicit versions
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
                AND created_by != 'run'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (workspace_name,),
            ).fetchone()
            return dict(row) if row else None

    def get_next_version_number(self, workspace_name: str) -> str:
        """Get the next version number for a workspace.

        Includes pruned versions in the count to ensure version numbers
        are never reused (v1, v2, ... v50 pruned, next is v51 not v1).

        Args:
            workspace_name: Workspace name

        Returns:
            Next version string (e.g., "v1", "v2")
        """
        # Include pruned versions to ensure continuous numbering
        versions = self.list_versions(workspace_name, include_pruned=True)
        return f"v{len(versions) + 1}"

    # --- Version tag operations ---

    def create_tag(self, workspace_name: str, version: str, tag_name: str) -> VersionTagRow:
        """Create a tag for a version.

        Tags can be applied retroactively to any existing version.

        Args:
            workspace_name: Workspace name
            version: Version to tag (e.g., "v1", "v2")
            tag_name: Name for the tag (e.g., "baseline-working")

        Returns:
            Dict with tag info

        Raises:
            GoldfishError: If version doesn't exist or tag already exists
        """
        from goldfish.errors import GoldfishError

        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            # Check version exists in this workspace
            version_row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ? AND version = ?
                """,
                (workspace_name, version),
            ).fetchone()

            if not version_row:
                raise GoldfishError(
                    f"Version '{version}' not found in workspace '{workspace_name}'",
                    details={"workspace": workspace_name, "version": version},
                )

            # Check tag doesn't already exist
            existing_tag = conn.execute(
                """
                SELECT * FROM workspace_version_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag_name),
            ).fetchone()

            if existing_tag:
                raise GoldfishError(
                    f"Tag '{tag_name}' already exists in workspace '{workspace_name}'",
                    details={"workspace": workspace_name, "tag_name": tag_name},
                )

            # Create the tag
            conn.execute(
                """
                INSERT INTO workspace_version_tags
                (workspace_name, version, tag_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (workspace_name, version, tag_name, timestamp),
            )

            return cast(
                VersionTagRow,
                {
                    "workspace_name": workspace_name,
                    "version": version,
                    "tag_name": tag_name,
                    "created_at": timestamp,
                },
            )

    def delete_tag(self, workspace_name: str, tag_name: str) -> None:
        """Delete a tag.

        Args:
            workspace_name: Workspace name
            tag_name: Name of the tag to delete

        Raises:
            GoldfishError: If tag doesn't exist
        """
        from goldfish.errors import GoldfishError

        with self._conn() as conn:
            # Check tag exists
            existing = conn.execute(
                """
                SELECT * FROM workspace_version_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag_name),
            ).fetchone()

            if not existing:
                raise GoldfishError(
                    f"Tag '{tag_name}' not found in workspace '{workspace_name}'",
                    details={"workspace": workspace_name, "tag_name": tag_name},
                )

            conn.execute(
                """
                DELETE FROM workspace_version_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag_name),
            )

    def list_tags(self, workspace_name: str) -> list[VersionTagRow]:
        """List all tags for a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            List of tag dicts, ordered by creation time
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workspace_version_tags
                WHERE workspace_name = ?
                ORDER BY created_at ASC
                """,
                (workspace_name,),
            ).fetchall()
            return [cast(VersionTagRow, dict(row)) for row in rows]

    def get_version_tags(self, workspace_name: str, version: str) -> list[VersionTagRow]:
        """Get all tags for a specific version.

        Args:
            workspace_name: Workspace name
            version: Version to get tags for

        Returns:
            List of tag dicts for this version
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workspace_version_tags
                WHERE workspace_name = ? AND version = ?
                ORDER BY created_at ASC
                """,
                (workspace_name, version),
            ).fetchall()
            return [cast(VersionTagRow, dict(row)) for row in rows]

    # --- Version pruning operations ---

    def is_version_tagged(self, workspace_name: str, version: str) -> bool:
        """Check if a version has any tags (and thus is protected from pruning).

        Args:
            workspace_name: Workspace name
            version: Version to check

        Returns:
            True if version has at least one tag
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as count FROM workspace_version_tags
                WHERE workspace_name = ? AND version = ?
                """,
                (workspace_name, version),
            ).fetchone()
            count: int = row["count"]
            return count > 0

    def _parse_version_number(self, version: str) -> int:
        """Parse version string to integer (e.g., 'v24' -> 24).

        Raises:
            GoldfishError: If version format is invalid
        """
        from goldfish.errors import GoldfishError

        if not version or not version.startswith("v"):
            raise GoldfishError(f"Invalid version format: {version}. Expected 'v<number>'")
        try:
            num = int(version[1:])
            if num < 1:
                raise GoldfishError(f"Version number must be positive: {version}")
            return num
        except ValueError as e:
            raise GoldfishError(f"Invalid version format: {version}") from e

    def prune_version(self, workspace_name: str, version: str, reason: str) -> PrunedVersionRow:
        """Prune a version (soft delete).

        Pruned versions are hidden from list_versions() by default but
        can still be accessed with include_pruned=True.

        Args:
            workspace_name: Workspace name
            version: Version to prune
            reason: Why pruning (min 15 chars)

        Returns:
            Dict with pruned version info

        Raises:
            GoldfishError: If version doesn't exist or is tagged (protected)
        """
        from goldfish.errors import GoldfishError

        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            # Atomic update with WHERE clause to prevent TOCTOU race condition
            # Checks if version is tagged in the same query
            result = conn.execute(
                """
                UPDATE workspace_versions
                SET pruned_at = ?, prune_reason = ?
                WHERE workspace_name = ? AND version = ?
                AND NOT EXISTS (
                    SELECT 1 FROM workspace_version_tags
                    WHERE workspace_name = ? AND version = ?
                )
                """,
                (timestamp, reason, workspace_name, version, workspace_name, version),
            )

            if result.rowcount == 0:
                # Update failed - determine why (doesn't exist vs tagged)
                version_row = conn.execute(
                    """
                    SELECT * FROM workspace_versions
                    WHERE workspace_name = ? AND version = ?
                    """,
                    (workspace_name, version),
                ).fetchone()

                if not version_row:
                    raise GoldfishError(
                        f"Version '{version}' not found in workspace '{workspace_name}'",
                        details={"workspace": workspace_name, "version": version},
                    )

                # Check if tagged
                tag_count = conn.execute(
                    """
                    SELECT COUNT(*) as count FROM workspace_version_tags
                    WHERE workspace_name = ? AND version = ?
                    """,
                    (workspace_name, version),
                ).fetchone()["count"]

                if tag_count > 0:
                    raise GoldfishError(
                        f"Cannot prune version '{version}' because it has tags. "
                        f"Remove tags first with untag_version(), then prune.",
                        details={"workspace": workspace_name, "version": version},
                    )

                # Already pruned
                if version_row["pruned_at"] is not None:
                    raise GoldfishError(
                        f"Version '{version}' is already pruned",
                        details={"workspace": workspace_name, "version": version},
                    )

                # Fallback for unknown error
                raise GoldfishError(
                    f"Failed to prune version '{version}' for unknown reason",
                    details={"workspace": workspace_name, "version": version},
                )

            return cast(
                PrunedVersionRow,
                {
                    "workspace_name": workspace_name,
                    "version": version,
                    "pruned_at": timestamp,
                    "prune_reason": reason,
                },
            )

    def prune_versions(self, workspace_name: str, from_version: str, to_version: str, reason: str) -> dict:
        """Prune a range of versions (inclusive).

        Tagged versions within the range are skipped (not pruned).

        Args:
            workspace_name: Workspace name
            from_version: Start version (e.g., "v3")
            to_version: End version (e.g., "v7")
            reason: Why pruning

        Returns:
            Dict with pruned_count and skipped_tagged count
        """
        timestamp = datetime.now(UTC).isoformat()

        # Extract version numbers
        from_num = self._parse_version_number(from_version)
        to_num = self._parse_version_number(to_version)

        with self._conn() as conn:
            # 1. Count tagged versions in range (to report skipped)
            # Use CAST(SUBSTR(version, 2) AS INTEGER) to extract number from "v123"
            row = conn.execute(
                """
                SELECT COUNT(*) as count
                FROM workspace_versions v
                JOIN workspace_version_tags t ON v.workspace_name = t.workspace_name AND v.version = t.version
                WHERE v.workspace_name = ?
                AND v.pruned_at IS NULL
                AND CAST(SUBSTR(v.version, 2) AS INTEGER) BETWEEN ? AND ?
                """,
                (workspace_name, from_num, to_num),
            ).fetchone()
            skipped_tagged = row["count"]

            # 2. Prune non-tagged versions in range
            result = conn.execute(
                """
                UPDATE workspace_versions
                SET pruned_at = ?, prune_reason = ?
                WHERE workspace_name = ?
                AND pruned_at IS NULL
                AND CAST(SUBSTR(version, 2) AS INTEGER) BETWEEN ? AND ?
                AND NOT EXISTS (
                    SELECT 1 FROM workspace_version_tags t
                    WHERE t.workspace_name = workspace_versions.workspace_name
                    AND t.version = workspace_versions.version
                )
                """,
                (timestamp, reason, workspace_name, from_num, to_num),
            )
            pruned_count = result.rowcount

        return {
            "workspace_name": workspace_name,
            "from_version": from_version,
            "to_version": to_version,
            "pruned_count": pruned_count,
            "skipped_tagged": skipped_tagged,
        }

    def prune_before_tag(self, workspace_name: str, tag_name: str, reason: str) -> dict:
        """Prune all versions before a tagged milestone.

        The tagged version itself is NOT pruned.

        Args:
            workspace_name: Workspace name
            tag_name: Tag marking the milestone
            reason: Why pruning

        Returns:
            Dict with pruned_count, skipped_tagged, and tag_version
        """
        from goldfish.errors import GoldfishError

        timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            # Get the tagged version
            tag_row = conn.execute(
                """
                SELECT version FROM workspace_version_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag_name),
            ).fetchone()

            if not tag_row:
                raise GoldfishError(
                    f"Tag '{tag_name}' not found in workspace '{workspace_name}'",
                    details={"workspace": workspace_name, "tag_name": tag_name},
                )

            tagged_version = tag_row["version"]
            tagged_num = self._parse_version_number(tagged_version)

            # 1. Count tagged versions before cutoff (excluding the cutoff itself)
            row = conn.execute(
                """
                SELECT COUNT(*) as count
                FROM workspace_versions v
                JOIN workspace_version_tags t ON v.workspace_name = t.workspace_name AND v.version = t.version
                WHERE v.workspace_name = ?
                AND v.pruned_at IS NULL
                AND CAST(SUBSTR(v.version, 2) AS INTEGER) < ?
                """,
                (workspace_name, tagged_num),
            ).fetchone()
            skipped_tagged = row["count"]

            # 2. Prune non-tagged versions before cutoff
            result = conn.execute(
                """
                UPDATE workspace_versions
                SET pruned_at = ?, prune_reason = ?
                WHERE workspace_name = ?
                AND pruned_at IS NULL
                AND CAST(SUBSTR(version, 2) AS INTEGER) < ?
                AND NOT EXISTS (
                    SELECT 1 FROM workspace_version_tags t
                    WHERE t.workspace_name = workspace_versions.workspace_name
                    AND t.version = workspace_versions.version
                )
                """,
                (timestamp, reason, workspace_name, tagged_num),
            )
            pruned_count = result.rowcount

        return {
            "workspace_name": workspace_name,
            "tag_name": tag_name,
            "tag_version": tagged_version,
            "pruned_count": pruned_count,
            "skipped_tagged": skipped_tagged,
        }

    def unprune_version(self, workspace_name: str, version: str) -> dict:
        """Restore a pruned version.

        Args:
            workspace_name: Workspace name
            version: Version to restore

        Returns:
            Dict with restored version info
        """
        from goldfish.errors import GoldfishError

        with self._conn() as conn:
            # Check version exists and is pruned
            version_row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ? AND version = ?
                """,
                (workspace_name, version),
            ).fetchone()

            if not version_row:
                raise GoldfishError(
                    f"Version '{version}' not found in workspace '{workspace_name}'",
                    details={"workspace": workspace_name, "version": version},
                )

            if version_row["pruned_at"] is None:
                raise GoldfishError(
                    f"Version '{version}' is not pruned",
                    details={"workspace": workspace_name, "version": version},
                )

            # Restore the version
            conn.execute(
                """
                UPDATE workspace_versions
                SET pruned_at = NULL, prune_reason = NULL
                WHERE workspace_name = ? AND version = ?
                """,
                (workspace_name, version),
            )

            return {
                "workspace_name": workspace_name,
                "version": version,
                "restored": True,
            }

    def unprune_versions(self, workspace_name: str, from_version: str, to_version: str) -> dict:
        """Restore a range of pruned versions.

        Args:
            workspace_name: Workspace name
            from_version: Start version (e.g., "v4")
            to_version: End version (e.g., "v6")

        Returns:
            Dict with unpruned_count
        """

        # Extract version numbers
        from_num = self._parse_version_number(from_version)
        to_num = self._parse_version_number(to_version)

        unpruned_count = 0

        with self._conn() as conn:
            # Get all pruned versions in range
            rows = conn.execute(
                """
                SELECT version FROM workspace_versions
                WHERE workspace_name = ?
                AND pruned_at IS NOT NULL
                """,
                (workspace_name,),
            ).fetchall()

            for row in rows:
                version = row["version"]
                try:
                    version_num = self._parse_version_number(version)
                except Exception:
                    continue

                if from_num <= version_num <= to_num:
                    conn.execute(
                        """
                        UPDATE workspace_versions
                        SET pruned_at = NULL, prune_reason = NULL
                        WHERE workspace_name = ? AND version = ?
                        """,
                        (workspace_name, version),
                    )
                    unpruned_count += 1

        return {
            "workspace_name": workspace_name,
            "from_version": from_version,
            "to_version": to_version,
            "unpruned_count": unpruned_count,
        }

    def get_pruned_count(self, workspace_name: str) -> int:
        """Get the count of pruned versions in a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            Number of pruned versions
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as count FROM workspace_versions
                WHERE workspace_name = ?
                AND pruned_at IS NOT NULL
                """,
                (workspace_name,),
            ).fetchone()
            count: int = row["count"]
            return count

    # --- Stage run operations ---

    def create_stage_run(
        self,
        stage_run_id: str,
        workspace_name: str,
        version: str,
        stage_name: str,
        pipeline_run_id: str | None = None,
        pipeline_name: str | None = None,
        job_id: str | None = None,
        profile: str | None = None,
        hints: dict | None = None,
        config: dict | None = None,
        inputs: dict | None = None,
        reason: dict | None = None,
        preflight_errors: list[str] | None = None,
        preflight_warnings: list[str] | None = None,
        backend_type: str | None = None,
        backend_handle: str | None = None,
    ) -> None:
        """Create a new stage run.

        Args:
            stage_run_id: Stage run ID
            workspace_name: Workspace name
            version: Workspace version
            stage_name: Stage name
            pipeline_run_id: Pipeline grouping ID
            pipeline_name: Named pipeline (train, inference, etc.)
            job_id: Parent job ID (legacy run_job)
            profile: Resolved profile name
            hints: Hint dict
            config: Full merged config (base + overrides, computed by caller)
            inputs: Resolved input URIs/refs
            reason: Structured RunReason dict (description, hypothesis, approach, etc.)
            preflight_errors: Preflight validation errors (if any)
            preflight_warnings: Preflight validation warnings (if any)
            backend_type: local|gce
            backend_handle: container_id or instance_name for cancel/logs
        """
        timestamp = datetime.now(UTC).isoformat()
        config_json = json.dumps(config) if config is not None else None
        hints_json = json.dumps(hints) if hints else None
        inputs_json = json.dumps(inputs) if inputs else None
        reason_json = json.dumps(reason) if reason else None
        preflight_errors_json = json.dumps(preflight_errors) if preflight_errors else None
        preflight_warnings_json = json.dumps(preflight_warnings) if preflight_warnings else None

        with self._conn() as conn:
            # Compute attempt_num: increment after a successful run, otherwise continue current attempt
            attempt_num = self._compute_attempt_num(conn, workspace_name, stage_name)

            # Initial state machine state: PREPARING with GCS_CHECK phase
            # NOTE: status column is deprecated - uses default value, only state matters
            from goldfish.state_machine.types import ProgressPhase, StageState

            initial_state = StageState.PREPARING
            initial_phase = ProgressPhase.GCS_CHECK.value

            conn.execute(
                """
                INSERT INTO stage_runs
                (id, job_id, pipeline_run_id, workspace_name, pipeline_name, version, stage_name,
                 started_at, profile, hints_json, config_json, inputs_json, reason_json,
                 preflight_errors_json, preflight_warnings_json,
                 backend_type, backend_handle, attempt_num,
                 state, phase, state_entered_at, phase_updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stage_run_id,
                    job_id,
                    pipeline_run_id,
                    workspace_name,
                    pipeline_name,
                    version,
                    stage_name,
                    timestamp,
                    profile,
                    hints_json,
                    config_json,
                    inputs_json,
                    reason_json,
                    preflight_errors_json,
                    preflight_warnings_json,
                    backend_type,
                    backend_handle,
                    attempt_num,
                    initial_state.value,
                    initial_phase,
                    timestamp,  # state_entered_at = same as started_at initially
                    timestamp,  # phase_updated_at = same as state_entered_at initially
                ),
            )

            # Record initial run_start pseudo-event for audit/provenance.
            # This makes the audit trail complete even before the first explicit transition.
            conn.execute(
                """
                INSERT INTO stage_state_transitions
                (stage_run_id, from_state, to_state, event,
                 phase, termination_cause, exit_code, exit_code_exists, error_message,
                 source, created_at)
                VALUES (?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?)
                """,
                (
                    stage_run_id,
                    "none",
                    initial_state,
                    "run_start",
                    initial_phase,
                    None,
                    None,
                    0,
                    None,
                    "executor",
                    timestamp,
                ),
            )

    def _compute_attempt_num(self, conn: sqlite3.Connection, workspace_name: str, stage_name: str) -> int:
        """Compute attempt number for a new stage run.

        Attempt groups consecutive runs on the same stage. A new attempt starts
        after a run is marked with outcome='success'.

        Args:
            conn: Database connection (already in transaction)
            workspace_name: Workspace name
            stage_name: Stage name

        Returns:
            Attempt number (1-based)
        """
        # Get the most recent run for this workspace/stage
        row = conn.execute(
            """
            SELECT attempt_num, outcome FROM stage_runs
            WHERE workspace_name = ? AND stage_name = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (workspace_name, stage_name),
        ).fetchone()

        if row is None:
            # First run for this stage
            return 1

        prev_attempt = row["attempt_num"] or 1
        prev_outcome = row["outcome"]

        # If previous run was successful, start a new attempt
        if prev_outcome == "success":
            return prev_attempt + 1

        # Otherwise continue the current attempt
        return prev_attempt

    def update_run_outcome(self, stage_run_id: str, outcome: str, note: str | None = None) -> bool:
        """Update the outcome of a stage run.

        Args:
            stage_run_id: Stage run ID
            outcome: 'success' or 'bad_results'
            note: Optional note about the outcome

        Returns:
            True if updated, False if run not found
        """
        from goldfish.state_machine.types import StageState

        if outcome not in ("success", "bad_results"):
            raise ValueError(f"Invalid outcome: {outcome}. Must be 'success' or 'bad_results'")

        with self._conn() as conn:
            # Only update completed runs (state machine is source of truth)
            result = conn.execute(
                """
                UPDATE stage_runs
                SET outcome = ?
                WHERE id = ? AND state = ?
                """,
                (outcome, stage_run_id, StageState.COMPLETED.value),
            )
            return result.rowcount > 0

    def list_attempts(
        self,
        workspace_name: str,
        stage_name: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """List attempts (grouped consecutive runs) for a workspace/stage.

        Args:
            workspace_name: Workspace name
            stage_name: Optional stage name filter
            limit: Max attempts to return

        Returns:
            List of attempt summaries with run counts and status
        """
        from goldfish.state_machine.types import StageState

        # Build query to group runs by attempt
        # Use state column (source of truth) for lifecycle counts
        query = """
            SELECT
                stage_name,
                attempt_num,
                COUNT(*) as run_count,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as completed_count,
                SUM(CASE WHEN state IN (?, ?, ?) THEN 1 ELSE 0 END) as failed_count,
                SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN outcome = 'bad_results' THEN 1 ELSE 0 END) as bad_results_count,
                MIN(version) as first_version,
                MAX(version) as last_version,
                MIN(started_at) as started_at,
                MAX(completed_at) as ended_at
            FROM stage_runs
            WHERE workspace_name = ?
        """
        params: list = [
            StageState.COMPLETED.value,
            StageState.FAILED.value,
            StageState.TERMINATED.value,
            StageState.CANCELED.value,
            workspace_name,
        ]

        if stage_name:
            query += " AND stage_name = ?"
            params.append(stage_name)

        query += """
            GROUP BY stage_name, attempt_num
            ORDER BY stage_name, attempt_num DESC
            LIMIT ?
        """
        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

            attempts = []
            for row in rows:
                # Determine attempt status
                if row["success_count"] > 0:
                    status = "closed"  # Has a successful run
                elif row["run_count"] == row["failed_count"]:
                    status = "all_failed"  # All runs crashed
                else:
                    status = "open"  # Still iterating

                attempts.append(
                    {
                        "stage": row["stage_name"],
                        "attempt": row["attempt_num"],
                        "runs": row["run_count"],
                        "completed": row["completed_count"],
                        "failed": row["failed_count"],
                        "success": row["success_count"],
                        "bad_results": row["bad_results_count"],
                        "versions": f"{row['first_version']}→{row['last_version']}"
                        if row["first_version"] != row["last_version"]
                        else row["first_version"],
                        "started": row["started_at"][:19] if row["started_at"] else None,
                        "ended": row["ended_at"][:19] if row["ended_at"] else None,
                        "status": status,
                    }
                )

            return attempts

    def get_attempt_context(self, workspace_name: str, stage_name: str, attempt_num: int) -> dict | None:
        """Get context about a specific attempt.

        Args:
            workspace_name: Workspace name
            stage_name: Stage name
            attempt_num: Attempt number to get context for

        Returns:
            Dict with attempt summary or None if not found
        """
        from goldfish.state_machine.types import StageState

        # Use state column (source of truth) for statistics
        query = """
            SELECT
                COUNT(*) as run_count,
                SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as completed_count,
                SUM(CASE WHEN state IN (?, ?, ?) THEN 1 ELSE 0 END) as failed_count,
                SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN outcome = 'bad_results' THEN 1 ELSE 0 END) as bad_results_count
            FROM stage_runs
            WHERE workspace_name = ?
            AND stage_name = ?
            AND attempt_num = ?
        """
        params: list = [
            StageState.COMPLETED.value,
            StageState.FAILED.value,
            StageState.TERMINATED.value,
            StageState.CANCELED.value,
            workspace_name,
            stage_name,
            attempt_num,
        ]

        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
            if not row or row["run_count"] == 0:
                return None

            # Determine attempt status
            if row["success_count"] > 0:
                status = "closed"  # Has a successful run
            elif row["run_count"] == row["failed_count"]:
                status = "all_failed"  # All runs crashed
            else:
                status = "open"  # Still iterating

            return {
                "attempt": attempt_num,
                "runs_in_attempt": row["run_count"],
                "completed": row["completed_count"],
                "failed": row["failed_count"],
                "success": row["success_count"],
                "bad_results": row["bad_results_count"],
                "status": status,
            }

    def update_stage_run_status(
        self,
        stage_run_id: str,
        completed_at: str | None = None,
        log_uri: str | None = None,
        artifact_uri: str | None = None,
        outputs_json: dict | None = None,
        error: str | None = None,
        progress: str | None = None,
    ) -> None:
        """Update stage run metadata fields.

        Note: The state machine is now the source of truth for run state.
        Use transition() from goldfish.state_machine for state changes.
        This method updates auxiliary fields like completed_at, log_uri, etc.

        Args:
            stage_run_id: Stage run ID
            completed_at: Completion timestamp
            log_uri: Log file location
            artifact_uri: Artifact URI if produced
            outputs_json: Outputs map
            error: Error message if failed
            progress: User-facing training progress string (e.g., "Epoch 10/10")
        """
        fields: list[str] = []
        params: list = []

        if completed_at is not None:
            fields.append("completed_at = ?")
            params.append(completed_at)
        if log_uri is not None:
            fields.append("log_uri = ?")
            params.append(log_uri)
        if artifact_uri is not None:
            fields.append("artifact_uri = ?")
            params.append(artifact_uri)
        if outputs_json is not None:
            fields.append("outputs_json = ?")
            params.append(json.dumps(outputs_json))
        if error is not None:
            fields.append("error = ?")
            params.append(error)
        if progress is not None:
            fields.append("progress = ?")
            params.append(progress)

        if not fields:
            return  # Nothing to update

        params.append(stage_run_id)
        query = f"UPDATE stage_runs SET {', '.join(fields)} WHERE id = ?"

        with self._conn() as conn:
            conn.execute(query, params)

    def update_stage_run_outcome(self, stage_run_id: str, outcome: str) -> None:
        """Update stage run outcome.

        Args:
            stage_run_id: Stage run ID
            outcome: New outcome (success, bad_results)
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET outcome = ? WHERE id = ?",
                (outcome, stage_run_id),
            )

    def update_stage_run_gcs_outage(self, stage_run_id: str, gcs_outage_started: str | None) -> None:
        """Update stage run GCS outage start time.

        Used by the state machine to track GCS outages. When GCS becomes unavailable
        while checking for exit codes, the outage start time is recorded. After 1 hour,
        the run is transitioned to a terminal state.

        Args:
            stage_run_id: Stage run ID.
            gcs_outage_started: ISO timestamp when outage started, or None to clear.
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET gcs_outage_started = ? WHERE id = ?",
                (gcs_outage_started, stage_run_id),
            )

    def get_stage_run(self, stage_run_id: str) -> dict | None:
        """Get a stage run by ID.

        Args:
            stage_run_id: Stage run ID

        Returns:
            Stage run dict or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM stage_runs WHERE id = ?",
                (stage_run_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_stage_runs(
        self,
        workspace_name: str | None = None,
        stage_name: str | None = None,
        state: str | None = None,
        outcome: str | None = None,
        pipeline_run_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List stage runs with optional filters.

        Args:
            workspace_name: Filter by workspace
            stage_name: Filter by stage
            state: Filter by state (source of truth for lifecycle)
            outcome: Filter by outcome (e.g., 'success', 'bad_results')
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of stage run dicts
        """
        query = "SELECT * FROM stage_runs WHERE 1=1"
        params: list = []

        if workspace_name:
            query += " AND workspace_name = ?"
            params.append(workspace_name)
        if stage_name:
            query += " AND stage_name = ?"
            params.append(stage_name)
        if pipeline_run_id:
            query += " AND pipeline_run_id = ?"
            params.append(pipeline_run_id)
        if state:
            query += " AND state = ?"
            params.append(state)
        if outcome:
            query += " AND outcome = ?"
            params.append(outcome)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def list_stage_runs_with_total(
        self,
        workspace_name: str | None = None,
        stage_name: str | None = None,
        state: str | None = None,
        outcome: str | None = None,
        pipeline_run_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List stage runs plus total_count using a window function (single query)."""
        query = "SELECT *, COUNT(*) OVER() AS total_count FROM stage_runs WHERE 1=1"
        params: list = []

        if workspace_name:
            query += " AND workspace_name = ?"
            params.append(workspace_name)
        if stage_name:
            query += " AND stage_name = ?"
            params.append(stage_name)
        if pipeline_run_id:
            query += " AND pipeline_run_id = ?"
            params.append(pipeline_run_id)
        if state:
            query += " AND state = ?"
            params.append(state)
        if outcome:
            query += " AND outcome = ?"
            params.append(outcome)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def count_stage_runs(
        self,
        workspace_name: str | None = None,
        stage_name: str | None = None,
        state: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> int:
        """Count stage runs for pagination."""
        query = "SELECT COUNT(*) FROM stage_runs WHERE 1=1"
        params: list = []

        if workspace_name:
            query += " AND workspace_name = ?"
            params.append(workspace_name)
        if stage_name:
            query += " AND stage_name = ?"
            params.append(stage_name)
        if pipeline_run_id:
            query += " AND pipeline_run_id = ?"
            params.append(pipeline_run_id)
        if state:
            query += " AND state = ?"
            params.append(state)

        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
            count: int = row[0] if row else 0
            return count

    # --- Dashboard methods ---

    def get_recent_failed_runs(self, limit: int = 10) -> list[dict]:
        """Get recently failed stage runs across all workspaces.

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of failed runs with workspace, stage, error, and timestamp
        """
        from goldfish.state_machine.types import StageState

        # Terminal failure states (state machine is source of truth)
        failed_states = (
            StageState.FAILED.value,
            StageState.TERMINATED.value,
        )
        placeholders = ", ".join("?" for _ in failed_states)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, workspace_name, stage_name, state, error, completed_at, reason_json
                FROM stage_runs
                WHERE state IN ({placeholders})
                ORDER BY completed_at DESC
                LIMIT ?
                """,
                (*failed_states, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_active_runs(self) -> list[dict]:
        """Get all currently active (non-terminal) stage runs.

        Returns:
            List of active runs with state info
        """
        from goldfish.state_machine.types import StageState

        # Active states (non-terminal) - v1.2
        active_states = (
            StageState.PREPARING.value,
            StageState.BUILDING.value,
            StageState.LAUNCHING.value,
            StageState.RUNNING.value,
            StageState.POST_RUN.value,
            StageState.AWAITING_USER_FINALIZATION.value,
        )
        placeholders = ", ".join("?" for _ in active_states)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, workspace_name, stage_name, state, started_at, reason_json
                FROM stage_runs
                WHERE state IN ({placeholders})
                ORDER BY started_at DESC
                """,
                active_states,
            ).fetchall()
            return [dict(row) for row in rows]

    def get_recent_outcomes(self, limit: int = 10) -> list[dict]:
        """Get recent run outcomes for trend visibility.

        Args:
            limit: Maximum number of outcomes to return

        Returns:
            List of recent outcomes (success/bad_results) with metadata
        """
        from goldfish.state_machine.types import StageState

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT workspace_name, stage_name, outcome, completed_at
                FROM stage_runs
                WHERE outcome IS NOT NULL
                AND state = ?
                ORDER BY completed_at DESC
                LIMIT ?
                """,
                (StageState.COMPLETED.value, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_all_stage_runs(
        self,
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List stage runs across ALL workspaces (newest first).

        Args:
            state: Optional state filter (state machine is source of truth)
            limit: Maximum runs to return
            offset: Pagination offset

        Returns:
            List of runs with total_count included via window function
        """
        query = "SELECT *, COUNT(*) OVER() AS total_count FROM stage_runs WHERE 1=1"
        params: list = []

        if state:
            query += " AND state = ?"
            params.append(state)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def get_queued_stages_for_pipeline(self, pipeline_run_id: str) -> list[dict]:
        """Get queued stages from pipeline_stage_queue that don't have stage_runs yet.

        Returns queue entries that are pending/running but haven't created a stage_run record.
        These represent stages that are queued but not yet processing.

        Args:
            pipeline_run_id: Pipeline run ID to filter by

        Returns:
            List of dicts with stage info for display in list_runs
        """
        with self._conn() as conn:
            # Get queue entries that don't have a corresponding stage_run yet
            rows = conn.execute(
                """
                SELECT
                    q.id,
                    q.pipeline_run_id,
                    q.stage_name,
                    q.status,
                    p.workspace_name,
                    p.pipeline_name,
                    p.started_at
                FROM pipeline_stage_queue q
                JOIN pipeline_runs p ON p.id = q.pipeline_run_id
                WHERE q.pipeline_run_id = ?
                AND q.stage_run_id IS NULL
                AND q.status IN (?, ?)
                ORDER BY q.id
                """,
                (pipeline_run_id, PipelineStatus.PENDING, PipelineStatus.RUNNING),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_pipeline_run_status(self, pipeline_run_id: str) -> dict | None:
        """Get detailed status of a pipeline run including queue state.

        Args:
            pipeline_run_id: Pipeline run ID

        Returns:
            Dict with pipeline run info and queue status, or None if not found
        """
        with self._conn() as conn:
            # Get pipeline run
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE id = ?",
                (pipeline_run_id,),
            ).fetchone()
            if not row:
                return None
            prun = dict(row)

            # Get queue entries - use SELECT * to handle schema variations
            queue = conn.execute(
                """
                SELECT *
                FROM pipeline_stage_queue
                WHERE pipeline_run_id = ?
                ORDER BY id
                """,
                (pipeline_run_id,),
            ).fetchall()

            return {
                "pipeline_run_id": prun["id"],
                "workspace": prun["workspace_name"],
                "pipeline": prun["pipeline_name"],
                "status": prun["status"],
                "started_at": prun["started_at"],
                "completed_at": prun.get("completed_at"),
                "error": prun.get("error"),
                "queue": [dict(q) for q in queue],
            }

    def get_latest_stage_run(
        self,
        workspace_name: str,
        stage_name: str,
        status: str | None = None,
    ) -> dict | None:
        """Get the most recent stage run for a workspace and stage.

        Args:
            workspace_name: Workspace name
            stage_name: Stage name
            status: Filter by status (e.g., "completed")

        Returns:
            Latest stage run dict or None if not found
        """
        query = """
            SELECT * FROM stage_runs
            WHERE workspace_name = ? AND stage_name = ?
        """
        params = [workspace_name, stage_name]

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY started_at DESC LIMIT 1"

        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    # --- Signal lineage operations ---

    def add_signal(
        self,
        stage_run_id: str,
        signal_name: str,
        signal_type: str,
        storage_location: str,
        size_bytes: int | None = None,
        is_artifact: bool = False,
    ) -> None:
        """Add a signal produced by a stage run.

        Args:
            stage_run_id: Stage run ID that produced this signal
            signal_name: Signal name (e.g., "tokens", "features")
            signal_type: Signal type (npy, csv, directory, file, dataset)
            storage_location: Where the signal is stored (GCS path, local path)
            size_bytes: Size in bytes
            is_artifact: Whether this is a permanent artifact
        """
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO signal_lineage
                (stage_run_id, signal_name, signal_type, storage_location, size_bytes, is_artifact)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (stage_run_id, signal_name, signal_type, storage_location, size_bytes, is_artifact),
            )

    def add_signal_with_source(
        self,
        stage_run_id: str,
        signal_name: str,
        signal_type: str,
        storage_location: str,
        source_stage_run_id: str | None = None,
        source_stage_version_id: int | None = None,
        size_bytes: int | None = None,
        is_artifact: bool = False,
    ) -> None:
        """Add a signal with upstream source tracking.

        Args:
            stage_run_id: Stage run ID that has this signal as input
            signal_name: Signal name (e.g., "features", "tokens")
            signal_type: Signal type (input, npy, csv, etc.)
            storage_location: Where the signal is stored
            source_stage_run_id: Upstream stage run that produced this input
            source_stage_version_id: Upstream stage version for lineage tracking
            size_bytes: Size in bytes
            is_artifact: Whether this is a permanent artifact
        """
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO signal_lineage
                (stage_run_id, signal_name, signal_type, storage_location,
                 source_stage_run_id, source_stage_version_id, size_bytes, is_artifact)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stage_run_id,
                    signal_name,
                    signal_type,
                    storage_location,
                    source_stage_run_id,
                    source_stage_version_id,
                    size_bytes,
                    is_artifact,
                ),
            )

    def set_stage_run_backend(
        self,
        stage_run_id: str,
        backend_type: str,
        backend_handle: str,
        instance_zone: str | None = None,
    ) -> None:
        """Persist backend info for cancellation/logging.

        Args:
            stage_run_id: Stage run ID
            backend_type: Backend type (local or gce)
            backend_handle: Container ID or instance name
            instance_zone: GCE zone where instance was launched (None for local)
        """
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE stage_runs
                SET backend_type = ?, backend_handle = ?, instance_zone = ?
                WHERE id = ?
                """,
                (backend_type, backend_handle, instance_zone, stage_run_id),
            )

    def get_signal(self, stage_run_id: str, signal_name: str) -> dict | None:
        """Get a specific signal.

        Args:
            stage_run_id: Stage run ID
            signal_name: Signal name

        Returns:
            Signal dict or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM signal_lineage
                WHERE stage_run_id = ? AND signal_name = ?
                """,
                (stage_run_id, signal_name),
            ).fetchone()
            return dict(row) if row else None

    def list_signals(
        self,
        stage_run_id: str | None = None,
        consumed_by: str | None = None,
        source_stage_run_id: str | None = None,
        is_artifact: bool | None = None,
        signal_type: str | None = None,
    ) -> list[dict]:
        """List signals with optional filters.

        Args:
            stage_run_id: Filter by producing stage run
            consumed_by: Filter by consuming stage run
            source_stage_run_id: Filter by source stage run (for downstream tracking)
            is_artifact: Filter by artifact status
            signal_type: Filter by signal type (e.g., 'input', 'npy')

        Returns:
            List of signal dicts
        """
        query = "SELECT * FROM signal_lineage WHERE 1=1"
        params: list = []

        if stage_run_id:
            query += " AND stage_run_id = ?"
            params.append(stage_run_id)
        if consumed_by:
            query += " AND consumed_by = ?"
            params.append(consumed_by)
        if source_stage_run_id:
            query += " AND source_stage_run_id = ?"
            params.append(source_stage_run_id)
        if is_artifact is not None:
            query += " AND is_artifact = ?"
            params.append(1 if is_artifact else 0)
        if signal_type:
            query += " AND signal_type = ?"
            params.append(signal_type)

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def list_inputs_for_runs(self, run_ids: list[str]) -> dict[str, list[dict]]:
        """Efficiently fetch input signal info for a list of stage runs.

        Returns:
            Dict mapping run_id to list of input signal records.
        """
        if not run_ids:
            return {}

        placeholders = ",".join(["?"] * len(run_ids))
        query = f"""
            SELECT stage_run_id, signal_name, signal_type, storage_location,
                   source_stage_run_id, source_stage_version_id
            FROM signal_lineage
            WHERE stage_run_id IN ({placeholders})
            AND signal_type = 'input'
        """

        result: dict[str, list[dict]] = {run_id: [] for run_id in run_ids}
        with self._conn() as conn:
            rows = conn.execute(query, run_ids).fetchall()
            for row in rows:
                run_id = row["stage_run_id"]
                if run_id in result:
                    result[run_id].append(dict(row))
        return result

    def mark_signal_consumed(
        self,
        stage_run_id: str,
        signal_name: str,
        consumed_by: str,
    ) -> None:
        """Mark a signal as consumed by a stage run.

        Args:
            stage_run_id: Stage run ID that produced the signal
            signal_name: Signal name
            consumed_by: Stage run ID that consumed it
        """
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE signal_lineage
                SET consumed_by = ?
                WHERE stage_run_id = ? AND signal_name = ?
                """,
                (consumed_by, stage_run_id, signal_name),
            )

    # ==================== Stage Version Methods ====================

    def get_or_create_stage_version(
        self,
        workspace: str,
        stage: str,
        git_sha: str,
        config_hash: str,
    ) -> tuple[int, int, bool]:
        """Get existing or create new stage version.

        A stage version is a unique combination of (workspace, stage, git_sha, config_hash).
        Version numbers auto-increment per stage within a workspace.

        Uses transaction with retry to handle concurrent creation.

        Args:
            workspace: Workspace name
            stage: Stage name
            git_sha: Git commit SHA
            config_hash: SHA256 hash of stage config

        Returns:
            Tuple of (stage_version_id, version_num, is_new)
        """
        with self._conn() as conn:
            # Try to find existing
            row = conn.execute(
                """
                SELECT id, version_num FROM stage_versions
                WHERE workspace_name = ? AND stage_name = ?
                AND git_sha = ? AND config_hash = ?
                """,
                (workspace, stage, git_sha, config_hash),
            ).fetchone()

            if row:
                return (row["id"], row["version_num"], False)

            # Create new - get next version number
            max_row = conn.execute(
                """
                SELECT COALESCE(MAX(version_num), 0) as max_v FROM stage_versions
                WHERE workspace_name = ? AND stage_name = ?
                """,
                (workspace, stage),
            ).fetchone()
            next_version = max_row["max_v"] + 1

            try:
                cursor = conn.execute(
                    """
                    INSERT INTO stage_versions
                    (workspace_name, stage_name, version_num, git_sha, config_hash)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (workspace, stage, next_version, git_sha, config_hash),
                )
                row_id = cursor.lastrowid
                assert row_id is not None  # Always set after INSERT
                return (row_id, next_version, True)
            except sqlite3.IntegrityError:
                # Race condition - another process created it, fetch again
                row = conn.execute(
                    """
                    SELECT id, version_num FROM stage_versions
                    WHERE workspace_name = ? AND stage_name = ?
                    AND git_sha = ? AND config_hash = ?
                    """,
                    (workspace, stage, git_sha, config_hash),
                ).fetchone()
                return (row["id"], row["version_num"], False)

    def get_stage_version(
        self,
        workspace: str,
        stage: str,
        version_num: int,
    ) -> StageVersionRow | None:
        """Get a specific stage version by number.

        Args:
            workspace: Workspace name
            stage: Stage name
            version_num: Version number (1, 2, 3, ...)

        Returns:
            StageVersionRow or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM stage_versions
                WHERE workspace_name = ? AND stage_name = ? AND version_num = ?
                """,
                (workspace, stage, version_num),
            ).fetchone()
            return cast(StageVersionRow, dict(row)) if row else None

    def list_stage_versions(
        self,
        workspace: str,
        stage: str | None = None,
    ) -> list[StageVersionRow]:
        """List all stage versions for a workspace.

        Args:
            workspace: Workspace name
            stage: Optional stage name filter

        Returns:
            List of StageVersionRow
        """
        with self._conn() as conn:
            if stage:
                rows = conn.execute(
                    """
                    SELECT * FROM stage_versions
                    WHERE workspace_name = ? AND stage_name = ?
                    ORDER BY stage_name, version_num
                    """,
                    (workspace, stage),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM stage_versions
                    WHERE workspace_name = ?
                    ORDER BY stage_name, version_num
                    """,
                    (workspace,),
                ).fetchall()
            return [cast(StageVersionRow, dict(row)) for row in rows]

    def get_stage_version_for_run(self, stage_run_id: str) -> StageVersionRow | None:
        """Get the stage version associated with a stage run.

        Args:
            stage_run_id: Stage run ID

        Returns:
            StageVersionRow or None if not linked
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT sv.* FROM stage_versions sv
                JOIN stage_runs sr ON sr.stage_version_id = sv.id
                WHERE sr.id = ?
                """,
                (stage_run_id,),
            ).fetchone()
            return cast(StageVersionRow, dict(row)) if row else None

    def update_stage_run_version(
        self,
        stage_run_id: str,
        stage_version_id: int,
    ) -> None:
        """Link a stage run to its stage version.

        Args:
            stage_run_id: Stage run ID
            stage_version_id: Stage version ID
        """
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE stage_runs
                SET stage_version_id = ?
                WHERE id = ?
                """,
                (stage_version_id, stage_run_id),
            )

    # --- Lineage Query Methods ---

    def get_latest_completed_stage_run(
        self,
        workspace: str,
        stage_name: str,
    ) -> dict | None:
        """Get the most recent completed run of a stage in a workspace.

        Args:
            workspace: Workspace name
            stage_name: Stage name

        Returns:
            Stage run dict or None if no completed runs exist
        """
        from goldfish.state_machine.types import StageState

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM stage_runs
                WHERE workspace_name = ? AND stage_name = ? AND state = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (workspace, stage_name, StageState.COMPLETED.value),
            ).fetchone()
            return dict(row) if row else None

    def get_downstream_runs(self, stage_version_id: int) -> list[dict]:
        """Find all stage runs that used this stage version as input.

        Args:
            stage_version_id: Stage version ID

        Returns:
            List of stage run dicts that consumed this version
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT sr.* FROM stage_runs sr
                JOIN signal_lineage sl ON sl.stage_run_id = sr.id
                WHERE sl.source_stage_version_id = ?
                ORDER BY sr.started_at DESC
                """,
                (stage_version_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_lineage_tree(
        self,
        stage_run_id: str,
        max_depth: int = 10,
    ) -> dict | None:
        """Build upstream lineage tree recursively.

        Shows what version of each stage produced the inputs,
        recursively back to source datasets.

        Args:
            stage_run_id: Stage run ID to trace lineage for
            max_depth: Maximum recursion depth (default 10)

        Returns:
            Nested lineage tree dict, or None if run not found
        """
        return self._build_lineage_node(stage_run_id, max_depth, current_depth=0)

    def _build_lineage_node(
        self,
        stage_run_id: str,
        max_depth: int,
        current_depth: int,
    ) -> dict | None:
        """Recursively build a lineage tree node."""
        with self._conn() as conn:
            # Get the stage run
            run_row = conn.execute(
                "SELECT * FROM stage_runs WHERE id = ?",
                (stage_run_id,),
            ).fetchone()

            if not run_row:
                return None

            run = dict(run_row)

            # Get stage version info if available
            stage_version_num = None
            git_sha = None
            config_hash = None

            if run.get("stage_version_id"):
                sv_row = conn.execute(
                    "SELECT * FROM stage_versions WHERE id = ?",
                    (run["stage_version_id"],),
                ).fetchone()
                if sv_row:
                    sv = dict(sv_row)
                    stage_version_num = sv["version_num"]
                    git_sha = sv["git_sha"]
                    config_hash = sv["config_hash"]

            # Build node
            node: dict = {
                "run_id": stage_run_id,
                "stage": run["stage_name"],
                "stage_version_num": stage_version_num,
                "git_sha": git_sha,
                "config_hash": config_hash,
                "inputs": {},
            }

            # Get input signals with source tracking
            signal_rows = conn.execute(
                """
                SELECT * FROM signal_lineage
                WHERE stage_run_id = ? AND source_stage_run_id IS NOT NULL
                """,
                (stage_run_id,),
            ).fetchall()

            for signal_row in signal_rows:
                signal = dict(signal_row)
                signal_name = signal["signal_name"]
                source_run_id = signal["source_stage_run_id"]
                source_version_id = signal["source_stage_version_id"]

                # Get source stage info
                source_stage = None
                source_version_num = None

                if source_run_id:
                    source_run_row = conn.execute(
                        "SELECT stage_name FROM stage_runs WHERE id = ?",
                        (source_run_id,),
                    ).fetchone()
                    if source_run_row:
                        source_stage = source_run_row["stage_name"]

                if source_version_id:
                    source_sv_row = conn.execute(
                        "SELECT version_num FROM stage_versions WHERE id = ?",
                        (source_version_id,),
                    ).fetchone()
                    if source_sv_row:
                        source_version_num = source_sv_row["version_num"]

                input_info: dict = {
                    "source_type": "stage",
                    "source_stage": source_stage,
                    "source_stage_run_id": source_run_id,
                    "source_stage_version_num": source_version_num,
                    "storage_location": signal["storage_location"],
                }

                # Recursively build upstream if within depth limit
                if current_depth < max_depth - 1 and source_run_id:
                    upstream = self._build_lineage_node(source_run_id, max_depth, current_depth + 1)
                    if upstream:
                        input_info["upstream"] = upstream

                node["inputs"][signal_name] = input_info

            return node

    # =========================================================================
    # Metrics CRUD
    # =========================================================================

    def insert_metric(
        self,
        stage_run_id: str,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: str | None = None,
    ) -> None:
        """Insert a metric data point and update summary."""
        from datetime import UTC, datetime

        if timestamp is None:
            timestamp = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_metrics (stage_run_id, name, value, step, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (stage_run_id, name, value, step, timestamp),
            )

            # Also update summary (keeps summary in sync with detail table)
            conn.execute(
                """
                INSERT INTO run_metrics_summary (
                    stage_run_id,
                    name,
                    min_value,
                    max_value,
                    last_value,
                    last_timestamp,
                    count
                )
                VALUES (?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(stage_run_id, name) DO UPDATE SET
                    min_value = CASE WHEN excluded.min_value < min_value THEN excluded.min_value ELSE min_value END,
                    max_value = CASE WHEN excluded.max_value > max_value THEN excluded.max_value ELSE max_value END,
                    last_value = CASE
                        WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                        THEN excluded.last_value
                        ELSE last_value
                    END,
                    last_timestamp = CASE
                        WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                        THEN excluded.last_timestamp
                        ELSE last_timestamp
                    END,
                    count = count + 1
                """,
                (stage_run_id, name, value, value, value, timestamp),
            )

    def _batch_insert_metrics_conn(
        self,
        conn: sqlite3.Connection,
        stage_run_id: str,
        metrics: list[dict],
        update_summary: bool = True,
    ) -> int:
        """Insert multiple metrics using an existing connection.

        Returns number of inserted rows (duplicates ignored).
        """
        from datetime import UTC, datetime

        from goldfish.validation import validate_batch_size

        if not metrics:
            return 0

        validate_batch_size(len(metrics))

        now = datetime.now(UTC).isoformat()
        metric_data = [
            (
                stage_run_id,
                m["name"],
                m["value"],
                m.get("step"),
                m.get("timestamp") or now,
            )
            for m in metrics
        ]

        last_id_row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM run_metrics WHERE stage_run_id = ?",
            (stage_run_id,),
        ).fetchone()
        last_id = int(last_id_row[0]) if last_id_row else 0

        before_changes = conn.total_changes

        conn.executemany(
            """
            INSERT OR IGNORE INTO run_metrics (stage_run_id, name, value, step, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            metric_data,
        )

        inserted = conn.total_changes - before_changes

        if update_summary and inserted > 0:
            rows = conn.execute(
                """
                SELECT name, value, timestamp
                FROM run_metrics
                WHERE stage_run_id = ? AND id > ?
                ORDER BY id ASC
                """,
                (stage_run_id, last_id),
            ).fetchall()
            self._upsert_metrics_summary_batch_conn(conn, stage_run_id, rows)

        return inserted

    def _upsert_metrics_summary_batch_conn(
        self,
        conn: sqlite3.Connection,
        stage_run_id: str,
        rows: list[sqlite3.Row],
    ) -> None:
        """Incrementally upsert summary rows based on newly inserted metrics."""
        if not rows:
            return

        class _SummaryStats(TypedDict):
            min_value: float
            max_value: float
            count: int
            last_timestamp: str
            last_value: float

        summary: dict[str, _SummaryStats] = {}
        for row in rows:
            name = row["name"]
            value = float(row["value"])
            timestamp = cast(str, row["timestamp"])
            entry = summary.get(name)
            if entry is None:
                summary[name] = {
                    "min_value": value,
                    "max_value": value,
                    "count": 1,
                    "last_timestamp": timestamp,
                    "last_value": value,
                }
                continue
            entry["min_value"] = min(entry["min_value"], value)
            entry["max_value"] = max(entry["max_value"], value)
            entry["count"] = int(entry["count"]) + 1
            last_ts = entry["last_timestamp"]
            if last_ts is None or timestamp >= last_ts:
                entry["last_timestamp"] = timestamp
                entry["last_value"] = value

        payload = [
            (
                stage_run_id,
                name,
                data["min_value"],
                data["max_value"],
                data["last_value"],
                data["last_timestamp"],
                data["count"],
            )
            for name, data in summary.items()
        ]

        conn.executemany(
            """
            INSERT INTO run_metrics_summary (
                stage_run_id,
                name,
                min_value,
                max_value,
                last_value,
                last_timestamp,
                count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stage_run_id, name) DO UPDATE SET
                min_value = CASE WHEN excluded.min_value < min_value THEN excluded.min_value ELSE min_value END,
                max_value = CASE WHEN excluded.max_value > max_value THEN excluded.max_value ELSE max_value END,
                last_value = CASE
                    WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                    THEN excluded.last_value
                    ELSE last_value
                END,
                last_timestamp = CASE
                    WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                    THEN excluded.last_timestamp
                    ELSE last_timestamp
                END,
                count = count + excluded.count
            """,
            payload,
        )

    def batch_insert_metrics(
        self,
        stage_run_id: str,
        metrics: list[dict],
        update_summary: bool = True,
    ) -> None:
        """Insert multiple metrics in a single transaction using executemany.

        Args:
            stage_run_id: Stage run ID
            metrics: List of metric dicts with keys: name, value, step, timestamp
            update_summary: If True, rebuild summaries for affected metric names

        Raises:
            InvalidBatchSizeError: If batch size exceeds 10,000 items
        """

        # Empty batch is a no-op
        if not metrics:
            return

        with self._conn() as conn:
            self._batch_insert_metrics_conn(conn, stage_run_id, metrics, update_summary=update_summary)

    def _rebuild_metrics_summary_for_names_conn(
        self,
        conn: sqlite3.Connection,
        stage_run_id: str,
        metric_names: set[str],
    ) -> None:
        """Rebuild summaries for a subset of metric names (chunked)."""
        if not metric_names:
            return

        names = sorted(metric_names)
        chunk_size = 500  # Stay under SQLite variable limit

        for i in range(0, len(names), chunk_size):
            chunk = names[i : i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            conn.execute(
                f"DELETE FROM run_metrics_summary WHERE stage_run_id = ? AND name IN ({placeholders})",
                [stage_run_id, *chunk],
            )
            conn.execute(
                f"""
                INSERT INTO run_metrics_summary (
                    stage_run_id,
                    name,
                    min_value,
                    max_value,
                    last_value,
                    last_timestamp,
                    count
                )
                WITH ranked AS (
                    SELECT
                        stage_run_id,
                        name,
                        value,
                        timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY stage_run_id, name
                            ORDER BY timestamp DESC, id DESC
                        ) as rn
                    FROM run_metrics
                    WHERE stage_run_id = ? AND name IN ({placeholders})
                )
                SELECT
                    stage_run_id,
                    name,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    MAX(CASE WHEN rn = 1 THEN value END) as last_value,
                    MAX(CASE WHEN rn = 1 THEN timestamp END) as last_timestamp,
                    COUNT(*) as count
                FROM ranked
                GROUP BY stage_run_id, name
                """,
                [stage_run_id, *chunk],
            )

    def _rebuild_metrics_summary_conn(self, conn: sqlite3.Connection, stage_run_id: str) -> None:
        """Rebuild summary for all metrics in a stage run (single pass)."""
        conn.execute("DELETE FROM run_metrics_summary WHERE stage_run_id = ?", (stage_run_id,))
        conn.execute(
            """
            INSERT INTO run_metrics_summary (
                stage_run_id,
                name,
                min_value,
                max_value,
                last_value,
                last_timestamp,
                count
            )
            WITH ranked AS (
                SELECT
                    stage_run_id,
                    name,
                    value,
                    timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY stage_run_id, name
                        ORDER BY timestamp DESC, id DESC
                    ) as rn
                FROM run_metrics
                WHERE stage_run_id = ?
            )
            SELECT
                stage_run_id,
                name,
                MIN(value) as min_value,
                MAX(value) as max_value,
                MAX(CASE WHEN rn = 1 THEN value END) as last_value,
                MAX(CASE WHEN rn = 1 THEN timestamp END) as last_timestamp,
                COUNT(*) as count
            FROM ranked
            GROUP BY stage_run_id, name
            """,
            (stage_run_id,),
        )

    def rebuild_metrics_summary(self, stage_run_id: str) -> None:
        """Rebuild metrics summary for a stage run."""
        with self._conn() as conn:
            self._rebuild_metrics_summary_conn(conn, stage_run_id)

    def upsert_metric_summary(
        self,
        stage_run_id: str,
        name: str,
        value: float,
        timestamp: str | None = None,
    ) -> None:
        """Update or insert a metric summary (min, max, last, count).

        Uses CASE WHEN for correct min/max comparison (MIN/MAX aggregates
        don't work in ON CONFLICT UPDATE clause).
        """
        from datetime import UTC, datetime

        if timestamp is None:
            timestamp = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_metrics_summary (
                    stage_run_id,
                    name,
                    min_value,
                    max_value,
                    last_value,
                    last_timestamp,
                    count
                )
                VALUES (?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(stage_run_id, name) DO UPDATE SET
                    min_value = CASE WHEN excluded.min_value < min_value THEN excluded.min_value ELSE min_value END,
                    max_value = CASE WHEN excluded.max_value > max_value THEN excluded.max_value ELSE max_value END,
                    last_value = CASE
                        WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                        THEN excluded.last_value
                        ELSE last_value
                    END,
                    last_timestamp = CASE
                        WHEN last_timestamp IS NULL OR excluded.last_timestamp >= last_timestamp
                        THEN excluded.last_timestamp
                        ELSE last_timestamp
                    END,
                    count = count + 1
                """,
                (stage_run_id, name, value, value, value, timestamp),
            )

    def get_run_metrics(
        self,
        stage_run_id: str,
        metric_name: str | None = None,
        metric_prefix: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[MetricRow]:
        """Get metrics for a stage run with optional filtering and SQL-level pagination.

        Args:
            stage_run_id: Stage run ID to filter by
            metric_name: Optional metric name to filter by
            limit: Maximum number of results to return (SQL LIMIT)
            offset: Number of results to skip (SQL OFFSET)

        Returns:
            List of MetricRow dicts
        """
        from goldfish.db.types import MetricRow

        with self._conn() as conn:
            # Build query dynamically
            query = """
                SELECT id, stage_run_id, name, value, step, timestamp
                FROM run_metrics
                WHERE stage_run_id = ?
            """
            params: list = [stage_run_id]

            if metric_name:
                query += " AND name = ?"
                params.append(metric_name)
            elif metric_prefix:
                query += " AND name LIKE ?"
                params.append(f"{metric_prefix}%")

            query += " ORDER BY timestamp ASC, id ASC"

            # Add SQL-level pagination (critical for performance)
            if limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            elif offset:
                # SQLite requires LIMIT when OFFSET is used; -1 means "no limit"
                query += " LIMIT -1 OFFSET ?"
                params.append(offset)

            rows = conn.execute(query, params).fetchall()
            return [cast(MetricRow, dict(row)) for row in rows]

    def count_run_metrics(
        self,
        stage_run_id: str,
        metric_name: str | None = None,
        metric_prefix: str | None = None,
    ) -> int:
        """Get total count of metrics for pagination.

        Args:
            stage_run_id: Stage run ID to count metrics for
            metric_name: Optional metric name to filter by

        Returns:
            Total count of matching metrics
        """
        with self._conn() as conn:
            query = "SELECT COALESCE(SUM(count), 0) FROM run_metrics_summary WHERE stage_run_id = ?"
            params: list = [stage_run_id]

            if metric_name:
                query += " AND name = ?"
                params.append(metric_name)
            elif metric_prefix:
                query += " AND name LIKE ?"
                params.append(f"{metric_prefix}%")

            result = conn.execute(query, params).fetchone()
            return int(result[0]) if result else 0

    def get_metrics_summary(
        self,
        stage_run_id: str,
        metric_name: str | None = None,
        metric_prefix: str | None = None,
    ) -> list[MetricsSummaryRow]:
        """Get aggregated metrics summary for a stage run.

        Args:
            stage_run_id: Stage run ID to filter by
            metric_name: Optional metric name to filter by (pushed to SQL for efficiency)

        Returns:
            List of MetricsSummaryRow dicts
        """
        from goldfish.db.types import MetricsSummaryRow

        with self._conn() as conn:
            query = """
                SELECT stage_run_id, name, min_value, max_value, last_value, last_timestamp, count
                FROM run_metrics_summary
                WHERE stage_run_id = ?
            """
            params: list = [stage_run_id]

            if metric_name:
                query += " AND name = ?"
                params.append(metric_name)
            elif metric_prefix:
                query += " AND name LIKE ?"
                params.append(f"{metric_prefix}%")

            query += " ORDER BY name ASC"

            rows = conn.execute(query, params).fetchall()

            return [cast(MetricsSummaryRow, dict(row)) for row in rows]

    def get_metrics_trends(
        self,
        stage_run_id: str,
        metric_names: list[str] | None = None,
    ) -> dict[str, list[float]]:
        """Get the two most recent values for each metric to calculate trends.

        Args:
            stage_run_id: Stage run ID to filter by
            metric_names: Optional list of metric names to filter by

        Returns:
            Dict mapping metric name to list of values [prev, last] or [last]
        """
        with self._conn() as conn:
            query = """
                WITH RankedMetrics AS (
                    SELECT
                        name,
                        value,
                        ROW_NUMBER() OVER (PARTITION BY name ORDER BY timestamp DESC, id DESC) as rank
                    FROM run_metrics
                    WHERE stage_run_id = ?
            """
            params: list[Any] = [stage_run_id]

            if metric_names:
                placeholders = ",".join(["?"] * len(metric_names))
                query += " AND name IN (" + placeholders + ")"
                params.extend(metric_names)

            query += """
                )
                SELECT name, value
                FROM RankedMetrics
                WHERE rank <= 2
                ORDER BY name, rank DESC
            """

            rows = conn.execute(query, params).fetchall()

            trends: dict[str, list[float]] = {}
            for row in rows:
                name = row["name"]
                if name not in trends:
                    trends[name] = []
                trends[name].append(row["value"])

            return trends

    def list_metric_names(
        self,
        stage_run_id: str,
        metric_prefix: str | None = None,
    ) -> list[str]:
        """List distinct metric names for a stage run."""
        with self._conn() as conn:
            query = """
                SELECT name
                FROM run_metrics_summary
                WHERE stage_run_id = ?
            """
            params: list = [stage_run_id]
            if metric_prefix:
                query += " AND name LIKE ?"
                params.append(f"{metric_prefix}%")
            query += " ORDER BY name ASC"
            rows = conn.execute(query, params).fetchall()
            return [row[0] for row in rows]

    def insert_artifact(
        self,
        stage_run_id: str,
        name: str,
        path: str,
        backend_url: str | None = None,
        created_at: str | None = None,
    ) -> None:
        """Insert an artifact record."""
        from datetime import UTC, datetime

        if created_at is None:
            created_at = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_artifacts (stage_run_id, name, path, backend_url, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (stage_run_id, name, path, backend_url, created_at),
            )

    def _batch_insert_artifacts_conn(
        self,
        conn: sqlite3.Connection,
        stage_run_id: str,
        artifacts: list[dict],
    ) -> int:
        """Insert artifacts using an existing connection."""
        from datetime import UTC, datetime

        if not artifacts:
            return 0

        now = datetime.now(UTC).isoformat()

        artifact_data = [
            (
                stage_run_id,
                a["name"],
                a["path"],
                a.get("backend_url"),
                a.get("created_at") or a.get("timestamp") or now,
            )
            for a in artifacts
        ]

        before_changes = conn.total_changes
        conn.executemany(
            """
            INSERT INTO run_artifacts (stage_run_id, name, path, backend_url, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            artifact_data,
        )
        return conn.total_changes - before_changes

    def batch_insert_artifacts(
        self,
        stage_run_id: str,
        artifacts: list[dict],
    ) -> None:
        """Insert multiple artifacts in a single transaction using executemany.

        Args:
            stage_run_id: Stage run ID
            artifacts: List of artifact dicts with keys: name, path, backend_url, timestamp/created_at
        """
        if not artifacts:
            return

        with self._conn() as conn:
            self._batch_insert_artifacts_conn(conn, stage_run_id, artifacts)

    def get_run_artifacts(
        self,
        stage_run_id: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[ArtifactRow]:
        """Get artifacts for a stage run."""
        from goldfish.db.types import ArtifactRow

        with self._conn() as conn:
            query = """
                SELECT id, stage_run_id, name, path, backend_url, created_at
                FROM run_artifacts
                WHERE stage_run_id = ?
                ORDER BY created_at ASC, id ASC
            """
            params: list = [stage_run_id]
            if limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            elif offset:
                query += " LIMIT -1 OFFSET ?"
                params.append(offset)

            rows = conn.execute(query, params).fetchall()

            return [cast(ArtifactRow, dict(row)) for row in rows]

    def count_run_artifacts(self, stage_run_id: str) -> int:
        """Get total count of artifacts for pagination."""
        with self._conn() as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM run_artifacts WHERE stage_run_id = ?",
                (stage_run_id,),
            ).fetchone()
            return int(result[0]) if result else 0

    # =========================================================================
    # SVS (Semantic Validation System) CRUD
    # =========================================================================

    def create_svs_review(
        self,
        stage_run_id: str,
        review_type: str,
        model_used: str,
        prompt_hash: str,
        decision: str,
        reviewed_at: str,
        signal_name: str | None = None,
        stats_json: str | None = None,
        response_text: str | None = None,
        parsed_findings: str | None = None,
        policy_overrides: str | None = None,
        duration_ms: int | None = None,
    ) -> int:
        """Create a new SVS review record.

        Args:
            stage_run_id: Stage run ID
            review_type: 'pre_run' | 'during_run' | 'post_run'
            model_used: Model identifier (e.g., 'claude-opus-4-5-20251101')
            prompt_hash: SHA256 of prompt for deduplication
            decision: 'approved' | 'blocked' | 'warned'
            reviewed_at: ISO timestamp
            signal_name: Signal name (for post-run reviews)
            stats_json: JSON string of input statistics
            response_text: Raw AI response
            parsed_findings: JSON string of structured findings
            policy_overrides: JSON string of policy overrides
            duration_ms: Review duration in milliseconds

        Returns:
            Review ID
        """
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO svs_reviews (
                    stage_run_id, signal_name, review_type, model_used, prompt_hash,
                    stats_json, response_text, parsed_findings, decision,
                    policy_overrides, reviewed_at, duration_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stage_run_id,
                    signal_name,
                    review_type,
                    model_used,
                    prompt_hash,
                    stats_json,
                    response_text,
                    parsed_findings,
                    decision,
                    policy_overrides,
                    reviewed_at,
                    duration_ms,
                ),
            )
            return cursor.lastrowid or 0

    def get_svs_reviews(
        self,
        stage_run_id: str,
        review_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SVSReviewRow]:
        """Get SVS reviews for a stage run.

        Args:
            stage_run_id: Stage run ID to filter by
            review_type: Optional review type filter ('pre_run', 'during_run', 'post_run')
            limit: Maximum number of results (default 100)
            offset: Number of results to skip (default 0)

        Returns:
            List of SVSReviewRow dicts
        """
        with self._conn() as conn:
            query = "SELECT * FROM svs_reviews WHERE stage_run_id = ?"
            params: list = [stage_run_id]

            if review_type:
                query += " AND review_type = ?"
                params.append(review_type)

            query += " ORDER BY reviewed_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = conn.execute(query, params).fetchall()
            return [cast(SVSReviewRow, dict(row)) for row in rows]

    def get_svs_review(self, review_id: int) -> SVSReviewRow | None:
        """Get a single SVS review by ID.

        Args:
            review_id: Review ID

        Returns:
            SVSReviewRow or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM svs_reviews WHERE id = ?",
                (review_id,),
            ).fetchone()
            return cast(SVSReviewRow, dict(row)) if row else None

    def get_recent_svs_reviews(self, limit: int = 10) -> list[SVSReviewRow]:
        """Get recent SVS reviews across all runs.

        Args:
            limit: Maximum number of reviews to return

        Returns:
            List of recent SVSReviewRow dicts, newest first
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT r.*, s.workspace_name, s.stage_name
                FROM svs_reviews r
                JOIN stage_runs s ON r.stage_run_id = s.id
                ORDER BY r.reviewed_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [cast(SVSReviewRow, dict(row)) for row in rows]

    def get_unnotified_svs_reviews(self, limit: int = 50) -> list[SVSReviewRow]:
        """Get SVS reviews that haven't been shown in dashboard yet.

        Args:
            limit: Maximum number of reviews to return

        Returns:
            List of unnotified SVSReviewRow dicts with workspace/stage info, newest first
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT r.*, s.workspace_name, s.stage_name
                FROM svs_reviews r
                JOIN stage_runs s ON r.stage_run_id = s.id
                WHERE r.notified = 0
                ORDER BY r.reviewed_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [cast(SVSReviewRow, dict(row)) for row in rows]

    def mark_svs_reviews_notified(self, review_ids: list[int]) -> int:
        """Mark SVS reviews as notified (shown in dashboard).

        Args:
            review_ids: List of review IDs to mark as notified

        Returns:
            Number of rows updated
        """
        if not review_ids:
            return 0
        with self._conn() as conn:
            placeholders = ",".join("?" * len(review_ids))
            cursor = conn.execute(
                f"UPDATE svs_reviews SET notified = 1 WHERE id IN ({placeholders})",
                review_ids,
            )
            return cursor.rowcount

    def create_failure_pattern(
        self,
        pattern_id: str,
        symptom: str,
        root_cause: str,
        detection_heuristic: str,
        prevention: str,
        created_at: str,
        severity: str | None = None,
        stage_type: str | None = None,
        source_run_id: str | None = None,
        source_workspace: str | None = None,
        confidence: str | None = None,
        status: str = "pending",
    ) -> str:
        """Create a new failure pattern.

        Args:
            pattern_id: UUID for the pattern
            symptom: What went wrong
            root_cause: Why it happened
            detection_heuristic: How to detect it
            prevention: How to prevent it
            created_at: ISO timestamp
            severity: CRITICAL | HIGH | MEDIUM | LOW
            stage_type: Stage type filter (e.g., 'train', 'preprocess')
            source_run_id: Stage run that triggered extraction
            source_workspace: Workspace where pattern was discovered
            confidence: HIGH | MEDIUM | LOW
            status: Pattern status (pending, approved, rejected, archived)

        Returns:
            Pattern ID
        """
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO failure_patterns (
                    id, symptom, root_cause, detection_heuristic, prevention,
                    created_at, severity, stage_type, source_run_id, source_workspace,
                    confidence, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pattern_id,
                    symptom,
                    root_cause,
                    detection_heuristic,
                    prevention,
                    created_at,
                    severity,
                    stage_type,
                    source_run_id,
                    source_workspace,
                    confidence,
                    status,
                ),
            )
            return pattern_id

    def get_failure_pattern(self, pattern_id: str) -> FailurePatternRow | None:
        """Get a single failure pattern by ID.

        Args:
            pattern_id: Pattern ID

        Returns:
            FailurePatternRow or None if not found
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM failure_patterns WHERE id = ?",
                (pattern_id,),
            ).fetchone()
            return cast(FailurePatternRow, dict(row)) if row else None

    def list_failure_patterns(
        self,
        status: str | None = None,
        stage_type: str | None = None,
        severity: str | None = None,
        enabled: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[FailurePatternRow]:
        """List failure patterns with optional filters.

        Args:
            status: Filter by status (pending, approved, rejected, archived)
            stage_type: Filter by stage type
            severity: Filter by severity (CRITICAL, HIGH, MEDIUM, LOW)
            enabled: Filter by enabled status
            limit: Maximum number of results (default 100)
            offset: Number of results to skip (default 0)

        Returns:
            List of FailurePatternRow dicts
        """
        with self._conn() as conn:
            query = "SELECT * FROM failure_patterns WHERE 1=1"
            params: list = []

            if status:
                query += " AND status = ?"
                params.append(status)
            if stage_type:
                query += " AND stage_type = ?"
                params.append(stage_type)
            if severity:
                query += " AND severity = ?"
                params.append(severity)
            if enabled is not None:
                query += " AND enabled = ?"
                params.append(1 if enabled else 0)

            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = conn.execute(query, params).fetchall()
            return [cast(FailurePatternRow, dict(row)) for row in rows]

    def count_failure_patterns(
        self,
        status: str | None = None,
        stage_type: str | None = None,
        severity: str | None = None,
        enabled: bool | None = None,
    ) -> int:
        """Count failure patterns matching filters.

        Args:
            status: Filter by status
            stage_type: Filter by stage type
            severity: Filter by severity
            enabled: Filter by enabled status

        Returns:
            Total count of matching patterns
        """
        with self._conn() as conn:
            query = "SELECT COUNT(*) FROM failure_patterns WHERE 1=1"
            params: list = []

            if status:
                query += " AND status = ?"
                params.append(status)
            if stage_type:
                query += " AND stage_type = ?"
                params.append(stage_type)
            if severity:
                query += " AND severity = ?"
                params.append(severity)
            if enabled is not None:
                query += " AND enabled = ?"
                params.append(1 if enabled else 0)

            result = conn.execute(query, params).fetchone()
            return int(result[0]) if result else 0

    def update_failure_pattern(self, pattern_id: str, **updates: Any) -> bool:
        """Update failure pattern fields.

        Args:
            pattern_id: Pattern ID
            **updates: Fields to update (status, approved_at, approved_by, etc.)

        Returns:
            True if updated, False if not found
        """
        if not updates:
            return False

        # Build dynamic SET clause
        fields = []
        params: list = []
        for key, value in updates.items():
            fields.append(f"{key} = ?")
            params.append(value)

        params.append(pattern_id)
        query = f"UPDATE failure_patterns SET {', '.join(fields)} WHERE id = ?"

        with self._conn() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def update_stage_run_preflight(
        self,
        stage_run_id: str,
        errors: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> bool:
        """Update preflight validation results for a stage run.

        Args:
            stage_run_id: Stage run ID
            errors: Preflight validation errors (list of strings)
            warnings: Preflight validation warnings (list of strings)

        Returns:
            True if updated, False if not found
        """
        fields: list[str] = []
        params: list = []

        if errors is not None:
            fields.append("preflight_errors_json = ?")
            params.append(json.dumps(errors))
        if warnings is not None:
            fields.append("preflight_warnings_json = ?")
            params.append(json.dumps(warnings))

        if not fields:
            return False

        params.append(stage_run_id)
        query = f"UPDATE stage_runs SET {', '.join(fields)} WHERE id = ?"

        with self._conn() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def increment_pattern_occurrence(self, pattern_id: str, last_seen_at: str) -> bool:
        """Increment occurrence count and update last seen timestamp.

        Args:
            pattern_id: Pattern ID
            last_seen_at: ISO timestamp of last occurrence

        Returns:
            True if updated, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE failure_patterns
                SET occurrence_count = occurrence_count + 1, last_seen_at = ?
                WHERE id = ?
                """,
                (last_seen_at, pattern_id),
            )
            return cursor.rowcount > 0

    def update_signal_lineage_stats(
        self,
        stage_run_id: str,
        signal_name: str,
        stats_json: str,
    ) -> bool:
        """Update stats_json for a signal in lineage.

        Args:
            stage_run_id: Stage run ID
            signal_name: Signal name
            stats_json: JSON string of statistics

        Returns:
            True if updated, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE signal_lineage
                SET stats_json = ?
                WHERE stage_run_id = ? AND signal_name = ?
                """,
                (stats_json, stage_run_id, signal_name),
            )
            return cursor.rowcount > 0

    def update_stage_run_svs_findings(
        self,
        stage_run_id: str,
        svs_findings_json: str,
    ) -> bool:
        """Update svs_findings_json for a stage run.

        Args:
            stage_run_id: Stage run ID
            svs_findings_json: JSON string of SVS findings

        Returns:
            True if updated, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE stage_runs
                SET svs_findings_json = ?
                WHERE id = ?
                """,
                (svs_findings_json, stage_run_id),
            )
            return cursor.rowcount > 0

    # =========================================================================
    # Docker Builds CRUD
    # =========================================================================

    def insert_docker_build(
        self,
        build_id: str,
        image_type: str,
        target: str,
        backend: str,
        started_at: str,
        image_tag: str | None = None,
        registry_tag: str | None = None,
        cloud_build_id: str | None = None,
        workspace_name: str | None = None,
        version: str | None = None,
        content_hash: str | None = None,
    ) -> None:
        """Insert a new Docker build record.

        Args:
            build_id: Unique build ID (e.g., "build-abc12345")
            image_type: "cpu" or "gpu"
            target: "base", "project", or "workspace"
            backend: "local" or "cloud"
            started_at: ISO timestamp
            image_tag: Local Docker tag
            registry_tag: Full registry tag
            cloud_build_id: GCP Cloud Build operation ID (if backend=cloud)
            workspace_name: Workspace name (for workspace builds only)
            version: Workspace version (for workspace builds only)
            content_hash: SHA256 of build context (for cache hit detection)
        """
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO docker_builds (
                    id, image_type, target, backend, cloud_build_id,
                    status, image_tag, registry_tag, started_at,
                    workspace_name, version, content_hash
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                """,
                (
                    build_id,
                    image_type,
                    target,
                    backend,
                    cloud_build_id,
                    image_tag,
                    registry_tag,
                    started_at,
                    workspace_name,
                    version,
                    content_hash,
                ),
            )

    def get_docker_build(self, build_id: str) -> "DockerBuildRow | None":
        """Get a Docker build by ID.

        Args:
            build_id: Build ID to fetch

        Returns:
            DockerBuildRow if found, None otherwise
        """
        from goldfish.db.types import DockerBuildRow

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE id = ?
                """,
                (build_id,),
            ).fetchone()
            return cast(DockerBuildRow, dict(row)) if row else None

    def get_docker_build_by_workspace(self, workspace_name: str, version: str) -> "DockerBuildRow | None":
        """Get the most recent Docker build for a workspace+version.

        Args:
            workspace_name: Workspace name
            version: Workspace version

        Returns:
            DockerBuildRow if found, None otherwise
        """
        from goldfish.db.types import DockerBuildRow

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE workspace_name = ? AND version = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (workspace_name, version),
            ).fetchone()
            return cast(DockerBuildRow, dict(row)) if row else None

    def get_latest_docker_build_for_workspace(self, workspace_name: str) -> "DockerBuildRow | None":
        """Get the most recent completed Docker build for a workspace (any version).

        Used for Docker layer caching - we want to use any previous successful build
        as a cache source, regardless of which version it was built for.

        Args:
            workspace_name: Workspace name

        Returns:
            DockerBuildRow if found, None otherwise
        """
        from goldfish.db.types import DockerBuildRow

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE workspace_name = ? AND status = 'completed' AND registry_tag IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (workspace_name,),
            ).fetchone()
            return cast(DockerBuildRow, dict(row)) if row else None

    def get_docker_build_by_content_hash(self, workspace_name: str, content_hash: str) -> "DockerBuildRow | None":
        """Get a completed Docker build by content hash.

        Used to detect unchanged workspace content - if we've built this exact
        content before, we can skip the build entirely.

        Args:
            workspace_name: Workspace name (to scope the search)
            content_hash: SHA256 of build context

        Returns:
            DockerBuildRow if a completed build with this hash exists, None otherwise
        """
        from goldfish.db.types import DockerBuildRow

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE workspace_name = ?
                  AND content_hash = ?
                  AND status = 'completed'
                  AND registry_tag IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (workspace_name, content_hash),
            ).fetchone()
            return cast(DockerBuildRow, dict(row)) if row else None

    def update_docker_build_status(
        self,
        build_id: str,
        status: str,
        error: str | None = None,
        completed_at: str | None = None,
        cloud_build_id: str | None = None,
        logs_uri: str | None = None,
        image_tag: str | None = None,
        registry_tag: str | None = None,
    ) -> bool:
        """Update status and related fields for a Docker build.

        Args:
            build_id: Build ID to update
            status: New status (pending, building, completed, failed, cancelled)
            error: Error message (for failed status)
            completed_at: Completion timestamp (for completed/failed status)
            cloud_build_id: GCP Cloud Build ID (for cloud builds)
            logs_uri: GCS path to logs (for cloud builds)
            image_tag: Local Docker tag (on success)
            registry_tag: Full registry tag (on success)

        Returns:
            True if updated, False if build not found
        """
        with self._conn() as conn:
            # Build dynamic update
            fields = ["status = ?"]
            params: list[str | None] = [status]

            if error is not None:
                fields.append("error = ?")
                params.append(error)
            if completed_at is not None:
                fields.append("completed_at = ?")
                params.append(completed_at)
            if cloud_build_id is not None:
                fields.append("cloud_build_id = ?")
                params.append(cloud_build_id)
            if logs_uri is not None:
                fields.append("logs_uri = ?")
                params.append(logs_uri)
            if image_tag is not None:
                fields.append("image_tag = ?")
                params.append(image_tag)
            if registry_tag is not None:
                fields.append("registry_tag = ?")
                params.append(registry_tag)

            params.append(build_id)

            cursor = conn.execute(
                f"""
                UPDATE docker_builds
                SET {", ".join(fields)}
                WHERE id = ?
                """,
                params,
            )
            return cursor.rowcount > 0

    def list_docker_builds(
        self,
        status: str | None = None,
        backend: str | None = None,
        image_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list["DockerBuildRow"]:
        """List Docker builds with optional filters.

        Args:
            status: Filter by status (pending, building, completed, failed, cancelled)
            backend: Filter by backend (local, cloud)
            image_type: Filter by image type (cpu, gpu)
            limit: Maximum results to return
            offset: Pagination offset

        Returns:
            List of DockerBuildRow
        """
        from goldfish.db.types import DockerBuildRow

        conditions = []
        params: list = []

        if status is not None:
            conditions.append("status = ?")
            params.append(status)
        if backend is not None:
            conditions.append("backend = ?")
            params.append(backend)
        if image_type is not None:
            conditions.append("image_type = ?")
            params.append(image_type)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE {where_clause}
                ORDER BY started_at DESC
                LIMIT ? OFFSET ?
                """,
                params,
            ).fetchall()
            return [cast(DockerBuildRow, dict(row)) for row in rows]

    def get_active_docker_builds(self) -> list["DockerBuildRow"]:
        """Get all in-progress Docker builds (pending or building).

        Returns:
            List of active DockerBuildRow
        """
        from goldfish.db.types import DockerBuildRow

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, image_type, target, backend, cloud_build_id,
                       status, image_tag, registry_tag, started_at,
                       completed_at, error, logs_uri, workspace_name,
                       version, content_hash, created_at
                FROM docker_builds
                WHERE status IN ('pending', 'building')
                ORDER BY started_at DESC
                """,
            ).fetchall()
            return [cast(DockerBuildRow, dict(row)) for row in rows]

    def delete_docker_build(self, build_id: str) -> bool:
        """Delete a Docker build record.

        Args:
            build_id: Build ID to delete

        Returns:
            True if deleted, False if not found
        """
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM docker_builds WHERE id = ?",
                (build_id,),
            )
            return cursor.rowcount > 0

    # =========================================================================
    # Backup History CRUD
    # =========================================================================

    def insert_backup(
        self,
        backup_id: str,
        tier: str,
        trigger: str,
        gcs_path: str,
        created_at: str,
        expires_at: str,
        trigger_details: dict | None = None,
        size_bytes: int | None = None,
    ) -> None:
        """Insert a new backup record.

        Args:
            backup_id: Unique backup ID (e.g., "backup-abc12345")
            tier: Backup tier ("event", "daily", "weekly", "monthly")
            trigger: What triggered the backup ("run", "save_version", etc.)
            gcs_path: GCS path to the backup file
            created_at: ISO timestamp when backup was created
            expires_at: ISO timestamp when backup expires
            trigger_details: Optional dict with workspace, version, run_id, etc.
            size_bytes: Compressed backup size in bytes
        """
        import json

        trigger_details_json = json.dumps(trigger_details) if trigger_details else None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO backup_history (
                    backup_id, tier, trigger, trigger_details_json,
                    gcs_path, size_bytes, created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    backup_id,
                    tier,
                    trigger,
                    trigger_details_json,
                    gcs_path,
                    size_bytes,
                    created_at,
                    expires_at,
                ),
            )

    def get_backup(self, backup_id: str) -> "BackupRow | None":
        """Get a backup by ID.

        Args:
            backup_id: Backup ID to fetch

        Returns:
            BackupRow if found, None otherwise
        """
        from goldfish.db.types import BackupRow

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT backup_id, tier, trigger, trigger_details_json,
                       gcs_path, size_bytes, created_at, expires_at, deleted_at
                FROM backup_history
                WHERE backup_id = ?
                """,
                (backup_id,),
            ).fetchone()
            return cast(BackupRow, dict(row)) if row else None

    def list_backups(
        self,
        tier: str | None = None,
        include_deleted: bool = False,
        limit: int = 50,
    ) -> list["BackupRow"]:
        """List backups with optional filters.

        Args:
            tier: Filter by tier ("event", "daily", "weekly", "monthly")
            include_deleted: Include soft-deleted backups
            limit: Maximum results to return

        Returns:
            List of BackupRow, ordered by created_at descending
        """
        from goldfish.db.types import BackupRow

        conditions = []
        params: list = []

        if tier is not None:
            conditions.append("tier = ?")
            params.append(tier)

        if not include_deleted:
            conditions.append("deleted_at IS NULL")

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT backup_id, tier, trigger, trigger_details_json,
                       gcs_path, size_bytes, created_at, expires_at, deleted_at
                FROM backup_history
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [cast(BackupRow, dict(row)) for row in rows]

    def mark_backup_deleted(self, backup_id: str) -> bool:
        """Mark a backup as deleted (soft delete).

        Args:
            backup_id: Backup ID to mark as deleted

        Returns:
            True if updated, False if backup not found
        """
        from datetime import UTC, datetime

        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE backup_history
                SET deleted_at = ?
                WHERE backup_id = ? AND deleted_at IS NULL
                """,
                (datetime.now(UTC).isoformat(), backup_id),
            )
            return cursor.rowcount > 0

    def get_last_backup(self, tier: str | None = None) -> "BackupRow | None":
        """Get the most recent backup.

        Args:
            tier: Optional filter by tier

        Returns:
            Most recent BackupRow if found, None otherwise
        """
        from goldfish.db.types import BackupRow

        conditions = ["deleted_at IS NULL"]
        params: list = []

        if tier is not None:
            conditions.append("tier = ?")
            params.append(tier)

        where_clause = " AND ".join(conditions)

        with self._conn() as conn:
            row = conn.execute(
                f"""
                SELECT backup_id, tier, trigger, trigger_details_json,
                       gcs_path, size_bytes, created_at, expires_at, deleted_at
                FROM backup_history
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
            return cast(BackupRow, dict(row)) if row else None

    def get_expired_backups(self) -> list["BackupRow"]:
        """Get all expired but not-yet-deleted backups.

        Returns:
            List of expired BackupRow
        """
        from datetime import UTC, datetime

        from goldfish.db.types import BackupRow

        now = datetime.now(UTC).isoformat()

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT backup_id, tier, trigger, trigger_details_json,
                       gcs_path, size_bytes, created_at, expires_at, deleted_at
                FROM backup_history
                WHERE expires_at < ? AND deleted_at IS NULL
                ORDER BY created_at ASC
                """,
                (now,),
            ).fetchall()
            return [cast(BackupRow, dict(row)) for row in rows]

    def count_backups_by_tier(self) -> dict[str, int]:
        """Count active backups grouped by tier.

        Returns:
            Dict mapping tier name to count
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT tier, COUNT(*) as count
                FROM backup_history
                WHERE deleted_at IS NULL
                GROUP BY tier
                """,
            ).fetchall()
            return {row["tier"]: row["count"] for row in rows}
