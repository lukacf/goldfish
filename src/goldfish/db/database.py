"""SQLite database connection and schema initialization."""

import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from goldfish.db.types import (
    ArtifactRow,
    AuditRow,
    JobInputWithSource,
    JobRow,
    LineageRow,
    MetricRow,
    MetricsSummaryRow,
    SourceRow,
    StageVersionRow,
)
from goldfish.models import JobStatus, PipelineStatus, StageRunStatus

# Load schema from file
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class DatabaseError(Exception):
    """Database operation failed."""

    def __init__(self, message: str, operation: str = ""):
        self.message = message
        self.operation = operation
        super().__init__(message)


class Database:
    """SQLite database manager for Goldfish."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise DatabaseError(
                f"Cannot create database directory at '{db_path.parent}': {e}",
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
                operation="init_schema",
            ) from e

        try:
            with self._conn() as conn:
                conn.executescript(schema)
        except sqlite3.Error as e:
            raise DatabaseError(
                f"Cannot initialize database schema: {e}",
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
                ("backend_type", "TEXT"),
                ("backend_handle", "TEXT"),
                ("artifact_uri", "TEXT"),
                ("stage_version_id", "INTEGER"),  # Links to stage_versions
                ("outcome", "TEXT"),  # NULL, 'success', 'bad_results' - semantic result quality
                ("attempt_num", "INTEGER"),  # Groups consecutive runs per stage
            ],
            "signal_lineage": [
                ("source_stage_run_id", "TEXT"),  # Upstream stage run
                ("source_stage_version_id", "INTEGER"),  # Upstream stage version
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
                """
            )
            # Bump schema version
            if current_version < 2:
                if version_row:
                    conn.execute("UPDATE schema_version SET version = 2")
                else:
                    conn.execute("INSERT INTO schema_version (version) VALUES (2)")

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
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
        conn = sqlite3.connect(self.db_path)
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

    def list_versions(self, workspace_name: str) -> list[dict]:
        """List all versions for a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            List of version dicts, ordered by creation time
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
                ORDER BY created_at ASC
                """,
                (workspace_name,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_latest_version(self, workspace_name: str) -> dict | None:
        """Get the most recent version for a workspace.

        Args:
            workspace_name: Workspace name

        Returns:
            Latest version dict or None if no versions
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
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

        Args:
            workspace_name: Workspace name

        Returns:
            Next version string (e.g., "v1", "v2")
        """
        versions = self.list_versions(workspace_name)
        return f"v{len(versions) + 1}"

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
            backend_type: local|gce
            backend_handle: container_id or instance_name for cancel/logs
        """
        timestamp = datetime.now(UTC).isoformat()
        config_json = json.dumps(config) if config is not None else None
        hints_json = json.dumps(hints) if hints else None
        inputs_json = json.dumps(inputs) if inputs else None
        reason_json = json.dumps(reason) if reason else None

        with self._conn() as conn:
            # Compute attempt_num: increment after a successful run, otherwise continue current attempt
            attempt_num = self._compute_attempt_num(conn, workspace_name, stage_name)

            conn.execute(
                """
                INSERT INTO stage_runs
                (id, job_id, pipeline_run_id, workspace_name, pipeline_name, version, stage_name, status,
                 started_at, profile, hints_json, config_json, inputs_json, reason_json, backend_type, backend_handle,
                 attempt_num)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stage_run_id,
                    job_id,
                    pipeline_run_id,
                    workspace_name,
                    pipeline_name,
                    version,
                    stage_name,
                    StageRunStatus.PENDING,
                    timestamp,
                    profile,
                    hints_json,
                    config_json,
                    inputs_json,
                    reason_json,
                    backend_type,
                    backend_handle,
                    attempt_num,
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
        if outcome not in ("success", "bad_results"):
            raise ValueError(f"Invalid outcome: {outcome}. Must be 'success' or 'bad_results'")

        with self._conn() as conn:
            # Only update completed runs
            result = conn.execute(
                """
                UPDATE stage_runs
                SET outcome = ?
                WHERE id = ? AND status = ?
                """,
                (outcome, stage_run_id, StageRunStatus.COMPLETED),
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
        # Build query to group runs by attempt
        query = """
            SELECT
                stage_name,
                attempt_num,
                COUNT(*) as run_count,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as completed_count,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as failed_count,
                SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN outcome = 'bad_results' THEN 1 ELSE 0 END) as bad_results_count,
                MIN(version) as first_version,
                MAX(version) as last_version,
                MIN(started_at) as started_at,
                MAX(completed_at) as ended_at
            FROM stage_runs
            WHERE workspace_name = ?
        """
        params: list = [StageRunStatus.COMPLETED, StageRunStatus.FAILED, workspace_name]

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

    def update_stage_run_status(
        self,
        stage_run_id: str,
        status: str,
        completed_at: str | None = None,
        log_uri: str | None = None,
        artifact_uri: str | None = None,
        progress: str | None = None,
        outputs_json: dict | None = None,
        error: str | None = None,
    ) -> None:
        """Update stage run status.

        Args:
            stage_run_id: Stage run ID
            status: New status (pending, running, completed, failed)
            completed_at: Completion timestamp
            log_uri: Log file location
            artifact_uri: Artifact URI if produced
            progress: Progress string
            outputs_json: Outputs map
            error: Error message if failed
        """
        fields = ["status = ?"]
        params: list = [status]
        if completed_at is not None:
            fields.append("completed_at = ?")
            params.append(completed_at)
        if log_uri is not None:
            fields.append("log_uri = ?")
            params.append(log_uri)
        if artifact_uri is not None:
            fields.append("artifact_uri = ?")
            params.append(artifact_uri)
        if progress is not None:
            fields.append("progress = ?")
            params.append(progress)
        if outputs_json is not None:
            fields.append("outputs_json = ?")
            params.append(json.dumps(outputs_json))
        if error is not None:
            fields.append("error = ?")
            params.append(error)

        params.append(stage_run_id)
        query = f"UPDATE stage_runs SET {', '.join(fields)} WHERE id = ?"

        with self._conn() as conn:
            conn.execute(query, params)

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
        status: str | None = None,
        pipeline_run_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List stage runs with optional filters.

        Args:
            workspace_name: Filter by workspace
            stage_name: Filter by stage
            status: Filter by status
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
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def list_stage_runs_with_total(
        self,
        workspace_name: str | None = None,
        stage_name: str | None = None,
        status: str | None = None,
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
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def count_stage_runs(
        self,
        workspace_name: str | None = None,
        stage_name: str | None = None,
        status: str | None = None,
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
        if status:
            query += " AND status = ?"
            params.append(status)

        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
            count: int = row[0] if row else 0
            return count

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
    ) -> None:
        """Persist backend info for cancellation/logging."""
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE stage_runs
                SET backend_type = ?, backend_handle = ?
                WHERE id = ?
                """,
                (backend_type, backend_handle, stage_run_id),
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
        is_artifact: bool | None = None,
    ) -> list[dict]:
        """List signals with optional filters.

        Args:
            stage_run_id: Filter by producing stage run
            consumed_by: Filter by consuming stage run
            is_artifact: Filter by artifact status

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
        if is_artifact is not None:
            query += " AND is_artifact = ?"
            params.append(1 if is_artifact else 0)

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

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
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM stage_runs
                WHERE workspace_name = ? AND stage_name = ? AND status = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (workspace, stage_name, StageRunStatus.COMPLETED),
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
        """Insert a metric data point."""
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

    def batch_insert_metrics(
        self,
        stage_run_id: str,
        metrics: list[dict],
    ) -> None:
        """Insert multiple metrics in a single transaction.

        Args:
            stage_run_id: Stage run ID
            metrics: List of metric dicts with keys: name, value, step, timestamp
        """
        from datetime import UTC, datetime

        with self._conn() as conn:
            for m in metrics:
                # Ensure timestamp is set
                timestamp = m.get("timestamp")
                if timestamp is None:
                    timestamp = datetime.now(UTC).isoformat()

                # Insert metric
                conn.execute(
                    """
                    INSERT INTO run_metrics (stage_run_id, name, value, step, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (stage_run_id, m["name"], m["value"], m.get("step"), timestamp),
                )

                # Update summary (in same transaction)
                value = m["value"]
                conn.execute(
                    """
                    INSERT INTO run_metrics_summary (stage_run_id, name, min_value, max_value, last_value, count)
                    VALUES (?, ?, ?, ?, ?, 1)
                    ON CONFLICT(stage_run_id, name) DO UPDATE SET
                        min_value = MIN(min_value, excluded.min_value),
                        max_value = MAX(max_value, excluded.max_value),
                        last_value = excluded.last_value,
                        count = count + 1
                    """,
                    (stage_run_id, m["name"], value, value, value),
                )

    def upsert_metric_summary(
        self,
        stage_run_id: str,
        name: str,
        value: float,
    ) -> None:
        """Update or insert a metric summary (min, max, last, count)."""
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_metrics_summary (stage_run_id, name, min_value, max_value, last_value, count)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(stage_run_id, name) DO UPDATE SET
                    min_value = MIN(min_value, excluded.min_value),
                    max_value = MAX(max_value, excluded.max_value),
                    last_value = excluded.last_value,
                    count = count + 1
                """,
                (stage_run_id, name, value, value, value),
            )

    def get_run_metrics(
        self,
        stage_run_id: str,
        metric_name: str | None = None,
    ) -> list[MetricRow]:
        """Get metrics for a stage run, optionally filtered by metric name."""
        from goldfish.db.types import MetricRow

        with self._conn() as conn:
            if metric_name:
                rows = conn.execute(
                    """
                    SELECT id, stage_run_id, name, value, step, timestamp
                    FROM run_metrics
                    WHERE stage_run_id = ? AND name = ?
                    ORDER BY timestamp ASC
                    """,
                    (stage_run_id, metric_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, stage_run_id, name, value, step, timestamp
                    FROM run_metrics
                    WHERE stage_run_id = ?
                    ORDER BY timestamp ASC
                    """,
                    (stage_run_id,),
                ).fetchall()

            return [cast(MetricRow, dict(row)) for row in rows]

    def get_metrics_summary(
        self,
        stage_run_id: str,
    ) -> list[MetricsSummaryRow]:
        """Get aggregated metrics summary for a stage run."""
        from goldfish.db.types import MetricsSummaryRow

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT stage_run_id, name, min_value, max_value, last_value, count
                FROM run_metrics_summary
                WHERE stage_run_id = ?
                ORDER BY name ASC
                """,
                (stage_run_id,),
            ).fetchall()

            return [cast(MetricsSummaryRow, dict(row)) for row in rows]

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

    def get_run_artifacts(
        self,
        stage_run_id: str,
    ) -> list[ArtifactRow]:
        """Get artifacts for a stage run."""
        from goldfish.db.types import ArtifactRow

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, stage_run_id, name, path, backend_url, created_at
                FROM run_artifacts
                WHERE stage_run_id = ?
                ORDER BY created_at ASC
                """,
                (stage_run_id,),
            ).fetchall()

            return [cast(ArtifactRow, dict(row)) for row in rows]
