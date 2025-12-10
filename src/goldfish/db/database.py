"""SQLite database connection and schema initialization."""

import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from goldfish.db.types import (
    AuditRow,
    JobInputWithSource,
    JobRow,
    LineageRow,
    SourceRow,
)
from goldfish.models import JobStatus

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
        self._init_schema()
        self._migrate_schema()

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
                ("backend_type", "TEXT"),
                ("backend_handle", "TEXT"),
                ("artifact_uri", "TEXT"),
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

    def get_source(self, source_id: str) -> SourceRow | None:
        """Get a source by ID."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
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
            return row[0] if row else 0

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
            return result[0] if result else 0

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
        config_override: dict | None = None,
        inputs: dict | None = None,
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
            config: Effective config used
            inputs: Resolved input URIs/refs
            backend_type: local|gce
            backend_handle: container_id or instance_name for cancel/logs
        """
        timestamp = datetime.now(UTC).isoformat()
        effective_config = config_override if config_override is not None else config
        if effective_config is not None:
            config_json = json.dumps(effective_config)
        else:
            config_json = None
        hints_json = json.dumps(hints) if hints else None
        inputs_json = json.dumps(inputs) if inputs else None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, job_id, pipeline_run_id, workspace_name, pipeline_name, version, stage_name, status,
                 started_at, profile, hints_json, config_json, inputs_json, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
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
                    backend_type,
                    backend_handle,
                ),
            )

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
            return row[0] if row else 0

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
