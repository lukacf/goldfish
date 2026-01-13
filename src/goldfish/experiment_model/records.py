"""Experiment record management.

This module handles creation and retrieval of experiment records,
which are user-facing entities representing runs or checkpoints.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, cast

from goldfish.validation import validate_workspace_name

if TYPE_CHECKING:
    from goldfish.db.database import Database

import json

from goldfish.db.types import ExperimentRecordRow, RunResultsRow, RunResultsSpecRow
from goldfish.experiment_model.schemas import validate_results_spec

# Crockford's Base32 alphabet (excludes I, L, O, U to avoid confusion)
_CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# Valid record types
RecordType = Literal["run", "checkpoint"]


def generate_record_id() -> str:
    """Generate a new ULID-like ID for experiment records.

    Generated IDs are:
    - Lexicographically sortable (newer IDs sort after older ones when
      timestamps differ; within the same millisecond, order depends on
      the random suffix)
    - 26 characters using Crockford's Base32
    - Unique with millisecond precision + random suffix

    Note:
        Within the same millisecond, IDs are NOT guaranteed to be
        monotonically increasing. This is acceptable for our use case
        since experiment records are created infrequently.

    Returns:
        A new ULID-like string.
    """
    # Get millisecond timestamp
    timestamp_ms = int(time.time() * 1000)

    # Encode timestamp into 10 base32 chars (50 bits capacity, ~48 bits used for current timestamps)
    timestamp_chars = []
    for _ in range(10):
        timestamp_chars.append(_CROCKFORD_ALPHABET[timestamp_ms & 0x1F])
        timestamp_ms >>= 5
    timestamp_part = "".join(reversed(timestamp_chars))

    # Generate 80 bits of randomness (16 chars in base32)
    random_bytes = os.urandom(10)
    random_int = int.from_bytes(random_bytes, "big")
    random_chars = []
    for _ in range(16):
        random_chars.append(_CROCKFORD_ALPHABET[random_int & 0x1F])
        random_int >>= 5
    random_part = "".join(reversed(random_chars))

    return timestamp_part + random_part


class ExperimentRecordManager:
    """Manages experiment records for runs and checkpoints.

    Experiment records are the user-facing entities that represent
    either a run (execution + results) or a checkpoint (snapshot).
    """

    def __init__(self, db: Database) -> None:
        """Initialize the manager.

        Args:
            db: Database instance for persistence.
        """
        self.db = db

    def create_run_record(
        self,
        workspace_name: str,
        version: str,
        stage_run_id: str,
    ) -> str:
        """Create a run record linked to a stage run.

        Also initializes the associated run_results with status=missing.

        Args:
            workspace_name: Workspace name
            version: Workspace version
            stage_run_id: Stage run ID to link to

        Returns:
            The generated record_id (ULID)

        Raises:
            ValidationError: If workspace_name is invalid
        """
        # Validate inputs
        validate_workspace_name(workspace_name)

        record_id = generate_record_id()
        created_at = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            # Insert experiment record
            conn.execute(
                """
                INSERT INTO experiment_records
                (record_id, workspace_name, type, stage_run_id, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, workspace_name, "run", stage_run_id, version, created_at),
            )

            # Initialize run_results with missing status
            conn.execute(
                """
                INSERT INTO run_results
                (stage_run_id, record_id, results_status, infra_outcome, ml_outcome)
                VALUES (?, ?, ?, ?, ?)
                """,
                (stage_run_id, record_id, "missing", "unknown", "unknown"),
            )

        return record_id

    def create_checkpoint_record(
        self,
        workspace_name: str,
        version: str,
    ) -> str:
        """Create a checkpoint record (no stage run).

        Args:
            workspace_name: Workspace name
            version: Workspace version

        Returns:
            The generated record_id (ULID)

        Raises:
            ValidationError: If workspace_name is invalid
        """
        # Validate inputs
        validate_workspace_name(workspace_name)

        record_id = generate_record_id()
        created_at = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            conn.execute(
                """
                INSERT INTO experiment_records
                (record_id, workspace_name, type, stage_run_id, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, workspace_name, "checkpoint", None, version, created_at),
            )

        return record_id

    def get_record(self, record_id: str) -> ExperimentRecordRow | None:
        """Get a record by its ID.

        Args:
            record_id: The record ID to look up

        Returns:
            The record row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM experiment_records WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(ExperimentRecordRow, dict(row))

    def get_record_by_stage_run(self, stage_run_id: str) -> ExperimentRecordRow | None:
        """Get a record by its stage_run_id.

        Args:
            stage_run_id: The stage run ID to look up

        Returns:
            The record row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM experiment_records WHERE stage_run_id = ?",
                (stage_run_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(ExperimentRecordRow, dict(row))

    def list_records(
        self,
        workspace_name: str,
        record_type: RecordType | None = None,
        version: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ExperimentRecordRow]:
        """List experiment records for a workspace.

        Args:
            workspace_name: Workspace name
            record_type: Filter by type ("run" or "checkpoint")
            version: Filter by workspace version
            limit: Maximum records to return
            offset: Number of records to skip

        Returns:
            List of record rows, ordered by record_id descending (newest first)

        Raises:
            ValidationError: If workspace_name is invalid
        """
        validate_workspace_name(workspace_name)

        query = """
            SELECT * FROM experiment_records
            WHERE workspace_name = ?
        """
        params: list[Any] = [workspace_name]

        if record_type is not None:
            query += " AND type = ?"
            params.append(record_type)

        if version is not None:
            query += " AND version = ?"
            params.append(version)

        # Use record_id for ordering since ULIDs are lexicographically sortable by time
        query += " ORDER BY record_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.db._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        return [cast(ExperimentRecordRow, dict(row)) for row in rows]

    def get_run_results(self, stage_run_id: str) -> RunResultsRow | None:
        """Get run results by stage_run_id.

        Args:
            stage_run_id: The stage run ID to look up

        Returns:
            The run results row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results WHERE stage_run_id = ?",
                (stage_run_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(RunResultsRow, dict(row))

    def get_run_results_by_record(self, record_id: str) -> RunResultsRow | None:
        """Get run results by record_id.

        Args:
            record_id: The experiment record ID to look up

        Returns:
            The run results row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(RunResultsRow, dict(row))

    def store_results_spec(
        self,
        stage_run_id: str,
        record_id: str,
        spec: dict[str, Any],
    ) -> None:
        """Store a results_spec for a run.

        Validates the spec before storage.

        Args:
            stage_run_id: Stage run ID
            record_id: Experiment record ID
            spec: The results_spec dict

        Raises:
            InvalidResultsSpecError: If spec validation fails
        """
        # Validate before storing
        validate_results_spec(spec)

        spec_json = json.dumps(spec)
        created_at = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_results_spec
                (stage_run_id, record_id, spec_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (stage_run_id, record_id, spec_json, created_at),
            )

    def get_results_spec(self, stage_run_id: str) -> RunResultsSpecRow | None:
        """Get results spec by stage_run_id.

        Args:
            stage_run_id: The stage run ID to look up

        Returns:
            The results spec row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results_spec WHERE stage_run_id = ?",
                (stage_run_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(RunResultsSpecRow, dict(row))

    def get_results_spec_by_record(self, record_id: str) -> RunResultsSpecRow | None:
        """Get results spec by record_id.

        Args:
            record_id: The experiment record ID to look up

        Returns:
            The results spec row or None if not found
        """
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT * FROM run_results_spec WHERE record_id = ?",
                (record_id,),
            ).fetchone()

        if row is None:
            return None

        return cast(RunResultsSpecRow, dict(row))

    def get_results_spec_parsed(self, stage_run_id: str) -> dict[str, Any] | None:
        """Get results spec as parsed dict.

        Args:
            stage_run_id: The stage run ID to look up

        Returns:
            The parsed spec dict or None if not found
        """
        row = self.get_results_spec(stage_run_id)
        if row is None:
            return None

        result: dict[str, Any] = json.loads(row["spec_json"])
        return result
