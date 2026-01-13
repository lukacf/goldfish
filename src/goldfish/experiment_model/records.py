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
from goldfish.experiment_model.schemas import (
    validate_finalize_results,
    validate_results_spec,
)

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

    def extract_auto_results(self, stage_run_id: str) -> dict[str, Any] | None:
        """Extract auto results from metrics summary.

        Reads the results_spec and looks up corresponding metrics
        to build the auto-extracted results.

        Args:
            stage_run_id: The stage run ID

        Returns:
            Auto results dict or None if no spec exists
        """
        # Get the spec to know which metrics to extract
        spec = self.get_results_spec_parsed(stage_run_id)
        if spec is None:
            return None

        # Get all metrics for this run
        with self.db._conn() as conn:
            rows = conn.execute(
                """
                SELECT name, last_value, min_value, max_value
                FROM run_metrics_summary
                WHERE stage_run_id = ?
                """,
                (stage_run_id,),
            ).fetchall()

        metrics = {row["name"]: row for row in rows}

        # Build auto results
        primary_metric = spec["primary_metric"]
        primary_data = metrics.get(primary_metric)

        auto_results: dict[str, Any] = {
            "primary_metric": primary_metric,
            "value": primary_data["last_value"] if primary_data else None,
            "direction": spec["direction"],
            "dataset_split": spec["dataset_split"],
        }

        # Add secondary metrics if specified
        secondary_metrics = spec.get("secondary_metrics", [])
        if secondary_metrics:
            secondary: dict[str, float | None] = {}
            for metric_name in secondary_metrics:
                metric_data = metrics.get(metric_name)
                secondary[metric_name] = metric_data["last_value"] if metric_data else None
            auto_results["secondary"] = secondary

        return auto_results

    def update_auto_results(
        self,
        stage_run_id: str,
        auto_results: dict[str, Any],
        run_status: str,
    ) -> None:
        """Update run_results with auto-extracted data.

        Args:
            stage_run_id: The stage run ID
            auto_results: The auto-extracted results
            run_status: The run status for deriving infra_outcome
        """
        results_auto_json = json.dumps(auto_results)
        infra_outcome = self.derive_infra_outcome(run_status)

        with self.db._conn() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET results_auto = ?,
                    results_status = ?,
                    infra_outcome = ?
                WHERE stage_run_id = ?
                """,
                (results_auto_json, "auto", infra_outcome, stage_run_id),
            )

    def derive_infra_outcome(self, run_status: str) -> str:
        """Derive infra_outcome from run status.

        Args:
            run_status: The run status string

        Returns:
            The infra_outcome value
        """
        status_mapping = {
            "completed": "completed",
            "failed": "crashed",
            "preempted": "preempted",
            "canceled": "canceled",
        }
        return status_mapping.get(run_status, "unknown")

    def finalize_run(
        self,
        stage_run_id_or_record_id: str,
        results: dict[str, Any],
        finalized_by: str = "ml_claude",
    ) -> None:
        """Finalize a run with authoritative ML results.

        This sets the results_final, results_status=finalized, and ml_outcome.
        Preserves results_auto unchanged.

        Args:
            stage_run_id_or_record_id: Either a stage_run_id or record_id
            results: The finalize results dict
            finalized_by: Who is finalizing (default: ml_claude)

        Raises:
            InvalidFinalizeResultsError: If results validation fails
            ValueError: If the run/record is not found
        """
        # Validate results before anything else
        validate_finalize_results(results)

        # Resolve stage_run_id
        stage_run_id = self._resolve_stage_run_id(stage_run_id_or_record_id)

        results_json = json.dumps(results)
        ml_outcome = results["ml_outcome"]
        finalized_at = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET results_final = ?,
                    results_status = ?,
                    ml_outcome = ?,
                    finalized_by = ?,
                    finalized_at = ?
                WHERE stage_run_id = ?
                """,
                (results_json, "finalized", ml_outcome, finalized_by, finalized_at, stage_run_id),
            )

    def _resolve_stage_run_id(self, stage_run_id_or_record_id: str) -> str:
        """Resolve a stage_run_id or record_id to a stage_run_id.

        Args:
            stage_run_id_or_record_id: Either a stage_run_id or record_id

        Returns:
            The stage_run_id

        Raises:
            ValueError: If not found
        """
        # First try as stage_run_id
        run_results = self.get_run_results(stage_run_id_or_record_id)
        if run_results is not None:
            return stage_run_id_or_record_id

        # Try as record_id
        record = self.get_record(stage_run_id_or_record_id)
        if record is not None and record["stage_run_id"] is not None:
            return record["stage_run_id"]

        raise ValueError(f"Run or record not found: {stage_run_id_or_record_id}")

    def get_finalized_results(self, stage_run_id: str) -> dict[str, Any] | None:
        """Get finalized results as parsed dict.

        Args:
            stage_run_id: The stage run ID to look up

        Returns:
            The parsed finalized results dict or None if not finalized
        """
        run_results = self.get_run_results(stage_run_id)
        if run_results is None:
            return None

        results_final = run_results.get("results_final")
        if results_final is None:
            return None

        result: dict[str, Any] = json.loads(results_final)
        return result

    def compute_comparison(
        self,
        stage_run_id: str,
        workspace_name: str,
        stage_name: str,
        results: dict[str, Any],
    ) -> dict[str, Any]:
        """Compute comparison block for a run.

        Computes:
        - vs_previous: comparison to last finalized run for same stage
        - vs_best: comparison to baseline_run if specified in spec

        Args:
            stage_run_id: Current stage run ID
            workspace_name: Workspace name
            stage_name: Stage name
            results: Current results dict with 'value', 'primary_metric', 'direction'

        Returns:
            Comparison dict with vs_previous and vs_best fields
        """
        current_value = results.get("value")

        # Compute vs_previous
        vs_previous = self._compute_vs_previous(
            stage_run_id=stage_run_id,
            workspace_name=workspace_name,
            stage_name=stage_name,
            current_value=current_value,
        )

        # Compute vs_best (from baseline_run in spec if present)
        vs_best = self._compute_vs_best(
            stage_run_id=stage_run_id,
            current_value=current_value,
        )

        return {
            "vs_previous": vs_previous,
            "vs_best": vs_best,
        }

    def _compute_vs_previous(
        self,
        stage_run_id: str,
        workspace_name: str,
        stage_name: str,
        current_value: float | None,
    ) -> dict[str, Any] | None:
        """Compute vs_previous comparison.

        Finds the last finalized run for the same workspace and stage,
        excluding the current run.

        Args:
            stage_run_id: Current stage run ID to exclude
            workspace_name: Workspace name
            stage_name: Stage name
            current_value: Current result value

        Returns:
            Dict with record and delta, or None if no previous run
        """
        with self.db._conn() as conn:
            # Find finalized runs for same workspace/stage, excluding current
            rows = conn.execute(
                """
                SELECT rr.stage_run_id, rr.record_id, rr.results_final
                FROM run_results rr
                JOIN stage_runs sr ON rr.stage_run_id = sr.id
                WHERE sr.workspace_name = ?
                  AND sr.stage_name = ?
                  AND rr.results_status = 'finalized'
                  AND rr.stage_run_id != ?
                ORDER BY rr.finalized_at DESC
                LIMIT 1
                """,
                (workspace_name, stage_name, stage_run_id),
            ).fetchall()

        if not rows:
            return None

        prev_row = rows[0]
        prev_results_json = prev_row["results_final"]
        if prev_results_json is None:
            return None

        prev_results: dict[str, Any] = json.loads(prev_results_json)
        prev_value = prev_results.get("value")

        if prev_value is None or current_value is None:
            return None

        delta = current_value - prev_value

        return {
            "record": prev_row["record_id"],
            "delta": round(delta, 6),  # Avoid floating point noise
        }

    def _compute_vs_best(
        self,
        stage_run_id: str,
        current_value: float | None,
    ) -> dict[str, Any] | None:
        """Compute vs_best comparison using baseline_run from spec.

        Args:
            stage_run_id: Current stage run ID
            current_value: Current result value

        Returns:
            Dict with record, tag, and delta, or None if no baseline
        """
        # Get the spec to check for baseline_run
        spec = self.get_results_spec_parsed(stage_run_id)
        if spec is None:
            return None

        baseline_run = spec.get("baseline_run")
        if baseline_run is None:
            return None

        # TODO: Implement baseline resolution (tag reference, run_id, record_id)
        # For now, return None - this will be enhanced in a later phase
        return None

    def store_comparison(self, stage_run_id: str, comparison: dict[str, Any]) -> None:
        """Store comparison in run_results.

        Args:
            stage_run_id: Stage run ID
            comparison: Comparison dict to store
        """
        comparison_json = json.dumps(comparison)

        with self.db._conn() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET comparison = ?
                WHERE stage_run_id = ?
                """,
                (comparison_json, stage_run_id),
            )

    def get_comparison(self, stage_run_id: str) -> dict[str, Any] | None:
        """Get comparison from run_results.

        Args:
            stage_run_id: Stage run ID

        Returns:
            Parsed comparison dict or None
        """
        run_results = self.get_run_results(stage_run_id)
        if run_results is None:
            return None

        comparison_json = run_results.get("comparison")
        if comparison_json is None:
            return None

        result: dict[str, Any] = json.loads(comparison_json)
        return result

    def tag_record(self, record_id: str, tag: str) -> None:
        """Tag an experiment record.

        For run records, creates a run tag.
        For checkpoint records, creates a run tag (same table).
        Tag uniqueness is enforced per workspace.

        Args:
            record_id: The record ID to tag
            tag: The tag name

        Raises:
            ValueError: If tag is empty, record not found, or tag already exists
        """
        # Validate tag name
        if not tag or not tag.strip():
            raise ValueError("Tag name is invalid: cannot be empty")

        # Get the record
        record = self.get_record(record_id)
        if record is None:
            raise ValueError(f"Record not found: {record_id}")

        workspace_name = record["workspace_name"]
        created_at = datetime.now(UTC).isoformat()

        # Check for tag uniqueness in workspace
        with self.db._conn() as conn:
            existing = conn.execute(
                """
                SELECT 1 FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

            if existing:
                raise ValueError(f"Tag '{tag}' already exists in workspace '{workspace_name}'")

            # Insert the tag
            conn.execute(
                """
                INSERT INTO run_tags (workspace_name, record_id, tag_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (workspace_name, record_id, tag, created_at),
            )

    def get_record_tags(self, record_id: str) -> list[str]:
        """Get all tags for a record.

        Args:
            record_id: The record ID

        Returns:
            List of tag names
        """
        with self.db._conn() as conn:
            rows = conn.execute(
                "SELECT tag_name FROM run_tags WHERE record_id = ?",
                (record_id,),
            ).fetchall()

        return [row["tag_name"] for row in rows]

    def get_record_by_tag(self, workspace_name: str, tag: str) -> ExperimentRecordRow | None:
        """Look up a record by its tag.

        Args:
            workspace_name: Workspace name
            tag: Tag name

        Returns:
            The record row or None if tag not found
        """
        with self.db._conn() as conn:
            tag_row = conn.execute(
                """
                SELECT record_id FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

        if tag_row is None:
            return None

        return self.get_record(tag_row["record_id"])

    def remove_tag(self, workspace_name: str, tag: str) -> None:
        """Remove a tag from a workspace.

        Args:
            workspace_name: Workspace name
            tag: Tag name to remove
        """
        with self.db._conn() as conn:
            conn.execute(
                """
                DELETE FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            )

    def list_history(
        self,
        workspace_name: str,
        record_type: RecordType | None = None,
        stage: str | None = None,
        tagged: bool | str | None = None,
        sort_by: Literal["created", "metric"] = "created",
        desc: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List experiment history (runs + checkpoints).

        Args:
            workspace_name: Workspace name
            record_type: Filter by "run" or "checkpoint"
            stage: Filter by stage name
            tagged: True for any tagged, or specific tag name
            sort_by: Sort by "created" or "metric"
            desc: Sort descending if True
            limit: Max records to return
            offset: Records to skip

        Returns:
            Dict with 'records' list
        """
        validate_workspace_name(workspace_name)

        # Build query dynamically based on filters
        if stage is not None:
            # Need JOIN to stage_runs
            query = """
                SELECT er.* FROM experiment_records er
                JOIN stage_runs sr ON er.stage_run_id = sr.id
                WHERE er.workspace_name = ?
            """
        elif tagged is True:
            # Need JOIN to run_tags
            query = """
                SELECT DISTINCT er.* FROM experiment_records er
                JOIN run_tags rt ON er.record_id = rt.record_id
                WHERE er.workspace_name = ?
            """
        elif isinstance(tagged, str):
            # Specific tag filter
            query = """
                SELECT er.* FROM experiment_records er
                JOIN run_tags rt ON er.record_id = rt.record_id
                WHERE er.workspace_name = ? AND rt.tag_name = ?
            """
        else:
            query = """
                SELECT * FROM experiment_records
                WHERE workspace_name = ?
            """

        params: list[Any] = [workspace_name]

        if isinstance(tagged, str):
            params.append(tagged)

        if record_type is not None:
            query += " AND type = ?"
            params.append(record_type)

        if stage is not None:
            query += " AND sr.stage_name = ?"
            params.append(stage)

        # Sorting
        order_dir = "DESC" if desc else "ASC"
        if sort_by == "created":
            # ULID is lexicographically sortable by time
            query += f" ORDER BY record_id {order_dir}"
        else:
            # Metric sorting would require JOIN to run_results
            # For now, default to created
            query += f" ORDER BY record_id {order_dir}"

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.db._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        records = [cast(ExperimentRecordRow, dict(row)) for row in rows]

        return {"records": records}

    def inspect_record(
        self,
        ref: str,
        include: list[str] | None = None,
        workspace_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Inspect an experiment record.

        Args:
            ref: Record ID, @tag reference, or stage_run_id
            include: List of sections to include (results, tags, comparison)
            workspace_name: Required when ref is a @tag

        Returns:
            Dict with record details and requested sections, or None if not found
        """
        if include is None:
            include = []

        # Resolve the record
        record = self._resolve_record_ref(ref, workspace_name)
        if record is None:
            return None

        # Build result from record
        result: dict[str, Any] = dict(record)

        # Include requested sections
        if "results" in include and record["stage_run_id"]:
            run_results = self.get_run_results(record["stage_run_id"])
            if run_results:
                result["results"] = dict(run_results)

        if "tags" in include:
            result["tags"] = self.get_record_tags(record["record_id"])

        if "comparison" in include and record["stage_run_id"]:
            result["comparison"] = self.get_comparison(record["stage_run_id"])

        return result

    def _resolve_record_ref(
        self,
        ref: str,
        workspace_name: str | None = None,
    ) -> ExperimentRecordRow | None:
        """Resolve a reference to a record.

        Supports:
        - record_id: Direct lookup
        - @tag: Tag reference (requires workspace_name)
        - stage_run_id: Lookup by stage run

        Args:
            ref: The reference to resolve
            workspace_name: Required for @tag resolution

        Returns:
            The record row or None
        """
        # Check for @tag reference
        if ref.startswith("@"):
            if workspace_name is None:
                return None
            tag_name = ref[1:]
            return self.get_record_by_tag(workspace_name, tag_name)

        # Try direct record_id lookup
        record = self.get_record(ref)
        if record is not None:
            return record

        # Try stage_run_id lookup
        return self.get_record_by_stage_run(ref)

    # Terminal infra outcomes that require finalization before new runs
    _TERMINAL_INFRA_OUTCOMES = {"completed", "preempted", "crashed", "canceled"}

    def is_terminal_infra_outcome(self, infra_outcome: str) -> bool:
        """Check if an infra_outcome is terminal.

        Terminal outcomes require finalization before new runs can proceed.

        Args:
            infra_outcome: The infra outcome to check

        Returns:
            True if terminal, False otherwise
        """
        return infra_outcome in self._TERMINAL_INFRA_OUTCOMES

    def list_unfinalized_runs(self, workspace_name: str) -> list[dict[str, Any]]:
        """List terminal infra runs that are not finalized.

        Args:
            workspace_name: Workspace name

        Returns:
            List of unfinalized run result dicts
        """
        validate_workspace_name(workspace_name)

        terminal_outcomes = tuple(self._TERMINAL_INFRA_OUTCOMES)

        with self.db._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT rr.* FROM run_results rr
                JOIN experiment_records er ON rr.record_id = er.record_id
                WHERE er.workspace_name = ?
                  AND rr.infra_outcome IN ({','.join('?' * len(terminal_outcomes))})
                  AND rr.results_status != 'finalized'
                """,
                (workspace_name, *terminal_outcomes),
            ).fetchall()

        return [dict(row) for row in rows]

    def check_finalization_gate(self, workspace_name: str) -> dict[str, Any]:
        """Check if new runs are blocked by unfinalized runs.

        Args:
            workspace_name: Workspace name

        Returns:
            Dict with 'blocked' bool and 'unfinalized' list
        """
        unfinalized = self.list_unfinalized_runs(workspace_name)

        return {
            "blocked": len(unfinalized) > 0,
            "unfinalized": unfinalized,
        }

    def get_experiment_context(self, workspace_name: str) -> dict[str, Any]:
        """Get experiment context for mount/dashboard.

        Returns context including:
        - current_best: Best tagged record info
        - awaiting_finalization: Records needing finalization
        - recent_trend: Recent finalized values

        Args:
            workspace_name: Workspace name

        Returns:
            Dict with experiment context
        """
        validate_workspace_name(workspace_name)

        # Get current best tagged record
        current_best = self.get_current_best(workspace_name)

        # Get runs awaiting finalization
        awaiting_finalization = self.list_unfinalized_runs(workspace_name)

        # Get recent trend
        recent_trend = self.get_recent_trend(workspace_name, limit=10)

        return {
            "current_best": current_best,
            "awaiting_finalization": awaiting_finalization,
            "recent_trend": recent_trend,
        }

    def get_recent_trend(
        self,
        workspace_name: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Get recent finalized values for trend display.

        Args:
            workspace_name: Workspace name
            limit: Max records to return

        Returns:
            List of dicts with record_id and value
        """
        validate_workspace_name(workspace_name)

        with self.db._conn() as conn:
            rows = conn.execute(
                """
                SELECT rr.record_id, rr.results_final
                FROM run_results rr
                JOIN experiment_records er ON rr.record_id = er.record_id
                WHERE er.workspace_name = ?
                  AND rr.results_status = 'finalized'
                  AND rr.results_final IS NOT NULL
                ORDER BY rr.finalized_at DESC
                LIMIT ?
                """,
                (workspace_name, limit),
            ).fetchall()

        trend = []
        for row in rows:
            results_final: dict[str, Any] = json.loads(row["results_final"])
            trend.append(
                {
                    "record_id": row["record_id"],
                    "value": results_final.get("value"),
                }
            )

        return trend

    def get_current_best(
        self,
        workspace_name: str,
        tag_prefix: str = "best-",
    ) -> dict[str, Any] | None:
        """Get current best tagged record.

        Looks for tags with the given prefix (default "best-").

        Args:
            workspace_name: Workspace name
            tag_prefix: Tag name prefix to search for

        Returns:
            Dict with record_id, tag, metric, value or None
        """
        validate_workspace_name(workspace_name)

        with self.db._conn() as conn:
            # Find a tag starting with the prefix
            tag_row = conn.execute(
                """
                SELECT tag_name, record_id FROM run_tags
                WHERE workspace_name = ? AND tag_name LIKE ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (workspace_name, f"{tag_prefix}%"),
            ).fetchone()

            if tag_row is None:
                return None

            # Get the finalized results
            results_row = conn.execute(
                """
                SELECT results_final FROM run_results
                WHERE record_id = ? AND results_status = 'finalized'
                """,
                (tag_row["record_id"],),
            ).fetchone()

        if results_row is None or results_row["results_final"] is None:
            return None

        results_final: dict[str, Any] = json.loads(results_row["results_final"])

        return {
            "record_id": tag_row["record_id"],
            "tag": tag_row["tag_name"],
            "metric": results_final.get("primary_metric"),
            "value": results_final.get("value"),
        }
