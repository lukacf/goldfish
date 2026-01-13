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
    # Get millisecond timestamp and mask to 48 bits per ULID spec
    timestamp_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF  # 48-bit mask

    # Encode timestamp into 10 base32 chars (48 bits = 10 chars at 5 bits each, with
    # the most significant char limited to 3 bits, values 0-7)
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
        experiment_group: str | None = None,
    ) -> str:
        """Create a run record linked to a stage run.

        Also initializes the associated run_results with status=missing.

        Args:
            workspace_name: Workspace name
            version: Workspace version
            stage_run_id: Stage run ID to link to
            experiment_group: Optional grouping for filtering

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
                (record_id, workspace_name, type, stage_run_id, version, experiment_group, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (record_id, workspace_name, "run", stage_run_id, version, experiment_group, created_at),
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

    def save_results_spec(
        self,
        stage_run_id: str,
        record_id: str,
        spec: dict[str, Any],
    ) -> None:
        """Save a results_spec for a run.

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

        Does NOT overwrite results_status if the run is already finalized.

        Args:
            stage_run_id: The stage run ID
            auto_results: The auto-extracted results
            run_status: The run status for deriving infra_outcome
        """
        results_auto_json = json.dumps(auto_results)

        # Fetch error text from stage_runs for preemption detection
        error_text: str | None = None
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT error FROM stage_runs WHERE id = ?",
                (stage_run_id,),
            ).fetchone()
            if row:
                error_text = row["error"]

        infra_outcome = self.derive_infra_outcome(run_status, error_text)

        with self.db._conn() as conn:
            # Only update results_status if not already finalized
            # This preserves the authoritative finalization status
            conn.execute(
                """
                UPDATE run_results
                SET results_auto = ?,
                    results_status = CASE WHEN results_status = 'finalized' THEN results_status ELSE ? END,
                    infra_outcome = ?
                WHERE stage_run_id = ?
                """,
                (results_auto_json, "auto", infra_outcome, stage_run_id),
            )

    def derive_infra_outcome(self, run_status: str, error_text: str | None = None) -> str:
        """Derive infra_outcome from run status and error text.

        Preemptions are stored as "failed" with error text containing
        preemption-related keywords, so we check the error text to detect them.

        Args:
            run_status: The run status string
            error_text: Optional error message to check for preemption keywords

        Returns:
            The infra_outcome value
        """
        # Check for preemption keywords in error text (GCE spot preemption messages)
        if run_status == "failed" and error_text:
            error_lower = error_text.lower()
            preemption_keywords = [
                "preempted",
                "preemption",
                "spot instance terminated",
                "instance was preempted",
                "compute.instances.preempted",
                "scheduling.preemptible",
            ]
            if any(keyword in error_lower for keyword in preemption_keywords):
                return "preempted"

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
    ) -> dict[str, Any]:
        """Finalize a run with authoritative ML results.

        This sets the results_final, results_status=finalized, and ml_outcome.
        Preserves results_auto unchanged.
        Computes and stores comparison block (vs_previous, vs_best).

        Args:
            stage_run_id_or_record_id: Either a stage_run_id or record_id
            results: The finalize results dict
            finalized_by: Who is finalizing (default: ml_claude)

        Returns:
            Dict with record_id, stage_run_id, results_status, and comparison

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

        # Get the record for workspace_name
        record = self.get_record_by_stage_run(stage_run_id)
        if record is None:
            raise ValueError(f"Record not found for stage_run_id: {stage_run_id}")

        workspace_name = record["workspace_name"]
        record_id = record["record_id"]

        # Get stage_name from stage_runs table
        with self.db._conn() as conn:
            stage_row = conn.execute(
                "SELECT stage_name FROM stage_runs WHERE id = ?",
                (stage_run_id,),
            ).fetchone()
        stage_name = stage_row["stage_name"] if stage_row else "unknown"

        # Compute comparison block
        comparison = self.compute_comparison(
            stage_run_id=stage_run_id,
            workspace_name=workspace_name,
            stage_name=stage_name,
            results=results,
        )

        comparison_json = json.dumps(comparison)

        with self.db._conn() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET results_final = ?,
                    results_status = ?,
                    ml_outcome = ?,
                    finalized_by = ?,
                    finalized_at = ?,
                    comparison = ?
                WHERE stage_run_id = ?
                """,
                (results_json, "finalized", ml_outcome, finalized_by, finalized_at, comparison_json, stage_run_id),
            )

        return {
            "record_id": record_id,
            "stage_run_id": stage_run_id,
            "results_status": "finalized",
            "comparison": comparison,
        }

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

        # Compute config_diff (comparing vs_previous if available)
        config_diff: dict[str, list[Any]] | None = None
        if vs_previous is not None:
            config_diff = self._compute_config_diff(
                current_stage_run_id=stage_run_id,
                baseline_record_id=vs_previous["record"],
            )

        return {
            "vs_previous": vs_previous,
            "vs_best": vs_best,
            "config_diff": config_diff,
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
        """Compute vs_best comparison using baseline_run from spec, or best tagged record.

        Resolution order:
        1. If baseline_run specified in spec, resolve it:
           - Tag reference (starting with @, e.g., "@best-25m")
           - record_id (ULID)
           - stage_run_id
        2. Else, fall back to best tagged record (tag starting with "best-")

        Args:
            stage_run_id: Current stage run ID
            current_value: Current result value

        Returns:
            Dict with record, tag, and delta, or None if no baseline
        """
        if current_value is None:
            return None

        # Get the current record to know the workspace
        current_record = self.get_record_by_stage_run(stage_run_id)
        if current_record is None:
            return None

        workspace_name = current_record["workspace_name"]

        # Get the spec to check for baseline_run
        spec = self.get_results_spec_parsed(stage_run_id)
        baseline_run = spec.get("baseline_run") if spec else None

        # Resolve baseline_run to a record
        baseline_record: ExperimentRecordRow | None = None
        tag_name: str | None = None

        if baseline_run is not None:
            # Explicit baseline_run in spec
            if baseline_run.startswith("@"):
                # Tag reference - look up by tag
                resolved_tag = baseline_run[1:]  # Remove @ prefix
                tag_name = resolved_tag
                baseline_record = self.get_record_by_tag(workspace_name, resolved_tag)
            else:
                # Try as record_id first, then as stage_run_id
                baseline_record = self.get_record(baseline_run)
                if baseline_record is None:
                    baseline_record = self.get_record_by_stage_run(baseline_run)
        else:
            # No explicit baseline_run - fall back to best tagged record
            best_record_info = self.get_current_best(workspace_name, tag_prefix="best-")
            if best_record_info is not None:
                baseline_record = self.get_record(best_record_info["record_id"])
                tag_name = best_record_info.get("tag")  # Key is "tag" not "tag_name"

        if baseline_record is None:
            return None

        # Get baseline results
        baseline_stage_run_id = baseline_record.get("stage_run_id")
        if baseline_stage_run_id is None:
            return None

        baseline_results = self.get_finalized_results(baseline_stage_run_id)
        if baseline_results is None:
            return None

        baseline_value = baseline_results.get("value")
        if baseline_value is None or not isinstance(baseline_value, int | float):
            return None

        delta = current_value - baseline_value

        result: dict[str, Any] = {
            "record": baseline_record["record_id"],
            "delta": round(delta, 6),  # Avoid floating point noise
        }

        # Include tag if resolved via tag reference
        if tag_name:
            result["tag"] = tag_name

        return result

    def _compute_config_diff(
        self,
        current_stage_run_id: str,
        baseline_record_id: str,
    ) -> dict[str, list[Any]] | None:
        """Compute config diff between current run and baseline.

        Only includes changed keys; truncates large values.

        Args:
            current_stage_run_id: Current stage run ID
            baseline_record_id: Baseline record ID to compare against

        Returns:
            Dict mapping changed keys to [old_value, new_value], or None if unable to compute
        """
        # Get baseline record and its stage_run_id
        baseline_record = self.get_record(baseline_record_id)
        if baseline_record is None:
            return None

        baseline_stage_run_id = baseline_record.get("stage_run_id")
        if baseline_stage_run_id is None:
            return None

        # Get configs from stage_runs
        with self.db._conn() as conn:
            current_row = conn.execute(
                "SELECT config_json FROM stage_runs WHERE id = ?",
                (current_stage_run_id,),
            ).fetchone()

            baseline_row = conn.execute(
                "SELECT config_json FROM stage_runs WHERE id = ?",
                (baseline_stage_run_id,),
            ).fetchone()

        if current_row is None or baseline_row is None:
            return None

        current_config_json = current_row["config_json"]
        baseline_config_json = baseline_row["config_json"]

        if current_config_json is None or baseline_config_json is None:
            return None

        current_config: dict[str, Any] = json.loads(current_config_json)
        baseline_config: dict[str, Any] = json.loads(baseline_config_json)

        # Find changed keys
        diff: dict[str, list[Any]] = {}
        all_keys = set(current_config.keys()) | set(baseline_config.keys())

        for key in all_keys:
            current_val = current_config.get(key)
            baseline_val = baseline_config.get(key)

            if current_val != baseline_val:
                # Truncate large values
                diff[key] = [
                    self._truncate_value(baseline_val),
                    self._truncate_value(current_val),
                ]

        return diff if diff else None

    def _truncate_value(self, value: Any, max_len: int = 50) -> Any:
        """Truncate a value for config_diff display.

        Args:
            value: Value to potentially truncate
            max_len: Maximum string length

        Returns:
            Truncated value
        """
        if value is None:
            return None

        if isinstance(value, str) and len(value) > max_len:
            return value[:max_len] + "..."

        if isinstance(value, list | dict):
            as_str = json.dumps(value)
            if len(as_str) > max_len:
                return as_str[:max_len] + "..."

        return value

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

    def tag_record(self, ref: str, tag: str) -> dict[str, Any]:
        """Tag an experiment record.

        For run records, creates BOTH a run tag AND a version tag.
        For checkpoint records, creates only a version tag.
        Tag uniqueness is enforced per workspace across both tables.

        Args:
            ref: Record reference - can be record_id or stage_run_id (stage-*)
            tag: The tag name

        Returns:
            Dict with tag confirmation: record_id, tag, workspace_name, record_type

        Raises:
            ValueError: If tag is empty, record not found, or tag already exists
        """
        # Validate tag name
        if not tag or not tag.strip():
            raise ValueError("Tag name is invalid: cannot be empty")

        # Get the record - handle both record_id and stage_run_id
        record = None
        if ref.startswith("stage-"):
            record = self.get_record_by_stage_run(ref)
        else:
            record = self.get_record(ref)

        if record is None:
            raise ValueError(f"Record not found: {ref}")

        workspace_name = record["workspace_name"]
        version = record["version"]
        record_type = record["type"]
        created_at = datetime.now(UTC).isoformat()

        # Check for tag uniqueness across BOTH run_tags and workspace_version_tags
        with self.db._conn() as conn:
            # Check run_tags
            existing_run_tag = conn.execute(
                """
                SELECT 1 FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

            if existing_run_tag:
                raise ValueError(f"Tag '{tag}' already exists in workspace '{workspace_name}'")

            # Check workspace_version_tags
            existing_version_tag = conn.execute(
                """
                SELECT 1 FROM workspace_version_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

            if existing_version_tag:
                raise ValueError(f"Tag '{tag}' already exists in workspace '{workspace_name}'")

            # For run records: create BOTH run tag AND version tag
            # For checkpoint records: create ONLY version tag
            if record_type == "run":
                # Create run tag
                conn.execute(
                    """
                    INSERT INTO run_tags (workspace_name, record_id, tag_name, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (workspace_name, record["record_id"], tag, created_at),
                )

            # Always create version tag (for both run and checkpoint)
            conn.execute(
                """
                INSERT INTO workspace_version_tags (workspace_name, version, tag_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (workspace_name, version, tag, created_at),
            )

        return {
            "record_id": record["record_id"],
            "tag": tag,
            "workspace_name": workspace_name,
            "record_type": record_type,
        }

    def get_record_tags(self, record_id: str) -> list[str]:
        """Get all tags for a record (merged from run_tags and workspace_version_tags).

        For run records, tags exist in both tables.
        For checkpoint records, tags only exist in workspace_version_tags.

        Args:
            record_id: The record ID

        Returns:
            List of tag names (merged, deduplicated)
        """
        # Get the record to know workspace and version
        record = self.get_record(record_id)
        if record is None:
            return []

        workspace_name = record["workspace_name"]
        version = record["version"]

        with self.db._conn() as conn:
            # Get tags from run_tags (for run records)
            run_tag_rows = conn.execute(
                "SELECT tag_name FROM run_tags WHERE record_id = ?",
                (record_id,),
            ).fetchall()

            # Get tags from workspace_version_tags (for the record's version)
            version_tag_rows = conn.execute(
                "SELECT tag_name FROM workspace_version_tags WHERE workspace_name = ? AND version = ?",
                (workspace_name, version),
            ).fetchall()

        # Merge and deduplicate
        run_tags = {row["tag_name"] for row in run_tag_rows}
        version_tags = {row["tag_name"] for row in version_tag_rows}
        all_tags = run_tags | version_tags

        return sorted(all_tags)

    def get_record_by_tag(self, workspace_name: str, tag: str) -> ExperimentRecordRow | None:
        """Look up a record by its tag.

        Searches both run_tags (for run records) and workspace_version_tags (for checkpoints).

        Args:
            workspace_name: Workspace name
            tag: Tag name

        Returns:
            The record row or None if tag not found

        Raises:
            ValidationError: If workspace_name is invalid
            ValueError: If tag name is invalid
        """
        validate_workspace_name(workspace_name)

        if not tag or not tag.strip():
            raise ValueError("Tag name is invalid: cannot be empty")

        with self.db._conn() as conn:
            # First try run_tags (for run records)
            tag_row = conn.execute(
                """
                SELECT record_id FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

            if tag_row is not None:
                return self.get_record(tag_row["record_id"])

            # Fall back to workspace_version_tags (for checkpoint records)
            version_tag_row = conn.execute(
                """
                SELECT er.record_id FROM workspace_version_tags vt
                JOIN experiment_records er ON er.workspace_name = vt.workspace_name AND er.version = vt.version
                WHERE vt.workspace_name = ? AND vt.tag_name = ?
                """,
                (workspace_name, tag),
            ).fetchone()

            if version_tag_row is not None:
                return self.get_record(version_tag_row["record_id"])

        return None

    def remove_tag(self, workspace_name: str, tag: str) -> None:
        """Remove a tag from a workspace.

        Removes both run_tags and workspace_version_tags entries.

        Args:
            workspace_name: Workspace name
            tag: Tag name to remove

        Raises:
            ValidationError: If workspace_name is invalid
            ValueError: If tag name is invalid
        """
        validate_workspace_name(workspace_name)

        if not tag or not tag.strip():
            raise ValueError("Tag name is invalid: cannot be empty")

        with self.db._conn() as conn:
            # Remove from run_tags
            conn.execute(
                """
                DELETE FROM run_tags
                WHERE workspace_name = ? AND tag_name = ?
                """,
                (workspace_name, tag),
            )
            # Remove from workspace_version_tags
            conn.execute(
                """
                DELETE FROM workspace_version_tags
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
        metric: str | None = None,
        min_value: float | None = None,
        experiment_group: str | None = None,
        sort_by: Literal["created", "metric"] = "created",
        desc: bool = True,
        include_pruned: bool = False,
        include_internal_ids: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List experiment history (runs + checkpoints).

        Args:
            workspace_name: Workspace name
            record_type: Filter by "run" or "checkpoint"
            stage: Filter by stage name
            tagged: True for any tagged, or specific tag name
            metric: Filter by specific metric name
            min_value: Filter by minimum metric value
            experiment_group: Filter by experiment group
            sort_by: Sort by "created" or "metric"
            desc: Sort descending if True
            include_pruned: Include pruned records (default False)
            include_internal_ids: Include internal IDs in response (default False)
            limit: Max records to return
            offset: Records to skip

        Returns:
            Dict with 'records' list
        """
        validate_workspace_name(workspace_name)

        # Determine if we need metric filtering (requires JOIN to run_results)
        needs_metric_join = metric is not None or min_value is not None

        # Build query dynamically based on filters
        if stage is not None:
            # Need JOIN to stage_runs
            query = """
                SELECT er.* FROM experiment_records er
                JOIN stage_runs sr ON er.stage_run_id = sr.id
                WHERE er.workspace_name = ?
            """
        elif tagged is True:
            # For runs: match via run_tags (direct tag on record)
            # For runs without run_tags: match via version_tags if no run_tag exists for that tag
            # For checkpoints: match via version_tags
            query = """
                SELECT DISTINCT er.* FROM experiment_records er
                WHERE er.workspace_name = ?
                AND (
                    -- Records with direct run_tags
                    er.record_id IN (SELECT record_id FROM run_tags WHERE workspace_name = er.workspace_name)
                    OR (
                        -- Records on tagged versions where the tag is NOT from a run_tag
                        -- (i.e., tag was created via manage_versions only)
                        EXISTS (
                            SELECT 1 FROM workspace_version_tags vt
                            WHERE vt.workspace_name = er.workspace_name
                              AND vt.version = er.version
                              AND NOT EXISTS (
                                  SELECT 1 FROM run_tags rt
                                  WHERE rt.workspace_name = vt.workspace_name AND rt.tag_name = vt.tag_name
                              )
                        )
                    )
                )
            """
        elif isinstance(tagged, str):
            # Specific tag filter:
            # 1. Match records with that run_tag directly
            # 2. Match records on versions with that version_tag, but only if no run_tag exists
            #    for that tag (backwards compat for tags created via manage_versions)
            query = """
                SELECT DISTINCT er.* FROM experiment_records er
                WHERE er.workspace_name = ?
                AND (
                    -- Records with this specific run_tag
                    er.record_id IN (SELECT record_id FROM run_tags WHERE workspace_name = er.workspace_name AND tag_name = ?)
                    OR (
                        -- Records on versions with this tag, but only if tag was NOT created via tag_record
                        EXISTS (
                            SELECT 1 FROM workspace_version_tags vt
                            WHERE vt.workspace_name = er.workspace_name
                              AND vt.version = er.version
                              AND vt.tag_name = ?
                              AND NOT EXISTS (
                                  SELECT 1 FROM run_tags rt
                                  WHERE rt.workspace_name = vt.workspace_name AND rt.tag_name = vt.tag_name
                              )
                        )
                    )
                )
            """
        else:
            query = """
                SELECT er.* FROM experiment_records er
                WHERE er.workspace_name = ?
            """

        params: list[Any] = [workspace_name]

        if isinstance(tagged, str):
            # Tag appears twice in the query (once for each subquery)
            params.append(tagged)
            params.append(tagged)

        if record_type is not None:
            query += " AND er.type = ?"
            params.append(record_type)

        if stage is not None:
            query += " AND sr.stage_name = ?"
            params.append(stage)

        # Experiment group filtering
        if experiment_group is not None:
            query += " AND er.experiment_group = ?"
            params.append(experiment_group)

        # Include pruned filtering (via workspace_versions)
        if not include_pruned:
            query += " AND NOT EXISTS (SELECT 1 FROM workspace_versions wv WHERE wv.workspace_name = er.workspace_name AND wv.version = er.version AND wv.pruned_at IS NOT NULL)"

        # Metric filtering (requires JOIN to run_results)
        if needs_metric_join:
            # Add JOIN to run_results for metric filtering
            # This only applies to run records (checkpoint records don't have results)
            query += """
                AND er.type = 'run'
                AND EXISTS (
                    SELECT 1 FROM run_results rr
                    WHERE rr.stage_run_id = er.stage_run_id
            """
            if metric is not None:
                # Filter by metric name in either results_final or results_auto
                query += """
                    AND (
                        json_extract(rr.results_final, '$.primary_metric') = ?
                        OR json_extract(rr.results_auto, '$.primary_metric') = ?
                    )
                """
                params.append(metric)
                params.append(metric)
            if min_value is not None:
                # Filter by min value in either results_final or results_auto
                query += """
                    AND (
                        CAST(json_extract(rr.results_final, '$.value') AS REAL) >= ?
                        OR CAST(json_extract(rr.results_auto, '$.value') AS REAL) >= ?
                    )
                """
                params.append(min_value)
                params.append(min_value)
            query += ")"

        # Sorting
        order_dir = "DESC" if desc else "ASC"
        if sort_by == "metric":
            # Sort by metric value from run_results
            # Use COALESCE to handle both results_final and results_auto
            # Note: This sorts only run records; checkpoints have no metric
            query += f"""
                ORDER BY (
                    SELECT COALESCE(
                        CAST(json_extract(rr.results_final, '$.value') AS REAL),
                        CAST(json_extract(rr.results_auto, '$.value') AS REAL)
                    )
                    FROM run_results rr
                    WHERE rr.stage_run_id = er.stage_run_id
                ) {order_dir} NULLS LAST, er.record_id {order_dir}
            """
        else:
            # Default to created (ULID is lexicographically sortable by time)
            query += f" ORDER BY er.record_id {order_dir}"

        # First get total count (without LIMIT/OFFSET)
        count_query = query.replace("SELECT DISTINCT er.*", "SELECT COUNT(DISTINCT er.record_id) as cnt")
        count_query = count_query.replace("SELECT er.*", "SELECT COUNT(er.record_id) as cnt")
        # Remove ORDER BY clause for count query
        if " ORDER BY " in count_query:
            count_query = count_query[: count_query.index(" ORDER BY ")]

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.db._conn() as conn:
            # Get total count first (params without limit/offset)
            count_params = params[:-2]  # Remove limit/offset
            count_row = conn.execute(count_query, count_params).fetchone()
            # Ensure total_count is an int (handles mock scenarios in tests)
            total_count = 0
            if count_row:
                cnt_val = count_row["cnt"]
                if isinstance(cnt_val, int):
                    total_count = cnt_val

            rows = conn.execute(query, params).fetchall()

        # Enrich records with tags, results_status, ml_outcome, stage
        records: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)

            # Enrich with tags (merged from run_tags and workspace_version_tags)
            # Note: We inline this instead of calling get_record_tags to avoid
            # redundant lookups since we already have workspace_name and version
            record_id = record["record_id"]
            ws_name = record["workspace_name"]
            ver = record.get("version")
            with self.db._conn() as conn_tags:
                run_tag_rows = conn_tags.execute(
                    "SELECT tag_name FROM run_tags WHERE record_id = ?",
                    (record_id,),
                ).fetchall()
                if ver:
                    version_tag_rows = conn_tags.execute(
                        "SELECT tag_name FROM workspace_version_tags WHERE workspace_name = ? AND version = ?",
                        (ws_name, ver),
                    ).fetchall()
                else:
                    version_tag_rows = []
            run_tags = {r["tag_name"] for r in run_tag_rows}
            version_tags = {r["tag_name"] for r in version_tag_rows}
            record["tags"] = sorted(run_tags | version_tags)

            # For run records, enrich with results_status, ml_outcome, stage
            stage_run_id = record.get("stage_run_id")
            if stage_run_id is not None and record["type"] == "run":
                # Get run results
                run_results = self.get_run_results(stage_run_id)
                if run_results:
                    record["results_status"] = run_results.get("results_status")
                    record["ml_outcome"] = run_results.get("ml_outcome")
                else:
                    record["results_status"] = "missing"
                    record["ml_outcome"] = "unknown"

                # Get stage name
                with self.db._conn() as conn2:
                    stage_row = conn2.execute(
                        "SELECT stage_name FROM stage_runs WHERE id = ?",
                        (stage_run_id,),
                    ).fetchone()
                    if stage_row:
                        record["stage"] = stage_row["stage_name"]

                # Include internal_ids if requested
                if include_internal_ids:
                    record["stage_run_id"] = stage_run_id
                else:
                    # Remove internal ID from output
                    record.pop("stage_run_id", None)
            else:
                # Checkpoint records don't have results/stage
                if not include_internal_ids:
                    record.pop("stage_run_id", None)

            records.append(record)

        return {
            "records": records,
            "total": total_count,
            "has_more": offset + len(records) < total_count,
        }

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
                # Parse JSON fields
                parsed_results: dict[str, Any] = dict(run_results)
                results_auto = parsed_results.get("results_auto")
                if results_auto and isinstance(results_auto, str):
                    try:
                        parsed_results["results_auto"] = json.loads(results_auto)
                    except (json.JSONDecodeError, TypeError):
                        pass  # Keep as string if parsing fails
                results_final = parsed_results.get("results_final")
                if results_final and isinstance(results_final, str):
                    try:
                        parsed_results["results_final"] = json.loads(results_final)
                    except (json.JSONDecodeError, TypeError):
                        pass
                comparison_val = parsed_results.get("comparison")
                if comparison_val and isinstance(comparison_val, str):
                    try:
                        parsed_results["comparison"] = json.loads(comparison_val)
                    except (json.JSONDecodeError, TypeError):
                        pass
                result["results"] = parsed_results

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
            List of unfinalized run result dicts with:
            - record_id, stage_run_id, infra_outcome, results_status
            - stage_name (from stage_runs)
        """
        validate_workspace_name(workspace_name)

        terminal_outcomes = list(self._TERMINAL_INFRA_OUTCOMES)
        # Build parameterized placeholders for IN clause
        placeholders = ", ".join("?" for _ in terminal_outcomes)

        query = f"""
            SELECT rr.*, sr.stage_name FROM run_results rr
            JOIN experiment_records er ON rr.record_id = er.record_id
            JOIN stage_runs sr ON rr.stage_run_id = sr.id
            WHERE er.workspace_name = ?
              AND rr.infra_outcome IN ({placeholders})
              AND rr.results_status != 'finalized'
        """

        with self.db._conn() as conn:
            rows = conn.execute(
                query,
                [workspace_name, *terminal_outcomes],
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

        # Compute regression alerts (comparing recent trend to best)
        regression_alerts = self._compute_regression_alerts(
            current_best=current_best,
            recent_trend=recent_trend,
        )

        return {
            "current_best": current_best,
            "awaiting_finalization": awaiting_finalization,
            "recent_trend": recent_trend,
            "regression_alerts": regression_alerts,
        }

    def _compute_regression_alerts(
        self,
        current_best: dict[str, Any] | None,
        recent_trend: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Compute regression alerts by comparing recent runs to best.

        Args:
            current_best: Current best record info
            recent_trend: Recent finalized runs

        Returns:
            List of regression alert dicts
        """
        if current_best is None or not recent_trend:
            return []

        best_value = current_best.get("value")
        best_metric = current_best.get("metric")
        if best_value is None:
            return []

        alerts: list[dict[str, Any]] = []
        for entry in recent_trend:
            value = entry.get("value")
            if value is None or not isinstance(value, int | float):
                continue

            # Only compare if metrics match
            entry_metric = entry.get("primary_metric")
            if entry_metric != best_metric:
                continue

            # Get direction and tolerance from the entry
            direction = entry.get("direction", "maximize")
            tolerance = entry.get("tolerance")
            if tolerance is None:
                tolerance = 0.01  # Default tolerance

            # Compute delta
            delta = value - best_value

            # Check for regression based on direction
            # For "maximize": regression is when value < best_value - tolerance
            # For "minimize": regression is when value > best_value + tolerance
            is_regression = False
            if direction == "maximize" and delta < -tolerance:
                is_regression = True
            elif direction == "minimize" and delta > tolerance:
                is_regression = True

            if is_regression:
                alerts.append(
                    {
                        "record_id": entry.get("record_id"),
                        "value": value,
                        "best_value": best_value,
                        "delta": round(delta, 6),
                        "direction": direction,
                    }
                )

        return alerts

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
                    "primary_metric": results_final.get("primary_metric"),
                    "direction": results_final.get("direction", "maximize"),
                    "tolerance": results_final.get("tolerance"),
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
        Checks both run_tags and workspace_version_tags for backwards compatibility.

        Args:
            workspace_name: Workspace name
            tag_prefix: Tag name prefix to search for

        Returns:
            Dict with record_id, tag, metric, value or None
        """
        validate_workspace_name(workspace_name)

        tag_name: str | None = None
        record_id: str | None = None

        with self.db._conn() as conn:
            # First try run_tags
            tag_row = conn.execute(
                """
                SELECT tag_name, record_id FROM run_tags
                WHERE workspace_name = ? AND tag_name LIKE ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (workspace_name, f"{tag_prefix}%"),
            ).fetchone()

            if tag_row:
                tag_name = tag_row["tag_name"]
                record_id = tag_row["record_id"]
            else:
                # Fall back to workspace_version_tags and resolve to record
                version_tag_row = conn.execute(
                    """
                    SELECT vt.tag_name, er.record_id
                    FROM workspace_version_tags vt
                    JOIN experiment_records er
                        ON er.workspace_name = vt.workspace_name AND er.version = vt.version
                    WHERE vt.workspace_name = ? AND vt.tag_name LIKE ?
                    ORDER BY vt.created_at DESC
                    LIMIT 1
                    """,
                    (workspace_name, f"{tag_prefix}%"),
                ).fetchone()

                if version_tag_row:
                    tag_name = version_tag_row["tag_name"]
                    record_id = version_tag_row["record_id"]

            if record_id is None:
                return None

            # Get the finalized results
            results_row = conn.execute(
                """
                SELECT results_final FROM run_results
                WHERE record_id = ? AND results_status = 'finalized'
                """,
                (record_id,),
            ).fetchone()

        if results_row is None or results_row["results_final"] is None:
            return None

        results_final: dict[str, Any] = json.loads(results_row["results_final"])

        return {
            "record_id": record_id,
            "tag": tag_name,
            "metric": results_final.get("primary_metric"),
            "value": results_final.get("value"),
        }
