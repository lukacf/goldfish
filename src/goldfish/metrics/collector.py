"""Metrics collector for post-run database storage.

Reads metrics.jsonl from GCS after stage completion and populates the database.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from goldfish.db.database import Database
from goldfish.metrics.utils import normalize_metric_step, normalize_metric_timestamp, normalize_metric_value
from goldfish.validation import (
    ValidationError,
    validate_artifact_path,
    validate_metric_name,
    validate_metric_value,
)

logger = logging.getLogger(__name__)

# Security limits for metrics collection
MAX_METRICS_FILE_SIZE = 100 * 1024 * 1024  # 100MB
MAX_METRICS_LINES = 1_000_000  # 1M lines
BATCH_SIZE = 1000  # Process in chunks to avoid memory spikes
MAX_ERROR_MESSAGES = 1000  # Cap error list to avoid memory blowups


def _read_max_metric_names() -> int:
    env_val = os.environ.get("GOLDFISH_METRICS_MAX_NAMES")
    if not env_val:
        return 10000
    try:
        limit = int(env_val)
    except ValueError:
        return 10000
    return max(1, min(100000, limit))


MAX_METRIC_NAMES_PER_RUN = _read_max_metric_names()


class _CollectionAbort(Exception):
    """Internal exception to abort collection and trigger rollback."""


@dataclass
class CollectionResult:
    """Result of metrics collection with error visibility."""

    metrics_count: int = 0
    artifacts_count: int = 0
    skipped_count: int = 0
    errors: list[str] = field(default_factory=list)
    errors_truncated: bool = False


class MetricsCollector:
    """Collect metrics from JSONL files and store in database."""

    def __init__(self, db: Database):
        self.db = db

    def collect_from_file(self, stage_run_id: str, metrics_file: Path) -> CollectionResult:
        """Collect metrics from a JSONL file and store in database.

        Streams the file in chunks to avoid memory spikes with large files.
        Enforces security limits on file size and line count.

        Args:
            stage_run_id: Stage run ID
            metrics_file: Path to metrics.jsonl file

        Returns:
            CollectionResult with collection stats and any validation errors
        """
        result = CollectionResult()

        if not metrics_file.exists():
            logger.debug(f"No metrics file found for {stage_run_id}: {metrics_file}")
            return result

        # Security check: File size limit (prevents memory exhaustion)
        try:
            file_size = metrics_file.stat().st_size
            if file_size > MAX_METRICS_FILE_SIZE:
                error_msg = f"Metrics file too large: {file_size} bytes (max {MAX_METRICS_FILE_SIZE})"
                logger.error(error_msg)
                self._record_error(result, error_msg)
                return result
        except OSError as e:
            logger.error("Cannot stat metrics file for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Cannot read metrics file")
            return result

        # Stream file in chunks to avoid loading all into memory
        metrics_batch: list[dict] = []
        artifacts_batch: list[dict] = []
        step_modes: dict[str, str] = {}
        metric_names: set[str] = set()
        validated_names: set[str] = set()
        line_count = 0

        try:
            with self.db.transaction() as conn:
                with open(metrics_file) as f:
                    for line in f:
                        line_count += 1

                        # Security check: Line count limit
                        if line_count > MAX_METRICS_LINES:
                            error_msg = f"Metrics file exceeds max lines: {MAX_METRICS_LINES}"
                            logger.error(error_msg)
                            self._record_error(result, error_msg)
                            raise _CollectionAbort()

                        line = line.strip()
                        if not line:
                            continue

                        entry = self._parse_entry(line, result)
                        if entry is None:
                            continue

                        entry_type = entry.get("type")

                        if entry_type == "metric":
                            self._process_metric_entry(
                                entry,
                                metrics_batch,
                                result,
                                step_modes,
                                metric_names,
                                validated_names,
                            )
                        elif entry_type == "artifact":
                            self._process_artifact_entry(entry, artifacts_batch, result)
                        elif entry_type is None:
                            # Old format without "type" field - infer type from fields present
                            self._process_legacy_entry(
                                entry,
                                metrics_batch,
                                artifacts_batch,
                                result,
                                step_modes,
                                metric_names,
                                validated_names,
                            )
                        else:
                            result.skipped_count += 1
                            self._record_error(result, f"Unknown entry type: {entry_type}")
                            logger.warning("Skipping entry with unknown type: %s", entry_type)

                        # Flush metrics batch when it reaches BATCH_SIZE (streaming)
                        if len(metrics_batch) >= BATCH_SIZE:
                            inserted = self.db._batch_insert_metrics_conn(
                                conn, stage_run_id, metrics_batch, update_summary=False
                            )
                            result.metrics_count += inserted
                            metrics_batch.clear()

                # Insert remaining metrics
                if metrics_batch:
                    inserted = self.db._batch_insert_metrics_conn(
                        conn, stage_run_id, metrics_batch, update_summary=False
                    )
                    result.metrics_count += inserted

                # Insert all artifacts (typically smaller, no need for chunking)
                if artifacts_batch:
                    result.artifacts_count = self.db._batch_insert_artifacts_conn(conn, stage_run_id, artifacts_batch)

                # Rebuild summary once for the run (O(n) instead of O(n^2))
                if result.metrics_count > 0:
                    self.db._rebuild_metrics_summary_conn(conn, stage_run_id)

            logger.info(
                f"Collected metrics for {stage_run_id}: "
                f"{result.metrics_count} metrics, {result.artifacts_count} artifacts"
                + (f", {result.skipped_count} skipped" if result.skipped_count > 0 else "")
            )

            self._log_audit(stage_run_id, result)
            return result

        except _CollectionAbort:
            # Transaction rolled back; clear counts
            result.metrics_count = 0
            result.artifacts_count = 0
            self._log_audit(stage_run_id, result)
            return result
        except OSError as e:
            logger.error("I/O error reading metrics file for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "I/O error reading metrics file")
            self._log_audit(stage_run_id, result)
            return result
        except sqlite3.IntegrityError as e:
            logger.error("Database integrity error for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Database integrity error storing metrics")
            self._log_audit(stage_run_id, result)
            return result
        except sqlite3.Error as e:
            logger.error("Database error for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Database error storing metrics")
            self._log_audit(stage_run_id, result)
            return result
        except Exception as e:
            logger.error("Unexpected error collecting metrics for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Unexpected error collecting metrics")
            self._log_audit(stage_run_id, result)
            return result

    def collect_from_file_incremental(
        self,
        stage_run_id: str,
        metrics_file: Path,
        start_offset: int = 0,
        *,
        step_modes: dict[str, str] | None = None,
        metric_names: set[str] | None = None,
        validated_names: set[str] | None = None,
    ) -> tuple[CollectionResult, int]:
        """Incrementally collect new metrics from a JSONL file.

        Reads from start_offset (byte offset) and only ingests complete lines.

        Returns:
            (CollectionResult, new_offset)
        """
        result = CollectionResult()

        if not metrics_file.exists():
            logger.debug("No metrics file found for %s: %s", stage_run_id, metrics_file)
            return result, start_offset

        try:
            file_size = metrics_file.stat().st_size
            if file_size > MAX_METRICS_FILE_SIZE:
                error_msg = f"Metrics file too large: {file_size} bytes (max {MAX_METRICS_FILE_SIZE})"
                logger.error(error_msg)
                self._record_error(result, error_msg)
                return result, start_offset
        except OSError as e:
            logger.error("Cannot stat metrics file for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Cannot read metrics file")
            return result, start_offset

        if file_size < start_offset:
            start_offset = 0

        if file_size == start_offset:
            return result, start_offset

        metrics_batch: list[dict] = []
        artifacts_batch: list[dict] = []
        step_modes = step_modes if step_modes is not None else {}
        metric_names = metric_names if metric_names is not None else set()
        validated_names = validated_names if validated_names is not None else set()
        line_count = 0
        new_offset = start_offset

        try:
            with self.db.transaction() as conn:
                with open(metrics_file, "rb") as f:
                    f.seek(start_offset)

                    while True:
                        line_start = f.tell()
                        line = f.readline()
                        if not line:
                            new_offset = f.tell()
                            break
                        if not line.endswith(b"\n"):
                            new_offset = line_start
                            break

                        new_offset = f.tell()
                        line_count += 1

                        if line_count > MAX_METRICS_LINES:
                            error_msg = f"Metrics file exceeds max lines: {MAX_METRICS_LINES}"
                            logger.error(error_msg)
                            self._record_error(result, error_msg)
                            raise _CollectionAbort()

                        line_str = line.decode("utf-8", errors="replace").strip()
                        if not line_str:
                            continue

                        entry = self._parse_entry(line_str, result)
                        if entry is None:
                            continue

                        entry_type = entry.get("type")

                        if entry_type == "metric":
                            self._process_metric_entry(
                                entry,
                                metrics_batch,
                                result,
                                step_modes,
                                metric_names,
                                validated_names,
                            )
                        elif entry_type == "artifact":
                            self._process_artifact_entry(entry, artifacts_batch, result)
                        elif entry_type is None:
                            self._process_legacy_entry(
                                entry,
                                metrics_batch,
                                artifacts_batch,
                                result,
                                step_modes,
                                metric_names,
                                validated_names,
                            )
                        else:
                            result.skipped_count += 1
                            self._record_error(result, f"Unknown entry type: {entry_type}")
                            logger.warning("Skipping entry with unknown type: %s", entry_type)

                        if len(metrics_batch) >= BATCH_SIZE:
                            inserted = self.db._batch_insert_metrics_conn(
                                conn, stage_run_id, metrics_batch, update_summary=True
                            )
                            result.metrics_count += inserted
                            metrics_batch.clear()

                if metrics_batch:
                    inserted = self.db._batch_insert_metrics_conn(
                        conn, stage_run_id, metrics_batch, update_summary=True
                    )
                    result.metrics_count += inserted

                if artifacts_batch:
                    result.artifacts_count = self.db._batch_insert_artifacts_conn(conn, stage_run_id, artifacts_batch)

            if result.metrics_count or result.artifacts_count:
                logger.info(
                    "Incremental metrics sync for %s: %s metrics, %s artifacts",
                    stage_run_id,
                    result.metrics_count,
                    result.artifacts_count,
                )

            self._log_audit(stage_run_id, result)
            return result, new_offset

        except _CollectionAbort:
            result.metrics_count = 0
            result.artifacts_count = 0
            self._log_audit(stage_run_id, result)
            return result, start_offset
        except OSError as e:
            logger.error("I/O error reading metrics file for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "I/O error reading metrics file")
            self._log_audit(stage_run_id, result)
            return result, start_offset
        except sqlite3.Error as e:
            logger.error("Database error for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Database error storing metrics")
            self._log_audit(stage_run_id, result)
            return result, start_offset
        except Exception as e:
            logger.error("Unexpected error collecting metrics for %s: %s", stage_run_id, type(e).__name__)
            self._record_error(result, "Unexpected error collecting metrics")
            self._log_audit(stage_run_id, result)
            return result, start_offset

    def _log_audit(self, stage_run_id: str, result: CollectionResult) -> None:
        """Best-effort audit logging for metrics collection."""
        try:
            row = self.db.get_stage_run(stage_run_id)
            if row:
                self.db.log_audit(
                    operation="collect_metrics",
                    reason="metrics collection",
                    workspace=row.get("workspace_name"),
                    details={
                        "run_id": stage_run_id,
                        "metrics_count": result.metrics_count,
                        "artifacts_count": result.artifacts_count,
                        "skipped_count": result.skipped_count,
                    },
                )
        except Exception:
            logger.warning("Failed to log audit entry for metrics collection", exc_info=True)

    def _parse_entry(self, line: str, result: CollectionResult) -> dict | None:
        """Parse a JSON line, returning None on error."""
        try:
            entry: dict = json.loads(line)
            return entry
        except json.JSONDecodeError as e:
            result.skipped_count += 1
            self._record_error(result, f"Invalid JSON: {e}")
            logger.warning(f"Skipping invalid JSON entry: {e}")
            return None

    def _process_metric_entry(
        self,
        entry: dict,
        metrics_batch: list[dict],
        result: CollectionResult,
        step_modes: dict[str, str],
        metric_names: set[str],
        validated_names: set[str],
    ) -> None:
        """Process a metric entry with explicit type."""
        if "name" not in entry or "value" not in entry:
            result.skipped_count += 1
            self._record_error(result, "Metric entry missing required fields")
            logger.warning("Skipping metric entry missing required fields")
            return

        try:
            if entry["name"] not in validated_names:
                validate_metric_name(entry["name"])
                validated_names.add(entry["name"])
            value = normalize_metric_value(entry["value"])
            validate_metric_value(value)
            entry["step"] = normalize_metric_step(entry.get("step"))
            if not self._ensure_metric_name_limit(entry["name"], metric_names, result):
                return
            if not self._ensure_step_mode(entry["name"], entry["step"], step_modes, result):
                return
            entry["value"] = value
            entry["timestamp"] = normalize_metric_timestamp(entry.get("timestamp"))
            metrics_batch.append(entry)
        except ValidationError as e:
            result.skipped_count += 1
            self._record_error(result, str(e))
            logger.warning(f"Skipping invalid metric: {e}")

    def _process_artifact_entry(self, entry: dict, artifacts_batch: list[dict], result: CollectionResult) -> None:
        """Process an artifact entry with explicit type."""
        if "name" not in entry or "path" not in entry:
            result.skipped_count += 1
            self._record_error(result, "Artifact entry missing required fields")
            logger.warning("Skipping artifact entry missing required fields")
            return

        try:
            validate_metric_name(entry["name"])
            validate_artifact_path(entry["path"])
            timestamp = entry.get("timestamp")
            if timestamp is not None:
                entry["timestamp"] = normalize_metric_timestamp(timestamp)
            artifacts_batch.append(entry)
        except ValidationError as e:
            result.skipped_count += 1
            self._record_error(result, str(e))
            logger.warning(f"Skipping invalid artifact: {e}")

    def _process_legacy_entry(
        self,
        entry: dict,
        metrics_batch: list[dict],
        artifacts_batch: list[dict],
        result: CollectionResult,
        step_modes: dict[str, str],
        metric_names: set[str],
        validated_names: set[str],
    ) -> None:
        """Process old format entry without explicit type field (backward compatibility)."""
        if "value" in entry and "name" in entry:
            # Has value field → metric
            logger.debug(f"Collecting old-format metric: {entry.get('name')}")
            try:
                if entry["name"] not in validated_names:
                    validate_metric_name(entry["name"])
                    validated_names.add(entry["name"])
                value = normalize_metric_value(entry["value"])
                validate_metric_value(value)
                entry["step"] = normalize_metric_step(entry.get("step"))
                if not self._ensure_metric_name_limit(entry["name"], metric_names, result):
                    return
                if not self._ensure_step_mode(entry["name"], entry["step"], step_modes, result):
                    return
                entry["value"] = value
                entry["timestamp"] = normalize_metric_timestamp(entry.get("timestamp"))
                metrics_batch.append(entry)
            except ValidationError as e:
                result.skipped_count += 1
                self._record_error(result, str(e))
                logger.warning(f"Skipping invalid old-format metric: {e}")
        elif "path" in entry and "name" in entry:
            # Has path field → artifact
            logger.debug(f"Collecting old-format artifact: {entry.get('name')}")
            try:
                validate_metric_name(entry["name"])
                validate_artifact_path(entry["path"])
                timestamp = entry.get("timestamp")
                if timestamp is not None:
                    entry["timestamp"] = normalize_metric_timestamp(timestamp)
                artifacts_batch.append(entry)
            except ValidationError as e:
                result.skipped_count += 1
                self._record_error(result, str(e))
                logger.warning(f"Skipping invalid old-format artifact: {e}")
        else:
            result.skipped_count += 1
            self._record_error(result, "Entry with unknown format (missing 'type' and can't infer)")
            logger.warning("Skipping entry with unknown format")

    def _record_error(self, result: CollectionResult, message: str) -> None:
        """Record an error with a cap to avoid memory blowups."""
        if len(result.errors) < MAX_ERROR_MESSAGES:
            result.errors.append(message)
        else:
            result.errors_truncated = True

    def _ensure_step_mode(
        self,
        name: str,
        step: int | None,
        step_modes: dict[str, str],
        result: CollectionResult,
    ) -> bool:
        """Ensure step usage is consistent for a given metric name."""
        mode = "none" if step is None else "value"
        existing = step_modes.get(name)
        if existing is None:
            step_modes[name] = mode
            return True
        if existing != mode:
            result.skipped_count += 1
            self._record_error(result, f"Metric '{name}' logged with mixed step modes (None and int)")
            logger.warning("Skipping metric with mixed step modes: %s", name)
            return False
        return True

    def _ensure_metric_name_limit(
        self,
        name: str,
        metric_names: set[str],
        result: CollectionResult,
    ) -> bool:
        """Ensure per-run metric name limit is respected."""
        if name in metric_names:
            return True
        if len(metric_names) >= MAX_METRIC_NAMES_PER_RUN:
            result.skipped_count += 1
            self._record_error(
                result,
                f"Too many unique metric names (limit {MAX_METRIC_NAMES_PER_RUN})",
            )
            logger.warning("Skipping metric due to name limit: %s", name)
            return False
        metric_names.add(name)
        return True
