"""SQLite implementation of MetricsStore."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.types import MetricRow


class SQLiteMetricsStore:
    """SQLite-backed MetricsStore implementation."""

    def __init__(self, db: Database, settings: GoldfishSettings) -> None:
        self._db = db
        self.settings = settings

    def record_metric(self, run_id: str, name: str, value: float, step: int | None) -> None:
        """Record a single metric observation."""
        timestamp = datetime.now(UTC).isoformat()
        with self._db._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_metrics (stage_run_id, name, value, step, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, name, value, step, timestamp),
            )
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
                (run_id, name, value, value, value, timestamp),
            )

    def get_metrics(self, run_id: str) -> list[MetricRow]:
        """Return all metrics for a stage run."""
        with self._db._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, stage_run_id, name, value, step, timestamp
                FROM run_metrics
                WHERE stage_run_id = ?
                ORDER BY timestamp ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
            return [cast(MetricRow, dict(row)) for row in rows]
