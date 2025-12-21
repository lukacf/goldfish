"""Metrics collector for post-run database storage.

Reads metrics.jsonl from GCS after stage completion and populates the database.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from goldfish.db.database import Database

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collect metrics from JSONL files and store in database."""

    def __init__(self, db: Database):
        self.db = db

    def collect_from_file(self, stage_run_id: str, metrics_file: Path) -> dict:
        """Collect metrics from a JSONL file and store in database.

        Args:
            stage_run_id: Stage run ID
            metrics_file: Path to metrics.jsonl file

        Returns:
            Dict with collection stats (metrics_count, artifacts_count)
        """
        if not metrics_file.exists():
            logger.debug(f"No metrics file found for {stage_run_id}: {metrics_file}")
            return {"metrics_count": 0, "artifacts_count": 0}

        metrics_count = 0
        artifacts_count = 0

        try:
            with open(metrics_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        entry = json.loads(line)
                        entry_type = entry.get("type")

                        if entry_type == "metric":
                            # Insert individual metric
                            self.db.insert_metric(
                                stage_run_id=stage_run_id,
                                name=entry["name"],
                                value=entry["value"],
                                step=entry.get("step"),
                                timestamp=entry["timestamp"],
                            )

                            # Update summary
                            self.db.upsert_metric_summary(
                                stage_run_id=stage_run_id,
                                name=entry["name"],
                                value=entry["value"],
                            )

                            metrics_count += 1

                        elif entry_type == "artifact":
                            # Insert artifact record
                            self.db.insert_artifact(
                                stage_run_id=stage_run_id,
                                name=entry["name"],
                                path=entry["path"],
                                backend_url=entry.get("backend_url"),
                                created_at=entry["timestamp"],
                            )

                            artifacts_count += 1

                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Skipping invalid metrics entry: {e}")
                        continue

            logger.info(
                f"Collected metrics for {stage_run_id}: " f"{metrics_count} metrics, {artifacts_count} artifacts"
            )

            return {"metrics_count": metrics_count, "artifacts_count": artifacts_count}

        except Exception as e:
            logger.error(f"Failed to collect metrics for {stage_run_id}: {e}")
            # Return partial counts - don't fail the stage if metrics collection fails
            return {"metrics_count": metrics_count, "artifacts_count": artifacts_count}
