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

        # Parse all entries into batches first
        metrics_batch = []
        artifacts_batch = []
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
                            # Validate required fields before adding to batch
                            if "name" in entry and "value" in entry:
                                metrics_batch.append(entry)
                            else:
                                logger.warning(f"Skipping metric entry missing required fields: {entry}")
                        elif entry_type == "artifact":
                            # Validate required fields before adding to batch
                            if "name" in entry and "path" in entry:
                                artifacts_batch.append(entry)
                            else:
                                logger.warning(f"Skipping artifact entry missing required fields: {entry}")

                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Skipping invalid metrics entry: {e}")
                        continue

            # Insert all metrics in a single transaction
            if metrics_batch:
                self.db.batch_insert_metrics(stage_run_id, metrics_batch)
                metrics_count = len(metrics_batch)

            # Insert artifacts
            for artifact in artifacts_batch:
                self.db.insert_artifact(
                    stage_run_id=stage_run_id,
                    name=artifact["name"],
                    path=artifact["path"],
                    backend_url=artifact.get("backend_url"),
                    created_at=artifact["timestamp"],
                )
                artifacts_count += 1

            logger.info(
                f"Collected metrics for {stage_run_id}: " f"{metrics_count} metrics, {artifacts_count} artifacts"
            )

            return {"metrics_count": metrics_count, "artifacts_count": artifacts_count}

        except Exception as e:
            logger.error(f"Failed to collect metrics for {stage_run_id}: {e}")
            # Return partial counts - don't fail the stage if metrics collection fails
            return {"metrics_count": metrics_count, "artifacts_count": artifacts_count}
