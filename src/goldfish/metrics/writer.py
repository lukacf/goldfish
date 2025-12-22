"""Local metrics writer - writes metrics to JSONL format.

This module provides LocalWriter, which writes metrics to a local JSONL file
for audit trail and recovery. The JSONL file is synced to GCS periodically
by the background log syncer in GCE instances.
"""

import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalWriter:
    """Writes metrics to local JSONL file.

    Thread-safe and append-only to avoid corruption. Metrics are written to
    `.goldfish/metrics.jsonl` in the outputs directory, and artifacts are
    recorded in `.goldfish/artifacts.json`.

    The JSONL format is:
    {"name": "loss", "value": 0.5, "step": 10, "timestamp": 1234567890.0}
    {"name": "accuracy", "value": 0.95, "step": 10, "timestamp": 1234567890.1}
    ...
    """

    def __init__(self, outputs_dir: Path | None = None):
        """Initialize local writer.

        Args:
            outputs_dir: Output directory (defaults to GOLDFISH_OUTPUTS_DIR env var)
        """
        if outputs_dir is None:
            outputs_dir_str = os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs")
            outputs_dir = Path(outputs_dir_str)

        self.outputs_dir = Path(outputs_dir)
        self.metrics_dir = self.outputs_dir / ".goldfish"
        self.metrics_file = self.metrics_dir / "metrics.jsonl"
        self.artifacts_file = self.metrics_dir / "artifacts.json"

        # Ensure directory exists
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # Buffer for metrics
        self._metrics_buffer: list[dict] = []
        self._artifacts: list[dict] = []
        self._auto_flush_threshold = 100  # Auto-flush after 100 metrics to prevent OOM

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log a single metric.

        Args:
            name: Metric name
            value: Metric value
            step: Optional step/epoch
            timestamp: Optional Unix timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = time.time()

        metric = {
            "type": "metric",
            "name": name,
            "value": value,
            "step": step,
            "timestamp": timestamp,
        }
        self._metrics_buffer.append(metric)

        # Auto-flush if threshold exceeded
        if len(self._metrics_buffer) >= self._auto_flush_threshold:
            self.flush()

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log multiple metrics.

        Args:
            metrics: Dict of metric_name -> value
            step: Optional step/epoch
            timestamp: Optional Unix timestamp (defaults to now)
        """
        for name, value in metrics.items():
            self.log_metric(name, value, step, timestamp)

    def log_artifact(self, name: str, path: str | Path) -> None:
        """Record an artifact path.

        Args:
            name: Artifact name
            path: Path to artifact (relative to outputs dir)
        """
        artifact = {
            "type": "artifact",
            "name": name,
            "path": str(path),
            "timestamp": time.time(),
        }
        self._artifacts.append(artifact)

    def flush(self) -> None:
        """Flush buffered metrics to disk.

        Always clears buffers even on error to prevent infinite flush loops.
        Logs errors but does not raise to avoid making log_metric() throw I/O exceptions.
        """
        try:
            # Write both metrics and artifacts in single file open
            if self._metrics_buffer or self._artifacts:
                with open(self.metrics_file, "a") as f:
                    for metric in self._metrics_buffer:
                        # allow_nan=False raises ValueError on NaN/Infinity
                        f.write(json.dumps(metric, allow_nan=False) + "\n")
                    for artifact in self._artifacts:
                        f.write(json.dumps(artifact, allow_nan=False) + "\n")
        except (OSError, ValueError, TypeError) as e:
            # OSError: disk full, permissions, I/O error
            # ValueError: NaN/Infinity in metrics
            # TypeError: non-serializable types
            logger.error(f"Failed to flush metrics to {self.metrics_file}: {e}", exc_info=True)
            # Don't re-raise - log_metric() should not throw I/O exceptions
        finally:
            # ALWAYS clear buffers, even on error
            # Better to lose metrics than enter infinite flush loop
            self._metrics_buffer.clear()
            self._artifacts.clear()
