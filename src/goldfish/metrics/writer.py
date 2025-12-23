"""Local metrics writer - writes metrics to JSONL format.

This module provides LocalWriter, which writes metrics to a local JSONL file
for audit trail and recovery. The JSONL file is synced to GCS periodically
by the background log syncer in GCE instances.
"""

import json
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path

from goldfish.metrics.utils import normalize_metric_step, normalize_metric_timestamp, normalize_metric_value

logger = logging.getLogger(__name__)


class MetricsFlushError(RuntimeError):
    """Raised when metrics cannot be flushed to disk."""

    def __init__(self, message: str, lost_count: int, errors: list[str]):
        super().__init__(message)
        self.lost_count = lost_count
        self.errors = errors


class LocalWriter:
    """Writes metrics to local JSONL file.

    Thread-safe and append-only to avoid corruption. Metrics and artifacts
    are written to `.goldfish/metrics.jsonl` in the outputs directory.

    The JSONL format is:
    {"name": "loss", "value": 0.5, "step": 10, "timestamp": 1700000000.0}
    {"name": "accuracy", "value": 0.95, "step": 10, "timestamp": 1700000000.1}
    ...
    """

    def __init__(self, outputs_dir: Path | None = None, auto_flush_threshold: int = 100):
        """Initialize local writer.

        Args:
            outputs_dir: Output directory (defaults to GOLDFISH_OUTPUTS_DIR env var)
            auto_flush_threshold: Number of metrics to buffer before auto-flushing
                                  (default: 100, range: 10-10000)
        """
        if outputs_dir is None:
            outputs_dir_str = os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs")
            outputs_dir = Path(outputs_dir_str)

        self.outputs_dir = Path(outputs_dir)
        self.metrics_dir = self.outputs_dir / ".goldfish"
        self.metrics_file = self.metrics_dir / "metrics.jsonl"

        # Ensure directory exists
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # Thread safety lock
        self._lock = threading.Lock()

        # Buffer for metrics
        self._metrics_buffer: list[dict] = []
        self._artifacts: list[dict] = []
        self._auto_flush_threshold = max(10, min(10000, auto_flush_threshold))
        self._metric_step_modes: dict[str, str] = {}

        # Error tracking for flush failures (silent data loss detection)
        self._flush_errors: list[str] = []
        self._metrics_lost: int = 0

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: str | float | None = None,
    ) -> None:
        """Log a single metric.

        Args:
            name: Metric name
            value: Metric value
            step: Optional step/epoch
            timestamp: Optional timestamp - ISO 8601 string or Unix float (defaults to now in UTC)

        Raises:
            InvalidMetricNameError: If name is invalid
            InvalidMetricValueError: If value is NaN or infinite
        """
        from goldfish.validation import InvalidMetricStepError, validate_metric_name, validate_metric_value

        # Validate inputs early (strict mode - fail fast)
        validate_metric_name(name)
        value = normalize_metric_value(value)
        validate_metric_value(value)
        step = normalize_metric_step(step)

        # Enforce consistent step usage per metric
        step_mode = "none" if step is None else "value"
        existing_mode = self._metric_step_modes.get(name)
        if existing_mode is None:
            self._metric_step_modes[name] = step_mode
        elif existing_mode != step_mode:
            raise InvalidMetricStepError(
                str(step),
                f"metric '{name}' logged with mixed step modes (None and int)",
            )

        # Normalize timestamp to ISO 8601 string (UTC)
        ts_str = normalize_metric_timestamp(timestamp)

        metric = {
            "type": "metric",
            "name": name,
            "value": value,
            "step": step,
            "timestamp": ts_str,
        }

        # Thread-safe buffer append
        with self._lock:
            self._metrics_buffer.append(metric)

            # Auto-flush if threshold exceeded (inside lock)
            if len(self._metrics_buffer) >= self._auto_flush_threshold:
                self._flush_unlocked(raise_on_error=True)

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: str | float | None = None,
    ) -> None:
        """Log multiple metrics.

        Args:
            metrics: Dict of metric_name -> value
            step: Optional step/epoch
            timestamp: Optional timestamp - ISO 8601 string or Unix float (defaults to now)
        """
        for name, value in metrics.items():
            self.log_metric(name, value, step, timestamp)

    def log_artifact(self, name: str, path: str | Path, backend_url: str | None = None) -> None:
        """Record an artifact path.

        Args:
            name: Artifact name
            path: Path to artifact (relative to outputs dir)

        Raises:
            InvalidMetricNameError: If name is invalid
            InvalidArtifactPathError: If path contains traversal or is absolute
        """
        from goldfish.validation import validate_artifact_path, validate_metric_name

        # Validate inputs early (strict mode - fail fast)
        validate_metric_name(name)
        validate_artifact_path(str(path))

        artifact = {
            "type": "artifact",
            "name": name,
            "path": str(path),
            "timestamp": datetime.now(UTC).isoformat(),
        }
        if backend_url:
            artifact["backend_url"] = backend_url

        # Thread-safe buffer append
        with self._lock:
            self._artifacts.append(artifact)

    def flush(self) -> None:
        """Flush buffered metrics to disk (thread-safe).

        Always clears buffers even on error to prevent infinite flush loops.
        Raises MetricsFlushError on failure to avoid silent data loss.
        """
        with self._lock:
            self._flush_unlocked(raise_on_error=True)

    def _flush_unlocked(self, raise_on_error: bool) -> None:
        """Internal flush without locking - caller must hold self._lock."""
        metrics_count = len(self._metrics_buffer)

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
            error_msg = f"Failed to flush {metrics_count} metrics: {type(e).__name__} ({e})"
            logger.error(f"Failed to flush metrics to {self.metrics_file}: {e}", exc_info=True)
            # Track error for later inspection
            self._flush_errors.append(error_msg)
            self._metrics_lost += metrics_count
            if raise_on_error:
                raise MetricsFlushError(error_msg, self._metrics_lost, list(self._flush_errors)) from e
        finally:
            # ALWAYS clear buffers, even on error
            # Better to lose metrics than enter infinite flush loop
            self._metrics_buffer.clear()
            self._artifacts.clear()

    def get_flush_errors(self) -> list[str]:
        """Get list of flush errors that occurred (data loss indicator).

        Returns:
            List of error messages from failed flush operations.
            Empty list means no data loss occurred.
        """
        with self._lock:
            return list(self._flush_errors)

    def get_metrics_lost_count(self) -> int:
        """Get count of metrics that were lost due to flush failures.

        Returns:
            Number of metrics that failed to write to disk.
        """
        with self._lock:
            return self._metrics_lost

    def had_flush_errors(self) -> bool:
        """Check if any flush errors occurred (quick data loss check).

        Returns:
            True if any metrics were lost due to flush failures.
        """
        with self._lock:
            return len(self._flush_errors) > 0
