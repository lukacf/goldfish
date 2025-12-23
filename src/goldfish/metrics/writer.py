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

from goldfish.errors import GoldfishError
from goldfish.metrics.utils import normalize_metric_step, normalize_metric_timestamp, normalize_metric_value

logger = logging.getLogger(__name__)


class MetricsFlushError(GoldfishError):
    """Raised when metrics cannot be flushed to disk."""

    def __init__(self, message: str, lost_count: int, errors: list[str]):
        super().__init__(message, {"lost_count": lost_count, "errors": errors})
        self.lost_count = lost_count
        self.errors = errors


class MetricsInitializationError(GoldfishError):
    """Raised when metrics writer cannot be initialized."""

    def __init__(self, message: str, path: Path | None = None):
        details = {"path": str(path)} if path is not None else None
        super().__init__(message, details)


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
        elif not isinstance(outputs_dir, Path):
            outputs_dir = Path(str(outputs_dir))

        self.outputs_dir = Path(outputs_dir)
        if not self.outputs_dir.is_absolute():
            raise MetricsInitializationError("Outputs directory must be an absolute path", self.outputs_dir)
        if self.outputs_dir.is_symlink():
            raise MetricsInitializationError("Outputs directory cannot be a symlink", self.outputs_dir)
        if self.outputs_dir.exists() and not self.outputs_dir.is_dir():
            raise MetricsInitializationError("Outputs directory must be a directory", self.outputs_dir)
        self.metrics_dir = self.outputs_dir / ".goldfish"
        self.metrics_file = self.metrics_dir / "metrics.jsonl"

        # Ensure directory exists
        try:
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise MetricsInitializationError(f"Failed to create outputs directory: {exc}", self.metrics_dir) from exc

        # Thread safety lock
        self._lock = threading.Lock()

        # Buffer for metrics
        self._metrics_buffer: list[dict] = []
        self._artifacts: list[dict] = []
        self._auto_flush_threshold = max(10, min(10000, auto_flush_threshold))
        self._metric_step_modes: dict[str, str] = {}
        self._validated_metric_names: set[str] = set()
        self._max_metric_names = self._read_max_metric_names()

        # Error tracking for flush failures (silent data loss detection)
        self._flush_errors: list[str] = []
        self._metrics_lost: int = 0
        self._validation_errors: list[str] = []

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: str | float | None = None,
    ) -> bool:
        """Log a single metric.

        Args:
            name: Metric name
            value: Metric value
            step: Optional step/epoch
            timestamp: Optional timestamp - ISO 8601 string or Unix float (defaults to now in UTC)

        Raises:
            InvalidMetricNameError: If name is invalid
            InvalidMetricValueError: If value is NaN or infinite

        Returns:
            True if the metric was logged, False if it was skipped.
        """
        from goldfish.validation import (
            InvalidMetricNameError,
            validate_metric_name,
            validate_metric_value,
        )

        if name not in self._metric_step_modes and len(self._metric_step_modes) >= self._max_metric_names:
            raise InvalidMetricNameError(
                name,
                f"too many unique metric names (limit {self._max_metric_names})",
            )

        # Validate inputs early (strict mode - fail fast)
        if name not in self._validated_metric_names:
            validate_metric_name(name)
            self._validated_metric_names.add(name)
        value = normalize_metric_value(value)
        validate_metric_value(value)
        step = normalize_metric_step(step)

        # Enforce consistent step usage per metric
        step_mode = "none" if step is None else "value"
        existing_mode = self._metric_step_modes.get(name)
        if existing_mode is None:
            self._metric_step_modes[name] = step_mode
        elif existing_mode != step_mode:
            error_msg = f"metric '{name}' logged with mixed step modes (None and int)"
            logger.warning(error_msg)
            self._validation_errors.append(error_msg)
            self._metrics_lost += 1
            return False

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

        return True

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

        Retains buffers on I/O errors to allow retry.
        Raises MetricsFlushError on failure to avoid silent data loss.
        """
        with self._lock:
            self._flush_unlocked(raise_on_error=True)

    def _flush_unlocked(self, raise_on_error: bool) -> None:
        """Internal flush without locking - caller must hold self._lock."""
        valid_metrics: list[dict] = []
        valid_artifacts: list[dict] = []
        metrics_lines: list[str] = []
        artifact_lines: list[str] = []

        # Pre-serialize metrics to catch type/NaN errors before writing
        for metric in self._metrics_buffer:
            try:
                metrics_lines.append(json.dumps(metric, allow_nan=False))
                valid_metrics.append(metric)
            except (ValueError, TypeError) as exc:
                error_msg = f"Failed to serialize metric '{metric.get('name', '?')}': {type(exc).__name__} ({exc})"
                logger.error(error_msg)
                self._flush_errors.append(error_msg)
                self._metrics_lost += 1

        for artifact in self._artifacts:
            try:
                artifact_lines.append(json.dumps(artifact, allow_nan=False))
                valid_artifacts.append(artifact)
            except (ValueError, TypeError) as exc:
                error_msg = f"Failed to serialize artifact '{artifact.get('name', '?')}': {type(exc).__name__} ({exc})"
                logger.error(error_msg)
                self._flush_errors.append(error_msg)
                self._metrics_lost += 1

        metrics_count = len(metrics_lines)

        wrote = False
        try:
            # Write both metrics and artifacts in single file open
            if metrics_lines or artifact_lines:
                with open(self.metrics_file, "a") as f:
                    for line in metrics_lines:
                        f.write(line + "\n")
                    for line in artifact_lines:
                        f.write(line + "\n")
                wrote = True
        except (OSError, ValueError, TypeError) as e:
            # OSError: disk full, permissions, I/O error
            # ValueError: NaN/Infinity in metrics
            # TypeError: non-serializable types
            error_msg = f"Failed to flush {metrics_count} metrics: {type(e).__name__} ({e})"
            logger.error(f"Failed to flush metrics to {self.metrics_file}: {e}", exc_info=True)
            # Track error for later inspection
            self._flush_errors.append(error_msg)
            if raise_on_error:
                raise MetricsFlushError(error_msg, self._metrics_lost, list(self._flush_errors)) from e
        finally:
            if wrote:
                self._metrics_buffer.clear()
                self._artifacts.clear()
            else:
                # Retain valid entries for retry; drop invalid ones already counted as lost
                self._metrics_buffer = valid_metrics
                self._artifacts = valid_artifacts

    def _read_max_metric_names(self) -> int:
        """Read max unique metric names from env (with sane bounds)."""
        env_val = os.environ.get("GOLDFISH_METRICS_MAX_NAMES")
        if not env_val:
            return 10000
        try:
            limit = int(env_val)
        except ValueError:
            return 10000
        return max(1, min(100000, limit))

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

    def get_validation_errors(self) -> list[str]:
        """Get validation errors recorded during logging."""
        with self._lock:
            return list(self._validation_errors)

    def had_flush_errors(self) -> bool:
        """Check if any flush errors occurred (quick data loss check).

        Returns:
            True if any metrics were lost due to flush failures.
        """
        with self._lock:
            return len(self._flush_errors) > 0
