"""Local metrics writer - writes metrics to JSONL format.

This module provides LocalWriter, which writes metrics to a local JSONL file
for audit trail and recovery. The JSONL file is synced to GCS periodically
by the background log syncer in GCE instances.
"""

import json
import logging
import os
import threading
import time
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

    def __init__(
        self,
        outputs_dir: Path | None = None,
        auto_flush_threshold: int = 100,
        auto_flush_interval: float = 30.0,
    ):
        """Initialize local writer.

        Args:
            outputs_dir: Output directory (defaults to GOLDFISH_OUTPUTS_DIR env var)
            auto_flush_threshold: Number of metrics to buffer before auto-flushing
                                  (default: 100, range: 10-10000)
            auto_flush_interval: Maximum seconds between flushes for real-time visibility
                                 (default: 30.0). Set to 0 to disable time-based flushing.
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

        # Create empty metrics file eagerly so log syncer can detect it
        # This ensures the file exists even before the first metric is logged/flushed
        try:
            self.metrics_file.touch(exist_ok=True)
        except OSError as exc:
            raise MetricsInitializationError(f"Failed to create metrics file: {exc}", self.metrics_file) from exc

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

        # Time-based auto-flush for real-time visibility
        self._auto_flush_interval = max(0.0, auto_flush_interval)
        self._last_flush_time: float = time.time()

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

        Returns:
            True if the metric was logged, False if it was skipped.
        """
        from goldfish.validation import (
            InvalidMetricNameError,
            InvalidMetricStepError,
            InvalidMetricTimestampError,
            InvalidMetricValueError,
            validate_metric_name,
            validate_metric_value,
        )

        if name not in self._metric_step_modes and len(self._metric_step_modes) >= self._max_metric_names:
            self._record_validation_error(
                str(
                    InvalidMetricNameError(
                        name,
                        f"too many unique metric names (limit {self._max_metric_names})",
                    )
                )
            )
            return False

        # Validate inputs early (strict mode - fail fast)
        if name not in self._validated_metric_names:
            try:
                validate_metric_name(name)
            except InvalidMetricNameError as exc:
                self._record_validation_error(str(exc))
                return False
            self._validated_metric_names.add(name)
        try:
            value = normalize_metric_value(value)
            validate_metric_value(value)
        except InvalidMetricValueError as exc:
            self._record_validation_error(str(exc))
            return False
        try:
            step = normalize_metric_step(step)
        except InvalidMetricStepError as exc:
            self._record_validation_error(str(exc))
            return False

        # Enforce consistent step usage per metric
        step_mode = "none" if step is None else "value"
        existing_mode = self._metric_step_modes.get(name)
        if existing_mode is None:
            self._metric_step_modes[name] = step_mode
        elif existing_mode != step_mode:
            self._record_validation_error(f"metric '{name}' logged with mixed step modes (None and int)")
            return False

        # Normalize timestamp to ISO 8601 string (UTC)
        try:
            ts_str = normalize_metric_timestamp(timestamp)
        except InvalidMetricTimestampError as exc:
            self._record_validation_error(str(exc))
            return False

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

            # Auto-flush if threshold exceeded OR time interval exceeded
            should_flush = len(self._metrics_buffer) >= self._auto_flush_threshold
            if not should_flush and self._auto_flush_interval > 0:
                elapsed = time.time() - self._last_flush_time
                should_flush = elapsed >= self._auto_flush_interval and len(self._metrics_buffer) > 0

            if should_flush:
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
        from goldfish.validation import (
            InvalidArtifactPathError,
            InvalidMetricNameError,
            validate_artifact_path,
            validate_metric_name,
        )

        # Validate inputs early (strict mode - fail fast)
        try:
            validate_metric_name(name)
            validate_artifact_path(str(path))
        except (InvalidMetricNameError, InvalidArtifactPathError) as exc:
            self._record_validation_error(str(exc))
            return

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

    def _record_validation_error(self, message: str) -> None:
        """Record a validation error and count it as lost."""
        logger.warning(message)
        with self._lock:
            self._validation_errors.append(message)
            self._metrics_lost += 1

    def record_validation_error(self, message: str) -> None:
        """Public helper to record validation errors from callers."""
        self._record_validation_error(message)

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
                self._last_flush_time = time.time()
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
