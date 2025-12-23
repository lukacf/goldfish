"""Metrics collection API for Goldfish stages.

This module provides the public API for logging metrics and artifacts from stage code.

Example usage in stage code:
    from goldfish.metrics import log_metric, log_metrics, log_artifact, finish

    # Log individual metrics
    log_metric("loss", 0.5, step=1)
    log_metric("accuracy", 0.92, step=1)

    # Log multiple metrics at once
    log_metrics({"precision": 0.89, "recall": 0.91}, step=1)

    # Log artifacts
    log_artifact("model", "model.pt")

    # Finalize (optional - happens automatically at stage end)
    finish()

The metrics API automatically:
- Writes to local JSONL file (.goldfish/metrics.jsonl) for audit trail
- Syncs to configured backend (W&B, MLflow) if GOLDFISH_METRICS_BACKEND is set
- Handles backend failures gracefully (stage continues even if backend fails)

Optional configuration:
- GOLDFISH_METRICS_FLUSH_THRESHOLD: auto-flush after N metrics (default 100)
"""

from __future__ import annotations

import atexit
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.metrics.logger import MetricsLogger

if TYPE_CHECKING:
    pass

# Global logger instance (lazily initialized)
_global_logger: MetricsLogger | None = None
_logger_lock = threading.Lock()
_auto_finalize_registered = False


def _get_or_create_logger() -> MetricsLogger:
    """Get or create the global MetricsLogger instance.

    The logger is lazily initialized on first metric call. It reads configuration
    from environment variables:
    - GOLDFISH_OUTPUTS_DIR: Output directory (default: /mnt/outputs)
    - GOLDFISH_RUN_ID: Stage run ID
    - GOLDFISH_WORKSPACE: Workspace name
    - GOLDFISH_STAGE: Stage name
    - GOLDFISH_METRICS_BACKEND: Backend name (e.g., "wandb", "mlflow")

    Returns:
        Global MetricsLogger instance
    """
    global _global_logger, _auto_finalize_registered

    # Ensure only one logger is created in concurrent scenarios
    with _logger_lock:
        if _global_logger is not None:
            return _global_logger

        # Read configuration from environment
        outputs_dir_str = os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs")
        outputs_dir = Path(outputs_dir_str)

        run_id = os.environ.get("GOLDFISH_RUN_ID")
        workspace = os.environ.get("GOLDFISH_WORKSPACE")
        stage = os.environ.get("GOLDFISH_STAGE")
        config_str = os.environ.get("GOLDFISH_CONFIG", "{}")

        # Parse config (it's JSON-encoded in the env var)
        import json

        try:
            config = json.loads(config_str)
        except json.JSONDecodeError:
            config = {}

        # Metrics flush configuration
        flush_threshold = None
        threshold_str = os.environ.get("GOLDFISH_METRICS_FLUSH_THRESHOLD")
        if threshold_str:
            try:
                flush_threshold = int(threshold_str)
            except ValueError:
                flush_threshold = None

        # Backend configuration - instantiate from registry
        backend = None
        backend_name = os.environ.get("GOLDFISH_METRICS_BACKEND")
        if backend_name:
            from goldfish.metrics.backends import get_registry

            registry = get_registry()
            backend_class = registry.get(backend_name)

            if backend_class is not None:
                if backend_class.is_available():
                    backend = backend_class()
                else:
                    import logging

                    available = registry.list_available()
                    logging.warning(
                        f"Metrics backend '{backend_name}' requested but not available. "
                        f"Available backends: {available}. Falling back to local-only metrics."
                    )
            else:
                import logging

                logging.warning(
                    f"Unknown metrics backend '{backend_name}'. "
                    f"Available backends: {registry.list_backends()}. "
                    f"Falling back to local-only metrics."
                )

        # Create logger
        _global_logger = MetricsLogger(
            outputs_dir=outputs_dir,
            backend=backend,
            run_id=run_id,
            config=config,
            workspace=workspace,
            stage=stage,
            auto_flush_threshold=flush_threshold,
        )

        if not _auto_finalize_registered:
            atexit.register(_auto_finalize)
            _auto_finalize_registered = True

        return _global_logger


def log_metric(
    name: str,
    value: float,
    step: int | None = None,
    timestamp: str | float | None = None,
) -> None:
    """Log a single metric value.

    Args:
        name: Metric name (e.g., "loss", "accuracy"). Use slashes for grouping (e.g., "train/loss").
        value: Metric value (bool values are rejected; use 0/1 instead).
        step: Optional step/epoch number. Use None for stepless metrics (consistent per metric).
        timestamp: Optional ISO 8601 string (UTC) or Unix timestamp float.

    Example:
        log_metric("loss", 0.5, step=1)
        log_metric("train/accuracy", 0.92, step=1, timestamp="2024-01-01T00:00:00Z")
        log_metric("learning_rate", 0.001)  # stepless metric
    """
    logger = _get_or_create_logger()
    logger.log_metric(name, value, step, timestamp)


def log_metrics(
    metrics: dict[str, float],
    step: int | None = None,
    timestamp: str | float | None = None,
) -> None:
    """Log multiple metrics at once.

    Args:
        metrics: Dict of metric_name -> value
        step: Optional step/epoch number (consistent per metric)
        timestamp: Optional ISO 8601 string (UTC) or Unix timestamp float

    Example:
        log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)
    """
    logger = _get_or_create_logger()
    logger.log_metrics(metrics, step, timestamp)


def log_artifact(name: str, path: str | Path) -> str | None:
    """Log an artifact (file or directory).

    Args:
        name: Artifact name (e.g., "model", "predictions")
        path: Relative path under outputs dir (no absolute paths)

    Returns:
        Backend URL if available (e.g., W&B run URL), else None.

    Example:
        log_artifact("model", "model.pt")
        log_artifact("checkpoints", "checkpoints/epoch_10")
    """
    logger = _get_or_create_logger()
    return logger.log_artifact(name, path)


def log_artifacts(artifacts: dict[str, str | Path]) -> dict[str, str | None]:
    """Log multiple artifacts at once.

    Args:
        artifacts: Dict of artifact_name -> relative path

    Returns:
        Dict of artifact_name -> backend URL (or None)
    """
    logger = _get_or_create_logger()
    return logger.log_artifacts(artifacts)


def finish() -> str | None:
    """Finalize metrics collection.

    Flushes buffered metrics to disk and calls backend.finish() if configured.
    This is optional - the logger will automatically finalize at stage end
    using an atexit hook (won't run on SIGKILL/crash). Safe to call multiple times.

    Returns:
        Optional URL to the run in the backend's UI (e.g., W&B run page)

    Example:
        url = finish()
        if url:
            print(f"View run at: {url}")
    """
    if _global_logger is not None:
        return _global_logger.finish()
    return None


def _auto_finalize() -> None:
    """Auto-finalize metrics at process exit."""
    try:
        if _global_logger is not None:
            _global_logger.finish()
    except Exception:
        # Avoid raising during interpreter shutdown
        import logging

        logging.getLogger(__name__).exception("Auto-finalize of metrics failed")


def _reset_global_logger() -> None:
    """Reset the global logger instance.

    This is primarily for testing purposes - it allows tests to start with a
    fresh logger instance. Not intended for use in stage code.
    """
    global _global_logger, _auto_finalize_registered
    _global_logger = None
    _auto_finalize_registered = False


def had_backend_errors() -> bool:
    """Return True if the backend failed during this run."""
    logger = _get_or_create_logger()
    return logger.had_backend_errors()


def get_backend_errors() -> list[str]:
    """Get backend error messages (if any)."""
    logger = _get_or_create_logger()
    return logger.get_backend_errors()


__all__ = [
    "log_metric",
    "log_metrics",
    "log_artifact",
    "log_artifacts",
    "finish",
    "had_backend_errors",
    "get_backend_errors",
]
