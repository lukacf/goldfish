"""MetricsLogger - orchestration layer for metrics collection.

This module provides MetricsLogger, which orchestrates the LocalWriter (always on)
and optional MetricsBackend (pluggable, e.g., W&B, MLflow). It provides:

- Lazy backend initialization (only on first metric call)
- Graceful degradation (backend failures don't crash the stage)
- Unified interface for logging metrics and artifacts
- Context manager support for automatic finalization
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.metrics.writer import LocalWriter

if TYPE_CHECKING:
    from goldfish.metrics.backends.base import MetricsBackend

logger = logging.getLogger(__name__)


class MetricsLogger:
    """Orchestrates metrics collection to local JSONL and optional backend.

    This is the internal coordination layer that ensures:
    1. Local JSONL writer always runs (audit trail + crash recovery)
    2. Backend (if configured) receives real-time updates
    3. Backend failures don't crash the stage (graceful degradation)
    4. Backend is lazily initialized on first metric call

    Example:
        # Without backend
        logger = MetricsLogger(outputs_dir=Path("/mnt/outputs"))
        logger.log_metric("loss", 0.5, step=1)
        logger.finish()

        # With backend
        from goldfish.metrics.backends.wandb import WandBBackend
        backend = WandBBackend()
        logger = MetricsLogger(
            outputs_dir=Path("/mnt/outputs"),
            backend=backend,
            run_id="stage-abc123",
            config={"lr": 0.01},
            workspace="baseline",
            stage="train"
        )
        logger.log_metric("loss", 0.5, step=1)
        url = logger.finish()  # Returns W&B run URL
    """

    def __init__(
        self,
        outputs_dir: Path | None = None,
        backend: MetricsBackend | None = None,
        run_id: str | None = None,
        config: dict | None = None,
        workspace: str | None = None,
        stage: str | None = None,
    ):
        """Initialize metrics logger.

        Args:
            outputs_dir: Output directory (defaults to GOLDFISH_OUTPUTS_DIR)
            backend: Optional metrics backend (e.g., WandBBackend)
            run_id: Goldfish stage run ID (required if backend is set)
            config: Stage configuration dict (for backend hyperparameters)
            workspace: Workspace name (for backend tagging)
            stage: Stage name (for backend tagging)
        """
        # LocalWriter always runs
        self.local_writer = LocalWriter(outputs_dir=outputs_dir)

        # Backend is optional
        self.backend = backend
        self._backend_initialized = False
        self._backend_failed = False

        # Run metadata for backend initialization
        self.run_id = run_id
        self.config = config or {}
        self.workspace = workspace
        self.stage = stage

    def _ensure_backend_initialized(self) -> None:
        """Lazy initialization of backend on first metric call.

        Catches initialization errors and marks backend as failed for graceful
        degradation. This is called on every metric/artifact call, but only
        initializes once.
        """
        if self.backend is None:
            return

        if self._backend_initialized or self._backend_failed:
            return

        # Try to initialize backend
        try:
            if self.run_id and self.workspace and self.stage:
                self.backend.init_run(
                    run_id=self.run_id,
                    config=self.config,
                    workspace=self.workspace,
                    stage=self.stage,
                )
                self._backend_initialized = True
                logger.info(f"Initialized backend '{self.backend.name()}' for run {self.run_id}")
            else:
                logger.warning("Backend provided but run metadata missing, backend will not be initialized")
                self._backend_failed = True
        except Exception as e:
            logger.error(
                f"Failed to initialize backend '{self.backend.name()}': {e}",
                exc_info=True,
            )
            self._backend_failed = True

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log a single metric value.

        Args:
            name: Metric name (e.g., "loss", "accuracy")
            value: Metric value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp
        """
        # Always write to local
        self.local_writer.log_metric(name, value, step, timestamp)

        # Try backend if configured
        self._ensure_backend_initialized()
        if self._backend_initialized and not self._backend_failed:
            try:
                self.backend.log_metric(name, value, step, timestamp)  # type: ignore
            except Exception as e:
                logger.error(
                    f"Backend '{self.backend.name()}' failed to log metric '{name}': {e}"  # type: ignore
                )
                self._backend_failed = True

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log multiple metrics at once.

        Args:
            metrics: Dict of metric_name -> value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp
        """
        # Always write to local
        self.local_writer.log_metrics(metrics, step, timestamp)

        # Try backend if configured
        self._ensure_backend_initialized()
        if self._backend_initialized and not self._backend_failed:
            try:
                self.backend.log_metrics(metrics, step, timestamp)  # type: ignore
            except Exception as e:
                logger.error(
                    f"Backend '{self.backend.name()}' failed to log metrics: {e}"  # type: ignore
                )
                self._backend_failed = True

    def log_artifact(self, name: str, path: str | Path) -> None:
        """Log an artifact (file or directory).

        Args:
            name: Artifact name (e.g., "model", "predictions")
            path: Path to artifact relative to outputs dir
        """
        # Always write to local
        self.local_writer.log_artifact(name, path)

        # Try backend if configured
        self._ensure_backend_initialized()
        if self._backend_initialized and not self._backend_failed:
            try:
                # Backend expects absolute Path
                if not isinstance(path, Path):
                    path = Path(path)
                # Make absolute relative to outputs dir if needed
                if not path.is_absolute():
                    path = self.local_writer.outputs_dir / path
                self.backend.log_artifact(name, path)  # type: ignore
            except Exception as e:
                logger.error(
                    f"Backend '{self.backend.name()}' failed to log artifact '{name}': {e}"  # type: ignore
                )
                self._backend_failed = True

    def flush(self) -> None:
        """Flush buffered metrics to disk.

        This only affects the LocalWriter. Backends typically flush in real-time.
        """
        self.local_writer.flush()

    def finish(self) -> str | None:
        """Finalize the metrics collection.

        Flushes local writer and calls backend.finish() if configured.

        Returns:
            Optional URL to the run in the backend's UI (e.g., W&B run page)
        """
        # Flush local
        self.flush()

        # Try backend finish if configured
        if self._backend_initialized and not self._backend_failed:
            try:
                url = self.backend.finish()  # type: ignore
                logger.info(f"Backend '{self.backend.name()}' finished successfully")  # type: ignore
                return url
            except Exception as e:
                logger.error(
                    f"Backend '{self.backend.name()}' failed to finish: {e}",  # type: ignore
                    exc_info=True,
                )
                return None

        return None

    def __enter__(self) -> MetricsLogger:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """Context manager exit - automatically calls finish()."""
        self.finish()
