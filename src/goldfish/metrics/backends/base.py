"""Base class for metrics backends.

Metrics backends are plugins that implement real-time sync to external systems
like Weights & Biases, MLflow, etc. All backends implement the MetricsBackend
abstract base class.
"""

from abc import ABC, abstractmethod
from pathlib import Path


class MetricsBackend(ABC):
    """Abstract base class for metrics backends.

    Backends are responsible for syncing metrics and artifacts to external
    systems in real-time. Each backend is initialized once per stage run and
    receives metric/artifact calls as they happen in the stage code.

    Example implementations:
    - WandBBackend: Syncs to Weights & Biases
    - MLflowBackend: Syncs to MLflow tracking server
    - PrometheusBackend: Exports metrics to Prometheus
    """

    @abstractmethod
    def init_run(
        self,
        run_id: str,
        config: dict,
        workspace: str,
        stage: str,
    ) -> None:
        """Initialize a run in the backend.

        Called once when the first metric is logged. Backends should create
        their run object here (e.g., wandb.init()).

        Args:
            run_id: Goldfish stage run ID (e.g., "stage-abc123")
            config: Stage configuration dict (for logging hyperparameters)
            workspace: Workspace name
            stage: Stage name
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def log_artifact(
        self,
        name: str,
        path: Path,
    ) -> None:
        """Log an artifact (file or directory).

        Args:
            name: Artifact name (e.g., "model", "predictions")
            path: Path to artifact (file or directory)
        """
        pass

    @abstractmethod
    def finish(self) -> str | None:
        """Finalize the run.

        Called when the stage completes. Backends should flush any buffered
        data and close connections.

        Returns:
            Optional URL to the run in the backend's UI (e.g., W&B run page)
        """
        pass

    @classmethod
    @abstractmethod
    def is_available(cls) -> bool:
        """Check if the backend is available.

        Returns True if the backend package is installed and configured.
        For example, WandBBackend would check if `wandb` is importable.

        Returns:
            True if backend is available, False otherwise
        """
        pass

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Return the backend name for registration.

        Used in configuration to select this backend (e.g., "wandb", "mlflow").

        Returns:
            Backend name as string
        """
        pass
