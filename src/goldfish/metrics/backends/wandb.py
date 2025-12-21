"""W&B (Weights & Biases) metrics backend.

This backend syncs metrics and artifacts to W&B in real-time. It requires:
- wandb package installed (optional dependency)
- WANDB_API_KEY environment variable set on host (passed to container)
- GOLDFISH_GIT_SHA environment variable (automatically set by StageExecutor)

Configuration via environment variables:
- GOLDFISH_WANDB_PROJECT: W&B project name (defaults to goldfish-{workspace})
- GOLDFISH_WANDB_ENTITY: W&B entity/team name (optional)
- WANDB_API_KEY: W&B API key for authentication (required)
- GOLDFISH_GIT_SHA: Git commit SHA for provenance (automatic)
"""

from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.metrics.backends.base import MetricsBackend

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class WandBBackend(MetricsBackend):
    """W&B metrics backend for real-time sync.

    Syncs metrics and artifacts to Weights & Biases. Requires wandb package
    and WANDB_API_KEY environment variable.

    Example:
        # In container with wandb installed and WANDB_API_KEY set
        backend = WandBBackend()
        backend.init_run(
            run_id="stage-abc123",
            config={"lr": 0.01},
            workspace="baseline",
            stage="train"
        )
        backend.log_metric("loss", 0.5, step=1)
        backend.finish()
    """

    def __init__(self) -> None:
        """Initialize W&B backend."""
        self._run = None
        self._initialized = False

    def init_run(
        self,
        run_id: str,
        config: dict,
        workspace: str,
        stage: str,
    ) -> None:
        """Initialize a W&B run.

        Args:
            run_id: Goldfish stage run ID (e.g., "stage-abc123")
            config: Stage configuration dict (logged as hyperparameters)
            workspace: Workspace name (used for tagging and default project)
            stage: Stage name (used for run naming and tagging)
        """
        # Import wandb here (lazy import) so the module loads even if wandb not installed
        import wandb

        # Get git SHA from environment (set by StageExecutor)
        git_sha = os.environ.get("GOLDFISH_GIT_SHA")

        # Get W&B project from environment
        # Priority: GOLDFISH_WANDB_PROJECT > GOLDFISH_PROJECT_NAME > fallback
        project = os.environ.get("GOLDFISH_WANDB_PROJECT")
        if not project:
            # Default to Goldfish project name
            project_name = os.environ.get("GOLDFISH_PROJECT_NAME")
            project = project_name if project_name else f"goldfish-{workspace}"

        # Get W&B group (for organizing related runs)
        # Default to workspace name to group all stages from same workspace
        group = os.environ.get("GOLDFISH_WANDB_GROUP", workspace)

        # Get W&B entity (team/user)
        entity = os.environ.get("GOLDFISH_WANDB_ENTITY")

        # Create W&B run with metadata
        init_kwargs = {
            "project": project,
            "group": group,  # Groups all stages from same workspace together
            "name": f"{stage}-{run_id}",
            "config": config,
            "tags": [workspace, stage],
            "notes": f"Goldfish run {run_id}",
        }

        # Add entity if specified
        if entity:
            init_kwargs["entity"] = entity

        # Add git SHA if available
        if git_sha:
            init_kwargs["settings"] = wandb.Settings(git_commit=git_sha)

        self._run = wandb.init(**init_kwargs)
        self._initialized = True

        logger.info(f"Initialized W&B run: project={project}, name={stage}-{run_id}, " f"git_sha={git_sha}")

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log a single metric to W&B.

        Args:
            name: Metric name (e.g., "loss", "accuracy")
            value: Metric value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp (W&B uses step, not timestamp)
        """
        import wandb

        # W&B uses step for x-axis, not timestamp
        wandb.log({name: value}, step=step)

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Log multiple metrics to W&B.

        Args:
            metrics: Dict of metric_name -> value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp (W&B uses step, not timestamp)
        """
        import wandb

        wandb.log(metrics, step=step)

    def log_artifact(
        self,
        name: str,
        path: Path,
    ) -> None:
        """Log an artifact to W&B.

        Args:
            name: Artifact name (e.g., "model", "predictions")
            path: Path to artifact (file or directory)
        """
        import wandb

        # Determine base path (parent of the artifact)
        base_path = str(path.parent)

        # For directories, use glob pattern
        if path.is_dir():
            # W&B expects "dir/*" pattern for directories
            artifact_path = str(path / "*")
        else:
            artifact_path = str(path)

        # wandb.save uploads files to W&B
        wandb.save(artifact_path, base_path=base_path)

        logger.info(f"Logged artifact '{name}' to W&B: {path}")

    def finish(self) -> str | None:
        """Finalize the W&B run.

        Returns:
            W&B run URL (e.g., "https://wandb.ai/team/project/runs/abc123")
        """
        import wandb

        # Get run URL before finishing
        url = None
        if self._run is not None:
            url = self._run.url

        # Finish the run (uploads any pending data)
        wandb.finish()

        logger.info(f"Finished W&B run: {url}")
        return url

    @classmethod
    def is_available(cls) -> bool:
        """Check if W&B backend is available.

        Returns True if wandb package is installed.

        Returns:
            True if wandb is available, False otherwise
        """
        return importlib.util.find_spec("wandb") is not None

    @classmethod
    def name(cls) -> str:
        """Return backend name.

        Returns:
            "wandb"
        """
        return "wandb"
