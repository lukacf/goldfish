"""W&B (Weights & Biases) metrics backend.

This backend syncs metrics and artifacts to W&B in real-time. It requires:
- wandb package installed (optional dependency)
- WANDB_API_KEY environment variable set on host (passed to container)
- GOLDFISH_GIT_SHA environment variable (automatically set by StageExecutor)

Configuration via environment variables:
- GOLDFISH_WANDB_PROJECT: W&B project name (defaults to goldfish-{workspace})
- GOLDFISH_WANDB_ENTITY: W&B entity/team name (optional)
- GOLDFISH_WANDB_ARTIFACT_MODE: "file" (wandb.save) or "artifact" (wandb.Artifact)
- GOLDFISH_WANDB_ARTIFACT_TYPE: Artifact type when using artifact mode (default "artifact")
- WANDB_API_KEY: W&B API key for authentication (required)
- GOLDFISH_GIT_SHA: Git commit SHA for provenance (automatic)
"""

from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.errors import GoldfishError
from goldfish.metrics.backends.base import MetricsBackend
from goldfish.validation import InvalidArtifactPathError

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
        try:
            import wandb
        except Exception as exc:
            logger.error("Failed to import wandb: %s", exc)
            raise GoldfishError("W&B backend unavailable", {"error": str(exc)}) from exc

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

        try:
            self._run = wandb.init(**init_kwargs)
        except Exception as exc:
            logger.error("W&B init failed: %s", exc)
            raise GoldfishError("W&B init failed", {"error": str(exc)}) from exc
        self._initialized = True

        logger.info(f"Initialized W&B run: project={project}, name={stage}-{run_id}, " f"git_sha={git_sha}")

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        """Log a single metric to W&B.

        Args:
            name: Metric name (e.g., "loss", "accuracy")
            value: Metric value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp (W&B uses step, not timestamp)
        """
        try:
            import wandb

            # W&B uses step for x-axis, not timestamp
            wandb.log({name: value}, step=step)
        except Exception as exc:
            logger.error("W&B log_metric failed: %s", exc)
            raise GoldfishError("W&B log_metric failed", {"error": str(exc)}) from exc

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        """Log multiple metrics to W&B.

        Args:
            metrics: Dict of metric_name -> value
            step: Optional step/epoch number
            timestamp: Optional Unix timestamp (W&B uses step, not timestamp)
        """
        try:
            import wandb

            wandb.log(metrics, step=step)
        except Exception as exc:
            logger.error("W&B log_metrics failed: %s", exc)
            raise GoldfishError("W&B log_metrics failed", {"error": str(exc)}) from exc

    def log_artifact(
        self,
        name: str,
        path: Path,
    ) -> str | None:
        """Log an artifact to W&B.

        Args:
            name: Artifact name (e.g., "model", "predictions")
            path: Path to artifact (file or directory)
        """
        try:
            import wandb
        except Exception as exc:
            logger.error("Failed to import wandb: %s", exc)
            raise GoldfishError("W&B backend unavailable", {"error": str(exc)}) from exc

        if path.is_symlink():
            raise InvalidArtifactPathError(str(path), "artifact path cannot be a symlink")

        artifact_mode = os.environ.get("GOLDFISH_WANDB_ARTIFACT_MODE", "file").strip().lower()
        if artifact_mode == "artifact":
            artifact_type = os.environ.get("GOLDFISH_WANDB_ARTIFACT_TYPE", "artifact")
            artifact = wandb.Artifact(name=name, type=artifact_type)

            if path.is_dir():
                for file_path in path.rglob("*"):
                    if file_path.is_symlink():
                        raise InvalidArtifactPathError(str(file_path), "artifact path cannot contain symlinks")
                artifact.add_dir(str(path))
            else:
                artifact.add_file(str(path))

            try:
                if self._run is not None and hasattr(self._run, "log_artifact"):
                    logged = self._run.log_artifact(artifact)
                else:
                    logged = wandb.log_artifact(artifact)
            except Exception as exc:
                logger.error("W&B artifact logging failed: %s", exc)
                raise GoldfishError("W&B artifact logging failed", {"error": str(exc)}) from exc

            url = getattr(logged, "url", None)
            if url:
                return str(url)
            if self._run is not None:
                return str(self._run.url)
            return None

        # Default mode: wandb.save
        if path.is_dir():
            for file_path in path.rglob("*"):
                if file_path.is_symlink():
                    raise InvalidArtifactPathError(str(file_path), "artifact path cannot contain symlinks")
                if file_path.is_file():
                    try:
                        wandb.save(str(file_path), base_path=str(path))
                    except Exception as exc:
                        logger.error("W&B save failed: %s", exc)
                        raise GoldfishError("W&B artifact save failed", {"error": str(exc)}) from exc
        else:
            try:
                wandb.save(str(path), base_path=str(path.parent))
            except Exception as exc:
                logger.error("W&B save failed: %s", exc)
                raise GoldfishError("W&B artifact save failed", {"error": str(exc)}) from exc

        logger.info(f"Logged artifact '{name}' to W&B: {path}")

        if self._run is not None:
            return self._run.url
        return None

    def finish(self) -> str | None:
        """Finalize the W&B run.

        Returns:
            W&B run URL (e.g., "https://wandb.ai/team/project/runs/abc123")
        """
        try:
            import wandb
        except Exception as exc:
            logger.error("Failed to import wandb: %s", exc)
            raise GoldfishError("W&B backend unavailable", {"error": str(exc)}) from exc

        # Get run URL before finishing
        url = None
        if self._run is not None:
            url = self._run.url

        # Finish the run (uploads any pending data)
        try:
            wandb.finish()
        except Exception as exc:
            logger.error("W&B finish failed: %s", exc)
            raise GoldfishError("W&B finish failed", {"error": str(exc)}) from exc

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
