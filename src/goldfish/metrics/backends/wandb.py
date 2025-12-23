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
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

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
        self._last_error: str | None = None

    def _record_error(self, message: str) -> None:
        logger.error(message)
        self._last_error = message

    def consume_error(self) -> str | None:
        error = self._last_error
        self._last_error = None
        return error

    def _iter_files_no_symlinks(self, root: Path) -> Iterable[Path]:
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            for dirname in list(dirnames):
                dir_path = Path(dirpath) / dirname
                if dir_path.is_symlink():
                    raise InvalidArtifactPathError(str(dir_path), "artifact path cannot contain symlinks")
            for filename in filenames:
                file_path = Path(dirpath) / filename
                if file_path.is_symlink():
                    raise InvalidArtifactPathError(str(file_path), "artifact path cannot contain symlinks")
                if file_path.is_file():
                    yield file_path

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
            self._record_error(f"Failed to import wandb: {exc}")
            return

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
            self._record_error(f"W&B init failed: {exc}")
            return
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
            self._record_error(f"W&B log_metric failed: {exc}")

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
            self._record_error(f"W&B log_metrics failed: {exc}")

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
            self._record_error(f"Failed to import wandb: {exc}")
            return None

        if path.is_symlink():
            raise InvalidArtifactPathError(str(path), "artifact path cannot be a symlink")

        artifact_mode = os.environ.get("GOLDFISH_WANDB_ARTIFACT_MODE", "file").strip().lower()
        if artifact_mode == "artifact":
            artifact_type = os.environ.get("GOLDFISH_WANDB_ARTIFACT_TYPE", "artifact")
            artifact = wandb.Artifact(name=name, type=artifact_type)

            if path.is_dir():
                for file_path in self._iter_files_no_symlinks(path):
                    artifact.add_file(str(file_path), name=str(file_path.relative_to(path)))
            else:
                artifact.add_file(str(path))

            try:
                if self._run is not None and hasattr(self._run, "log_artifact"):
                    logged = self._run.log_artifact(artifact)
                else:
                    logged = wandb.log_artifact(artifact)
            except Exception as exc:
                self._record_error(f"W&B artifact logging failed: {exc}")
                return None

            url = getattr(logged, "url", None)
            if url:
                return str(url)
            if self._run is not None:
                return str(self._run.url)
            return None

        # Default mode: wandb.save
        if path.is_dir():
            for file_path in self._iter_files_no_symlinks(path):
                try:
                    wandb.save(str(file_path), base_path=str(path))
                except Exception as exc:
                    self._record_error(f"W&B save failed: {exc}")
                    return None
        else:
            try:
                wandb.save(str(path), base_path=str(path.parent))
            except Exception as exc:
                self._record_error(f"W&B save failed: {exc}")
                return None

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
            self._record_error(f"Failed to import wandb: {exc}")
            return None

        # Get run URL before finishing
        url = None
        if self._run is not None:
            url = self._run.url

        # Finish the run (uploads any pending data)
        try:
            wandb.finish()
        except Exception as exc:
            self._record_error(f"W&B finish failed: {exc}")
            return None

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
