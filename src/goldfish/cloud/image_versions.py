"""Unified image version resolution for Goldfish.

This module provides a single source of truth for image version resolution,
consolidating the previously scattered version logic.

Version resolution precedence:
- BASE images: config -> DB -> default constant (Goldfish ships base images)
- PROJECT images: config -> DB -> None (project images are user-built, NO default)

Usage:
    from goldfish.cloud.image_versions import ImageVersionResolver

    resolver = ImageVersionResolver(config, db)

    # Base images always resolve (Goldfish ships them)
    base_version = resolver.get_version("gpu", "base")
    print(f"Using base image version {base_version.version} from {base_version.source}")

    # Project images return None if not built yet
    project_version = resolver.get_version("gpu", "project")
    if project_version is None:
        # Caller decides: build new project image OR fall back to base
        pass
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from goldfish.config import GoldfishConfig
    from goldfish.db.database import Database

# =============================================================================
# Default Version Constants
# =============================================================================

# Default base image version - used when no config or DB version exists
# This should match the latest stable goldfish-base-{cpu,gpu} version
# NOTE: Only BASE images have defaults (Goldfish ships them).
#       Project images do NOT have defaults - they're user-built.
BASE_IMAGE_VERSION_DEFAULT = "v10"

# Base image names (short names, resolved with artifact_registry)
BASE_IMAGE_CPU = "goldfish-base-cpu"
BASE_IMAGE_GPU = "goldfish-base-gpu"

# Public fallback images (used when no artifact_registry configured)
PUBLIC_BASE_IMAGE_CPU = "quay.io/jupyter/pytorch-notebook:python-3.11"
PUBLIC_BASE_IMAGE_GPU = "nvcr.io/nvidia/pytorch:24.01-py3"

# Bare Python fallback (requires requirements.txt)
FALLBACK_BASE_IMAGE = "python:3.11-slim"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ImageVersion:
    """Resolved image version with provenance tracking.

    Attributes:
        version: Version string (e.g., "v10", "v1")
        source: Where the version came from ("config", "database", "default")
        registry_tag: Full registry tag if available, None for defaults
    """

    version: str
    source: str  # "config", "database", "default"
    registry_tag: str | None


# =============================================================================
# Version Resolver
# =============================================================================


class ImageVersionResolver:
    """Single source of truth for image version resolution.

    Resolution precedence:
    - BASE images: config -> DB -> default constant (Goldfish ships base images)
    - PROJECT images: config -> DB -> None (project images are user-built)

    CRITICAL: Project images return None when no version exists.
    This is NOT a bug. Project images are user-built, not Goldfish-shipped.
    The caller must decide: build a new project image OR fall back to base.

    This class consolidates the previously scattered version resolution logic from:
    - profiles.py: BASE_IMAGE_VERSION constant
    - manager.py: _get_base_image_version() method
    - config.py: DockerConfig.base_image_version field (which was previously unused!)

    Example:
        resolver = ImageVersionResolver(config, db)

        # Base images always resolve
        base_gpu = resolver.get_version("gpu", "base")
        print(f"Using {base_gpu.version} from {base_gpu.source}")

        # Project images may return None
        project_gpu = resolver.get_version("gpu", "project")
        if project_gpu is None:
            # Build new project image or fall back to base
            pass
    """

    def __init__(
        self,
        config: GoldfishConfig,
        db: Database | None = None,
    ):
        """Initialize the resolver.

        Args:
            config: GoldfishConfig instance with docker settings
            db: Optional Database instance for version tracking
        """
        self.config = config
        self.db = db

    def get_version(
        self,
        image_type: str,
        image_layer: str = "base",
    ) -> ImageVersion | None:
        """Get the effective version for an image.

        Args:
            image_type: "cpu" or "gpu"
            image_layer: "base" (goldfish-base-*) or "project" ({project}-*)

        Returns:
            ImageVersion with version string, source, and optional registry_tag.
            Returns None for project images when no config/DB version exists
            (project images are user-built, there's no default).

        Raises:
            ValueError: If image_layer is not "base" or "project"
        """
        if image_layer == "base":
            return self._resolve_base_version(image_type)
        elif image_layer == "project":
            return self._resolve_project_version(image_type)
        else:
            raise ValueError(f"Invalid image_layer: {image_layer}. Must be 'base' or 'project'")

    def _resolve_base_version(self, image_type: str) -> ImageVersion:
        """Resolve base image version with precedence: config -> DB -> default.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            ImageVersion for base image
        """
        # Priority 1: Config override
        config_version = self.config.docker.base_image_version
        if config_version:
            return ImageVersion(
                version=config_version,
                source="config",
                registry_tag=self._make_registry_tag("base", image_type, config_version),
            )

        # Priority 2: Database current version
        if self.db is not None:
            db_info = self.db.get_current_base_image_version(image_type)
            if db_info is not None:
                return ImageVersion(
                    version=str(db_info["version"]),
                    source="database",
                    registry_tag=db_info.get("registry_tag"),
                )

        # Priority 3: Default constant
        return ImageVersion(
            version=BASE_IMAGE_VERSION_DEFAULT,
            source="default",
            registry_tag=None,
        )

    def _resolve_project_version(self, image_type: str) -> ImageVersion | None:
        """Resolve project image version with precedence: config -> DB -> None.

        CRITICAL: Returns None when no version exists. This is intentional.
        Project images are user-built, not Goldfish-shipped. There is NO default.
        The caller must decide: build a new project image OR fall back to base.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            ImageVersion for project image, or None if not yet built
        """
        # Priority 1: Config override
        config_version = getattr(self.config.docker, "project_image_version", None)
        if config_version:
            return ImageVersion(
                version=config_version,
                source="config",
                registry_tag=self._make_registry_tag("project", image_type, config_version),
            )

        # Priority 2: Database current version
        if self.db is not None:
            project_name = self.config.project_name
            db_info = self.db.get_current_project_image_version(project_name, image_type)
            if db_info is not None:
                return ImageVersion(
                    version=str(db_info["version"]),
                    source="database",
                    registry_tag=db_info.get("registry_tag"),
                )

        # No config, no DB version = project image not built yet
        # Return None so caller can decide: build new image or fall back to base
        return None

    def _make_registry_tag(
        self,
        image_layer: str,
        image_type: str,
        version: str,
    ) -> str | None:
        """Build a registry tag for an image.

        Args:
            image_layer: "base" or "project"
            image_type: "cpu" or "gpu"
            version: Version string (e.g., "v10")

        Returns:
            Full registry tag if artifact_registry is configured, None otherwise
        """
        if self.config.gce is None:
            return None

        registry = self.config.gce.effective_artifact_registry
        if not registry:
            return None

        if image_layer == "base":
            image_name = BASE_IMAGE_GPU if image_type == "gpu" else BASE_IMAGE_CPU
        else:
            image_name = f"{self.config.project_name}-{image_type}"

        return f"{registry}/{image_name}:{version}"
