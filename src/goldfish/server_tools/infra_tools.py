"""Goldfish MCP tools - Infrastructure Tools

Docker base image management for project-level customization.
"""

import logging

from goldfish.context import get_context, has_context
from goldfish.errors import GoldfishError
from goldfish.infra.base_images.manager import BaseImageManager
from goldfish.server_core import mcp
from goldfish.validation import validate_build_id, validate_image_type

logger = logging.getLogger("goldfish.server")

# Lazy singleton for BaseImageManager
_base_image_manager: BaseImageManager | None = None


def _get_base_image_manager() -> BaseImageManager:
    """Get or create the BaseImageManager singleton.

    Returns:
        BaseImageManager instance

    Raises:
        GoldfishError: If server not initialized
    """
    global _base_image_manager
    if _base_image_manager is None:
        if not has_context():
            raise GoldfishError("Server not initialized")
        ctx = get_context()
        _base_image_manager = BaseImageManager(ctx.project_root, ctx.config, db=ctx.db)
    return _base_image_manager


def _reset_base_image_manager() -> None:
    """Reset the manager singleton (used when config reloads)."""
    global _base_image_manager
    _base_image_manager = None


@mcp.tool()
def manage_base_images(
    action: str,
    image_type: str | None = None,
    no_cache: bool = False,
    wait: bool = False,
    target: str = "project",
    backend: str = "local",
) -> dict:
    """Unified tool for managing Docker base images.

    Manages two layers of Docker images:
    - "base": Goldfish base images (goldfish-base-{cpu,gpu}) with core ML libraries
    - "project": Project images ({project}-{cpu,gpu}) built on top with customizations

    Supports two build backends:
    - "local": Build using local Docker daemon (default)
    - "cloud": Build using Google Cloud Build (recommended for GPU images - faster, doesn't tie up local machine)

    Args:
        action: "list", "inspect", "check", "build", or "push"
        image_type: Required for inspect/build/push: "cpu" or "gpu"
        no_cache: For build action - force rebuild without Docker cache
        wait: For build action - if True blocks until complete, False returns immediately
              (only affects local builds; cloud builds always return immediately)
        target: "base" for goldfish base images, "project" (default) for project images
        backend: "local" (default) or "cloud" for Cloud Build

    Returns:
        Dict with action results:
        - list: Shows both base and project images with status
        - inspect: Shows Dockerfile contents, packages, config paths for editing
        - check: Compares local vs registry, recommends rebuild/push
        - build: Builds image (async by default, returns build_id)
        - push: Pushes built image to Artifact Registry

    Examples:
        # View all images (base + project)
        manage_base_images(action="list")

        # Build goldfish base GPU image locally (slow, ~20min for GPU)
        manage_base_images(action="build", image_type="gpu", target="base", wait=True)

        # Build goldfish base GPU image on Cloud Build (faster, doesn't tie up local machine)
        manage_base_images(action="build", image_type="gpu", target="base", backend="cloud")
        # Returns immediately with build_id, poll with get_build_status()

        # Push to Artifact Registry
        manage_base_images(action="push", image_type="gpu", target="base")

        # Build project image with extra packages
        manage_base_images(action="build", image_type="gpu", target="project", backend="cloud")
    """
    try:
        manager = _get_base_image_manager()

        if action == "list":
            return manager.list_images()

        elif action == "inspect":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for inspect action (use 'cpu' or 'gpu')",
                }
            validate_image_type(image_type)
            return manager.inspect_image(image_type)

        elif action == "check":
            return manager.check_images()

        elif action == "build":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for build action (use 'cpu' or 'gpu')",
                }
            validate_image_type(image_type)
            return manager.build_image(image_type, no_cache=no_cache, wait=wait, target=target, backend=backend)

        elif action == "push":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for push action (use 'cpu' or 'gpu')",
                }
            validate_image_type(image_type)
            return manager.push_image(image_type, target=target)

        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}. Valid actions: list, inspect, check, build, push",
            }

    except GoldfishError as e:
        logger.error(f"manage_base_images failed: {e}")
        return {"success": False, "error": e.message, "details": e.details}
    except Exception as e:
        logger.exception(f"Unexpected error in manage_base_images: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_build_status(build_id: str) -> dict:
    """Get status of a Docker image build operation.

    Works for both local and Cloud Build backends.
    For cloud builds, automatically polls Cloud Build API to get latest status.

    Args:
        build_id: Build ID returned from manage_base_images(action="build")

    Returns:
        Dict with:
        - build_id: The build ID
        - image_type: "cpu" or "gpu"
        - target: "base" or "project"
        - backend: "local" or "cloud"
        - status: "pending", "building", "completed", "failed", or "cancelled"
        - started_at: ISO timestamp
        - completed_at: ISO timestamp (if finished)
        - error: Error message (if failed)
        - image_tag: Local image tag (if completed, local builds only)
        - registry_tag: Registry image tag (cloud builds)
        - logs_tail: Last 20 lines of build output (local builds only)
        - logs_uri: URL to Cloud Build logs (cloud builds only)
        - cloud_build_id: GCP Cloud Build operation ID (cloud builds only)
    """
    try:
        validate_build_id(build_id)
        manager = _get_base_image_manager()
        return manager.get_build_status(build_id)

    except GoldfishError as e:
        logger.error(f"get_build_status failed: {e}")
        return {"success": False, "error": e.message, "details": e.details}
    except Exception as e:
        logger.exception(f"Unexpected error in get_build_status: {e}")
        return {"success": False, "error": str(e)}
