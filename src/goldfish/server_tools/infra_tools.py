"""Goldfish MCP tools - Infrastructure Tools

Docker base image management and warm pool management for project-level customization.
"""

from __future__ import annotations

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
    version: str | None = None,
) -> dict:
    """Unified tool for managing Docker base images.

    Manages two layers of Docker images:
    - "base": Goldfish base images (goldfish-base-{cpu,gpu}) with core ML libraries
    - "project": Project images ({project}-{cpu,gpu}) built on top with customizations

    Supports two build backends:
    - "local": Build using local Docker daemon (default)
    - "cloud": Build using Google Cloud Build (recommended for GPU images - faster, doesn't tie up local machine)

    Base image versions are tracked per-project in the database.

    Args:
        action: "list", "inspect", "check", "build", "push", "set_version", "list_versions", or "next_version"
        image_type: Required for most actions: "cpu" or "gpu"
        no_cache: For build action - force rebuild without Docker cache
        wait: For build action - if True blocks until complete, False returns immediately
              (only affects local builds; cloud builds always return immediately)
        target: "base" for goldfish base images, "project" (default) for project images
        backend: "local" (default) or "cloud" for Cloud Build
        version: For set_version action - version string (e.g., "v11")

    Returns:
        Dict with action results:
        - list: Shows both base and project images with status
        - inspect: Shows Dockerfile contents, packages, config paths for editing
        - check: Compares local vs registry, recommends rebuild/push
        - build: Builds image (async by default, returns build_id)
        - push: Pushes built image to Artifact Registry
        - set_version: Registers a version as current in the database
        - list_versions: Shows version history for an image type
        - next_version: Returns the next auto-incremented version number

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

        # Register new version in database after push
        manage_base_images(action="set_version", image_type="gpu", version="v11")

        # List version history
        manage_base_images(action="list_versions", image_type="gpu")

        # Get next version number for a new build
        manage_base_images(action="next_version", image_type="gpu")

        # Build project image with extra packages
        manage_base_images(action="build", image_type="gpu", target="project", backend="cloud")
    """
    try:
        manager = _get_base_image_manager()
        ctx = get_context()

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

        elif action == "set_version":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for set_version action (use 'cpu' or 'gpu')",
                }
            if not version:
                return {
                    "success": False,
                    "error": "version required for set_version action (e.g., 'v11')",
                }
            validate_image_type(image_type)
            # Build the registry tag
            registry_tag = manager._get_goldfish_base_registry_tag(image_type)
            # Replace the version in the tag (it uses the current DB version, so we need to fix it)
            base_tag = registry_tag.rsplit(":", 1)[0]
            registry_tag = f"{base_tag}:{version}"
            ctx.db.set_base_image_version(image_type, version, registry_tag)
            return {
                "success": True,
                "image_type": image_type,
                "version": version,
                "registry_tag": registry_tag,
                "message": f"Set {image_type} base image version to {version}",
            }

        elif action == "list_versions":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for list_versions action (use 'cpu' or 'gpu')",
                }
            validate_image_type(image_type)
            versions = ctx.db.list_base_image_versions(image_type)
            current = ctx.db.get_current_base_image_version(image_type)
            return {
                "success": True,
                "image_type": image_type,
                "current_version": current["version"] if current else None,
                "versions": versions,
            }

        elif action == "next_version":
            if not image_type:
                return {
                    "success": False,
                    "error": "image_type required for next_version action (use 'cpu' or 'gpu')",
                }
            validate_image_type(image_type)
            next_ver = ctx.db.get_next_base_image_version(image_type)
            return {
                "success": True,
                "image_type": image_type,
                "next_version": next_ver,
            }

        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}. Valid actions: list, inspect, check, build, push, set_version, list_versions, next_version",
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


# =============================================================================
# Warm Pool Tools
# =============================================================================


def _get_warm_pool_manager():
    """Get WarmPoolManager from context, or None if not enabled."""
    if not has_context():
        return None
    ctx = get_context()
    from goldfish.cloud.factory import create_warm_pool_manager

    return create_warm_pool_manager(ctx.db, ctx.config)


@mcp.tool()
def warm_pool_status() -> dict:
    """Show warm pool status: config, instances, and summary counts.

    Returns:
        Dict with warm pool configuration, instance list, and counts by status.
        If warm pool is not enabled, returns {enabled: False}.
    """
    try:
        mgr = _get_warm_pool_manager()
        if not mgr:
            return {"enabled": False, "message": "Warm pool is not enabled"}
        result: dict = mgr.pool_status()
        return result
    except Exception as e:
        logger.exception(f"warm_pool_status failed: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def warm_pool_cleanup() -> dict:
    """Emergency cleanup: delete warm pool instances.

    Deleting instances are retried immediately.
    Leased instances are canceled first when possible; stale leases are force-released.
    Unleased instances are transitioned to deleting and removed once GCE confirms deletion.

    Returns:
        Dict with count of deleted instances and any skipped instances.
    """
    try:
        mgr = _get_warm_pool_manager()
        if not mgr:
            return {"enabled": False, "message": "Warm pool is not enabled"}

        ctx = get_context()
        instances = mgr.list_instances()
        deleted = 0  # Actually removed from GCE
        canceled = 0  # Run canceled, deletion pending via daemon
        skipped: list[str] = []
        for inst in instances:
            name = inst["instance_name"]
            state = inst["state"]
            if state in ("gone",):
                continue
            if state == "deleting":
                # Retry gcloud delete, then verify VM is actually gone
                mgr.delete_gce_instance(name, inst["zone"])
                if mgr.check_instance_status(name, inst["zone"]) == "not_found":
                    mgr.controller.on_delete_confirmed(name)
                    mgr.delete_tracking_row(name)
                    deleted += 1
                # else: still exists, leave in deleting for daemon retry
                continue

            # For instances with active leases, cancel the owning run first.
            # This synchronizes the run state machine before releasing the lease,
            # preventing orphaned runs that think they still have a VM.
            lease_run_id = inst.get("current_lease_run_id")
            if lease_run_id:
                from goldfish.state_machine.cancel import cancel_run

                run_id = lease_run_id
                cancel_result = cancel_run(
                    ctx.db,
                    run_id,
                    reason="Emergency warm pool cleanup — instance being deleted",
                )
                if cancel_result.get("success"):
                    # cancel_run already calls on_run_terminal via cancel.py,
                    # which releases the lease and emits DELETE_REQUESTED for canceled runs.
                    # Actual GCE deletion happens later via daemon polling.
                    canceled += 1
                else:
                    # Cancel failed — run is missing, already terminal, or uncancelable.
                    # This is the exact scenario operators reach for this tool: stale
                    # leases from purged/crashed runs. Force-release the lease and
                    # delete the instance directly instead of skipping it.
                    logger.warning(
                        "warm_pool_cleanup: cancel_run failed for %s (run=%s): %s — force-releasing lease",
                        name,
                        run_id,
                        cancel_result.get("reason"),
                    )
                    mgr.force_release_lease(name)
                    result = mgr.controller.on_delete_requested(
                        name,
                        reason=f"emergency cleanup, cancel failed: {cancel_result.get('reason')}",
                    )
                    if result.success:
                        mgr.delete_gce_instance(name, inst["zone"])
                        if mgr.check_instance_status(name, inst["zone"]) == "not_found":
                            mgr.controller.on_delete_confirmed(name)
                            mgr.delete_tracking_row(name)
                            deleted += 1
                        else:
                            canceled += 1
            else:
                # No active lease — transition to deleting AND attempt GCE deletion now
                result = mgr.controller.on_delete_requested(
                    name,
                    reason="emergency cleanup",
                )
                if result.success:
                    # Attempt GCE deletion, then verify VM is gone before removing row
                    mgr.delete_gce_instance(name, inst["zone"])
                    if mgr.check_instance_status(name, inst["zone"]) == "not_found":
                        mgr.controller.on_delete_confirmed(name)
                        mgr.delete_tracking_row(name)
                        deleted += 1
                    else:
                        # Delete requested but VM still exists — daemon will retry
                        canceled += 1

        remaining = mgr.pool_status()
        return {
            "success": True,
            "deleted": deleted,
            "deletion_pending": canceled,
            "skipped": skipped,
            "remaining": remaining["total"],
            "remaining_instances": remaining.get("instances", []),
        }
    except Exception as e:
        logger.exception(f"warm_pool_cleanup failed: {e}")
        return {"success": False, "error": str(e)}
