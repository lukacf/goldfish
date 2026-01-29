"""Goldfish MCP tools - Backup Tools

Provides MCP tools for database backup management.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from goldfish.cloud.contracts import StorageURI
from goldfish.server_core import (
    _get_config,
    _get_db,
    _get_project_root,
    mcp,
)

if TYPE_CHECKING:
    from goldfish.backup.manager import BackupManager

logger = logging.getLogger("goldfish.server")

# Module-level cache for BackupManager
_backup_manager: BackupManager | None = None


def _get_backup_manager() -> BackupManager | None:
    """Get or create BackupManager.

    Returns None if GCS is not configured (backups require cloud storage).

    Returns:
        BackupManager instance or None if not available.
    """
    global _backup_manager

    if _backup_manager is not None:
        return _backup_manager

    config = _get_config()

    # Backups require GCS configuration
    if config.gcs is None:
        return None

    from goldfish.backup.manager import BackupManager

    # Database path is in the dev repo (sibling to project, not subdirectory)
    project_root = _get_project_root()
    dev_repo = config.get_dev_repo_path(project_root)
    db_path = dev_repo / ".goldfish" / "goldfish.db"

    # Backup bucket: use GCS bucket with "backups/" prefix
    bucket = config.gcs.bucket
    try:
        bucket_root = StorageURI.parse(bucket)
    except ValueError:
        bucket_root = StorageURI("gs", bucket, "")
    bucket_root = StorageURI(bucket_root.scheme, bucket_root.bucket, "")
    gcs_bucket = str(bucket_root.join("backups")).rstrip("/")

    _backup_manager = BackupManager(
        db=_get_db(),
        db_path=db_path,
        gcs_bucket=gcs_bucket,
    )

    return _backup_manager


def _reset_backup_manager() -> None:
    """Reset the backup manager cache.

    Called when config is reloaded.
    """
    global _backup_manager
    _backup_manager = None


def trigger_backup(trigger: str, details: dict | None = None) -> None:
    """Trigger a backup if conditions allow (fire-and-forget).

    This is called by other MCP tools to trigger automatic backups.
    Respects rate limiting and logs any errors without raising exceptions.

    Args:
        trigger: What triggered the backup (run, save_version, create_workspace, etc.)
        details: Optional details dict (workspace, version, run_id, etc.)

    """
    try:
        manager = _get_backup_manager()
        if manager is None:
            return  # GCS not configured, skip silently

        result = manager.maybe_backup(trigger=trigger, details=details)
        if result:
            logger.info(f"Auto-backup created: {result['tier']} tier " f"(trigger={trigger}, id={result['backup_id']})")
    except Exception as e:
        # Fire-and-forget: log error but don't fail the operation
        logger.warning(f"Auto-backup failed (non-fatal): {e}")


@mcp.tool()
def list_backups(tier: str | None = None, include_deleted: bool = False) -> dict:
    """List database backups.

    Args:
        tier: Filter by tier ("event", "daily", "weekly", "monthly")
        include_deleted: Include deleted backups.

    Returns:
        dict with backups list or error.
    """
    manager = _get_backup_manager()
    if manager is None:
        return {
            "success": False,
            "error": "Backups not available - GCS not configured",
        }

    try:
        backups = manager.list_backups(tier=tier, include_deleted=include_deleted)

        # Get counts by tier
        counts = _get_db().count_backups_by_tier()

        return {
            "success": True,
            "backups": backups,
            "counts": counts,
            "total": len(backups),
        }
    except Exception as e:
        logger.error(f"Failed to list backups: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_backup(trigger: str = "manual", details: dict | None = None) -> dict:
    """Create a database backup manually.

    Backups are usually created automatically by Goldfish operations.
    Use this for manual backups before risky operations.

    Args:
        trigger: What triggered this backup (default: "manual")
        details: Optional details dict (workspace, version, etc.)

    Returns:
        dict with backup info or error.
    """
    import shutil

    manager = _get_backup_manager()
    if manager is None:
        return {
            "success": False,
            "error": "Backups not available - GCS not configured",
        }

    # Pre-check: verify database file exists
    if not manager.db_path.exists():
        return {
            "success": False,
            "error": f"Database file not found: {manager.db_path}",
        }

    # Pre-check: verify gsutil is available
    if shutil.which("gsutil") is None:
        return {
            "success": False,
            "error": "gsutil command not found - install Google Cloud SDK",
        }

    try:
        result = manager.create_backup(trigger=trigger, details=details)

        if result is None:
            # create_backup returns None on gsutil upload failure
            return {
                "success": False,
                "error": f"Backup upload failed - check GCS bucket permissions for {manager.gcs_bucket}",
            }

        # Record in audit trail
        _get_db().log_audit(
            operation="create_backup",
            reason=f"Manual backup: {trigger}",
            slot=None,
            workspace=None,
            details={
                "backup_id": result["backup_id"],
                "tier": result["tier"],
                "trigger": trigger,
            },
        )

        return {
            "success": True,
            "backup_id": result["backup_id"],
            "tier": result["tier"],
            "gcs_path": result["gcs_path"],
            "size_bytes": result.get("size_bytes"),
        }
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def cleanup_backups() -> dict:
    """Clean up expired and excess backups.

    Removes:
    - Backups past their expiry date
    - Excess event backups (keeps max 20)

    Returns:
        dict with cleanup stats or error.
    """
    manager = _get_backup_manager()
    if manager is None:
        return {
            "success": False,
            "error": "Backups not available - GCS not configured",
        }

    try:
        result = manager.cleanup()

        # Record in audit trail
        _get_db().log_audit(
            operation="cleanup_backups",
            reason="Manual backup cleanup",
            slot=None,
            workspace=None,
            details=result,
        )

        return {
            "success": True,
            "deleted_expired": result["deleted_expired"],
            "deleted_excess": result["deleted_excess"],
        }
    except Exception as e:
        logger.error(f"Failed to cleanup backups: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_backup_status() -> dict:
    """Get backup system status and statistics.

    Returns:
        dict with backup status, counts by tier, last backup info.
    """
    manager = _get_backup_manager()
    if manager is None:
        return {
            "success": False,
            "error": "Backups not available - GCS not configured",
        }

    try:
        db = _get_db()
        counts = db.count_backups_by_tier()
        last_backup = db.get_last_backup()
        expired = db.get_expired_backups()

        return {
            "success": True,
            "enabled": True,
            "counts": counts,
            "last_backup": {
                "backup_id": last_backup["backup_id"],
                "tier": last_backup["tier"],
                "trigger": last_backup["trigger"],
                "created_at": last_backup["created_at"],
            }
            if last_backup
            else None,
            "expired_pending_cleanup": len(expired),
        }
    except Exception as e:
        logger.error(f"Failed to get backup status: {e}")
        return {"success": False, "error": str(e)}
