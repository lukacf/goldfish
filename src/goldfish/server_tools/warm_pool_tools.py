"""Goldfish MCP tools — Warm Pool Management.

Provides tools for viewing and managing warm pool instances.
"""

from __future__ import annotations

import logging

from goldfish.server_core import (
    _get_config,
    _get_db,
    mcp,
)

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def warm_pool_status() -> dict:
    """Show warm pool status — active instances, idle times, costs.

    Returns:
        dict with pool status information
    """
    config = _get_config()
    db = _get_db()

    if not config.gce or not config.gce.warm_pool.enabled:
        return {
            "enabled": False,
            "message": "Warm pool is not enabled. Set gce.warm_pool.enabled=true in goldfish.yaml.",
        }

    instances = db.list_warm_instances()
    wp_config = config.gce.warm_pool

    summary: dict = {
        "enabled": True,
        "max_instances": wp_config.max_instances,
        "idle_timeout_minutes": wp_config.idle_timeout_minutes,
        "profiles": wp_config.profiles or "all",
        "total": len(instances),
        "by_status": {},
        "instances": [],
    }

    for inst in instances:
        status = inst["status"]
        summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
        summary["instances"].append(
            {
                "name": inst["instance_name"],
                "zone": inst["zone"],
                "machine_type": inst["machine_type"],
                "gpu_count": inst["gpu_count"],
                "status": status,
                "idle_since": inst.get("idle_since"),
                "current_run": inst.get("current_stage_run_id"),
                "image": inst.get("image_tag"),
            }
        )

    return summary


@mcp.tool()
def warm_pool_cleanup(reason: str) -> dict:
    """Emergency: delete ALL warm pool instances immediately.

    Use this when you need to stop all warm instances to save costs.

    Args:
        reason: Why you're cleaning up (audit trail)

    Returns:
        dict with cleanup results
    """
    config = _get_config()
    db = _get_db()

    if not config.gce:
        return {"success": False, "error": "GCE not configured"}

    from goldfish.cloud.factory import create_warm_pool_manager

    manager = create_warm_pool_manager(config, db)
    if not manager:
        return {"success": False, "error": "Warm pool manager could not be created"}

    reaped = manager.reap_all()

    db.log_audit(
        operation="warm_pool_cleanup",
        reason=reason,
        details={"instances_deleted": reaped},
    )

    return {"success": True, "instances_deleted": reaped, "reason": reason}
