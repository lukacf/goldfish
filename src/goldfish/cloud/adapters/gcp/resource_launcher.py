"""Capacity-aware resource launcher for GCE.

Ported from legacy infra/resource_launcher.py.
Provides intelligent multi-zone capacity search with retry logic.
"""

import json
import subprocess
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from goldfish.errors import GoldfishError

# Capacity error patterns from GCE
# These are matched as substrings (lowered) against gcloud stderr.
# IMPORTANT: GCE uses "enough resources" (not "sufficient") in zone
# resource pool exhaustion errors. Missing patterns cause spot launches
# to fail on the first zone without retrying others.
CAPACITY_PATTERNS = (
    "zone_resource_pool_exhausted",
    "does not have sufficient resources",
    "does not have enough resources",
    "quota",
    "was not able to fulfil",
    "resource is not available",
    "insufficient",
    "is not available in zone",
)


class CapacityError(Exception):
    """Raised when GCE reports a capacity or quota issue."""

    pass


@dataclass
class LaunchSelection:
    """Selected resource configuration for a launch."""

    resource: str
    zone: str
    preemptible: bool


@dataclass
class LaunchResult:
    """Result of a successful instance launch."""

    instance_name: str
    disk_name: str | None
    selection: LaunchSelection
    timings: dict[str, float]
    attempt_log: list[dict[str, Any]]
    run_id: str | None = None  # Run identifier for tracking
    log_uri: str | None = None  # GCS URI for logs
    artifact_uri: str | None = None  # GCS URI for artifacts


def run_gcloud(
    cmd: list[str],
    *,
    allow_capacity: bool = False,
    check: bool = True,
    timeout: int = 60,
    project_id: str | None = None,
) -> subprocess.CompletedProcess:
    """Run gcloud command with capacity error detection.

    Args:
        cmd: Command list (e.g., ["gcloud", "compute", "instances", "create", ...])
        allow_capacity: If True, raise CapacityError on capacity issues
        check: Raise exception on non-zero exit code
        timeout: Command timeout in seconds (default 60)
        project_id: Explicit GCP project ID to use

    Returns:
        CompletedProcess
    """
    if project_id and "--project" not in cmd:
        cmd.append(f"--project={project_id}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        if check:
            raise GoldfishError(f"gcloud command timed out after {timeout}s: {' '.join(cmd)}") from None
        return subprocess.CompletedProcess(cmd, 1, "", f"Timed out after {timeout}s")

    if result.returncode == 0:
        return result

    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()

    # Check for capacity errors
    if allow_capacity and any(pattern in lowered for pattern in CAPACITY_PATTERNS):
        raise CapacityError(output.strip())

    if check:
        raise GoldfishError(f"gcloud command failed: {output.strip()}")

    return result


def order_resources(
    resources: list[dict[str, Any]],
    gpu_preference: Iterable[str],
    force_gpu: str | None,
) -> list[dict[str, Any]]:
    """Order resources by GPU preference.

    Args:
        resources: List of resource dicts from catalog
        gpu_preference: Ordered list of preferred GPU types (e.g., ["h100", "a100", "none"])
        force_gpu: If set, only include this GPU type. Can be either the short type
                   name (e.g., "h100") or the GCE accelerator name (e.g., "nvidia-h100-80gb").

    Returns:
        Ordered list of resources
    """
    # Group by GPU type (short name like "h100")
    by_type: dict[str, list[dict[str, Any]]] = {}
    for res in resources:
        gpu_info = res.get("gpu") or {}
        gpu_type = (gpu_info.get("type") or "none").lower()
        res["_gpu_type"] = gpu_type
        by_type.setdefault(gpu_type, []).append(res)

    if force_gpu:
        forced = force_gpu.lower()

        # Try matching by short type first (backward compatibility)
        if forced in by_type:
            return by_type[forced]

        # If not found, try matching by accelerator name
        # This handles the case where gce_launcher passes the GCE accelerator
        # name (e.g., "nvidia-h100-80gb") instead of the short type ("h100")
        filtered = [r for r in resources if (r.get("gpu", {}).get("accelerator") or "").lower() == forced]
        if filtered:
            return filtered

        raise GoldfishError(f"force_gpu={force_gpu} not present in resource catalog")

    # Order by preference
    ordered_types: list[str] = []
    for pref in gpu_preference or []:
        pref = pref.lower()
        if pref in by_type and pref not in ordered_types:
            ordered_types.append(pref)

    # Add remaining types
    for gpu_type in by_type:
        if gpu_type not in ordered_types:
            ordered_types.append(gpu_type)

    # Flatten
    ordered: list[dict[str, Any]] = []
    for gpu_type in ordered_types:
        ordered.extend(by_type.get(gpu_type, []))

    return ordered


def mode_order(resource: dict[str, Any], preference: str, force_mode: str | None) -> list[str]:
    """Determine preemptible mode ordering for a resource.

    Args:
        resource: Resource dict
        preference: "spot_first" or "on_demand_first"
        force_mode: "spot" or "on_demand" to force specific mode

    Returns:
        List of modes to try (e.g., ["spot", "on_demand"])
    """
    preempt_allowed = resource.get("preemptible_allowed") or resource.get("preemptible", False)

    if force_mode == "spot":
        return ["spot"] if preempt_allowed else []

    if force_mode == "on_demand":
        return ["on_demand"] if resource.get("on_demand_allowed", True) else []

    preferred = ["spot", "on_demand"] if preference == "spot_first" else ["on_demand", "spot"]

    modes: list[str] = []
    for mode in preferred:
        if mode == "spot" and preempt_allowed:
            modes.append("spot")
        if mode == "on_demand" and resource.get("on_demand_allowed", True):
            modes.append("on_demand")

    return modes


def cleanup_disk(disk_name: str, zone: str) -> None:
    """Delete disk (best effort).

    Args:
        disk_name: Disk name
        zone: GCE zone
    """
    run_gcloud(
        [
            "gcloud",
            "compute",
            "disks",
            "delete",
            disk_name,
            f"--zone={zone}",
            "--quiet",
        ],
        check=False,
    )


def wait_for_instance_ready(
    instance_name: str,
    zone: str,
    project_id: str | None = None,
    timeout_sec: int = 120,
    poll_interval: float = 2.0,
) -> None:
    """Wait for GCE instance to reach RUNNING state.

    After `gcloud compute instances create` returns, the instance may still be in
    PROVISIONING or STAGING state. This function polls until the instance is
    fully in RUNNING state, which is required before metadata operations can succeed.

    Args:
        instance_name: Instance name
        zone: GCE zone
        project_id: Optional GCP project ID
        timeout_sec: Maximum time to wait (default 120s for GPU instances)
        poll_interval: Time between polls (default 2s)

    Raises:
        GoldfishError: If instance doesn't reach RUNNING within timeout
    """
    import logging

    logger = logging.getLogger(__name__)
    deadline = time.time() + timeout_sec
    last_status = None

    while time.time() < deadline:
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "describe",
            instance_name,
            f"--zone={zone}",
            "--format=value(status)",
        ]
        if project_id:
            cmd.append(f"--project={project_id}")

        result = run_gcloud(cmd, check=False, project_id=project_id, timeout=30)

        if result.returncode == 0:
            status = result.stdout.strip()
            if status != last_status:
                logger.info("Instance %s status: %s", instance_name, status)
                last_status = status

            if status == "RUNNING":
                logger.info("Instance %s is ready", instance_name)
                return
            elif status in ("TERMINATED", "STOPPED", "SUSPENDED"):
                raise GoldfishError(f"Instance {instance_name} reached unexpected state: {status}")
            # PROVISIONING, STAGING - keep waiting
        else:
            # Instance might not exist yet in API - keep waiting
            logger.debug("Instance %s not yet queryable: %s", instance_name, result.stderr)

        time.sleep(poll_interval)

    raise GoldfishError(
        f"Instance {instance_name} did not reach RUNNING state within {timeout_sec}s " f"(last status: {last_status})"
    )


class ResourceLauncher:
    """Capacity-aware launcher driven by resource catalog.

    Searches across multiple zones and GPU types, retrying on capacity errors.

    Args:
        resources: List of resource dicts from catalog
        gpu_preference: Ordered list of GPU types (e.g., ["h100", "a100", "none"])
        force_gpu: Restrict to single GPU type
        preemptible_preference: "spot_first" or "on_demand_first"
        force_preemptible: Force "spot" or "on_demand"
        zones_override: Restrict search to these zones
        search_timeout_sec: Maximum search time (default 600)
        initial_backoff_sec: Initial retry backoff (default 5)
        backoff_multiplier: Backoff multiplier (default 1.5)
        max_attempts: Maximum attempts (default 100)
        project_id: GCP project ID
    """

    def __init__(
        self,
        resources: list[dict[str, Any]],
        *,
        gpu_preference: list[str] | None = None,
        force_gpu: str | None = None,
        preemptible_preference: str = "spot_first",
        force_preemptible: str | None = None,
        zones_override: list[str] | None = None,
        search_timeout_sec: int = 600,
        initial_backoff_sec: float = 5,
        backoff_multiplier: float = 1.5,
        max_attempts: int = 100,
        project_id: str | None = None,
        service_account: str | None = None,
    ) -> None:
        if not resources:
            raise GoldfishError("resources list is empty")

        self.resources = resources
        self.gpu_preference = gpu_preference or []
        self.force_gpu = force_gpu
        self.preemptible_preference = preemptible_preference
        self.force_preemptible = force_preemptible
        self.zone_filter = set(zones_override) if zones_override else None
        self.search_timeout = search_timeout_sec
        self.initial_backoff = initial_backoff_sec
        self.backoff_multiplier = backoff_multiplier
        self.max_attempts = max_attempts
        self.project_id = project_id
        self.service_account = service_account

        self.ordered_resources = order_resources(resources, self.gpu_preference, self.force_gpu)

    def launch(
        self,
        *,
        instance_name: str,
        startup_script: str,
        disk_name: str | None = None,
        snapshot: str | None = None,
        extra_disks: list[dict[str, Any]] | None = None,
        data_disk_mode: str = "ro",
    ) -> LaunchResult:
        """Launch instance with capacity search.

        Args:
            instance_name: Instance name
            startup_script: Startup script content
            disk_name: Optional disk to create from snapshot
            snapshot: Snapshot for data disk
            extra_disks: Additional disks to attach
            data_disk_mode: Mode for data disk ("ro" or "rw")

        Returns:
            LaunchResult with instance details

        Raises:
            GoldfishError: If no capacity found within timeout/attempts
        """
        # Write startup script to temp file
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write(startup_script)
            startup_path = Path(f.name)

        attempt_log: list[dict[str, Any]] = []
        deadline = time.time() + self.search_timeout
        backoff = self.initial_backoff
        attempts = 0
        selection: LaunchSelection | None = None
        timings: dict[str, float] | None = None

        try:
            # Outer loop: cycle through all resources/modes/zones repeatedly
            # until deadline or max_attempts. This enables long capacity waits
            # (e.g., 1 hour) where zone A may free up after zones B/C were tried.
            while selection is None and attempts < self.max_attempts and time.time() < deadline:
                made_attempt = False
                # Reset backoff each cycle so zone A gets retried promptly
                # after exhausting all zones, not after minutes of exponential sleep.
                backoff = self.initial_backoff

                for resource in self.ordered_resources:
                    candidate_zones = [
                        z for z in resource.get("zones", []) if not self.zone_filter or z in self.zone_filter
                    ]

                    if not candidate_zones:
                        continue

                    mode_seq = mode_order(resource, self.preemptible_preference, self.force_preemptible)

                    if not mode_seq:
                        continue

                    for mode in mode_seq:
                        preemptible = mode == "spot"

                        for zone in candidate_zones:
                            if selection is not None or attempts >= self.max_attempts or time.time() > deadline:
                                break

                            attempts += 1
                            made_attempt = True
                            attempt_entry = {
                                "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                                "resource": resource["name"],
                                "zone": zone,
                                "preemptible": preemptible,
                            }

                            try:
                                sel, timing = self._attempt_launch(
                                    resource=resource,
                                    zone=zone,
                                    preemptible=preemptible,
                                    disk_name=disk_name,
                                    instance_name=instance_name,
                                    startup_path=startup_path,
                                    snapshot=snapshot,
                                    extra_disks=extra_disks or [],
                                    data_disk_mode=data_disk_mode,
                                )

                                selection = sel
                                timings = timing
                                attempt_entry.update(
                                    {
                                        "status": "success",
                                        "disk_create_sec": timing.get("disk_create_sec"),
                                        "instance_create_sec": timing.get("instance_create_sec"),
                                    }
                                )
                                attempt_log.append(attempt_entry)
                                break

                            except CapacityError as exc:
                                attempt_entry["status"] = "capacity"
                                attempt_entry["error"] = str(exc)[:500]
                                attempt_log.append(attempt_entry)

                                sleep_time = min(backoff, max(0, deadline - time.time()))
                                if sleep_time > 0:
                                    time.sleep(sleep_time)

                                backoff = min(backoff * self.backoff_multiplier, self.search_timeout)
                                continue

                        if selection is not None or attempts >= self.max_attempts or time.time() > deadline:
                            break

                    if selection is not None or attempts >= self.max_attempts or time.time() > deadline:
                        break

                # No valid resource/mode/zone combinations exist — don't spin forever
                if not made_attempt:
                    break

            if selection is None or timings is None:
                snippet = json.dumps(attempt_log[-3:], indent=2) if attempt_log else "none"
                raise GoldfishError(f"Failed to acquire capacity within budget; last attempts:\n{snippet}")

        finally:
            startup_path.unlink(missing_ok=True)

        return LaunchResult(
            instance_name=instance_name,
            disk_name=disk_name,
            selection=selection,
            timings=timings,
            attempt_log=attempt_log,
        )

    def _attempt_launch(
        self,
        *,
        resource: dict[str, Any],
        zone: str,
        preemptible: bool,
        disk_name: str | None,
        instance_name: str,
        startup_path: Path,
        snapshot: str | None,
        extra_disks: list[dict[str, Any]],
        data_disk_mode: str,
    ) -> tuple[LaunchSelection, dict[str, float]]:
        """Attempt to launch instance in specific zone/resource.

        Args:
            resource: Resource dict from catalog
            zone: GCE zone
            preemptible: Use preemptible instance
            disk_name: Optional disk name
            instance_name: Instance name
            startup_path: Path to startup script
            snapshot: Snapshot for data disk
            extra_disks: Additional disks
            data_disk_mode: Disk mode ("ro" or "rw")

        Returns:
            Tuple of (LaunchSelection, timings dict)

        Raises:
            CapacityError: If capacity not available
        """
        timings: dict[str, float] = {}
        scratch_attached = False

        # Create data disk from snapshot if requested
        if disk_name and snapshot:
            data_disk = resource.get("data_disk") or {}
            start = time.time()

            cmd_disk = [
                "gcloud",
                "compute",
                "disks",
                "create",
                disk_name,
                f"--zone={zone}",
                f"--type={data_disk.get('type', 'pd-ssd')}",
                f"--size={data_disk.get('size_gb', 10)}GB",
                f"--source-snapshot={snapshot}",
                "--quiet",
            ]

            if self.project_id:
                cmd_disk.append(f"--project={self.project_id}")

            run_gcloud(cmd_disk, allow_capacity=True, project_id=self.project_id)
            timings["disk_create_sec"] = round(time.time() - start, 2)
            scratch_attached = True

        # Build instance create command
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "create",
            instance_name,
            f"--zone={zone}",
            f"--machine-type={resource['machine_type']}",
            f"--boot-disk-size={resource.get('boot_disk', {}).get('size_gb', 100)}GB",
            f"--boot-disk-type={resource.get('boot_disk', {}).get('type', 'pd-ssd')}",
            f"--metadata-from-file=startup-script={startup_path}",
            "--scopes=https://www.googleapis.com/auth/cloud-platform",
            "--quiet",
        ]
        if self.service_account:
            cmd.append(f"--service-account={self.service_account}")

        # Boot disk image
        boot_disk = resource.get("boot_disk", {})
        image = boot_disk.get("image")
        image_family = boot_disk.get("image_family")
        image_project = boot_disk.get("image_project")

        if image:
            cmd.append(f"--image={image}")
        elif image_family:
            cmd.append(f"--image-family={image_family}")
            if image_project:
                cmd.append(f"--image-project={image_project}")

        # Attach data disk
        metadata_entries: list[str] = []
        if scratch_attached and disk_name:
            cmd.append(f"--disk=name={disk_name},device-name={disk_name},mode={data_disk_mode}")

        # Extra disks
        for disk in extra_disks:
            cmd.append(
                "--disk="
                + ",".join(
                    [
                        f"name={disk['name']}",
                        f"device-name={disk.get('device_name', disk['name'])}",
                        f"mode={disk.get('mode', 'rw')}",
                    ]
                )
            )

        # GPU configuration
        gpu_info = resource.get("gpu") or {}
        accelerator = gpu_info.get("accelerator")
        count = gpu_info.get("count", 0)
        has_gpu = bool(gpu_info.get("type"))
        machine_type = resource.get("machine_type", "")

        # A3 machine types have integrated H100 GPUs - don't pass --accelerator
        # The GPU is already part of the machine type (e.g., a3-highgpu-1g = 1x H100)
        if accelerator and count and not machine_type.startswith("a3-"):
            cmd.extend(["--accelerator", f"count={count},type={accelerator}"])

        if has_gpu:
            cmd.append("--maintenance-policy=TERMINATE")
            metadata_entries.append("install-nvidia-driver=True")

        if preemptible:
            # Spot VMs: --provisioning-model=SPOT disables restart automatically.
            # --instance-termination-action=STOP keeps the VM around for log retrieval.
            # --reservation-affinity=none required (spot can't use reservations).
            cmd.append("--provisioning-model=SPOT")
            cmd.append("--instance-termination-action=STOP")
            cmd.append("--reservation-affinity=none")
        elif has_gpu:
            # On-demand GPU VMs: restart on failure (not available for spot)
            cmd.append("--restart-on-failure")

        if metadata_entries:
            cmd.append("--metadata=" + ",".join(metadata_entries))

        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        # Launch instance synchronously so gcloud surfaces capacity errors (503)
        # that the retry loop can catch and try the next zone. A3 VMs can take
        # 5+ minutes to provision, so the timeout must be generous.
        default_timeout = 600 if has_gpu else 120
        instance_timeout = resource.get("launch_timeout_seconds", default_timeout)
        start = time.time()
        try:
            run_gcloud(cmd, allow_capacity=True, project_id=self.project_id, timeout=instance_timeout)
        except CapacityError:
            if scratch_attached and disk_name:
                cleanup_disk(disk_name, zone)
            raise
        except Exception:
            if scratch_attached and disk_name:
                cleanup_disk(disk_name, zone)
            raise

        timings["instance_create_sec"] = round(time.time() - start, 2)

        # With --async, the instance is not yet RUNNING. Goldfish's own
        # wait_for_completion() handles the provisioning wait. The metadata
        # syncer retries on its own. No blocking wait needed here.

        return (
            LaunchSelection(resource=resource["name"], zone=zone, preemptible=preemptible),
            timings,
        )
