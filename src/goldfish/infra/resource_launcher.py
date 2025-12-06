"""Capacity-aware resource launcher for GCE.

Ported from legacy infra/resource_launcher.py.
Provides intelligent multi-zone capacity search with retry logic.
"""

import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from goldfish.errors import GoldfishError

# Capacity error patterns from GCE
CAPACITY_PATTERNS = (
    "zone_resource_pool_exhausted",
    "does not have sufficient resources",
    "quota",
    "was not able to fulfil",
    "resource is not available",
    "insufficient",
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
    disk_name: Optional[str]
    selection: LaunchSelection
    timings: Dict[str, float]
    attempt_log: List[Dict[str, Any]]
    run_id: Optional[str] = None  # Run identifier for tracking
    log_uri: Optional[str] = None  # GCS URI for logs
    artifact_uri: Optional[str] = None  # GCS URI for artifacts


def run_gcloud(
    cmd: List[str], *, allow_capacity: bool = False, check: bool = True
) -> subprocess.CompletedProcess:
    """Run gcloud command with capacity error detection.

    Args:
        cmd: Command list (e.g., ["gcloud", "compute", "instances", "create", ...])
        allow_capacity: If True, raise CapacityError on capacity issues
        check: Raise exception on non-zero exit code

    Returns:
        CompletedProcess

    Raises:
        CapacityError: If allow_capacity=True and capacity error detected
        GoldfishError: If check=True and command fails (non-capacity)
    """
    result = subprocess.run(cmd, capture_output=True, text=True)

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
    resources: List[Dict[str, Any]],
    gpu_preference: Iterable[str],
    force_gpu: Optional[str],
) -> List[Dict[str, Any]]:
    """Order resources by GPU preference.

    Args:
        resources: List of resource dicts from catalog
        gpu_preference: Ordered list of preferred GPU types (e.g., ["h100", "a100", "none"])
        force_gpu: If set, only include this GPU type

    Returns:
        Ordered list of resources
    """
    # Group by GPU type
    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for res in resources:
        gpu_info = res.get("gpu") or {}
        gpu_type = (gpu_info.get("type") or "none").lower()
        res["_gpu_type"] = gpu_type
        by_type.setdefault(gpu_type, []).append(res)

    if force_gpu:
        forced_type = force_gpu.lower()
        if forced_type not in by_type:
            raise GoldfishError(
                f"force_gpu={force_gpu} not present in resource catalog"
            )
        return by_type[forced_type]

    # Order by preference
    ordered_types: List[str] = []
    for pref in (gpu_preference or []):
        pref = pref.lower()
        if pref in by_type and pref not in ordered_types:
            ordered_types.append(pref)

    # Add remaining types
    for gpu_type in by_type:
        if gpu_type not in ordered_types:
            ordered_types.append(gpu_type)

    # Flatten
    ordered: List[Dict[str, Any]] = []
    for gpu_type in ordered_types:
        ordered.extend(by_type.get(gpu_type, []))

    return ordered


def mode_order(
    resource: Dict[str, Any], preference: str, force_mode: Optional[str]
) -> List[str]:
    """Determine preemptible mode ordering for a resource.

    Args:
        resource: Resource dict
        preference: "spot_first" or "on_demand_first"
        force_mode: "spot" or "on_demand" to force specific mode

    Returns:
        List of modes to try (e.g., ["spot", "on_demand"])
    """
    if force_mode == "spot":
        return ["spot"] if resource.get("preemptible_allowed") else []

    if force_mode == "on_demand":
        return ["on_demand"] if resource.get("on_demand_allowed") else []

    preferred = (
        ["spot", "on_demand"] if preference == "spot_first" else ["on_demand", "spot"]
    )

    modes: List[str] = []
    for mode in preferred:
        if mode == "spot" and resource.get("preemptible_allowed"):
            modes.append("spot")
        if mode == "on_demand" and resource.get("on_demand_allowed"):
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
        resources: List[Dict[str, Any]],
        *,
        gpu_preference: Optional[List[str]] = None,
        force_gpu: Optional[str] = None,
        preemptible_preference: str = "spot_first",
        force_preemptible: Optional[str] = None,
        zones_override: Optional[List[str]] = None,
        search_timeout_sec: int = 600,
        initial_backoff_sec: float = 5,
        backoff_multiplier: float = 1.5,
        max_attempts: int = 100,
        project_id: Optional[str] = None,
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

        self.ordered_resources = order_resources(
            resources, self.gpu_preference, self.force_gpu
        )

    def launch(
        self,
        *,
        instance_name: str,
        startup_script: str,
        disk_name: Optional[str] = None,
        snapshot: Optional[str] = None,
        extra_disks: Optional[List[Dict[str, Any]]] = None,
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

        attempt_log: List[Dict[str, Any]] = []
        deadline = time.time() + self.search_timeout
        backoff = self.initial_backoff
        attempts = 0
        selection: Optional[LaunchSelection] = None
        timings: Optional[Dict[str, float]] = None

        try:
            for resource in self.ordered_resources:
                candidate_zones = [
                    z
                    for z in resource.get("zones", [])
                    if not self.zone_filter or z in self.zone_filter
                ]

                if not candidate_zones:
                    continue

                mode_seq = mode_order(
                    resource, self.preemptible_preference, self.force_preemptible
                )

                if not mode_seq:
                    continue

                for mode in mode_seq:
                    preemptible = mode == "spot"

                    for zone in candidate_zones:
                        if (
                            selection is not None
                            or attempts >= self.max_attempts
                            or time.time() > deadline
                        ):
                            break

                        attempts += 1
                        attempt_entry = {
                            "timestamp": datetime.utcnow().isoformat(
                                timespec="seconds"
                            )
                            + "Z",
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
                                    "instance_create_sec": timing.get(
                                        "instance_create_sec"
                                    ),
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

                            backoff = min(
                                backoff * self.backoff_multiplier, self.search_timeout
                            )
                            continue

                    if (
                        selection is not None
                        or attempts >= self.max_attempts
                        or time.time() > deadline
                    ):
                        break

            if selection is None or timings is None:
                snippet = (
                    json.dumps(attempt_log[-3:], indent=2) if attempt_log else "none"
                )
                raise GoldfishError(
                    f"Failed to acquire capacity within budget; last attempts:\n{snippet}"
                )

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
        resource: Dict[str, Any],
        zone: str,
        preemptible: bool,
        disk_name: Optional[str],
        instance_name: str,
        startup_path: Path,
        snapshot: Optional[str],
        extra_disks: List[Dict[str, Any]],
        data_disk_mode: str,
    ) -> tuple[LaunchSelection, Dict[str, float]]:
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
        timings: Dict[str, float] = {}
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

            run_gcloud(cmd_disk, allow_capacity=True)
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
        metadata_entries: List[str] = []
        if scratch_attached and disk_name:
            cmd.append(
                f"--disk=name={disk_name},device-name={disk_name},mode={data_disk_mode}"
            )

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

        if accelerator and count:
            cmd.extend(["--accelerator", f"count={count},type={accelerator}"])

        if has_gpu:
            cmd.append("--maintenance-policy=TERMINATE")
            cmd.append("--restart-on-failure")
            metadata_entries.append("install-nvidia-driver=True")

        if preemptible:
            cmd.append("--preemptible")

        if metadata_entries:
            cmd.append("--metadata=" + ",".join(metadata_entries))

        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        # Launch instance
        start = time.time()
        try:
            run_gcloud(cmd, allow_capacity=True)
        except CapacityError:
            if scratch_attached and disk_name:
                cleanup_disk(disk_name, zone)
            raise
        except Exception:
            if scratch_attached and disk_name:
                cleanup_disk(disk_name, zone)
            raise

        timings["instance_create_sec"] = round(time.time() - start, 2)

        return (
            LaunchSelection(
                resource=resource["name"], zone=zone, preemptible=preemptible
            ),
            timings,
        )
