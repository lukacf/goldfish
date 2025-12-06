#!/usr/bin/env python
"""Reusable capacity-search launcher utilities for CE/POC jobs.

This module is intentionally pure-Python and depends only on stdlib. All
configuration is passed as plain dictionaries (no OmegaConf/OmegaDict). The
expected top-level keys are:

```
{
  "bucket": "gs://bucket/prefix",      # required
  "snapshot_id": "optional-snapshot", # optional
  "run": {                             # optional tuning overrides
    "gpu_preference": ["h100", "a100", "none"],
    "force_gpu": null,
    "preemptible_preference": "spot_first",  # or on_demand_first
    "force_preemptible": null,                # "spot"|"on_demand"
    "search_timeout_sec": 900,
    "initial_backoff_sec": 5,
    "backoff_multiplier": 1.5,
    "max_attempts": 100,
  },
  "resources": [ ... ],                 # required list of resource dicts
}
```

The ResourceLauncher never mutates the provided config; callers can safely
reuse shared dictionaries across launches.
"""
from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

CAPACITY_PATTERNS = (
    "zone_resource_pool_exhausted",
    "does not have sufficient resources",
    "quota",
    "was not able to fulfil",
    "resource is not available",
    "insufficient",
)


class CapacityError(RuntimeError):
    """Raised when GCE reports a capacity or quota issue."""


@dataclass
class LaunchSelection:
    resource: str
    zone: str
    preemptible: bool


@dataclass
class LaunchResult:
    run_id: str
    instance_name: str
    disk_name: str | None
    selection: LaunchSelection
    timings: dict[str, float]
    attempt_log: list[dict[str, Any]]
    log_uri: str
    artifact_uri: str
    stage_log_uri: str


def split_bucket_uri(uri: str) -> tuple[str, str]:
    """Split a gs://bucket/prefix URI into bucket and prefix components."""

    if not uri.startswith("gs://"):
        raise ValueError(f"Bucket URI must start with gs://, got {uri}")
    remainder = uri[5:]
    if "/" in remainder:
        bucket, prefix = remainder.split("/", 1)
    else:
        bucket, prefix = remainder, ""
    return bucket, prefix.strip("/")


def format_run_id(prefix: str | None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    base = prefix or "job"
    return f"{base}-{timestamp}"


def sanitize(name: str) -> str:
    return name.replace("_", "-")


def run_gcloud(
    cmd: list[str], *, allow_capacity: bool = False, check: bool = True
) -> subprocess.CompletedProcess:
    print("$", " ".join(shlex.quote(c) for c in cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        return result
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    if allow_capacity and any(pattern in lowered for pattern in CAPACITY_PATTERNS):
        raise CapacityError(output.strip())
    if check:
        raise RuntimeError(output.strip())
    return result


def order_resources(
    resources: List[Dict[str, Any]], gpu_preference: Iterable[str], force_gpu: str | None
) -> List[Dict[str, Any]]:
    by_type: dict[str, list[dict[str, Any]]] = {}
    for res in resources:
        gpu_info = res.get("gpu") or {}
        gpu_type = (gpu_info.get("type") or "none").lower()
        res["_gpu_type"] = gpu_type
        by_type.setdefault(gpu_type, []).append(res)

    if force_gpu:
        forced_type = force_gpu.lower()
        if forced_type not in by_type:
            raise SystemExit(f"force_gpu={force_gpu} not present in resource catalog")
        ordered_types = [forced_type]
    else:
        ordered_types = []
        for pref in (gpu_preference or []):
            pref = pref.lower()
            if pref in by_type and pref not in ordered_types:
                ordered_types.append(pref)
        for gpu_type in by_type:
            if gpu_type not in ordered_types:
                ordered_types.append(gpu_type)
    ordered: list[dict[str, Any]] = []
    for gpu_type in ordered_types:
        ordered.extend(by_type.get(gpu_type, []))
    return ordered


def mode_order(
    resource: Dict[str, Any], preference: str, force_mode: str | None
) -> list[str]:
    if force_mode == "spot":
        return ["spot"] if resource.get("preemptible_allowed") else []
    if force_mode == "on_demand":
        return ["on_demand"] if resource.get("on_demand_allowed") else []
    preferred = ["spot", "on_demand"] if preference == "spot_first" else ["on_demand", "spot"]
    modes: list[str] = []
    for mode in preferred:
        if mode == "spot" and resource.get("preemptible_allowed"):
            modes.append("spot")
        if mode == "on_demand" and resource.get("on_demand_allowed"):
            modes.append("on_demand")
    return modes


def cleanup_disk(disk_name: str, zone: str) -> None:
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
    """Capacity-aware launcher driven by run/resource config.

    Args:
        cfg: Plain mapping containing at minimum ``bucket`` and ``resources``.
        gpu_preference_override: Optional override for gpu preference ordering.
        force_gpu: If set, restricts search to a single GPU type.
        preemptible_mode_override: Override for spot/on-demand ordering (``spot_first``|``on_demand_first``).
        force_preemptible: Force a specific mode (``spot``|``on_demand``).
        zones_override: Restrict search to these zones.
        search_timeout_sec: Override global search deadline (seconds).
        initial_backoff_sec: Override initial backoff between attempts.
        backoff_multiplier: Backoff multiplier.
        max_attempts: Maximum total attempts before giving up.
    """

    def __init__(
        self,
        cfg: Mapping[str, Any],
        *,
        gpu_preference_override: list[str] | None = None,
        force_gpu: str | None = None,
        preemptible_mode_override: str | None = None,
        force_preemptible: str | None = None,
        zones_override: list[str] | None = None,
        search_timeout_sec: int | None = None,
        initial_backoff_sec: float | None = None,
        backoff_multiplier: float | None = None,
        max_attempts: int | None = None,
    ) -> None:
        if "bucket" not in cfg:
            raise SystemExit("config.bucket is required")
        self.cfg: Mapping[str, Any] = cfg
        self.bucket_uri = str(cfg.get("bucket"))
        self.bucket_name, self.bucket_prefix = split_bucket_uri(self.bucket_uri)
        self.snapshot_id = cfg.get("snapshot_id")

        run_cfg: Mapping[str, Any] = cfg.get("run") or {}
        self.gpu_preference = gpu_preference_override or list(run_cfg.get("gpu_preference") or [])
        self.force_gpu = force_gpu or run_cfg.get("force_gpu")
        self.preemptible_preference = (
            preemptible_mode_override or run_cfg.get("preemptible_preference", "spot_first")
        )
        self.force_preemptible = force_preemptible or run_cfg.get("force_preemptible")
        self.search_timeout = search_timeout_sec or int(run_cfg.get("search_timeout_sec", 600))
        self.initial_backoff = initial_backoff_sec or float(run_cfg.get("initial_backoff_sec", 5))
        self.backoff_multiplier = backoff_multiplier or float(run_cfg.get("backoff_multiplier", 1.5))
        self.max_attempts = max_attempts or int(run_cfg.get("max_attempts", 100))
        self.zone_filter = set(zones_override or []) if zones_override else None
        self.resources = list(cfg.get("resources") or [])
        if not self.resources:
            raise SystemExit("config.resources is empty")
        self.ordered_resources = order_resources(self.resources, self.gpu_preference, self.force_gpu)

    def launch(
        self,
        *,
        run_id: str,
        startup_script: str,
        run_path: str,
        disk_name: str | None = None,
        instance_name: str | None = None,
        extra_disks: list[dict[str, Any]] | None = None,
        scratch_snapshot: str | None = None,
        data_disk_mode: str = "ro",
        metadata: dict[str, Any] | None = None,
    ) -> LaunchResult:
        bucket_path = "/".join(part for part in [self.bucket_prefix, run_path] if part)
        log_uri = f"gs://{self.bucket_name}/{bucket_path}/logs/train.log"
        artifact_uri = f"gs://{self.bucket_name}/{bucket_path}/artifacts"
        stage_uri = f"gs://{self.bucket_name}/{bucket_path}/logs/stage_times.log"

        if not instance_name:
            instance_name = sanitize(f"job-{run_id}")[:60]
        if scratch_snapshot is None:
            scratch_snapshot = self.snapshot_id
        if scratch_snapshot:
            disk_name = disk_name or sanitize(f"data-{run_id}")[:60]
        
        # If disk_name was passed (or set above), keep it. Don't force it to None if no snapshot.

        with tempfile.NamedTemporaryFile("w", delete=False) as tmp_script:
            tmp_script.write(startup_script)
            startup_path = Path(tmp_script.name)

        attempt_log: list[dict[str, Any]] = []
        deadline = time.time() + self.search_timeout
        backoff = self.initial_backoff
        attempts = 0
        selection: LaunchSelection | None = None
        timings: dict[str, float] | None = None

        try:
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
                                snapshot=scratch_snapshot,
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
            if selection is None or timings is None:
                snippet = json.dumps(attempt_log[-3:], indent=2) if attempt_log else "none"
                raise SystemExit(
                    "Failed to acquire capacity within budget; last attempts:\n" + snippet
                )
        finally:
            startup_path.unlink(missing_ok=True)

        return LaunchResult(
            run_id=run_id,
            instance_name=instance_name,
            disk_name=disk_name,
            selection=selection,
            timings=timings,
            attempt_log=attempt_log,
            log_uri=log_uri,
            artifact_uri=artifact_uri,
            stage_log_uri=stage_uri,
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
        timings: dict[str, float] = {}
        scratch_attached = False
        if disk_name:
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
                "--quiet",
            ]
            if snapshot:
                cmd_disk.append(f"--source-snapshot={snapshot}")
            
            run_gcloud(
                cmd_disk,
                allow_capacity=True,
            )
            timings["disk_create_sec"] = round(time.time() - start, 2)
            scratch_attached = True

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
        metadata_entries: list[str] = []
        if scratch_attached and disk_name:
            cmd.append(
                f"--disk=name={disk_name},device-name={disk_name},mode={data_disk_mode}"
            )
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

        return LaunchSelection(resource=resource["name"], zone=zone, preemptible=preemptible), timings
