"""Storage tier helpers for bridging between GCS and Hyperdisk snapshots."""
from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from datetime import datetime, timezone


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def get_location(config: Dict[str, Any], tier: str) -> Optional[str]:
    state = config.get("state") or {}
    locations = state.get("locations") or {}
    if isinstance(locations, dict):
        return locations.get(tier)
    return None


def needs_bridging(source_tier: str | None, required_tier: str) -> bool:
    if not source_tier:
        return True
    return source_tier != required_tier


def snapshot_exists(snapshot_name: str) -> bool:
    try:
        _run(["gcloud", "compute", "snapshots", "describe", snapshot_name])
        return True
    except Exception:
        return False


def create_snapshot(gcs_uri: str, snapshot_name: str, zone: str, *, disk_size_gb: int = 600) -> str:
    """Create a Hyperdisk/PD snapshot from a GCS path via a temporary VM.

    Steps (best-effort, cleanup on failure):
      1) If snapshot already exists -> return name.
      2) Create temp disk.
      3) Create temp VM, attach disk, run startup script to format+mount+rsync GCS -> disk, shutdown.
      4) Snapshot the disk as `snapshot_name`.
      5) Delete temp VM and temp disk.

    Requires gcloud + appropriate IAM. Intended for GCE backend bridging.
    """

    if snapshot_exists(snapshot_name):
        return snapshot_name

    suffix = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    temp_disk = f"tmp-bridge-disk-{suffix}"
    temp_vm = f"tmp-bridge-vm-{suffix}"
    mount_point = "/mnt/bridge"

    # Strip trailing slash, then add /* to copy contents (not the directory itself)
    gcs_uri_clean = gcs_uri.rstrip("/")
    startup_script = f"""#!/bin/bash
set -uxo pipefail
trap 'shutdown -h now' EXIT
mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/disk/by-id/google-{temp_disk}
mkdir -p {mount_point}
mount /dev/disk/by-id/google-{temp_disk} {mount_point}
# Use gcloud storage which is pre-installed on GCE images (faster than gsutil and no apt needed)
# Wildcard copies contents directly without creating a subdirectory
gcloud storage cp -r "{gcs_uri_clean}/*" {mount_point}/
sync
"""

    try:
        # Create temp data disk
        _run(
            [
                "gcloud",
                "compute",
                "disks",
                "create",
                temp_disk,
                f"--zone={zone}",
                f"--size={disk_size_gb}GB",
                "--type=pd-ssd",
            ]
        )

        # Write startup script to temp file
        tmp_script = Path("/tmp") / f"startup-{suffix}.sh"
        tmp_script.write_text(startup_script)

        # Create temp VM with attached disk and startup script
        _run(
            [
                "gcloud",
                "compute",
                "instances",
                "create",
                temp_vm,
                f"--zone={zone}",
                "--machine-type=e2-standard-4",
                "--image-family=debian-12",
                "--image-project=debian-cloud",
                f"--metadata-from-file=startup-script={tmp_script}",
                f"--disk=name={temp_disk},device-name={temp_disk},mode=rw,boot=no,auto-delete=no",
            ]
        )

        # Wait for VM to stop (up to 20 minutes)
        for _ in range(120):
            status = (
                _run(
                    [
                        "gcloud",
                        "compute",
                        "instances",
                        "describe",
                        temp_vm,
                        f"--zone={zone}",
                        "--format=value(status)",
                    ]
                )
                .stdout.strip()
            )
            if status == "TERMINATED":
                break
            time.sleep(10)

        # Create snapshot from disk
        _run(
            [
                "gcloud",
                "compute",
                "disks",
                "snapshot",
                temp_disk,
                f"--snapshot-names={snapshot_name}",
                f"--zone={zone}",
            ]
        )
    finally:
        # Cleanup temp VM and disk (best-effort)
        _run(["gcloud", "compute", "instances", "delete", temp_vm, f"--zone={zone}", "--quiet"], check=False)
        _run(["gcloud", "compute", "disks", "delete", temp_disk, f"--zone={zone}", "--quiet"], check=False)
    return snapshot_name


def update_config_locations(config_path: Path, tier: str, location: str) -> None:
    cfg = yaml.safe_load(config_path.read_text())
    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid config at {config_path}")
    state = cfg.setdefault("state", {})
    locations = state.setdefault("locations", {})
    locations[tier] = location
    with config_path.open("w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
