"""GCE (Google Compute Engine) launcher for Goldfish stage execution.

Full implementation with feature parity to legacy infra code.
Provides:
- Capacity-aware multi-zone search via ResourceLauncher
- Disk management (hyperdisk create/attach/snapshot/delete)
- GCS sync for inputs/outputs
- GPU support with driver installation
- Instance monitoring and cleanup
"""

import json
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from goldfish.errors import GoldfishError
from goldfish.infra.resource_launcher import ResourceLauncher, run_gcloud, cleanup_disk
from goldfish.infra.startup_builder import build_startup_script


class GCELauncher:
    """Launch stage runs on Google Compute Engine with full infrastructure support.

    Integrates ResourceLauncher for capacity search and startup_builder for
    proper orchestration. Supports disk management and GCS sync.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        zone: str = "us-central1-a",
        bucket: Optional[str] = None,
        resources: Optional[List[Dict[str, Any]]] = None,
    ):
        """Initialize GCE launcher.

        Args:
            project_id: GCP project ID (uses default if None)
            zone: Default GCE zone (can be overridden per launch)
            bucket: GCS bucket for logs/artifacts (required for full functionality)
            resources: Resource catalog (list of resource dicts)
        """
        self.project_id = project_id
        self.default_zone = zone
        self.bucket = bucket
        self.resources = resources or []

    def launch_instance(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict,
        work_dir: Path,
        inputs_dir: Optional[Path] = None,
        outputs_dir: Optional[Path] = None,
        machine_type: str = "n1-standard-4",
        gpu_type: Optional[str] = None,
        gpu_count: int = 0,
        zones: Optional[List[str]] = None,
        use_capacity_search: bool = True,
    ) -> str:
        """Launch GCE instance for stage run.

        Args:
            image_tag: Docker image to run
            stage_run_id: Stage run identifier
            entrypoint_script: Bash script to run in container
            stage_config: Stage configuration dict
            work_dir: Local working directory
            inputs_dir: Directory to sync to GCS for inputs
            outputs_dir: Directory to sync from GCS for outputs
            machine_type: GCE machine type
            gpu_type: GPU accelerator type
            gpu_count: Number of GPUs
            zones: List of zones to search (None = use default)
            use_capacity_search: Use ResourceLauncher for capacity search

        Returns:
            Instance name

        Raises:
            GoldfishError: If launch fails
        """
        if not self.bucket:
            raise GoldfishError("GCS bucket required for GCE launcher")

        instance_name = self._sanitize_name(stage_run_id)

        # Build startup script using startup_builder
        bucket_name = self.bucket.replace("gs://", "")
        run_path = f"runs/{stage_run_id}"

        # Prepare environment variables
        env_map = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(stage_config),
            "GOLDFISH_RUN_ID": stage_run_id,
        }

        # Build startup script with proper orchestration
        startup_script = build_startup_script(
            bucket=bucket_name,
            bucket_prefix="",
            run_path=run_path,
            image=image_tag,
            entrypoint="/bin/bash",
            env_map=env_map,
            mounts=[("/mnt/entrypoint.sh", "/entrypoint.sh")],
            gcsfuse=True,
            pre_run_cmds=[
                f"cat > /mnt/entrypoint.sh << 'ENTRYPOINT_EOF'\n{entrypoint_script}\nENTRYPOINT_EOF",
                "chmod +x /mnt/entrypoint.sh",
            ],
        )

        if use_capacity_search and self.resources:
            # Use ResourceLauncher for capacity-aware search
            return self._launch_with_capacity_search(
                instance_name=instance_name,
                startup_script=startup_script,
                gpu_type=gpu_type,
                zones=zones,
            )
        else:
            # Simple launch without capacity search
            return self._launch_simple(
                instance_name=instance_name,
                startup_script=startup_script,
                machine_type=machine_type,
                gpu_type=gpu_type,
                gpu_count=gpu_count,
                zone=zones[0] if zones else self.default_zone,
            )

    def _launch_with_capacity_search(
        self,
        instance_name: str,
        startup_script: str,
        gpu_type: Optional[str],
        zones: Optional[List[str]],
    ) -> str:
        """Launch using ResourceLauncher for capacity search.

        Args:
            instance_name: Instance name
            startup_script: Startup script content
            gpu_type: GPU type to filter resources
            zones: Zones to search

        Returns:
            Instance name

        Raises:
            GoldfishError: If no capacity found
        """
        # Filter resources by GPU type
        if gpu_type:
            filtered_resources = [
                r
                for r in self.resources
                if (r.get("gpu", {}).get("type") or "none").lower()
                == gpu_type.lower()
            ]
        else:
            filtered_resources = [
                r for r in self.resources if not r.get("gpu", {}).get("type")
            ]

        if not filtered_resources:
            raise GoldfishError(
                f"No resources found for GPU type: {gpu_type or 'none'}"
            )

        # Create ResourceLauncher
        launcher = ResourceLauncher(
            resources=filtered_resources,
            gpu_preference=[gpu_type] if gpu_type else ["none"],
            force_gpu=gpu_type,
            zones_override=zones,
            project_id=self.project_id,
        )

        # Launch with capacity search
        result = launcher.launch(
            instance_name=instance_name,
            startup_script=startup_script,
        )

        return result.instance_name

    def _launch_simple(
        self,
        instance_name: str,
        startup_script: str,
        machine_type: str,
        gpu_type: Optional[str],
        gpu_count: int,
        zone: str,
    ) -> str:
        """Simple launch without capacity search.

        Args:
            instance_name: Instance name
            startup_script: Startup script content
            machine_type: Machine type
            gpu_type: GPU accelerator type
            gpu_count: GPU count
            zone: Zone

        Returns:
            Instance name

        Raises:
            GoldfishError: If launch fails
        """
        import tempfile

        # Write startup script
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sh") as f:
            f.write(startup_script)
            startup_path = Path(f.name)

        try:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "create",
                instance_name,
                f"--zone={zone}",
                f"--machine-type={machine_type}",
                "--boot-disk-size=100GB",
                "--boot-disk-type=pd-ssd",
                "--image-family=cos-stable",
                "--image-project=cos-cloud",
                f"--metadata-from-file=startup-script={startup_path}",
                "--scopes=https://www.googleapis.com/auth/cloud-platform",
                "--quiet",
            ]

            if gpu_type and gpu_count > 0:
                cmd.extend(["--accelerator", f"count={gpu_count},type={gpu_type}"])
                cmd.append("--maintenance-policy=TERMINATE")
                cmd.append("--restart-on-failure")
                cmd.append("--metadata=install-nvidia-driver=True")

            if self.project_id:
                cmd.append(f"--project={self.project_id}")

            run_gcloud(cmd)
            return instance_name

        finally:
            startup_path.unlink(missing_ok=True)

    def create_disk(
        self,
        disk_name: str,
        zone: str,
        size_gb: int = 100,
        disk_type: str = "pd-ssd",
        snapshot: Optional[str] = None,
    ) -> None:
        """Create a persistent disk.

        Args:
            disk_name: Disk name
            zone: GCE zone
            size_gb: Disk size in GB
            disk_type: Disk type (pd-ssd, pd-balanced, hyperdisk-balanced)
            snapshot: Optional snapshot to create from

        Raises:
            GoldfishError: If creation fails
        """
        cmd = [
            "gcloud",
            "compute",
            "disks",
            "create",
            disk_name,
            f"--zone={zone}",
            f"--type={disk_type}",
            f"--size={size_gb}GB",
            "--quiet",
        ]

        if snapshot:
            cmd.append(f"--source-snapshot={snapshot}")

        if disk_type == "hyperdisk-balanced":
            cmd.extend(
                [
                    "--provisioned-iops=80000",
                    "--provisioned-throughput=2400",
                ]
            )

        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd)

    def delete_disk(self, disk_name: str, zone: str) -> None:
        """Delete a persistent disk.

        Args:
            disk_name: Disk name
            zone: GCE zone
        """
        cleanup_disk(disk_name, zone)

    def snapshot_disk(
        self, disk_name: str, snapshot_name: str, zone: str
    ) -> None:
        """Create a snapshot of a disk.

        Args:
            disk_name: Disk name
            snapshot_name: Snapshot name
            zone: GCE zone

        Raises:
            GoldfishError: If snapshot fails
        """
        cmd = [
            "gcloud",
            "compute",
            "disks",
            "snapshot",
            disk_name,
            f"--snapshot-names={snapshot_name}",
            f"--zone={zone}",
            "--quiet",
        ]

        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd)

    def sync_to_gcs(self, local_path: Path, gcs_uri: str) -> None:
        """Sync local directory to GCS.

        Args:
            local_path: Local directory path
            gcs_uri: GCS URI (e.g., "gs://bucket/path")

        Raises:
            GoldfishError: If sync fails
        """
        cmd = ["gsutil", "-m", "rsync", "-r", str(local_path), gcs_uri]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise GoldfishError(f"GCS sync failed: {result.stderr}")

    def sync_from_gcs(self, gcs_uri: str, local_path: Path) -> None:
        """Sync GCS directory to local.

        Args:
            gcs_uri: GCS URI (e.g., "gs://bucket/path")
            local_path: Local directory path

        Raises:
            GoldfishError: If sync fails
        """
        local_path.mkdir(parents=True, exist_ok=True)
        cmd = ["gsutil", "-m", "rsync", "-r", gcs_uri, str(local_path)]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise GoldfishError(f"GCS sync failed: {result.stderr}")

    def get_instance_status(self, instance_name: str) -> str:
        """Get status of GCE instance.

        Args:
            instance_name: Instance identifier

        Returns:
            Status: "running", "completed", "failed", or "not_found"
        """
        instance_name = self._sanitize_name(instance_name)

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "describe",
            instance_name,
            f"--zone={self.default_zone}",
            "--format=value(status)",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        result = run_gcloud(cmd, check=False)

        if result.returncode != 0:
            return "not_found"

        status = result.stdout.strip()

        # Map GCE status to Goldfish status
        if status in ("PROVISIONING", "STAGING", "RUNNING"):
            return "running"
        elif status == "TERMINATED":
            # Check exit code in GCS if available
            if self.bucket:
                exit_code = self._get_exit_code(instance_name)
                return "completed" if exit_code == 0 else "failed"
            return "completed"
        elif status in ("STOPPING", "SUSPENDING", "SUSPENDED"):
            return "running"
        else:
            return "failed"

    def _get_exit_code(self, instance_name: str) -> int:
        """Get exit code from GCS.

        Args:
            instance_name: Instance identifier

        Returns:
            Exit code (0 if not found or error)
        """
        if not self.bucket:
            return 0

        try:
            result = subprocess.run(
                [
                    "gsutil",
                    "cat",
                    f"{self.bucket}/runs/{instance_name}/logs/exit_code.txt",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return int(result.stdout.strip() or "0")
        except Exception:
            return 0

    def get_instance_logs(self, instance_name: str) -> str:
        """Retrieve logs from GCE instance.

        Tries to fetch from GCS first, falls back to serial console.

        Args:
            instance_name: Instance identifier

        Returns:
            Instance logs as string
        """
        instance_name = self._sanitize_name(instance_name)

        # Try GCS first
        if self.bucket:
            try:
                result = subprocess.run(
                    [
                        "gsutil",
                        "cat",
                        f"{self.bucket}/runs/{instance_name}/logs/train.log",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                return result.stdout
            except Exception:
                pass

        # Fall back to serial console
        try:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "get-serial-port-output",
                instance_name,
                f"--zone={self.default_zone}",
                "--port=1",
            ]
            if self.project_id:
                cmd.append(f"--project={self.project_id}")

            result = run_gcloud(cmd, check=False)
            return result.stdout
        except Exception as e:
            return f"Failed to retrieve logs: {e}"

    def stop_instance(self, instance_name: str) -> None:
        """Stop GCE instance (preserves disk).

        Args:
            instance_name: Instance identifier
        """
        instance_name = self._sanitize_name(instance_name)

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "stop",
            instance_name,
            f"--zone={self.default_zone}",
            "--quiet",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd)

    def delete_instance(self, instance_name: str) -> None:
        """Delete GCE instance and boot disk.

        Args:
            instance_name: Instance identifier
        """
        instance_name = self._sanitize_name(instance_name)

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "delete",
            instance_name,
            f"--zone={self.default_zone}",
            "--quiet",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd, check=False)  # Don't fail if already deleted

    def wait_for_termination(
        self, instance_name: str, timeout_sec: int = 3600
    ) -> str:
        """Wait for instance to terminate.

        Polls instance status until it reaches TERMINATED state or timeout.

        Args:
            instance_name: Instance identifier
            timeout_sec: Maximum seconds to wait

        Returns:
            Final status: "completed" or "failed"

        Raises:
            GoldfishError: If timeout exceeded
        """
        instance_name = self._sanitize_name(instance_name)
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            status = self.get_instance_status(instance_name)

            if status == "not_found":
                raise GoldfishError(f"Instance {instance_name} not found")

            if status in ("completed", "failed"):
                return status

            time.sleep(10)  # Poll every 10 seconds

        # Timeout - get serial logs for debugging
        logs = self.get_instance_logs(instance_name)
        raise GoldfishError(
            f"Instance {instance_name} did not terminate within {timeout_sec}s. "
            f"Last logs:\n{logs[-1000:]}"
        )

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Sanitize name for GCE (lowercase, replace underscores with hyphens).

        Args:
            name: Original name

        Returns:
            Sanitized name (lowercase, hyphens only, truncated to 60 chars)
        """
        sanitized = name.replace("_", "-").lower()
        # Remove any non-alphanumeric except hyphens
        sanitized = "".join(c if c.isalnum() or c == "-" else "-" for c in sanitized)
        # Truncate to 60 chars (GCE limit is 63)
        return sanitized[:60]
