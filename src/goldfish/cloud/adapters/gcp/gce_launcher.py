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
import logging
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any

from goldfish.cloud.adapters.gcp.resource_launcher import ResourceLauncher, cleanup_disk, run_gcloud
from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script
from goldfish.errors import GoldfishError
from goldfish.state_machine.exit_code import ExitCodeResult, get_exit_code_gce
from goldfish.state_machine.types import StageState

logger = logging.getLogger(__name__)

# Configuration constants for hyperdisk
HYPERDISK_PROVISIONED_IOPS = 80000  # IOPS for hyperdisk-balanced
HYPERDISK_PROVISIONED_THROUGHPUT = 2400  # MB/s for hyperdisk-balanced

# Cost protection defaults - ALWAYS enforced unless explicitly overridden
# These prevent runaway instances from burning money
DEFAULT_MAX_RUNTIME_SECONDS = 6 * 3600  # 6 hours - hard limit, instance self-deletes
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 600  # 10 minutes - if job uses heartbeat()


@dataclass
class GCELaunchResult:
    """Result of a GCE instance launch.

    Contains the instance name and the zone where it was launched,
    enabling proper zone-aware operations (monitoring, cleanup, metadata access).
    """

    instance_name: str
    zone: str


class GCELauncher:
    """Launch stage runs on Google Compute Engine with full infrastructure support.

    Integrates ResourceLauncher for capacity search and startup_builder for
    proper orchestration. Supports disk management and GCS sync.
    """

    def __init__(
        self,
        project_id: str | None = None,
        zone: str = "not-configured",  # Must be configured via goldfish.yaml gce.zones
        bucket: str | None = None,
        resources: list[dict[str, Any]] | None = None,
        zones: list[str] | None = None,
        gpu_preference: list[str] | None = None,
        service_account: str | None = None,
    ):
        """Initialize GCE launcher.

        Args:
            project_id: GCP project ID (uses default if None)
            zone: Default GCE zone (must be configured in goldfish.yaml)
            bucket: GCS bucket for logs/artifacts (required for full functionality)
            resources: Resource catalog (list of resource dicts)
            zones: List of all available zones (for multi-zone lookups)
            gpu_preference: Ordered list of preferred GPU types for capacity search
        """
        self.project_id = project_id
        self.default_zone = zone
        self.bucket = bucket
        self.resources = resources or []
        self.zones = zones or [zone]  # Default to list containing just default_zone
        self.gpu_preference = gpu_preference or ["h100", "a100", "none"]
        self.service_account = service_account
        self._project_number: str | None = None
        self._zone_cache: dict[str, str] = {}  # Cache for instance zones

    @property
    def bucket_uri(self) -> str | None:
        """Return bucket as a gs:// URI.

        The bucket may be stored with or without the gs:// prefix.
        This property normalizes it for use with gsutil commands.
        """
        if not self.bucket:
            return None
        if self.bucket.startswith("gs://"):
            return self.bucket
        return f"gs://{self.bucket}"

    def _resolve_service_account(self) -> str | None:
        """Resolve the service account email for instances."""
        if self.service_account:
            return self.service_account
        if not self.project_id:
            return None
        if not self._project_number:
            cmd = [
                "gcloud",
                "projects",
                "describe",
                self.project_id,
                "--format=value(projectNumber)",
            ]
            result = run_gcloud(cmd, project_id=self.project_id)
            self._project_number = result.stdout.strip()
        if not self._project_number:
            return None
        return f"{self._project_number}-compute@developer.gserviceaccount.com"

    def launch_instance(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict,
        work_dir: Path,
        inputs_dir: Path | None = None,
        outputs_dir: Path | None = None,
        machine_type: str = "n1-standard-4",
        gpu_type: str | None = None,
        gpu_count: int = 0,
        zones: list[str] | None = None,
        use_capacity_search: bool = True,
        goldfish_env: dict[str, str] | None = None,
        preemptible: bool | None = None,
        warm_pool_idle_timeout_seconds: int | None = None,
    ) -> GCELaunchResult:
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
            goldfish_env: Goldfish environment variables (metrics, provenance, etc.)
            preemptible: Force spot (True), on-demand (False), or auto (None)

        Returns:
            GCELaunchResult with instance_name and zone

        Raises:
            GoldfishError: If launch fails
        """
        if not self.bucket:
            raise GoldfishError("GCS bucket required for GCE launcher")

        instance_name = self._sanitize_name(stage_run_id)

        # Build startup script using startup_builder
        # Strip both gs:// prefix AND trailing slash for consistent bucket comparison
        bucket_name = self.bucket.replace("gs://", "").rstrip("/")
        run_path = f"runs/{stage_run_id}"

        # Prepare environment variables
        env_map = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(stage_config),
            "GOLDFISH_RUN_ID": stage_run_id,
            "GOLDFISH_INPUTS_DIR": "/mnt/inputs",
            "GOLDFISH_OUTPUTS_DIR": "/mnt/outputs",
        }

        # Add Goldfish environment variables (metrics, provenance, etc.)
        if goldfish_env:
            for env_name, env_value in goldfish_env.items():
                # SECURITY: Validate env var name (alphanumeric + underscore only)
                if env_name.replace("_", "").isalnum():
                    env_map[env_name] = str(env_value)

        # Add user-defined environment variables from config
        # This allows configs to specify env vars like WANDB_API_KEY
        if "environment" in stage_config and isinstance(stage_config["environment"], dict):
            for env_name, env_value in stage_config["environment"].items():
                # SECURITY: Validate env var name (alphanumeric + underscore only)
                if env_name.replace("_", "").isalnum():
                    env_map[env_name] = str(env_value)

        # For GPU workloads, set LD_LIBRARY_PATH so FA3 can find libcuda.so at import time
        # This must be a Docker env var (-e flag), not exported in shell, because Python
        # loads the FA3 .so file at import time before any shell commands run
        #
        # Critical: /tmp/cuda-symlinks MUST come first because:
        # 1. NVIDIA mounts libcuda.so.1 (with version) at runtime
        # 2. FA3 wheel needs libcuda.so (without version) - hardcoded in DT_NEEDED
        # 3. We create a symlink libcuda.so -> libcuda.so.1 in /tmp/cuda-symlinks
        # 4. goldfish user can't write to /usr/lib, so symlink must be in user-writable dir
        if gpu_count and gpu_count > 0:
            env_map["LD_LIBRARY_PATH"] = "/tmp/cuda-symlinks:/usr/lib/x86_64-linux-gnu"

        # Build input staging commands
        # Transform gs://bucket/path to /mnt/gcs/path (symlinks on host)
        pre_run_cmds = [
            f"cat > /mnt/entrypoint.sh << 'ENTRYPOINT_EOF'\n{entrypoint_script}\nENTRYPOINT_EOF",
            "chmod +x /mnt/entrypoint.sh",
            "mkdir -p /mnt/inputs /mnt/outputs",
        ]

        # Stage inputs from GCS to /mnt/inputs/
        inputs = stage_config.get("inputs", {})
        debug_log = f"/mnt/gcs/{run_path}/logs/staging_debug.log"
        # Create logs directory first so debug logging works
        pre_run_cmds.append(f'mkdir -p "/mnt/gcs/{run_path}/logs"')
        pre_run_cmds.append(
            f'echo "DEBUG: Staging {len(inputs)} inputs, bucket_name={bucket_name}" | tee -a {debug_log}'
        )
        for input_name, input_config in inputs.items():
            # Handle both old format (string URI) and new format (dict with location)
            if isinstance(input_config, str):
                gcs_uri = input_config
            elif isinstance(input_config, dict):
                gcs_uri = input_config.get("location", "")
            else:
                pre_run_cmds.append(f'echo "DEBUG: Skipping {input_name} - not string or dict" | tee -a {debug_log}')
                continue

            pre_run_cmds.append(f'echo "DEBUG: Input {input_name} -> {gcs_uri}" | tee -a {debug_log}')

            if gcs_uri and gcs_uri.startswith("gs://"):
                # Extract bucket and path from gs://bucket/path
                uri_parts = gcs_uri.replace("gs://", "").split("/", 1)
                if len(uri_parts) == 2:
                    input_bucket, input_path = uri_parts
                    pre_run_cmds.append(
                        f'echo "DEBUG: input_bucket={input_bucket}, input_path={input_path}" | tee -a {debug_log}'
                    )
                    # If input is from same bucket, try gcsfuse symlink first; fall back to gsutil
                    if input_bucket == bucket_name:
                        # Symlink from gcsfuse mount (preferred for same-bucket inputs)
                        # But verify path exists first - gcsfuse may have stale cache or path may not exist
                        gcsfuse_path = f"/mnt/gcs/{input_path.rstrip('/')}"
                        pre_run_cmds.append(f'echo "DEBUG: Checking gcsfuse path {gcsfuse_path}" | tee -a {debug_log}')
                        # Use if/else to try symlink first, fall back to gsutil if gcsfuse path doesn't exist
                        pre_run_cmds.append(
                            f'if [ -e "{gcsfuse_path}" ] || [ -d "{gcsfuse_path}" ]; then '
                            f'echo "DEBUG: gcsfuse path exists, creating symlink" | tee -a {debug_log}; '
                            f'ln -sf "{gcsfuse_path}" "/mnt/inputs/{input_name}"; '
                            f'else '
                            f'echo "DEBUG: gcsfuse path not found, falling back to gsutil cp" | tee -a {debug_log}; '
                            f'if ! gsutil -m cp -r "{gcs_uri.rstrip("/")}" "/mnt/inputs/{input_name}"; then '
                            f'echo "ERROR: Failed to stage input {input_name} from {gcs_uri}" | tee -a {debug_log} /tmp/stderr.log; '
                            f'echo "The GCS path may not exist or you may lack permissions." | tee -a {debug_log} /tmp/stderr.log; '
                            f'exit 1; fi; '
                            f'fi'
                        )
                    else:
                        # Different bucket - use gsutil to copy
                        pre_run_cmds.append(f'echo "DEBUG: Different bucket, using gsutil cp" | tee -a {debug_log}')
                        pre_run_cmds.append(
                            f'if ! gsutil -m cp -r "{gcs_uri.rstrip("/")}" "/mnt/inputs/{input_name}"; then '
                            f'echo "ERROR: Failed to stage input {input_name} from {gcs_uri}" | tee -a {debug_log} /tmp/stderr.log; '
                            f'echo "The GCS path may not exist or you may lack permissions." | tee -a {debug_log} /tmp/stderr.log; '
                            f'exit 1; fi'
                        )

        # Debug: show what's in /mnt/inputs after staging
        pre_run_cmds.append(f'echo "DEBUG: Contents of /mnt/inputs:" | tee -a {debug_log}')
        pre_run_cmds.append(
            f'ls -la /mnt/inputs/ 2>&1 | tee -a {debug_log} || echo "DEBUG: /mnt/inputs is empty or does not exist" | tee -a {debug_log}'
        )

        # Make inputs/outputs accessible by container user (jovyan, UID 1000)
        # The startup script runs as root, but Docker container runs as non-root
        # Use -h to change symlink ownership (not the target), don't use -R to avoid following symlinks
        pre_run_cmds.append("chown 1000:100 /mnt/inputs /mnt/outputs")
        pre_run_cmds.append("chown -h 1000:100 /mnt/inputs/* 2>/dev/null || true")

        # Debug: verify permissions after chown
        pre_run_cmds.append(f'echo "DEBUG: Permissions after chown:" | tee -a {debug_log}')
        pre_run_cmds.append(f"ls -la /mnt/inputs/ 2>&1 | tee -a {debug_log}")
        pre_run_cmds.append(f'echo "DEBUG: gcsfuse mount info:" | tee -a {debug_log}')
        pre_run_cmds.append(f"mount | grep gcsfuse 2>&1 | tee -a {debug_log}")
        pre_run_cmds.append(f'echo "DEBUG: Testing access as user 1000:" | tee -a {debug_log}')
        pre_run_cmds.append(
            f'su -s /bin/bash -c "ls -la /mnt/inputs/" nobody 2>&1 | tee -a {debug_log} || echo "DEBUG: Access test failed" | tee -a {debug_log}'
        )

        # Build output staging commands (run after Docker)
        outputs_gcs_path = f"gs://{bucket_name}/{run_path}/outputs"
        # Upload outputs with retry and verification (large files like 327MB tokens.npz)
        # Use rsync instead of cp for better reliability with large files
        post_run_cmds = [
            'echo "Uploading outputs to GCS..."',
            f"for i in {{1..3}}; do "
            f'if timeout 600 gsutil -m rsync -r /mnt/outputs/ "{outputs_gcs_path}/"; then '
            f'echo "✓ Outputs uploaded successfully"; break; '
            f'else echo "✗ Output upload attempt $i failed"; '
            f"[ $i -lt 3 ] && sleep 5; fi; done",
        ]

        # Extract cost protection settings from stage config (with safe defaults)
        compute_config = stage_config.get("compute", {})
        max_runtime = compute_config.get("max_runtime_seconds", DEFAULT_MAX_RUNTIME_SECONDS)
        heartbeat_timeout = compute_config.get("heartbeat_timeout_seconds")  # None = no supervisor
        # Log sync interval for real-time log visibility (default 5s, configurable)
        log_sync_interval = compute_config.get("log_sync_interval", 5)

        # Build startup script with proper orchestration and cost protection
        # For GPU workloads, wrap the command to create libcuda.so symlink at runtime
        # FA3 pre-built wheels expect libcuda.so but nvidia-container-toolkit only mounts libcuda.so.1
        # CRITICAL: Create symlink in /tmp/cuda-symlinks (user-writable) not /usr/lib (root-only)
        if gpu_count and gpu_count > 0:
            docker_cmd = (
                "-c '"
                "mkdir -p /tmp/cuda-symlinks && "
                "ln -sf /usr/lib/x86_64-linux-gnu/libcuda.so.1 /tmp/cuda-symlinks/libcuda.so && "
                "exec /entrypoint.sh'"
            )
        else:
            docker_cmd = "/entrypoint.sh"

        startup_script = build_startup_script(
            bucket=bucket_name,
            bucket_prefix="",
            run_path=run_path,
            image=image_tag,
            entrypoint="/bin/bash",
            cmd=docker_cmd,
            env_map=env_map,
            mounts=[
                ("/mnt/entrypoint.sh", "/entrypoint.sh"),
                ("/mnt/gcs", "/mnt/gcs"),  # Mount gcsfuse for input access
                ("/mnt/inputs", "/mnt/inputs"),  # Input staging directory
                ("/mnt/outputs", "/mnt/outputs"),  # Output staging directory
            ],
            gcsfuse=True,
            pre_run_cmds=pre_run_cmds,
            post_run_cmds=post_run_cmds,
            # Cost protection - ALWAYS set max_runtime to prevent runaway instances
            max_runtime_seconds=max_runtime,
            heartbeat_timeout_seconds=heartbeat_timeout,
            # Real-time log visibility - sync logs to GCS every N seconds
            log_sync_interval=log_sync_interval,
            # GPU flag - profile-based, not runtime nvidia-smi detection
            gpu_count=gpu_count or 0,
            # Warm pool: if set, instance enters idle loop after Docker exits instead of self-deleting
            warm_pool_idle_timeout_seconds=warm_pool_idle_timeout_seconds,
        )

        if use_capacity_search and self.resources:
            # Use ResourceLauncher for capacity-aware search
            return self._launch_with_capacity_search(
                instance_name=instance_name,
                startup_script=startup_script,
                gpu_type=gpu_type,
                zones=zones,
                preemptible=preemptible,
                machine_type=machine_type,
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
                preemptible=preemptible,
            )

    def _launch_with_capacity_search(
        self,
        instance_name: str,
        startup_script: str,
        gpu_type: str | None,
        zones: list[str] | None,
        preemptible: bool | None = None,
        machine_type: str | None = None,
    ) -> GCELaunchResult:
        """Launch using ResourceLauncher for capacity search.

        Args:
            instance_name: Instance name
            startup_script: Startup script content
            gpu_type: GPU type to filter resources
            zones: Zones to search
            preemptible: Force spot (True), on-demand (False), or auto (None)
            machine_type: Exact machine type to match (e.g., "a3-highgpu-8g")

        Returns:
            GCELaunchResult with instance_name and zone

        Raises:
            GoldfishError: If no capacity found
        """
        # Filter resources to match the requested profile exactly.
        # Without machine_type filtering, profiles with the same GPU type but
        # different machine sizes (e.g., a3-highgpu-1g vs a3-highgpu-8g) would
        # all match, and the capacity search could pick the wrong one.
        if gpu_type:
            filtered_resources = [
                r for r in self.resources if (r.get("gpu", {}).get("accelerator") or "").lower() == gpu_type.lower()
            ]
        else:
            # No GPU requested - include resources with no GPU or gpu.type="none"
            filtered_resources = [
                r
                for r in self.resources
                if not r.get("gpu", {}).get("type") or r.get("gpu", {}).get("type", "").lower() == "none"
            ]

        # Further filter by machine_type if specified (critical for multi-GPU profiles)
        if machine_type and filtered_resources:
            exact = [r for r in filtered_resources if r.get("machine_type") == machine_type]
            if exact:
                filtered_resources = exact

        if not filtered_resources:
            raise GoldfishError(f"No resources found for GPU accelerator: {gpu_type or 'none'}")

        # Determine preemptible preference
        # None = default (spot_first), True = force spot, False = force on_demand
        force_preemptible: str | None = None
        if preemptible is True:
            force_preemptible = "spot"
        elif preemptible is False:
            force_preemptible = "on_demand"

        # Create ResourceLauncher
        # Use instance gpu_preference for ordering, but force_gpu restricts to specific type
        launcher = ResourceLauncher(
            resources=filtered_resources,
            gpu_preference=self.gpu_preference,
            force_gpu=gpu_type,
            force_preemptible=force_preemptible,
            zones_override=zones,
            project_id=self.project_id,
            service_account=self._resolve_service_account(),
        )

        # Launch with capacity search
        result = launcher.launch(
            instance_name=instance_name,
            startup_script=startup_script,
        )

        return GCELaunchResult(
            instance_name=result.instance_name,
            zone=result.selection.zone,
        )

    def _launch_simple(
        self,
        instance_name: str,
        startup_script: str,
        machine_type: str,
        gpu_type: str | None,
        gpu_count: int,
        zone: str,
        preemptible: bool | None = None,
    ) -> GCELaunchResult:
        """Simple launch without capacity search.

        Args:
            instance_name: Instance name
            startup_script: Startup script content
            machine_type: Machine type
            gpu_type: GPU accelerator type
            gpu_count: GPU count
            zone: Zone
            preemptible: Request spot/preemptible (True), on-demand (False), or default (None)

        Returns:
            GCELaunchResult with instance_name and zone

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
                "--image-family=debian-12",
                "--image-project=debian-cloud",
                f"--metadata-from-file=startup-script={startup_path}",
                "--scopes=https://www.googleapis.com/auth/cloud-platform",
                "--quiet",
            ]
            service_account = self._resolve_service_account()
            if service_account:
                cmd.append(f"--service-account={service_account}")

            if gpu_type and gpu_count > 0:
                # A3 machine types have integrated H100 GPUs - don't pass --accelerator
                # The GPU is already part of the machine type (e.g., a3-highgpu-1g = 1x H100)
                if not machine_type.startswith("a3-"):
                    cmd.extend(["--accelerator", f"count={gpu_count},type={gpu_type}"])
                cmd.append("--maintenance-policy=TERMINATE")
                cmd.append("--restart-on-failure")
                cmd.append("--metadata=install-nvidia-driver=True")

            # Handle spot/preemptible preference
            if preemptible is True:
                cmd.append("--provisioning-model=SPOT")

            if self.project_id:
                cmd.append(f"--project={self.project_id}")

            # GPU instances (especially H100) can take 2-3 minutes to create
            has_gpu = gpu_type and gpu_count > 0
            instance_timeout = 180 if has_gpu else 60
            run_gcloud(cmd, timeout=instance_timeout)

            # Wait for instance to be fully ready (RUNNING state)
            # This is required before metadata operations (set_signal) can succeed.
            from goldfish.cloud.adapters.gcp.resource_launcher import wait_for_instance_ready

            wait_for_instance_ready(
                instance_name=instance_name,
                zone=zone,
                project_id=self.project_id,
                timeout_sec=120,  # 2 minutes for GPU instances
                poll_interval=2.0,
            )

            return GCELaunchResult(instance_name=instance_name, zone=zone)

        finally:
            startup_path.unlink(missing_ok=True)

    def create_disk(
        self,
        disk_name: str,
        zone: str,
        size_gb: int = 100,
        disk_type: str = "pd-ssd",
        snapshot: str | None = None,
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
                    f"--provisioned-iops={HYPERDISK_PROVISIONED_IOPS}",
                    f"--provisioned-throughput={HYPERDISK_PROVISIONED_THROUGHPUT}",
                ]
            )

        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd, project_id=self.project_id)

    def delete_disk(self, disk_name: str, zone: str) -> None:
        """Delete a persistent disk.

        Args:
            disk_name: Disk name
            zone: GCE zone
        """
        cleanup_disk(disk_name, zone)

    def snapshot_disk(self, disk_name: str, snapshot_name: str, zone: str) -> None:
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

        run_gcloud(cmd, project_id=self.project_id)

    def sync_to_gcs(self, local_path: Path, gcs_uri: str) -> None:
        """Sync local directory to GCS.

        Args:
            local_path: Local directory path
            gcs_uri: GCS URI (e.g., "gs://bucket/path")

        Raises:
            GoldfishError: If sync fails
        """
        cmd = ["gsutil"]
        if self.project_id:
            cmd.extend(["-o", f"GSUtil:project_id={self.project_id}"])
        cmd.extend(["-m", "rsync", "-r", str(local_path), gcs_uri])
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
        cmd = ["gsutil"]
        if self.project_id:
            cmd.extend(["-o", f"GSUtil:project_id={self.project_id}"])
        cmd.extend(["-m", "rsync", "-r", gcs_uri, str(local_path)])
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise GoldfishError(f"GCS sync failed: {result.stderr}")

    def get_instance_status(self, instance_name: str) -> str:
        """Get status of GCE instance.

        Uses zone-agnostic lookup with instances list to handle capacity-aware
        launches where instance may be in any zone.

        Args:
            instance_name: Instance identifier

        Returns:
            Status: "running", "completed", "failed", or "not_found"
        """
        instance_name = self._sanitize_name(instance_name)

        # FIX #19: Use zone-agnostic instance lookup
        # This works regardless of which zone the instance was launched in
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "list",
            f"--filter=name={instance_name}",
            "--format=value(status)",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        result = run_gcloud(cmd, check=False, project_id=self.project_id)

        if result.returncode == 0:
            status = result.stdout.strip()
            if status:  # Instance found
                return self._map_gce_status(status, instance_name)
            return "not_found"

        # Distinguish real errors from not-found
        stderr = (result.stderr or "").lower()
        if "not found" in stderr or "could not fetch resource" in stderr or not stderr:
            return "not_found"
        raise GoldfishError(f"Failed to query instance status: {result.stderr}")

    def _map_gce_status(self, status: str, instance_name: str) -> str:
        """Map GCE instance status to Goldfish state.

        Args:
            status: GCE status string
            instance_name: Instance identifier

        Returns:
            Goldfish state: StageState.RUNNING, COMPLETED, or FAILED
        """
        # Map GCE status to Goldfish state
        if status in ("PROVISIONING", "STAGING", "RUNNING"):
            return StageState.RUNNING
        elif status == "TERMINATED":
            # Check exit code in GCS if available
            if self.bucket:
                exit_result = self._get_exit_code(instance_name)
                return (
                    StageState.COMPLETED
                    if (exit_result.exists and exit_result.code == 0 and not exit_result.gcs_error)
                    else StageState.FAILED
                )
            return StageState.COMPLETED
        elif status in ("STOPPING", "SUSPENDING", "SUSPENDED"):
            return StageState.RUNNING
        else:
            return StageState.FAILED

    def _find_instance_zone(self, instance_name: str) -> str | None:
        """Find which zone an instance is in.

        Tries default zone first, then searches all configured zones.
        Caches results to avoid repeated API calls.

        Args:
            instance_name: Instance identifier

        Returns:
            Zone name if found, None otherwise
        """
        instance_name = self._sanitize_name(instance_name)

        # Check cache first
        if instance_name in self._zone_cache:
            return self._zone_cache[instance_name]

        # Try default zone first (fast path)
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "describe",
            instance_name,
            f"--zone={self.default_zone}",
            "--format=value(name)",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        result = run_gcloud(cmd, check=False, project_id=self.project_id)
        if result.returncode == 0:
            self._zone_cache[instance_name] = self.default_zone
            return self.default_zone

        # Zone-agnostic lookup to avoid N calls
        cmd_list = [
            "gcloud",
            "compute",
            "instances",
            "list",
            f"--filter=name={instance_name}",
            "--format=value(zone)",
        ]
        if self.project_id:
            cmd_list.append(f"--project={self.project_id}")

        result = run_gcloud(cmd_list, check=False, project_id=self.project_id)
        if result.returncode == 0:
            zone: str = result.stdout.strip()
            if zone:
                self._zone_cache[instance_name] = zone
                return zone
        return None

    def _get_exit_code(self, instance_name: str, max_attempts: int = 5, retry_delay: float = 2.0) -> ExitCodeResult:
        """Get exit code from GCS with explicit, non-ambiguous semantics.

        This returns an ExitCodeResult to distinguish:
        - File exists with code → exists=True, code=N
        - File missing → exists=False, code=None (crash/preemption possible)
        - GCS error → gcs_error=True (transient/outage/auth issue)
        """
        bucket_uri = self.bucket_uri
        if not bucket_uri:
            # No bucket configured - assume success for local-like behavior.
            return ExitCodeResult.from_code(0)

        stage_run_id = self._sanitize_name(instance_name)
        try:
            storage = getattr(self, "_storage", None)
            if storage is None:
                from goldfish.cloud.adapters.gcp.storage import GCSStorage

                storage = GCSStorage(project=self.project_id)
                self._storage = storage
            return get_exit_code_gce(
                bucket_uri=bucket_uri,
                stage_run_id=stage_run_id,
                storage=storage,
                project_id=self.project_id,
                max_attempts=max_attempts,
                retry_delay=retry_delay,
            )
        except Exception as e:
            # Defensive: treat unexpected errors as GCS errors so callers don't
            # conflate them with a real non-zero exit code.
            return ExitCodeResult.from_gcs_error(str(e))

    def get_instance_logs(
        self,
        instance_name: str,
        tail_lines: int | None = None,
        since: str | None = None,
        retry_on_empty: bool = False,
    ) -> str:
        """Retrieve logs from GCE instance.

        Tries to fetch from GCS first, falls back to serial console.

        Args:
            instance_name: Instance identifier
            tail_lines: Number of lines from end to return
            since: Only return logs after this ISO timestamp
            retry_on_empty: If True, retry once after delay if logs are empty
                           (handles GCS eventual consistency)

        Returns:
            Instance logs as string
        """
        from collections import deque
        from datetime import datetime

        def _parse_dt(val: str) -> datetime | None:
            if not val:
                return None
            try:
                iso = val.replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except Exception:
                return None

        def _line_ts(line: str) -> datetime | None:
            first = (line.split() or [""])[0]
            return _parse_dt(first)

        def _collect(stream, since_dt: datetime | None) -> str:
            if tail_lines:
                buf: deque[str] = deque(maxlen=tail_lines)
                for ln in stream:
                    if since_dt:
                        ts = _line_ts(ln)
                        if ts and ts < since_dt:
                            continue
                    buf.append(ln)
                return "".join(buf)
            # No tail requested: keep all, optionally filter by since
            out = []
            for ln in stream:
                if since_dt:
                    ts = _line_ts(ln)
                    if ts and ts < since_dt:
                        continue
                out.append(ln)
            return "".join(out)

        instance_name = self._sanitize_name(instance_name)
        since_dt = _parse_dt(since) if since else None

        def _fetch_gcs_logs() -> str | None:
            """Fetch logs from GCS, return None if not available."""
            if not self.bucket_uri:
                logger.debug("_fetch_gcs_logs: no bucket_uri configured")
                return None

            try:
                # Try new format (stdout.log + stderr.log)
                stdout_path = f"{self.bucket_uri}/runs/{instance_name}/logs/stdout.log"
                stderr_path = f"{self.bucket_uri}/runs/{instance_name}/logs/stderr.log"
                logger.debug("Fetching logs from %s", stdout_path)

                # Fetch stdout using gcloud storage (faster than gsutil)
                cmd = ["gcloud", "storage", "cat", stdout_path]
                if self.project_id:
                    cmd.append(f"--project={self.project_id}")

                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    if not proc.stdout:
                        raise RuntimeError("No stdout from gcloud storage cat")
                    with proc.stdout:
                        stdout_output = _collect(proc.stdout, since_dt)
                finally:
                    try:
                        proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()

                # Check return code - if gcloud failed, try legacy format
                if proc.returncode != 0:
                    stderr_msg = proc.stderr.read() if proc.stderr else ""
                    logger.debug(
                        "gcloud storage cat failed for %s (rc=%d): %s",
                        stdout_path,
                        proc.returncode,
                        stderr_msg.strip(),
                    )
                    raise RuntimeError(f"gcloud storage cat failed: {stderr_msg}")

                logger.debug("Fetched %d bytes from stdout.log", len(stdout_output))

                # Fetch stderr (may not exist for older runs)
                stderr_output = ""
                try:
                    cmd2 = ["gcloud", "storage", "cat", stderr_path]
                    if self.project_id:
                        cmd2.append(f"--project={self.project_id}")

                    proc2 = subprocess.Popen(
                        cmd2,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        text=True,
                    )
                    try:
                        if proc2.stdout:
                            with proc2.stdout:
                                stderr_output = _collect(proc2.stdout, since_dt)
                    finally:
                        try:
                            proc2.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            proc2.kill()
                            proc2.wait()
                except Exception:
                    pass  # stderr.log may not exist

                # Combine stdout and stderr
                if stderr_output:
                    return stdout_output + "\n\n=== STDERR ===\n" + stderr_output
                return stdout_output

            except Exception as e:
                logger.debug("_fetch_gcs_logs failed for new format: %s", e)
                # Try legacy train.log format
                try:
                    cmd_legacy = ["gcloud", "storage", "cat", f"{self.bucket_uri}/runs/{instance_name}/logs/train.log"]
                    if self.project_id:
                        cmd_legacy.append(f"--project={self.project_id}")

                    proc = subprocess.Popen(
                        cmd_legacy,
                        stdout=subprocess.PIPE,
                        text=True,
                    )
                    try:
                        if not proc.stdout:
                            raise RuntimeError("No stdout from gcloud storage cat")
                        with proc.stdout:
                            output = _collect(proc.stdout, since_dt)
                    finally:
                        try:
                            proc.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
                    if proc.returncode == 0:
                        return output
                except Exception:
                    pass
                return None

        # Try GCS first - with optional retry on empty
        result = _fetch_gcs_logs()

        # Retry once if result is empty and retry_on_empty is enabled
        if retry_on_empty and (result is None or result == ""):
            time.sleep(5)  # Wait for GCS eventual consistency
            result = _fetch_gcs_logs()

        if result is not None:
            return result

        # Fall back to serial console - find correct zone first
        zone = self._find_instance_zone(instance_name)
        if not zone:
            return f"Instance {instance_name} not found in any configured zone"

        def _filter_serial_noise(lines: list[str]) -> list[str]:
            """Filter out noisy metadata syncer and startup script debug output."""
            noise_patterns = [
                "google_metadata_script_runner",
                'Metadata key("startup-script")',
                "SIG_JSON=",
                "REQ_ID=",
                "LAST_SEEN=",
                "CMD=sync",
                "curl -sf -H",
                "metadata.google.internal",
                "printf %s",
                "sed -n",
                "[[ sync ==",
                "[[ -n ",
                "sleep 1",
                "sleep $",
                "gcloud storage cp",
                "kill -0",
                "+ true",
                "++ ",  # bash debug prefixes for subshells
            ]
            filtered = []
            for line in lines:
                # Skip lines that match any noise pattern
                if any(pattern in line for pattern in noise_patterns):
                    continue
                filtered.append(line)
            return filtered

        try:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "get-serial-port-output",
                instance_name,
                f"--zone={zone}",
                "--port=1",
            ]
            if self.project_id:
                cmd.append(f"--project={self.project_id}")

            gcloud_result = run_gcloud(cmd, check=False, project_id=self.project_id)
            raw_lines = gcloud_result.stdout.splitlines(keepends=True)
            filtered_lines = _filter_serial_noise(raw_lines)
            return _collect(filtered_lines, since_dt)
        except Exception as e:
            return f"Failed to retrieve logs: {e}"

    def stop_instance(self, instance_name: str) -> None:
        """Stop GCE instance (preserves disk).

        Args:
            instance_name: Instance identifier

        Raises:
            GoldfishError: If instance not found in any zone
        """
        instance_name = self._sanitize_name(instance_name)

        # Find which zone the instance is in
        zone = self._find_instance_zone(instance_name)
        if not zone:
            raise GoldfishError(f"Instance {instance_name} not found in any configured zone")

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "stop",
            instance_name,
            f"--zone={zone}",
            "--quiet",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd, project_id=self.project_id)

    def delete_instance(self, instance_name: str) -> None:
        """Delete GCE instance and boot disk.

        Args:
            instance_name: Instance identifier
        """
        instance_name = self._sanitize_name(instance_name)
        logger.info("delete_instance called for %s (project=%s)", instance_name, self.project_id)

        # Find which zone the instance is in
        zone = self._find_instance_zone(instance_name)
        if not zone:
            # Instance not found - already deleted or never existed
            logger.warning("delete_instance: instance %s not found (may already be deleted)", instance_name)
            return

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "delete",
            instance_name,
            f"--zone={zone}",
            "--quiet",
        ]
        if self.project_id:
            cmd.append(f"--project={self.project_id}")

        run_gcloud(cmd, check=False)  # Don't fail if already deleted
        logger.info("delete_instance: deleted %s in zone %s", instance_name, zone)

    def wait_for_termination(self, instance_name: str, timeout_sec: int = 3600) -> str:
        """Wait for instance to terminate.

        Polls instance status until it reaches TERMINATED state or timeout.

        Args:
            instance_name: Instance identifier
            timeout_sec: Maximum seconds to wait

        Returns:
            Final state: StageState.COMPLETED or FAILED

        Raises:
            GoldfishError: If timeout exceeded
        """
        instance_name = self._sanitize_name(instance_name)
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            status = self.get_instance_status(instance_name)

            if status == "not_found":
                raise GoldfishError(f"Instance {instance_name} not found")

            if status in (StageState.COMPLETED, StageState.FAILED):
                return status

            time.sleep(10)  # Poll every 10 seconds

        # Timeout - get serial logs for debugging
        logs = self.get_instance_logs(instance_name)
        raise GoldfishError(
            f"Instance {instance_name} did not terminate within {timeout_sec}s. Last logs:\n{logs[-1000:]}"
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
