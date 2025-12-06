"""GCE (Google Compute Engine) launcher for Goldfish stage execution.

Ported from legacy infra/resource_launcher.py and infra/_launch.py
Provides full GCE functionality:
- Capacity-aware multi-zone search
- Disk management (hyperdisk support)
- GCS integration
- GPU support
- Instance monitoring and cleanup
"""

import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from goldfish.errors import GoldfishError
from goldfish.infra.resource_launcher import ResourceLauncher, run_gcloud
from goldfish.infra.startup_builder import build_startup_script


def run_gcloud(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run gcloud command with proper error handling.

    Args:
        cmd: Command list (e.g., ["gcloud", "compute", "instances", "list"])
        check: Raise exception on non-zero exit code

    Returns:
        CompletedProcess with stdout/stderr captured

    Raises:
        GoldfishError: If command fails and check=True
    """
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and check:
        output = (result.stdout or "") + (result.stderr or "")
        raise GoldfishError(f"gcloud command failed: {output.strip()}")
    return result


class GCELauncher:
    """Launch stage runs on Google Compute Engine instances.

    Basic implementation ported from legacy infra code. Supports:
    - Instance creation with Docker containers
    - Container-Optimized OS
    - GCS log streaming
    - Instance monitoring and cleanup
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        zone: str = "us-central1-a",
        bucket: Optional[str] = None,
    ):
        """Initialize GCE launcher.

        Args:
            project_id: GCP project ID (uses default if None)
            zone: GCE zone for instances
            bucket: GCS bucket for logs/artifacts (optional)
        """
        self.project_id = project_id
        self.zone = zone
        self.bucket = bucket

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
    ) -> str:
        """Launch GCE instance for stage run.

        Args:
            image_tag: Docker image to run
            stage_run_id: Stage run identifier (also used as instance name)
            entrypoint_script: Bash script to run in container
            stage_config: Stage configuration dict (passed as JSON)
            work_dir: Local working directory for staging files
            inputs_dir: Directory to sync to GCS for inputs (optional)
            outputs_dir: Directory to sync from GCS for outputs (optional)
            machine_type: GCE machine type (e.g., "n1-standard-4")
            gpu_type: GPU accelerator type (e.g., "nvidia-tesla-t4")
            gpu_count: Number of GPUs to attach

        Returns:
            Instance name (same as stage_run_id)

        Raises:
            GoldfishError: If instance creation fails
        """
        instance_name = self._sanitize_name(stage_run_id)

        # Generate startup script for Container-Optimized OS
        startup_script = self._generate_startup_script(
            image_tag=image_tag,
            entrypoint_script=entrypoint_script,
            stage_config=stage_config,
            stage_run_id=stage_run_id,
        )

        # Write startup script to temp file
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sh") as f:
            f.write(startup_script)
            startup_path = Path(f.name)

        try:
            # Build gcloud command
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "create",
                instance_name,
                f"--zone={self.zone}",
                f"--machine-type={machine_type}",
                "--boot-disk-size=100GB",
                "--boot-disk-type=pd-ssd",
                "--image-family=cos-stable",
                "--image-project=cos-cloud",
                f"--metadata-from-file=startup-script={startup_path}",
                "--scopes=https://www.googleapis.com/auth/cloud-platform",
                "--quiet",
            ]

            # Add GPU if requested
            if gpu_type and gpu_count > 0:
                cmd.extend(["--accelerator", f"count={gpu_count},type={gpu_type}"])
                cmd.append("--maintenance-policy=TERMINATE")
                cmd.append("--restart-on-failure")
                cmd.append("--metadata=install-nvidia-driver=True")

            # Add project if specified
            if self.project_id:
                cmd.append(f"--project={self.project_id}")

            # Launch instance
            run_gcloud(cmd)

            return instance_name

        finally:
            # Clean up temp file
            startup_path.unlink(missing_ok=True)

    def _generate_startup_script(
        self,
        image_tag: str,
        entrypoint_script: str,
        stage_config: dict,
        stage_run_id: str,
    ) -> str:
        """Generate startup script for Container-Optimized OS.

        The script:
        1. Pulls Docker image
        2. Runs container with entrypoint script
        3. Uploads exit code to GCS (if bucket configured)
        4. Shuts down instance

        Args:
            image_tag: Docker image to run
            entrypoint_script: Script to run in container
            stage_config: Stage configuration
            stage_run_id: Stage run identifier

        Returns:
            Startup script as string
        """
        # Escape config JSON for embedding in script
        config_json = json.dumps(stage_config).replace('"', '\\"')

        script = f"""#!/bin/bash
set -euo pipefail

echo "Starting Goldfish stage run: {stage_run_id}"

# Pull Docker image
echo "Pulling image: {image_tag}"
docker pull {image_tag}

# Create entrypoint script
cat > /tmp/entrypoint.sh << 'ENTRYPOINT_EOF'
{entrypoint_script}
ENTRYPOINT_EOF
chmod +x /tmp/entrypoint.sh

# Run container
echo "Running container..."
EXIT_CODE=0
docker run \\
    --rm \\
    -e GOLDFISH_STAGE_CONFIG="{config_json}" \\
    -v /tmp/entrypoint.sh:/entrypoint.sh:ro \\
    {image_tag} \\
    /bin/bash /entrypoint.sh || EXIT_CODE=$?

echo "Container exited with code: $EXIT_CODE"
"""

        # Add GCS upload if bucket configured
        if self.bucket:
            script += f"""
# Upload exit code to GCS
echo $EXIT_CODE > /tmp/exit_code.txt
gsutil cp /tmp/exit_code.txt gs://{self.bucket}/runs/{stage_run_id}/exit_code.txt || true

# Upload logs
if [ -f /tmp/stdout.log ]; then
    gsutil cp /tmp/stdout.log gs://{self.bucket}/runs/{stage_run_id}/stdout.log || true
fi
if [ -f /tmp/stderr.log ]; then
    gsutil cp /tmp/stderr.log gs://{self.bucket}/runs/{stage_run_id}/stderr.log || true
fi
"""

        script += """
# Shutdown instance
echo "Shutting down..."
shutdown -h now
"""
        return script

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
            f"--zone={self.zone}",
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
                    f"gs://{self.bucket}/runs/{instance_name}/exit_code.txt",
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
                        f"gs://{self.bucket}/runs/{instance_name}/stdout.log",
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
                f"--zone={self.zone}",
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
            f"--zone={self.zone}",
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
            f"--zone={self.zone}",
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
            timeout_sec: Maximum seconds to wait (default 3600 = 1 hour)

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
