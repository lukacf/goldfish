"""GCE implementation of RunBackend protocol.

Wraps the existing GCELauncher to implement the RunBackend protocol.
All GCE-specific code is contained in this adapter.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.cloud.contracts import (
    BackendCapabilities,
    BackendStatus,
    RunHandle,
    RunSpec,
    RunStatus,
)
from goldfish.errors import CapacityError, LaunchError, NotFoundError
from goldfish.infra.gce_launcher import GCELauncher
from goldfish.state_machine.types import StageState

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class GCERunBackend:
    """GCE implementation of RunBackend protocol.

    Wraps GCELauncher to provide a protocol-compatible interface.
    This adapter enables using GCE compute through the cloud abstraction layer.
    """

    def __init__(
        self,
        project_id: str | None = None,
        zones: list[str] | None = None,
        bucket: str | None = None,
        gpu_preference: list[str] | None = None,
        service_account: str | None = None,
    ) -> None:
        """Initialize GCE backend.

        Args:
            project_id: GCP project ID (uses default if None)
            zones: List of zones for capacity search
            bucket: GCS bucket for logs/artifacts
            gpu_preference: Ordered list of preferred GPU types
            service_account: Service account email for instances
        """
        default_zone = zones[0] if zones else "us-central1-a"
        self._launcher = GCELauncher(
            project_id=project_id,
            zone=default_zone,
            bucket=bucket,
            zones=zones,
            gpu_preference=gpu_preference,
            service_account=service_account,
        )
        self._project_id = project_id
        self._zones = zones or [default_zone]
        self._bucket = bucket

    @property
    def capabilities(self) -> BackendCapabilities:
        """Return GCE backend capabilities.

        GCE supports:
        - GPU (when requested via accelerator)
        - Spot/preemptible instances
        - Preemption (graceful SIGTERM handling)
        - Preemption detection (via metadata server)
        - Live logs (via GCS sync)
        - Metrics collection
        - Max 24h runtime (GCE limit)
        """
        return BackendCapabilities(
            supports_gpu=True,
            supports_spot=True,
            supports_preemption=True,
            supports_preemption_detection=True,
            supports_live_logs=True,
            supports_metrics=True,
            max_run_duration_hours=24,
        )

    def launch(self, spec: RunSpec) -> RunHandle:
        """Launch a GCE instance for the given spec.

        Args:
            spec: Run specification with image, command, resources, etc.

        Returns:
            Handle to the launched instance.

        Raises:
            CapacityError: If no capacity available in any zone.
            LaunchError: If instance launch fails.
        """
        try:
            # Build stage config from spec
            stage_config = {
                "inputs": spec.inputs,
                "compute": {
                    "max_runtime_seconds": spec.timeout_seconds,
                },
            }

            # Build environment variables
            goldfish_env = dict(spec.env)

            # Launch instance via GCELauncher
            # Note: GCELauncher.launch_instance has a complex signature that
            # includes entrypoint_script, work_dir, etc. For protocol compliance,
            # we construct minimal required arguments.

            # Create a minimal work directory
            work_dir = Path(tempfile.mkdtemp(prefix=f"goldfish-{spec.stage_run_id}-"))

            # Build entrypoint from command
            entrypoint_script = " ".join(spec.command) if spec.command else "echo 'No command'"

            # Determine machine type and GPU from spec
            machine_type = "n1-standard-4"  # Default
            gpu_type = None
            gpu_count = 0

            if spec.gpu_count and spec.gpu_count > 0:
                gpu_count = spec.gpu_count
                # Map GPU type - GCE uses different naming
                gpu_type = "nvidia-tesla-t4"  # Default GPU

            if spec.cpu_count:
                # Map CPU count to machine type (approximate)
                if spec.cpu_count <= 2:
                    machine_type = "n1-standard-2"
                elif spec.cpu_count <= 4:
                    machine_type = "n1-standard-4"
                elif spec.cpu_count <= 8:
                    machine_type = "n1-standard-8"
                else:
                    machine_type = "n1-standard-16"

            result = self._launcher.launch_instance(
                image_tag=spec.image,
                stage_run_id=spec.stage_run_id,
                entrypoint_script=entrypoint_script,
                stage_config=stage_config,
                work_dir=work_dir,
                machine_type=machine_type,
                gpu_type=gpu_type,
                gpu_count=gpu_count,
                zones=self._zones,
                goldfish_env=goldfish_env,
                preemptible=spec.spot,  # Pass spot preference to launcher
            )

            return RunHandle(
                stage_run_id=spec.stage_run_id,
                backend_type="gce",
                backend_handle=result.instance_name,
                zone=result.zone,
            )

        except Exception as e:
            error_msg = str(e).lower()
            if "quota" in error_msg or "capacity" in error_msg or "exhausted" in error_msg:
                raise CapacityError(
                    f"No capacity available: {e}",
                    zones_tried=self._zones,
                ) from e
            raise LaunchError(
                f"Failed to launch GCE instance: {e}",
                stage_run_id=spec.stage_run_id,
                cause="gce_error",
            ) from e

    def get_status(self, handle: RunHandle) -> BackendStatus:
        """Get current status of a GCE instance.

        Args:
            handle: Handle to the instance.

        Returns:
            Current backend status with exit code if terminated.

        Raises:
            NotFoundError: If instance no longer exists.
        """
        instance_name = handle.backend_handle

        try:
            # Use GCELauncher's status method
            status_str = self._launcher.get_instance_status(instance_name)

            # Map Goldfish state to RunStatus
            if status_str == StageState.RUNNING:
                return BackendStatus(status=RunStatus.RUNNING)
            elif status_str == StageState.COMPLETED:
                return BackendStatus(status=RunStatus.COMPLETED, exit_code=0)
            elif status_str == StageState.FAILED:
                # Try to get exit code for more detail
                exit_result = self._launcher._get_exit_code(instance_name)
                if exit_result.exists and exit_result.code is not None:
                    return BackendStatus.from_exit_code(exit_result.code)
                return BackendStatus(status=RunStatus.FAILED, exit_code=1)
            elif status_str == "not_found":
                raise NotFoundError(f"instance:{instance_name}")
            else:
                # Unknown status - treat as running
                return BackendStatus(status=RunStatus.RUNNING)

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"instance:{instance_name}") from e
            # For other errors, return unknown status
            logger.warning("Error getting status for %s: %s", instance_name, e)
            return BackendStatus(status=RunStatus.RUNNING)

    def get_logs(self, handle: RunHandle, tail: int = 200) -> str:
        """Get logs from a GCE instance.

        Args:
            handle: Handle to the instance.
            tail: Number of lines from end to return.

        Returns:
            Log content as string.
        """
        instance_name = handle.backend_handle

        try:
            return self._launcher.get_instance_logs(
                instance_name=instance_name,
                tail_lines=tail if tail > 0 else None,
            )
        except Exception as e:
            logger.warning("Error getting logs for %s: %s", instance_name, e)
            return ""

    def terminate(self, handle: RunHandle) -> None:
        """Terminate a GCE instance.

        Sends termination signal. Instance will be deleted.
        Idempotent: no error if already terminated.

        Args:
            handle: Handle to the instance.
        """
        instance_name = handle.backend_handle
        zone = handle.zone or self._launcher.default_zone

        try:
            from goldfish.infra.resource_launcher import run_gcloud

            cmd = [
                "gcloud",
                "compute",
                "instances",
                "delete",
                instance_name,
                f"--zone={zone}",
                "--quiet",
            ]
            if self._project_id:
                cmd.append(f"--project={self._project_id}")

            run_gcloud(cmd, check=False, project_id=self._project_id)
            logger.debug("Terminated instance %s in zone %s", instance_name, zone)

        except Exception as e:
            # Idempotent - ignore errors (instance may already be gone)
            logger.debug("Error terminating instance %s: %s", instance_name, e)

    def cleanup(self, handle: RunHandle) -> None:
        """Clean up resources for a terminated instance.

        For GCE, this is a no-op after terminate() since delete removes all resources.

        Args:
            handle: Handle to the instance.
        """
        # GCE instances are fully cleaned up by delete
        # No additional cleanup needed
        _ = handle  # Acknowledge parameter for protocol compliance
        pass
