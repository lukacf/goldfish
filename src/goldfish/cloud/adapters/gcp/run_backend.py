"""GCE implementation of RunBackend protocol.

Wraps the existing GCELauncher to implement the RunBackend protocol.
All GCE-specific code is contained in this adapter.
"""

from __future__ import annotations

import logging
import shlex
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
from goldfish.state_machine.types import StageState
from goldfish.validation import ValidationError

if TYPE_CHECKING:
    from goldfish.cloud.adapters.gcp.gce_launcher import GCELauncher
    from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager

logger = logging.getLogger(__name__)


# Default capabilities for GCE backend - used when backend instance not available
# These values match the defaults returned by GCERunBackend.capabilities property.
GCE_DEFAULT_CAPABILITIES = BackendCapabilities(
    supports_gpu=True,
    supports_spot=True,
    supports_preemption=True,
    supports_preemption_detection=True,
    supports_live_logs=True,
    supports_metrics=True,
    max_run_duration_hours=24,
    ack_timeout_seconds=3.0,
    ack_timeout_running_seconds=4.0,
    has_launch_delay=True,
    logs_unavailable_message="Logs not yet synced from instance",
    timeout_becomes_pending=True,
    status_message_for_preparing="Instance provisioning...",
    zone_resolution_method="handle",
)


class GCERunBackend:
    """GCE implementation of RunBackend protocol.

    Wraps GCELauncher to provide a protocol-compatible interface.
    This adapter enables using GCE compute through the cloud abstraction layer.
    """

    _launcher: GCELauncher
    _warm_pool: WarmPoolManager | None

    def __init__(
        self,
        project_id: str | None = None,
        zones: list[str] | None = None,
        bucket: str | None = None,
        gpu_preference: list[str] | None = None,
        service_account: str | None = None,
        warm_pool: WarmPoolManager | None = None,
    ) -> None:
        """Initialize GCE backend.

        Args:
            project_id: GCP project ID (uses default if None)
            zones: List of zones for capacity search (also used as global_zones for profiles)
            bucket: GCS bucket for logs/artifacts
            gpu_preference: Ordered list of preferred GPU types
            service_account: Service account email for instances
        """
        default_zone = zones[0] if zones else "us-central1-a"

        # Build resources from profiles for capacity search.
        # Resources are profile dicts containing machine_type, zones, gpu config.
        # GCELauncher uses these for multi-zone capacity search.
        resources = self._build_resources_from_profiles(global_zones=zones)

        # Import here to keep adapter imports lazy (gce_launcher has server-side deps)
        from goldfish.cloud.adapters.gcp.gce_launcher import GCELauncher

        self._launcher = GCELauncher(
            project_id=project_id,
            zone=default_zone,
            bucket=bucket,
            zones=zones,
            gpu_preference=gpu_preference,
            service_account=service_account,
            resources=resources,
        )
        self._project_id = project_id
        self._zones = zones or [default_zone]
        self._bucket = bucket
        self._warm_pool = warm_pool

    def _build_resources_from_profiles(self, global_zones: list[str] | None = None) -> list[dict]:
        """Build resources list from profile definitions.

        Resources are used by GCELauncher for capacity search across zones.
        Each resource contains machine_type, zones, gpu config needed for launch.

        Args:
            global_zones: Optional zones override to apply to all profiles.

        Returns:
            List of profile dicts ready for GCELauncher.
        """
        from goldfish.cloud.adapters.gcp.profiles import ProfileResolver

        resolver = ProfileResolver(global_zones=global_zones)
        resources: list[dict] = []

        for profile_name in resolver.list_profiles():
            try:
                profile = resolver.resolve(profile_name)
                resources.append(profile)
            except Exception as e:
                logger.warning("Failed to resolve profile '%s': %s", profile_name, e)

        return resources

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
            # GCE backend sync behavior - network latency, async log sync
            ack_timeout_seconds=3.0,  # GCE needs longer timeouts due to network latency
            ack_timeout_running_seconds=4.0,
            has_launch_delay=True,  # PROVISIONING/STAGING states before RUNNING
            logs_unavailable_message="Logs not yet synced from instance",
            timeout_becomes_pending=True,  # ACK timeout means "sync pending", not failure
            status_message_for_preparing="Instance provisioning...",
            zone_resolution_method="handle",  # Zone comes from handle.zone
        )

    def launch(self, spec: RunSpec) -> RunHandle:
        """Launch a GCE instance for the given spec.

        Args:
            spec: Run specification with image, command, resources, etc.

        Returns:
            Handle to the launched instance.

        Raises:
            ValidationError: If any input has non-GCS (non-gs://) scheme.
            CapacityError: If no capacity available in any zone.
            LaunchError: If instance launch fails.
        """
        # Validate input schemes - GCE only supports GCS (gs://) inputs
        # GCELauncher silently skips non-GCS inputs during staging, causing
        # confusing runtime failures when inputs are missing.
        for input_name, uri in spec.inputs.items():
            if uri.scheme != "gs":
                raise ValidationError(
                    f"GCE backend only supports GCS (gs://) inputs. "
                    f"Input '{input_name}' has scheme '{uri.scheme}://'",
                    value=str(uri),
                    field=input_name,
                )

        try:
            # Serialize StorageURIs to strings for GCELauncher
            # GCELauncher expects inputs as strings (gs://...), not StorageURI objects
            serialized_inputs = {name: str(uri) for name, uri in spec.inputs.items()}

            # Build stage config from spec
            stage_config = {
                "inputs": serialized_inputs,
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

            # Use dummy work_dir - GCELauncher doesn't actually use it
            # (all inputs/outputs are handled via GCS)
            work_dir = Path("/tmp")

            # Build entrypoint from command
            # GCE writes the script to /mnt/entrypoint.sh and runs via /bin/bash.
            # If command is ["sh", "-c", script], extract just the script to avoid
            # double-wrapping (bash running sh -c '...' which fails on pipefail).
            if spec.command and len(spec.command) == 3 and spec.command[:2] == ["sh", "-c"]:
                entrypoint_script = spec.command[2]
            elif spec.command:
                entrypoint_script = shlex.join(spec.command)
            else:
                entrypoint_script = "echo 'No command'"

            # Determine machine type and GPU from spec
            # CRITICAL: Use machine_type from profile (via RunSpec) for correct GPU/machine pairing.
            # H100 GPUs require A3 machines (a3-highgpu-1g), A100s require A2 (a2-highgpu-1g).
            # Hardcoding n1-standard-X causes GPU launches to fail.
            machine_type = spec.machine_type
            if not machine_type:
                # Fallback: no profile specified, use GCE default
                machine_type = "n1-standard-4"
            gpu_type = None
            gpu_count = 0

            if spec.gpu_count and spec.gpu_count > 0:
                gpu_count = spec.gpu_count
                # Use spec.gpu_type if provided, else default to T4
                gpu_type = spec.gpu_type or "nvidia-tesla-t4"

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
                # Propagate GCS error info in message for visibility
                if exit_result.gcs_error:
                    return BackendStatus(
                        status=RunStatus.FAILED,
                        exit_code=1,
                        message=f"GCS error retrieving exit code: {exit_result.error}",
                    )
                if exit_result.error:
                    return BackendStatus(
                        status=RunStatus.FAILED,
                        exit_code=1,
                        message=f"Exit code retrieval error: {exit_result.error}",
                    )
                return BackendStatus(status=RunStatus.FAILED, exit_code=1)
            elif status_str == "not_found":
                # Instance is gone - try to recover exit code from GCS
                # This handles spot preemption where instance disappears but wrote exit code
                exit_result = self._launcher._get_exit_code(instance_name)
                if exit_result.exists and exit_result.code is not None:
                    # Exit code found - instance ran and terminated
                    return BackendStatus.from_exit_code(
                        exit_result.code,
                        termination_cause="preemption" if exit_result.code != 0 else None,
                    )
                # GCS error - can't determine if exit code exists, return UNKNOWN
                if exit_result.gcs_error:
                    logger.warning(
                        "Instance %s not found and GCS error reading exit code: %s",
                        instance_name,
                        exit_result.error,
                    )
                    return BackendStatus(
                        status=RunStatus.UNKNOWN,
                        message=f"Instance not found; GCS error: {exit_result.error}",
                    )
                # No exit code found - truly not found
                raise NotFoundError(f"instance:{instance_name}")
            else:
                # Unknown status - treat as running
                return BackendStatus(status=RunStatus.RUNNING)

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"instance:{instance_name}") from e
            # For other errors, return unknown status (not RUNNING to avoid stuck runs)
            logger.warning("Error getting status for %s: %s", instance_name, e)
            return BackendStatus(status=RunStatus.UNKNOWN, message=str(e))

    def get_logs(self, handle: RunHandle, tail: int = 200, since: str | None = None) -> str:
        """Get logs from a GCE instance.

        Args:
            handle: Handle to the instance.
            tail: Number of lines from end to return.
            since: Only return logs after this ISO timestamp.

        Returns:
            Log content as string.
        """
        instance_name = handle.backend_handle

        try:
            return self._launcher.get_instance_logs(
                instance_name=instance_name,
                tail_lines=tail if tail > 0 else None,
                since=since,
            )
        except Exception as e:
            logger.warning("Error getting logs for %s: %s", instance_name, e)
            return f"[Error fetching logs: {e}]"

    def terminate(self, handle: RunHandle) -> None:
        """Terminate a GCE instance.

        Sends termination signal. Instance will be deleted.
        Delegates to GCELauncher.delete_instance which handles:
        - Zone lookup via _find_instance_zone() if needed
        - Idempotency (no error if already deleted)
        - Project ID configuration
        - Proper logging

        Args:
            handle: Handle to the instance.
        """
        instance_name = handle.backend_handle
        self._launcher.delete_instance(instance_name)

    def cleanup(self, handle: RunHandle) -> None:
        """Clean up resources for a terminated instance.

        For warm pool instances, release back to idle instead of deleting.
        For regular instances, this is a no-op (terminate() already deleted).

        Args:
            handle: Handle to the instance.
        """
        if handle.warm_instance and self._warm_pool:
            # Release warm instance back to pool instead of deleting
            self._warm_pool.release_instance(handle.backend_handle)
            logger.info("Released warm instance %s back to pool", handle.backend_handle)
        # Regular instances: no additional cleanup needed (terminate() handles deletion)

    def get_zone(self, handle: RunHandle) -> str | None:
        """Get the zone for a GCE instance.

        Args:
            handle: Handle to the instance.

        Returns:
            Zone string if known, None otherwise.
        """
        # Prefer zone from handle if available
        if handle.zone:
            return handle.zone

        # Fall back to launcher lookup
        try:
            return self._launcher._find_instance_zone(handle.backend_handle)
        except Exception:
            return None

    def get_output_dir(self, handle: RunHandle) -> Path | None:
        """Get the output directory for a GCE instance.

        GCE outputs go to GCS, not local paths. This always returns None.

        Args:
            handle: Handle to the instance.

        Returns:
            None (GCE uses GCS for outputs).
        """
        _ = handle  # Acknowledge parameter for protocol compliance
        return None
