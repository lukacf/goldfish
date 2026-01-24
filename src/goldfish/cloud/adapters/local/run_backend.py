"""Local Docker implementation of RunBackend protocol.

Runs stages in local Docker containers for development and testing.
Supports configurable simulation controls per LOCAL_PARITY_SPEC.
"""

from __future__ import annotations

import subprocess
import tempfile
import threading
import time
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
from goldfish.validation import (
    validate_docker_image,
    validate_env_key,
    validate_env_value,
    validate_signal_name,
    validate_stage_run_id,
)

if TYPE_CHECKING:
    from goldfish.cloud.adapters.local.storage import LocalObjectStorage
    from goldfish.config import LocalComputeConfig


class LocalRunBackend:
    """Local Docker backend for running stages.

    Uses Docker CLI to manage container lifecycle.
    Supports input/output mounting when storage is provided.
    Supports preemption simulation and zone availability per LOCAL_PARITY_SPEC.
    """

    def __init__(
        self,
        storage: LocalObjectStorage | None = None,
        config: LocalComputeConfig | None = None,
    ) -> None:
        """Initialize local Docker backend.

        Args:
            storage: Optional storage instance for resolving input URIs to local paths.
                    When provided, inputs are mounted as volumes.
            config: Optional compute config for simulation controls.
        """
        self._containers: dict[str, str] = {}  # stage_run_id -> container_id
        self._output_dirs: dict[str, Path] = {}  # stage_run_id -> temp output dir
        self._storage = storage
        self._preemption_timers: dict[str, threading.Timer] = {}
        self._preempted_containers: set[str] = set()  # container_ids preempted by simulation

        # Config-driven simulation controls
        self._docker_socket = config.docker_socket if config else "/var/run/docker.sock"
        self._preemption_after_seconds = config.simulate_preemption_after_seconds if config else None
        self._preemption_grace_period = config.preemption_grace_period_seconds if config else 30
        self._zone_availability = config.zone_availability if config else {"local-zone-1": True}

    @property
    def capabilities(self) -> BackendCapabilities:
        """Return backend capabilities.

        Local backend supports:
        - GPU if nvidia-docker runtime is available
        - Preemption via SIGTERM (always supported)
        - Preemption detection when simulation is configured
        - Live logs via docker logs
        """
        return BackendCapabilities(
            supports_gpu=self._check_nvidia_runtime(),
            supports_spot=False,  # Simulated only, not real spot pricing
            supports_preemption=True,  # Always supports SIGTERM graceful shutdown
            supports_preemption_detection=self._preemption_after_seconds is not None,
            supports_live_logs=True,
            supports_metrics=False,
            max_run_duration_hours=None,
            # Sync behavior - local backend is synchronous
            ack_timeout_seconds=1.0,
            ack_timeout_running_seconds=1.0,
            has_launch_delay=False,
            logs_unavailable_message="Logs not available",
            timeout_becomes_pending=False,
            status_message_for_preparing="Starting container...",
            zone_resolution_method="config",  # Local uses config-defined zones
        )

    def _check_nvidia_runtime(self) -> bool:
        """Check if nvidia-docker runtime is available."""
        try:
            result = subprocess.run(
                ["docker", "info", "--format", "{{.Runtimes}}"],
                capture_output=True,
                text=True,
                check=False,
            )
            return "nvidia" in result.stdout.lower()
        except Exception:
            return False

    def _preempt_container(self, handle: RunHandle) -> None:
        """Simulate GCE spot preemption by sending SIGTERM to container.

        Called by preemption timer after configured delay.
        Mimics GCE behavior: SIGTERM, wait grace period, then SIGKILL.
        """
        container_id = handle.backend_handle

        # Mark as preempted so get_status() returns correct termination cause
        self._preempted_containers.add(container_id)

        # Send SIGTERM (graceful shutdown signal)
        try:
            subprocess.run(
                ["docker", "kill", "--signal=SIGTERM", container_id],
                capture_output=True,
                check=False,  # Don't raise if already stopped
            )
        except Exception:
            return  # Container may already be gone

        # Wait grace period, then send SIGKILL if still running
        time.sleep(self._preemption_grace_period)

        try:
            # Check if container is still running
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Running}}", container_id],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip() == "true":
                # Container still running after grace period - force kill
                subprocess.run(
                    ["docker", "kill", "--signal=SIGKILL", container_id],
                    capture_output=True,
                    check=False,
                )
        except Exception:
            pass  # Container may have exited during grace period

    def launch(self, spec: RunSpec) -> RunHandle:
        """Launch a container for the given spec.

        Args:
            spec: Run specification with image, command, etc.

        Returns:
            Handle to the launched run.

        Raises:
            LaunchError: If container launch fails.
            CapacityError: If requested zone is unavailable.
        """
        # Validate all inputs before subprocess calls (security: prevent injection)
        validate_stage_run_id(spec.stage_run_id)
        validate_docker_image(spec.image)
        for key, value in spec.env.items():
            validate_env_key(key)
            validate_env_value(key, value)
        for signal_name in spec.inputs.keys():
            validate_signal_name(signal_name)

        # Check zone availability (simulate GCE zone capacity exhaustion)
        # Try zones in order until one is available (matches GCE multi-zone behavior)
        selected_zone = None
        zones_tried = []
        for zone, available in self._zone_availability.items():
            zones_tried.append(zone)
            if available:
                selected_zone = zone
                break

        if selected_zone is None:
            raise CapacityError(
                "No zones available for scheduling",
                zones_tried=zones_tried,
            )

        # Build docker run command
        cmd = [
            "docker",
            "run",
            "-d",  # detached
            "--name",
            f"goldfish-{spec.stage_run_id}",
        ]

        # Add resource limits from spec
        memory_gb = spec.memory_gb or 4.0
        cpu_count = spec.cpu_count or 2.0
        cmd.extend(["--memory", f"{memory_gb}g", "--cpus", str(cpu_count)])

        # Security hardening (per CLAUDE.md security model)
        cmd.extend(
            [
                "--pids-limit",
                "100",  # Prevent fork bombs
                "--user",
                "1000:1000",  # Run as non-root
            ]
        )

        # Add GPU support if requested and available
        if spec.gpu_count and spec.gpu_count > 0 and self._check_nvidia_runtime():
            cmd.extend(["--gpus", "all"])  # Use all available GPUs

        # Add environment variables
        for key, value in spec.env.items():
            cmd.extend(["-e", f"{key}={value}"])

        # Add timeout as environment variable if specified
        if spec.timeout_seconds:
            cmd.extend(["-e", f"GOLDFISH_TIMEOUT={spec.timeout_seconds}"])

        # Mount inputs if storage is available and inputs are specified
        if self._storage and spec.inputs:
            for signal_name, input_uri in spec.inputs.items():
                local_path = self._storage.get_local_path(input_uri)
                if local_path:
                    # Mount input as read-only at /mnt/inputs/{signal_name}
                    cmd.extend(["-v", f"{local_path}:/mnt/inputs/{signal_name}:ro"])
                    cmd.extend(["-e", f"GOLDFISH_INPUT_{signal_name.upper()}=/mnt/inputs/{signal_name}"])

        # Create output directory and mount if output_uri is specified
        if spec.output_uri:
            output_dir = Path(tempfile.mkdtemp(prefix=f"goldfish-{spec.stage_run_id}-"))
            self._output_dirs[spec.stage_run_id] = output_dir
            # Mount output as read-write at /mnt/outputs
            cmd.extend(["-v", f"{output_dir}:/mnt/outputs:rw"])
            cmd.extend(["-e", "GOLDFISH_OUTPUT_DIR=/mnt/outputs"])

        # Add image
        cmd.append(spec.image)

        # Add command if specified
        if spec.command:
            cmd.extend(spec.command)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            container_id = result.stdout.strip()
            self._containers[spec.stage_run_id] = container_id

            handle = RunHandle(
                stage_run_id=spec.stage_run_id,
                backend_type="local",
                backend_handle=container_id,
                zone=selected_zone,
            )

            # Start preemption timer if configured (simulates GCE spot preemption)
            if self._preemption_after_seconds is not None:
                timer = threading.Timer(
                    self._preemption_after_seconds,
                    self._preempt_container,
                    args=[handle],
                )
                timer.daemon = True  # Don't block process exit
                timer.start()
                self._preemption_timers[spec.stage_run_id] = timer

            return handle
        except FileNotFoundError as e:
            raise LaunchError(
                f"Docker not found: {e}. Is Docker installed?",
                stage_run_id=spec.stage_run_id,
                cause="docker_not_found",
            ) from e
        except PermissionError as e:
            raise LaunchError(
                f"Permission denied accessing Docker: {e}. Check Docker socket permissions.",
                stage_run_id=spec.stage_run_id,
                cause="docker_permission_denied",
            ) from e
        except subprocess.CalledProcessError as e:
            raise LaunchError(
                f"Failed to launch container: {e.stderr}",
                stage_run_id=spec.stage_run_id,
                cause="docker_error",
            ) from e

    def get_status(self, handle: RunHandle) -> BackendStatus:
        """Get current status of a run.

        Args:
            handle: Handle to the run.

        Returns:
            Current backend status.

        Raises:
            NotFoundError: If the container no longer exists.
        """
        container_id = handle.backend_handle

        # Check if container exists and get its state
        cmd = [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}:{{.State.ExitCode}}",
            container_id,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            state_info = result.stdout.strip()
            state, exit_code_str = state_info.split(":")
            exit_code = int(exit_code_str)

            # Map Docker state to RunStatus (per LOCAL_PARITY_SPEC)
            if state == "running":
                return BackendStatus(status=RunStatus.RUNNING)
            elif state == "exited":
                # Check if this was a simulated preemption (not OOM)
                if container_id in self._preempted_containers:
                    return BackendStatus.from_exit_code(exit_code, termination_cause="preemption")
                return BackendStatus.from_exit_code(exit_code)
            elif state == "created":
                # Container created but not yet started -> PREPARING
                return BackendStatus(status=RunStatus.PREPARING)
            elif state in ("dead", "removing"):
                # Dead or being removed -> TERMINATED
                return BackendStatus(status=RunStatus.TERMINATED)
            elif state == "paused":
                # Paused is still considered running (can resume)
                return BackendStatus(status=RunStatus.RUNNING)
            else:
                # restarting or unknown -> RUNNING
                return BackendStatus(status=RunStatus.RUNNING)

        except subprocess.CalledProcessError:
            # Container doesn't exist - raise NotFoundError per protocol
            raise NotFoundError(f"container:{container_id}") from None

    def get_logs(self, handle: RunHandle, tail: int = 200, since: str | None = None) -> str:
        """Get logs from a run.

        Args:
            handle: Handle to the run.
            tail: Number of lines to return from the end. 0 means all logs.
            since: Only return logs after this timestamp (ISO format or duration).

        Returns:
            Log output as string.
        """
        container_id = handle.backend_handle

        cmd = ["docker", "logs"]

        # tail=0 means "all logs" per protocol, but docker --tail 0 returns NO logs
        # So we omit --tail entirely when tail=0
        if tail > 0:
            cmd.extend(["--tail", str(tail)])

        if since is not None:
            cmd.extend(["--since", since])

        cmd.append(container_id)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout + result.stderr
        except subprocess.CalledProcessError:
            return ""

    def get_zone(self, handle: RunHandle) -> str | None:
        """Get the zone where a run is executing.

        Args:
            handle: Handle to the run.

        Returns:
            Zone name, or None if not available.
        """
        return handle.zone

    def terminate(self, handle: RunHandle) -> None:
        """Terminate a running container.

        Args:
            handle: Handle to the run to terminate.
        """
        # Cancel any pending preemption timer
        if handle.stage_run_id in self._preemption_timers:
            self._preemption_timers[handle.stage_run_id].cancel()
            del self._preemption_timers[handle.stage_run_id]

        container_id = handle.backend_handle

        # Send SIGTERM, wait 10s, then SIGKILL
        cmd = ["docker", "stop", "-t", "10", container_id]

        try:
            subprocess.run(cmd, capture_output=True, check=True)
        except subprocess.CalledProcessError:
            pass  # Container may already be stopped

    def cleanup(self, handle: RunHandle) -> None:
        """Clean up resources for a terminated run.

        Args:
            handle: Handle to the run to clean up.
        """
        # Cancel any pending preemption timer
        if handle.stage_run_id in self._preemption_timers:
            self._preemption_timers[handle.stage_run_id].cancel()
            del self._preemption_timers[handle.stage_run_id]

        container_id = handle.backend_handle

        # Remove container
        cmd = ["docker", "rm", "-f", container_id]

        try:
            subprocess.run(cmd, capture_output=True, check=True)
        except subprocess.CalledProcessError:
            pass  # Container may already be removed

        # Remove from tracking
        if handle.stage_run_id in self._containers:
            del self._containers[handle.stage_run_id]

        # Clean up output directory (but preserve if caller wants to copy outputs)
        # Note: In production, outputs should be copied to storage before cleanup
        if handle.stage_run_id in self._output_dirs:
            del self._output_dirs[handle.stage_run_id]

    def get_output_dir(self, handle: RunHandle) -> Path | None:
        """Get the local output directory for a run.

        Use this to access outputs before calling cleanup().

        Args:
            handle: Handle to the run.

        Returns:
            Path to output directory, or None if no output was configured.
        """
        return self._output_dirs.get(handle.stage_run_id)

    def wait_for_status(
        self,
        handle: RunHandle,
        target_statuses: set[RunStatus],
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> BackendStatus:
        """Wait for run to reach one of the target statuses.

        Args:
            handle: Handle to the run.
            target_statuses: Set of statuses to wait for.
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between status checks.

        Returns:
            Final status when target reached or timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            status = self.get_status(handle)
            if status.status in target_statuses:
                return status
            time.sleep(poll_interval)

        return self.get_status(handle)
