"""GCE execution provider implementation."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder
from goldfish.infra.gce_launcher import GCELauncher
from goldfish.providers.base import ExecutionProvider, ExecutionResult, ExecutionStatus


class GCEExecutionProvider(ExecutionProvider):
    """Execution provider for Google Compute Engine.

    Wraps existing GCELauncher and DockerBuilder with provider interface.
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize GCE execution provider.

        Expected config keys:
            - project_id: GCP project ID (optional, uses default if not set)
            - zone: Default GCE zone (default: "us-central1-a")
            - zones: List of all available zones for capacity search
            - bucket: GCS bucket for artifacts (required)
            - artifact_registry: Optional Artifact Registry URL
            - gpu_preference: List of GPU types in preference order
            - resources: Resource catalog for capacity search
            - service_account: Optional service account email
        """
        super().__init__(config)

        # Validate config is dict
        if not isinstance(config, dict):
            raise GoldfishError(f"GCE provider config must be dict, got {type(config).__name__}")

        # Extract and validate GCE configuration
        self.bucket = config.get("bucket")
        if not self.bucket:
            raise GoldfishError("GCE provider requires 'bucket' configuration")
        if not isinstance(self.bucket, str):
            raise GoldfishError(f"GCE provider 'bucket' must be string, got {type(self.bucket).__name__}")

        self.project_id = config.get("project_id")
        if self.project_id is not None and not isinstance(self.project_id, str):
            raise GoldfishError(f"GCE provider 'project_id' must be string, got {type(self.project_id).__name__}")

        self.zone = config.get("zone", "us-central1-a")
        if not isinstance(self.zone, str):
            raise GoldfishError(f"GCE provider 'zone' must be string, got {type(self.zone).__name__}")

        self.zones = config.get("zones", [self.zone])
        if not isinstance(self.zones, list):
            raise GoldfishError(f"GCE provider 'zones' must be list, got {type(self.zones).__name__}")

        self.artifact_registry = config.get("artifact_registry")
        if self.artifact_registry is not None and not isinstance(self.artifact_registry, str):
            raise GoldfishError(
                f"GCE provider 'artifact_registry' must be string, got {type(self.artifact_registry).__name__}"
            )

        self.gpu_preference = config.get("gpu_preference", ["h100", "a100", "none"])
        if not isinstance(self.gpu_preference, list):
            raise GoldfishError(f"GCE provider 'gpu_preference' must be list, got {type(self.gpu_preference).__name__}")

        self.resources = config.get("resources", [])
        if not isinstance(self.resources, list):
            raise GoldfishError(f"GCE provider 'resources' must be list, got {type(self.resources).__name__}")

        self.service_account = config.get("service_account")
        if self.service_account is not None and not isinstance(self.service_account, str):
            raise GoldfishError(
                f"GCE provider 'service_account' must be string, got {type(self.service_account).__name__}"
            )

        # Initialize GCE launcher
        self.gce_launcher = GCELauncher(
            project_id=self.project_id,
            zone=self.zone,
            bucket=self.bucket,
            resources=self.resources,
            zones=self.zones,
            gpu_preference=self.gpu_preference,
        )

        # Initialize Docker builder
        self.docker_builder = DockerBuilder(config=None)

    def build_image(
        self,
        image_tag: str,
        dockerfile_path: Path,
        context_path: Path,
        base_image: str | None = None,
    ) -> str:
        """Build Docker image and push to Artifact Registry if configured.

        Args:
            image_tag: Local image tag (format: goldfish-{workspace}-{version})
            dockerfile_path: Path to Dockerfile (unused, we generate)
            context_path: Workspace directory path
            base_image: Optional base image override

        Returns:
            Image tag (with registry prefix if pushed)
        """
        import re

        # Parse image tag using regex for robustness
        # Format: goldfish-{workspace}-{version}
        # Workspace can contain hyphens, version is everything after last hyphen
        match = re.match(r"^goldfish-(.+?)-([^-]+)$", image_tag)
        if not match:
            raise GoldfishError(f"Invalid image tag format: {image_tag}. Expected 'goldfish-{{workspace}}-{{version}}'")

        workspace_name = match.group(1)
        version = match.group(2)

        # Build image locally
        local_tag = self.docker_builder.build_image(
            workspace_dir=context_path,
            workspace_name=workspace_name,
            version=version,
            use_cache=True,
            base_image=base_image,
        )

        # If artifact registry configured, push to registry
        if self.artifact_registry:
            remote_tag = f"{self.artifact_registry}/{workspace_name}:{version}"

            # Tag for registry
            try:
                subprocess.run(
                    ["docker", "tag", local_tag, remote_tag],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                stderr = e.stderr if e.stderr else "Unknown error"
                raise GoldfishError(f"Failed to tag image for registry: {stderr}") from e

            # Push to registry
            try:
                subprocess.run(
                    ["docker", "push", remote_tag],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return remote_tag
            except subprocess.CalledProcessError as e:
                stderr = e.stderr if e.stderr else "Unknown error"
                raise GoldfishError(f"Failed to push image to registry: {stderr}") from e

        return local_tag

    def launch_stage(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict[str, Any],
        work_dir: Path,
        inputs_dir: Path | None = None,
        outputs_dir: Path | None = None,
        machine_type: str | None = None,
        gpu_type: str | None = None,
        gpu_count: int = 0,
        profile_hints: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Launch stage execution on GCE.

        Args:
            image_tag: Container image to run
            stage_run_id: Unique stage run identifier
            entrypoint_script: Shell script to execute
            stage_config: Stage configuration dict
            work_dir: Working directory for execution artifacts
            inputs_dir: Directory containing input data
            outputs_dir: Directory for output data
            machine_type: Machine type (e.g., "n1-standard-4")
            gpu_type: GPU accelerator type (e.g., "nvidia-tesla-a100")
            gpu_count: Number of GPUs
            profile_hints: Additional hints (zones, use_capacity_search, etc.)

        Returns:
            ExecutionResult with instance name and metadata
        """
        hints = profile_hints or {}

        # Extract zone hints
        zones = hints.get("zones", self.zones)
        use_capacity_search = hints.get("use_capacity_search", True)

        # Launch instance
        instance_name = self.gce_launcher.launch_instance(
            image_tag=image_tag,
            stage_run_id=stage_run_id,
            entrypoint_script=entrypoint_script,
            stage_config=stage_config,
            work_dir=work_dir,
            inputs_dir=inputs_dir,
            outputs_dir=outputs_dir,
            machine_type=machine_type or "n1-standard-4",
            gpu_type=gpu_type,
            gpu_count=gpu_count,
            zones=zones,
            use_capacity_search=use_capacity_search,
        )

        # Generate console hyperlink
        hyperlink = None
        if self.project_id and self.zone:
            hyperlink = (
                f"https://console.cloud.google.com/compute/instances/zones/"
                f"{self.zone}/instances/{instance_name}?project={self.project_id}"
            )

        return ExecutionResult(
            instance_id=instance_name,
            metadata={
                "project_id": self.project_id,
                "zone": self.zone,
                "machine_type": machine_type,
                "gpu_type": gpu_type,
                "gpu_count": gpu_count,
            },
            hyperlink=hyperlink,
        )

    def get_status(self, instance_id: str) -> ExecutionStatus:
        """Get GCE instance status.

        Args:
            instance_id: Instance name

        Returns:
            ExecutionStatus with current state
        """
        status = self.gce_launcher.get_instance_status(instance_id)

        # Map GCE status to standard states
        state_map = {
            "RUNNING": "running",
            "TERMINATED": "succeeded",  # Default to succeeded
            "STOPPING": "running",
            "PROVISIONING": "running",
            "STAGING": "running",
            "SUSPENDED": "failed",
        }

        state = state_map.get(status, "unknown")

        # For terminated instances, check if it was successful
        # GCE doesn't provide exit codes, so we rely on other signals
        exit_code = None
        message = f"Instance status: {status}"

        if status == "TERMINATED":
            # Check if instance terminated successfully by looking at logs/metadata
            # For now, default to succeeded; StageExecutor will verify outputs
            exit_code = 0

        return ExecutionStatus(
            state=state,
            exit_code=exit_code,
            message=message,
            metadata={"gce_status": status},
        )

    def get_logs(self, instance_id: str, tail: int | None = None) -> str:
        """Get instance logs from GCS.

        Args:
            instance_id: Instance name (used as stage_run_id)
            tail: Optional number of lines to return from end

        Returns:
            Log output
        """
        return self.gce_launcher.get_instance_logs(instance_id, tail_lines=tail)

    def cancel(self, instance_id: str) -> bool:
        """Cancel (delete) a GCE instance.

        Args:
            instance_id: Instance name

        Returns:
            True if cancelled
        """
        self.gce_launcher.delete_instance(instance_id)
        return True

    def supports_volumes(self) -> bool:
        """GCE supports hyperdisk volumes.

        Returns:
            True
        """
        return True

    def provision_volume(
        self,
        volume_id: str,
        size_gb: int,
        region: str | None = None,
    ) -> Any:
        """Provision a hyperdisk volume.

        Args:
            volume_id: Disk name
            size_gb: Size in gigabytes
            region: Target region (unused, uses zone)

        Returns:
            VolumeInfo with volume details
        """
        # GCE uses zones, not regions
        # For now, create disk in default zone
        # This could be extended to use region parameter

        from goldfish.providers.base import VolumeInfo

        try:
            subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "disks",
                    "create",
                    volume_id,
                    f"--size={size_gb}GB",
                    f"--zone={self.zone}",
                    "--type=hyperdisk-balanced",
                ]
                + ([f"--project={self.project_id}"] if self.project_id else []),
                check=True,
                capture_output=True,
            )

            return VolumeInfo(
                volume_id=volume_id,
                region=self.zone,
                size_gb=size_gb,
                metadata={"type": "hyperdisk-balanced"},
            )

        except subprocess.CalledProcessError as e:
            raise GoldfishError(f"Failed to provision volume: {e.stderr.decode()}") from e
