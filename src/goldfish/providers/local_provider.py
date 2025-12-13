"""Local execution and storage provider implementations."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder
from goldfish.infra.local_executor import LocalExecutor
from goldfish.providers.base import (
    ExecutionProvider,
    ExecutionResult,
    ExecutionStatus,
    StorageLocation,
    StorageProvider,
)


class LocalExecutionProvider(ExecutionProvider):
    """Execution provider for local Docker containers.

    Wraps existing LocalExecutor with provider interface.
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize local execution provider.

        Expected config keys:
            - work_dir: Optional working directory for container artifacts
        """
        super().__init__(config)

        self.work_dir = Path(config.get("work_dir", "/tmp/goldfish"))
        self.work_dir.mkdir(parents=True, exist_ok=True)

        # Initialize local executor and docker builder
        self.local_executor = LocalExecutor()
        self.docker_builder = DockerBuilder(config=None)

    def build_image(
        self,
        image_tag: str,
        dockerfile_path: Path,
        context_path: Path,
        base_image: str | None = None,
    ) -> str:
        """Build Docker image locally.

        Args:
            image_tag: Local image tag (format: goldfish-{workspace}-{version})
            dockerfile_path: Path to Dockerfile (unused, we generate)
            context_path: Workspace directory path
            base_image: Optional base image override

        Returns:
            Local image tag
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
        return self.docker_builder.build_image(
            workspace_dir=context_path,
            workspace_name=workspace_name,
            version=version,
            use_cache=True,
            base_image=base_image,
        )

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
        """Launch stage execution in local Docker container.

        Args:
            image_tag: Container image to run
            stage_run_id: Unique stage run identifier
            entrypoint_script: Shell script to execute
            stage_config: Stage configuration dict
            work_dir: Working directory for execution artifacts
            inputs_dir: Directory containing input data
            outputs_dir: Directory for output data
            machine_type: Ignored for local execution
            gpu_type: Ignored for local execution (Docker GPU support is complex)
            gpu_count: Ignored for local execution
            profile_hints: Additional hints (ignored)

        Returns:
            ExecutionResult with container ID
        """
        # Launch container
        container_id = self.local_executor.launch_container(
            image_tag=image_tag,
            stage_run_id=stage_run_id,
            entrypoint_script=entrypoint_script,
            stage_config=stage_config,
            work_dir=work_dir,
            inputs_dir=inputs_dir,
            outputs_dir=outputs_dir,
        )

        return ExecutionResult(
            instance_id=container_id,
            metadata={
                "backend": "local",
                "image_tag": image_tag,
            },
            hyperlink=None,
        )

    def get_status(self, instance_id: str) -> ExecutionStatus:
        """Get container status.

        Args:
            instance_id: Container ID

        Returns:
            ExecutionStatus with current state
        """
        status = self.local_executor.get_container_status(instance_id)

        # Map Docker status to standard states
        state_map = {
            "running": "running",
            "exited": "succeeded",  # Check exit code below
            "dead": "failed",
            "created": "running",
            "restarting": "running",
            "paused": "running",
            "removing": "running",
        }

        state = state_map.get(status, "unknown")

        # Get exit code if container has exited
        exit_code = None
        if status == "exited":
            # Get exit code from docker inspect
            try:
                import json

                result = subprocess.run(
                    ["docker", "inspect", instance_id],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode == 0:
                    inspect_data = json.loads(result.stdout)
                    if inspect_data:
                        exit_code = inspect_data[0]["State"].get("ExitCode", 1)
                        if exit_code != 0:
                            state = "failed"
            except Exception:
                # If we can't get exit code, assume failure for exited containers
                state = "failed"

        return ExecutionStatus(
            state=state,
            exit_code=exit_code,
            message=f"Container status: {status}",
            metadata={"docker_status": status},
        )

    def get_logs(self, instance_id: str, tail: int | None = None) -> str:
        """Get container logs.

        Args:
            instance_id: Container ID
            tail: Optional number of lines to return from end

        Returns:
            Log output
        """
        tail_lines = tail if tail is not None else 200
        return self.local_executor.get_container_logs(instance_id, tail_lines=tail_lines)

    def cancel(self, instance_id: str) -> bool:
        """Stop and remove container.

        Args:
            instance_id: Container ID

        Returns:
            True if cancelled
        """
        self.local_executor.stop_container(instance_id)
        return True


class LocalStorageProvider(StorageProvider):
    """Storage provider for local filesystem.

    Stores data in a local directory structure.
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize local storage provider.

        Expected config keys:
            - base_path: Base directory for storage (required)
            - datasets_prefix: Subdirectory for datasets (default: "datasets")
            - artifacts_prefix: Subdirectory for artifacts (default: "artifacts")
            - snapshots_prefix: Subdirectory for snapshots (default: "snapshots")
        """
        super().__init__(config)

        # Validate config is dict
        if not isinstance(config, dict):
            raise GoldfishError(f"Local storage provider config must be dict, got {type(config).__name__}")

        # Validate and extract base_path
        base_path = config.get("base_path")
        if not base_path:
            raise GoldfishError("Local storage provider requires 'base_path' configuration")
        if not isinstance(base_path, str):
            raise GoldfishError(f"Local storage provider 'base_path' must be string, got {type(base_path).__name__}")

        self.base_path = Path(base_path)

        # Validate and extract prefix fields
        self.datasets_prefix = config.get("datasets_prefix", "datasets")
        if not isinstance(self.datasets_prefix, str):
            raise GoldfishError(
                f"Local storage provider 'datasets_prefix' must be string, got {type(self.datasets_prefix).__name__}"
            )

        self.artifacts_prefix = config.get("artifacts_prefix", "artifacts")
        if not isinstance(self.artifacts_prefix, str):
            raise GoldfishError(
                f"Local storage provider 'artifacts_prefix' must be string, got {type(self.artifacts_prefix).__name__}"
            )

        self.snapshots_prefix = config.get("snapshots_prefix", "snapshots")
        if not isinstance(self.snapshots_prefix, str):
            raise GoldfishError(
                f"Local storage provider 'snapshots_prefix' must be string, got {type(self.snapshots_prefix).__name__}"
            )

        # Create base directories
        self.base_path.mkdir(parents=True, exist_ok=True)
        (self.base_path / self.datasets_prefix).mkdir(exist_ok=True)
        (self.base_path / self.artifacts_prefix).mkdir(exist_ok=True)
        (self.base_path / self.snapshots_prefix).mkdir(exist_ok=True)

    def _normalize_remote_path(self, remote_path: str) -> Path:
        """Convert remote_path to local filesystem path.

        Args:
            remote_path: Either dataset name or file:// URI

        Returns:
            Local Path object
        """
        if remote_path.startswith("file://"):
            # Direct file path
            return Path(remote_path[7:])

        # Treat as dataset name
        result_path: Path = self.base_path / self.datasets_prefix / remote_path
        return result_path

    def upload(
        self,
        local_path: Path,
        remote_path: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation:
        """Copy local file or directory to storage.

        Args:
            local_path: Local file or directory path
            remote_path: Remote path (dataset name or file:// URI)
            metadata: Optional metadata (stored as .metadata.json)

        Returns:
            StorageLocation with file:// URI
        """
        if not local_path.exists():
            raise GoldfishError(f"Local path not found: {local_path}")

        dest_path = self._normalize_remote_path(remote_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if local_path.is_dir():
                # Copy directory (dirs_exist_ok prevents race condition)
                if dest_path.exists():
                    shutil.rmtree(dest_path)
                shutil.copytree(local_path, dest_path, dirs_exist_ok=True)
            else:
                # Copy file
                shutil.copy2(local_path, dest_path)

            # Store metadata if provided
            if metadata:
                import json

                metadata_path = dest_path.parent / f"{dest_path.name}.metadata.json"
                metadata_path.write_text(json.dumps(metadata, indent=2))

            # Get size
            size_bytes = None
            if dest_path.is_file():
                size_bytes = dest_path.stat().st_size

            return StorageLocation(
                uri=f"file://{dest_path}",
                size_bytes=size_bytes,
                metadata=metadata,
                hyperlink=None,
            )

        except (OSError, PermissionError) as e:
            raise GoldfishError(f"Failed to copy to local storage: {e}") from e

    def download(
        self,
        remote_path: str,
        local_path: Path,
    ) -> Path:
        """Copy from storage to local filesystem.

        Args:
            remote_path: Remote path (dataset name or file:// URI)
            local_path: Local destination path

        Returns:
            Path to downloaded file/directory
        """
        source_path = self._normalize_remote_path(remote_path)

        if not source_path.exists():
            raise GoldfishError(f"Storage path not found: {source_path}")

        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if source_path.is_dir():
                if local_path.exists():
                    shutil.rmtree(local_path)
                shutil.copytree(source_path, local_path)
            else:
                shutil.copy2(source_path, local_path)

            return local_path

        except (OSError, PermissionError) as e:
            raise GoldfishError(f"Failed to copy from local storage: {e}") from e

    def exists(self, remote_path: str) -> bool:
        """Check if path exists in storage.

        Args:
            remote_path: Remote path (dataset name or file:// URI)

        Returns:
            True if exists
        """
        source_path = self._normalize_remote_path(remote_path)
        return source_path.exists()

    def get_size(self, remote_path: str) -> int | None:
        """Get size of stored object.

        Args:
            remote_path: Remote path (dataset name or file:// URI)

        Returns:
            Size in bytes, or None if directory
        """
        source_path = self._normalize_remote_path(remote_path)

        if not source_path.exists():
            return None

        if source_path.is_file():
            return source_path.stat().st_size

        # For directories, sum all files
        total_size = 0
        for item in source_path.rglob("*"):
            if item.is_file():
                total_size += item.stat().st_size
        return total_size

    def snapshot(
        self,
        remote_path: str,
        snapshot_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation:
        """Create a snapshot by copying to snapshots directory.

        Args:
            remote_path: Remote path to snapshot
            snapshot_id: Identifier for snapshot
            metadata: Optional metadata

        Returns:
            StorageLocation for snapshot
        """
        source_path = self._normalize_remote_path(remote_path)
        snapshot_path = self.base_path / self.snapshots_prefix / snapshot_id

        if not source_path.exists():
            raise GoldfishError(f"Source path not found: {source_path}")

        snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if source_path.is_dir():
                # dirs_exist_ok prevents race condition
                if snapshot_path.exists():
                    shutil.rmtree(snapshot_path)
                shutil.copytree(source_path, snapshot_path, dirs_exist_ok=True)
            else:
                shutil.copy2(source_path, snapshot_path)

            # Store metadata if provided
            if metadata:
                import json

                metadata_path = snapshot_path.parent / f"{snapshot_path.name}.metadata.json"
                metadata_path.write_text(json.dumps(metadata, indent=2))

            return StorageLocation(
                uri=f"file://{snapshot_path}",
                size_bytes=self.get_size(f"file://{snapshot_path}"),
                metadata=metadata,
                hyperlink=None,
            )

        except (OSError, PermissionError) as e:
            raise GoldfishError(f"Failed to create snapshot: {e}") from e
