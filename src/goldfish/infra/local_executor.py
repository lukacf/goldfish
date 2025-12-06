"""Local Docker container execution for Goldfish stages."""

import json
import subprocess
from pathlib import Path
from typing import Optional

from goldfish.errors import GoldfishError


class LocalExecutor:
    """Execute stage runs in Docker containers locally."""

    def launch_container(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict,
        work_dir: Path,
        inputs_dir: Optional[Path] = None,
        outputs_dir: Optional[Path] = None
    ) -> str:
        """Launch Docker container for stage run.

        Args:
            image_tag: Docker image to run
            stage_run_id: Stage run identifier (used as container name)
            entrypoint_script: Bash script to run in container
            stage_config: Stage configuration (passed as GOLDFISH_STAGE_CONFIG env var)
            work_dir: Working directory for container files
            inputs_dir: Directory to mount as /mnt/inputs
            outputs_dir: Directory to mount as /mnt/outputs

        Returns:
            Container ID (same as stage_run_id)
        """
        # Create entrypoint script file
        entrypoint_path = work_dir / f"{stage_run_id}-entrypoint.sh"
        entrypoint_path.write_text(entrypoint_script)
        entrypoint_path.chmod(0o755)

        # Build docker run command
        docker_cmd = [
            "docker", "run",
            "--name", stage_run_id,
            "--detach",  # Run in background
        ]

        # Set environment variable with stage config
        stage_config_json = json.dumps(stage_config)
        docker_cmd.extend(["-e", f"GOLDFISH_STAGE_CONFIG={stage_config_json}"])

        # Mount volumes
        if inputs_dir:
            docker_cmd.extend(["-v", f"{inputs_dir}:/mnt/inputs"])

        if outputs_dir:
            docker_cmd.extend(["-v", f"{outputs_dir}:/mnt/outputs"])

        # Mount entrypoint script
        docker_cmd.extend(["-v", f"{entrypoint_path}:/entrypoint.sh"])

        # Image and command
        docker_cmd.extend([
            image_tag,
            "/bin/bash", "/entrypoint.sh"
        ])

        # Launch container
        try:
            result = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Container launches in detached mode, return immediately
            return stage_run_id

        except FileNotFoundError:
            raise GoldfishError(
                "Docker not found. Please install Docker to run containers."
            )

    def get_container_status(self, container_id: str) -> str:
        """Get status of running container.

        Args:
            container_id: Container identifier

        Returns:
            Status: "running", "completed", "failed", or "not_found"
        """
        try:
            result = subprocess.run(
                ["docker", "inspect", container_id],
                capture_output=True,
                text=True,
                check=False
            )

            if result.returncode != 0:
                return "not_found"

            # Parse docker inspect output
            import json
            inspect_data = json.loads(result.stdout)

            if not inspect_data:
                return "not_found"

            state = inspect_data[0]["State"]
            status = state["Status"]

            if status == "running":
                return "running"
            elif status == "exited":
                exit_code = state.get("ExitCode", 1)
                return "completed" if exit_code == 0 else "failed"
            else:
                return status

        except Exception as e:
            raise GoldfishError(f"Failed to get container status: {e}")

    def get_container_logs(self, container_id: str) -> str:
        """Retrieve logs from container.

        Args:
            container_id: Container identifier

        Returns:
            Container logs as string
        """
        try:
            result = subprocess.run(
                ["docker", "logs", container_id],
                capture_output=True,
                text=True,
                check=False
            )

            return result.stdout

        except Exception as e:
            raise GoldfishError(f"Failed to get container logs: {e}")

    def stop_container(self, container_id: str) -> None:
        """Stop running container.

        Args:
            container_id: Container identifier
        """
        try:
            subprocess.run(
                ["docker", "stop", container_id],
                capture_output=True,
                check=False
            )

        except Exception as e:
            raise GoldfishError(f"Failed to stop container: {e}")

    def remove_container(self, container_id: str) -> None:
        """Remove stopped container.

        Args:
            container_id: Container identifier
        """
        try:
            subprocess.run(
                ["docker", "rm", container_id],
                capture_output=True,
                check=False
            )

        except Exception as e:
            raise GoldfishError(f"Failed to remove container: {e}")
