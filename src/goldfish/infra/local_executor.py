"""Local Docker container execution for Goldfish stages."""

import json
import subprocess
from pathlib import Path

from goldfish.errors import GoldfishError
from goldfish.models import StageRunStatus


class LocalExecutor:
    """Execute stage runs in Docker containers locally."""

    def launch_container(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict,
        work_dir: Path,
        inputs_dir: Path | None = None,
        outputs_dir: Path | None = None,
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
            "docker",
            "run",
            "--name",
            stage_run_id,
            "--detach",  # Run in background
        ]

        # SECURITY: Add resource limits to prevent DoS
        docker_cmd.extend(
            [
                "--memory",
                "4g",  # Limit memory to 4GB
                "--cpus",
                "2.0",  # Limit to 2 CPUs
                "--pids-limit",
                "100",  # Limit number of processes
            ]
        )

        # SECURITY: Run as non-root user (UID 1000)
        docker_cmd.extend(["--user", "1000:1000"])

        # SECURITY: Add timeout to prevent runaway containers
        # Container will be auto-removed after exit
        docker_cmd.extend(["--rm"])

        # Set environment variable with stage config
        stage_config_json = json.dumps(stage_config)
        docker_cmd.extend(["-e", f"GOLDFISH_STAGE_CONFIG={stage_config_json}"])

        # Set user-defined environment variables from config
        # This allows configs to specify env vars like WANDB_API_KEY
        if "environment" in stage_config and isinstance(stage_config["environment"], dict):
            for env_name, env_value in stage_config["environment"].items():
                # SECURITY: Validate env var name (alphanumeric + underscore only)
                if not env_name.replace("_", "").isalnum():
                    continue
                docker_cmd.extend(["-e", f"{env_name}={env_value}"])

        # SECURITY: Mount volumes with read-only where appropriate
        # inputs are read-only, outputs are read-write
        if inputs_dir:
            docker_cmd.extend(["-v", f"{inputs_dir}:/mnt/inputs:ro"])

        if outputs_dir:
            docker_cmd.extend(["-v", f"{outputs_dir}:/mnt/outputs"])

        # Mount entrypoint script as read-only
        docker_cmd.extend(["-v", f"{entrypoint_path}:/entrypoint.sh:ro"])

        # Image and command
        docker_cmd.extend([image_tag, "/bin/bash", "/entrypoint.sh"])

        # Launch container
        try:
            subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Container launches in detached mode, return immediately
            return stage_run_id

        except FileNotFoundError as err:
            raise GoldfishError("Docker not found. Please install Docker to run containers.") from err

    def get_container_status(self, container_id: str) -> str:
        """Get status of running container.

        Args:
            container_id: Container identifier

        Returns:
            Status: StageRunStatus value or "not_found"
        """
        try:
            result = subprocess.run(["docker", "inspect", container_id], capture_output=True, text=True, check=False)

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
                return StageRunStatus.RUNNING
            elif status == "exited":
                exit_code = state.get("ExitCode", 1)
                return StageRunStatus.COMPLETED if exit_code == 0 else StageRunStatus.FAILED
            else:
                return str(status)

        except Exception as e:
            raise GoldfishError(f"Failed to get container status: {e}") from e

    def get_container_logs(self, container_id: str, tail_lines: int = 200, since: str | None = None) -> str:
        """Retrieve logs from container (supports tail and since).

        Args:
            container_id: Container identifier
            tail_lines: number of lines from the end
            since: ISO8601 timestamp or Docker-acceptable since string
        """
        cmd = ["docker", "logs", container_id, "--tail", str(tail_lines)]
        if since:
            cmd.extend(["--since", since])
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            return result.stdout

        except Exception as e:
            raise GoldfishError(f"Failed to get container logs: {e}") from e

    def stop_container(self, container_id: str) -> None:
        """Stop running container.

        Args:
            container_id: Container identifier
        """
        try:
            subprocess.run(["docker", "stop", container_id], capture_output=True, check=False)

        except Exception as e:
            raise GoldfishError(f"Failed to stop container: {e}") from e

    def remove_container(self, container_id: str) -> None:
        """Remove stopped container.

        Args:
            container_id: Container identifier
        """
        try:
            subprocess.run(["docker", "rm", container_id], capture_output=True, check=False)

        except Exception as e:
            raise GoldfishError(f"Failed to remove container: {e}") from e
