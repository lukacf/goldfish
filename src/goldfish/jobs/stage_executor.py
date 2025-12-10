"""Stage execution engine for Goldfish."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

import yaml
from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.datasets.registry import DatasetRegistry
from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder
from goldfish.infra.local_executor import LocalExecutor
from goldfish.infra.gce_launcher import GCELauncher
from goldfish.infra.profiles import ProfileResolver
from goldfish.models import StageDef, StageRunInfo
from goldfish.utils import parse_optional_datetime
from goldfish.pipeline.manager import PipelineManager
from goldfish.workspace.manager import WorkspaceManager


class StageExecutor:
    """Execute individual pipeline stages."""

    def __init__(
        self,
        db: Database,
        config: GoldfishConfig,
        workspace_manager: WorkspaceManager,
        pipeline_manager: PipelineManager,
        project_root: Path,
        dataset_registry: Optional[DatasetRegistry] = None,
    ):
        self.db = db
        self.config = config
        self.workspace_manager = workspace_manager
        self.pipeline_manager = pipeline_manager
        self.project_root = project_root
        self.dataset_registry = dataset_registry

        # Initialize execution infrastructure
        self.docker_builder = DockerBuilder()
        self.local_executor = LocalExecutor()

        # Initialize profile resolver
        profile_overrides = None
        if config.gce and config.gce.profile_overrides:
            profile_overrides = config.gce.profile_overrides
        self.profile_resolver = ProfileResolver(profile_overrides=profile_overrides)

        # Initialize GCE launcher with full config
        gce_bucket = None
        gce_project = None
        gce_zone = "us-central1-a"
        gce_zones = None
        gce_resources = []

        if config.gcs:
            gce_bucket = config.gcs.bucket

        if config.gce:
            gce_project = config.gce.project_id
            if config.gce.zones:
                gce_zone = config.gce.zones[0]
                gce_zones = config.gce.zones  # Pass all zones for multi-zone lookups

        self.gce_launcher = GCELauncher(
            project_id=gce_project,
            zone=gce_zone,
            bucket=gce_bucket,
            resources=gce_resources,  # Will be set per-stage
            zones=gce_zones,
        )

    def run_stage(
        self,
        workspace: str,
        stage_name: str,
        pipeline_name: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        config_override: Optional[dict] = None,
        inputs_override: Optional[dict] = None,
        reason: Optional[str] = None,
        wait: bool = False,
    ) -> StageRunInfo:
        """Run a single pipeline stage.

        Args:
            workspace: Workspace name
            stage_name: Stage to run
            config_override: Override env vars from config
            inputs_override: Override input sources (for debugging)
            reason: Why this stage is being run

        Flow:
            1. Auto-version workspace (git tag)
            2. Load pipeline and stage definition
            3. Resolve input sources
            4. Build Docker image
            5. Generate entrypoint
            6. Launch container
            7. Monitor and track
        """
        # 1. Auto-version workspace
        version = self._auto_version(workspace, stage_name, reason)

        # 2. Load pipeline and stage
        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        stage = self._find_stage(pipeline, stage_name)

        # 2b. Load stage config and apply override
        stage_config = self._load_stage_config(workspace, stage_name) or {}
        if config_override:
            # shallow merge override
            stage_config.update(config_override)

        # 3. Resolve inputs
        inputs = self._resolve_inputs(workspace, stage, inputs_override)

        # 4. Generate stage run ID
        stage_run_id = f"stage-{uuid4().hex[:8]}"

        # 5. Create stage run record
        self._create_stage_run_record(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=version,
            stage_name=stage_name,
            inputs=inputs,
            config_override=config_override,
            reason=reason,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            profile=stage_config.get("compute", {}).get("profile") if 'compute' in stage_config else None,
            hints=stage_config.get("hints"),
            config=stage_config,
        )

        try:
            # 6. Build Docker image
            image_tag = self._build_docker_image(workspace, version)

            # 7. Launch container
            self._launch_container(stage_run_id, workspace, stage_name, image_tag, inputs)
        except Exception as e:
            # Mark failed immediately with error and re-raise
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                status="failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
            )
            raise

        info = StageRunInfo(
            stage_run_id=stage_run_id,
            pipeline_run_id=pipeline_run_id,
            workspace=workspace,
            pipeline=pipeline_name,
            version=version,
            stage=stage_name,
            status="running",
            started_at=datetime.now(timezone.utc),
            log_uri=str((self.project_root / ".goldfish" / "runs" / stage_run_id / "logs" / "output.log")),
            profile=stage_config.get("compute", {}).get("profile") if 'compute' in stage_config else None,
            hints=stage_config.get("hints"),
            config=stage_config,
            inputs=inputs,
        )

        if wait:
            self.wait_for_completion(stage_run_id)
            refreshed = self.db.get_stage_run(stage_run_id)
            if refreshed:
                return StageRunInfo(
                    **info.model_dump(),
                    status=refreshed.get("status", info.status),
                    completed_at=parse_optional_datetime(refreshed.get("completed_at")),
                    log_uri=refreshed.get("log_uri"),
                    artifact_uri=refreshed.get("artifact_uri"),
                    progress=refreshed.get("progress"),
                    outputs=json.loads(refreshed.get("outputs_json")) if refreshed.get("outputs_json") else None,
                    error=refreshed.get("error"),
                )

        return info

    def _resolve_inputs(
        self,
        workspace: str,
        stage: StageDef,
        inputs_override: Optional[dict] = None,
    ) -> dict[str, str]:
        """Resolve input sources (dataset, signal, or override).

        Returns dict: {input_name: source_location}
        """
        inputs = {}

        for input_name, input_def in stage.inputs.items():
            # Check for override
            if inputs_override and input_name in inputs_override:
                inputs[input_name] = inputs_override[input_name]
                continue

            # Resolve precedence: from_stage first, then dataset
            if input_def.from_stage:
                # Find output from previous stage
                # Get most recent successful run of source stage
                stage_runs = self.db.list_stage_runs(
                    workspace_name=workspace, stage_name=input_def.from_stage
                )

                # Find completed run with the signal
                source_run_id = None
                for run in stage_runs:
                    if run["status"] == "completed":
                        source_run_id = run["id"]
                        break

                if not source_run_id:
                    raise GoldfishError(
                        f"No successful run found for stage '{input_def.from_stage}'"
                    )

                # Get signal from that run
                signals = self.db.list_signals(stage_run_id=source_run_id)
                signal_name = input_def.name  # Signal name from input definition

                signal = None
                for s in signals:
                    if s["signal_name"] == signal_name:
                        signal = s
                        break

                if not signal:
                    raise GoldfishError(
                        f"Signal '{signal_name}' not found in stage run {source_run_id}"
                    )

                inputs[input_name] = signal["storage_location"]

            elif input_def.type == "dataset":
                # External dataset
                dataset = self.dataset_registry.get_dataset(input_def.dataset)
                inputs[input_name] = dataset.gcs_location

            else:
                raise GoldfishError(f"Cannot resolve input: {input_name}")

        return inputs

    def _auto_version(
        self, workspace: str, stage_name: str, reason: Optional[str]
    ) -> str:
        """Create automatic version for workspace.

        Returns version string (e.g., "v1", "v2")
        """
        # Ensure workspace lineage exists
        if not self.db.workspace_exists(workspace):
            self.db.create_workspace_lineage(
                workspace_name=workspace, description=f"Auto-created for {stage_name}"
            )

        # Get workspace path (must be mounted)
        workspace_path = self.workspace_manager.get_workspace_path(workspace)

        # Get current git SHA from the workspace
        git_sha = self.workspace_manager.git.get_head_sha(workspace_path, short=False)

        # Get next version number
        next_version = self.db.get_next_version_number(workspace)

        # Create git tag
        git_tag = f"{workspace}-{next_version}"
        self.workspace_manager.git.create_tag(workspace, git_tag, git_sha)

        # Create version record
        description = reason or f"Auto-version for {stage_name} run"
        self.db.create_version(
            workspace_name=workspace,
            version=next_version,
            git_tag=git_tag,
            git_sha=git_sha,
            created_by="run",
            description=description,
        )

        return next_version

    def _find_stage(self, pipeline, stage_name: str) -> StageDef:
        """Find stage definition in pipeline."""
        for stage in pipeline.stages:
            if stage.name == stage_name:
                return stage
        raise GoldfishError(f"Stage '{stage_name}' not found in pipeline")

    def _create_stage_run_record(
        self,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_name: str,
        inputs: dict,
        config_override: Optional[dict],
        reason: Optional[str],
        pipeline_run_id: Optional[str],
        pipeline_name: Optional[str],
        profile: Optional[str],
        hints: Optional[dict],
        config: Optional[dict],
    ):
        """Create stage run record in database."""
        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=version,
            stage_name=stage_name,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            config=config,
            inputs=inputs,
            profile=profile,
            hints=hints,
            backend_type=self.config.jobs.backend,
            backend_handle=stage_run_id,  # provisional handle for cancel/logs
        )

        # Record input signals in lineage
        for input_name, storage_location in inputs.items():
            # Add signal to lineage table
            self.db.add_signal(
                stage_run_id=stage_run_id,
                signal_name=input_name,
                signal_type="input",
                storage_location=storage_location
            )

    def _record_output_signals(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        gcs_base: Optional[str] = None,
    ):
        """Record output signals after stage completion.

        Reads output definitions from the pipeline and records them in the database
        so subsequent stages can resolve inputs. When running on GCE, outputs are
        assumed to be written to gs://{bucket}/runs/{stage_run_id}/outputs/{name}/
        unless an explicit *.gcs_location marker is present.
        """
        # Load pipeline and find stage definition
        try:
            pipeline = self.pipeline_manager.get_pipeline(workspace)
            stage = self._find_stage(pipeline, stage_name)
        except GoldfishError:
            # Pipeline or stage not found - skip output recording
            return

        # Get the outputs directory for this run (local backend)
        run_dir = self.project_root / ".goldfish" / "runs" / stage_run_id
        outputs_dir = run_dir / "outputs"

        outputs_payload = []

        # Record each output signal from the stage definition atomically
        with self.db._conn() as conn:
            for output_name, output_def in stage.outputs.items():
                # Determine storage location
                storage_location = str(outputs_dir / output_name)

                # Check if GCS location was written by the stage
                gcs_marker = outputs_dir / f"{output_name}.gcs_location"
                if gcs_marker.exists():
                    storage_location = gcs_marker.read_text().strip()
                elif gcs_base:
                    # Default GCS location for GCE runs
                    storage_location = f"{gcs_base.rstrip('/')}/{output_name}/"

                conn.execute(
                    """
                    INSERT INTO signal_lineage
                    (stage_run_id, signal_name, signal_type, storage_location, is_artifact)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        stage_run_id,
                        output_name,
                        output_def.type or "directory",
                        storage_location,
                        int(bool(output_def.artifact)),
                    ),
                )

                outputs_payload.append({
                    "name": output_name,
                    "type": output_def.type or "directory",
                    "storage_location": storage_location,
                    "from_stage_ref": f"{stage_name}/{output_name}",
                    "is_artifact": bool(output_def.artifact),
                })

            # Attach outputs JSON to stage_run row (do not override status)
            if outputs_payload:
                conn.execute(
                    "UPDATE stage_runs SET outputs_json=? WHERE id=?",
                    (json.dumps(outputs_payload), stage_run_id),
                )

        # Auto-register artifacts
        for output in outputs_payload:
            if output["is_artifact"]:
                source_id = output["name"]  # simplistic name; could namespace later
                source_name = f"{stage_name}_{output['name']}"
                if not self.db.source_exists(source_id):
                    try:
                        self.db.create_source(
                            source_id=source_id,
                            name=source_name,
                            gcs_location=output["storage_location"],
                            created_by=f"stage:{stage_run_id}",
                            description=f"Artifact from {stage_run_id}",
                        )
                    except Exception as e:
                        import logging
                        logging.getLogger(__name__).warning(
                            "Failed to auto-register artifact %s: %s", source_id, e
                        )

    def _persist_logs(self, stage_run_id: str, logs: str) -> str:
        """Write logs to local run directory and return path."""
        run_dir = self.project_root / ".goldfish" / "runs" / stage_run_id / "logs"
        run_dir.mkdir(parents=True, exist_ok=True)
        log_path = run_dir / "output.log"
        log_path.write_text(logs or "")
        return str(log_path)

    def _build_docker_image(self, workspace: str, version: str) -> str:
        """Build Docker image for this run.

        Returns image tag (local for local backend, registry for GCE backend).
        """
        # Get workspace directory
        workspace_dir = self.workspace_manager.get_workspace_path(workspace)

        # Build image using DockerBuilder
        local_image_tag = self.docker_builder.build_image(
            workspace_dir=workspace_dir,
            workspace_name=workspace,
            version=version,
            use_cache=True
        )

        # If using GCE backend and artifact_registry is configured, push to registry
        backend = self.config.jobs.backend
        if backend == "gce":
            if not self.config.gce:
                raise GoldfishError(
                    "GCE backend requires gce configuration in goldfish.yaml"
                )

            if not self.config.gce.artifact_registry:
                raise GoldfishError(
                    "GCE backend requires artifact_registry URL in gce configuration. "
                    "Example: artifact_registry: us-docker.pkg.dev/{project_id}/goldfish"
                )

            # Push image to Artifact Registry
            registry_image_tag = self.docker_builder.push_image(
                local_tag=local_image_tag,
                registry_url=self.config.gce.artifact_registry,
                workspace_name=workspace,
                version=version
            )

            return registry_image_tag

        return local_image_tag

    def _load_stage_config(self, workspace: str, stage_name: str) -> dict:
        """Load stage config from configs/{stage}.yaml.

        Args:
            workspace: Workspace name
            stage_name: Stage name

        Returns:
            Stage config dict (or empty dict if config doesn't exist)
        """
        workspace_path = self.workspace_manager.get_workspace_path(workspace)
        config_path = workspace_path / "configs" / f"{stage_name}.yaml"

        if not config_path.exists():
            return {}

        try:
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            # Log warning but don't fail - config is optional
            return {}

    def _resolve_profile_from_config(self, stage_config: dict) -> Optional[dict]:
        """Resolve profile from stage config.

        Args:
            stage_config: Stage config dict

        Returns:
            Resolved profile dict, or None if no profile specified
        """
        compute = stage_config.get("compute", {})

        # Check if profile is specified
        if "profile" not in compute:
            return None

        profile_name = compute["profile"]

        # Resolve profile using ProfileResolver
        try:
            return self.profile_resolver.resolve(profile_name)
        except Exception as e:
            raise GoldfishError(f"Failed to resolve profile '{profile_name}': {e}")

    @staticmethod
    def _poll_interval(elapsed: int) -> int:
        if elapsed < 60:
            return 5
        if elapsed < 600:
            return 10
        if elapsed < 3600:
            return 30
        return 60

    def _launch_container(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        image_tag: str,
        inputs: dict,
    ):
        """Launch Docker container (local) or GCE instance."""
        backend = self.config.jobs.backend

        if backend == "local":
            # Create work directory for this run
            run_dir = self.project_root / ".goldfish" / "runs" / stage_run_id
            run_dir.mkdir(parents=True, exist_ok=True)

            # Create inputs and outputs directories
            inputs_dir = run_dir / "inputs"
            outputs_dir = run_dir / "outputs"
            inputs_dir.mkdir(exist_ok=True)
            outputs_dir.mkdir(exist_ok=True)

            # Generate entrypoint script
            entrypoint_script = f"""#!/bin/bash
set -euo pipefail

echo "Running stage: {stage_name}"
cd /app
python -m modules.{stage_name}

echo "Stage completed successfully"
"""

            # Generate stage config
            stage_config = {
                "stage": stage_name,
                "inputs": inputs,
                "outputs": {}  # Will be populated by module
            }

            # Launch container using LocalExecutor
            self.local_executor.launch_container(
                image_tag=image_tag,
                stage_run_id=stage_run_id,
                entrypoint_script=entrypoint_script,
                stage_config=stage_config,
                work_dir=run_dir,
                inputs_dir=inputs_dir,
                outputs_dir=outputs_dir
            )

        elif backend == "gce":
            # Load stage config and resolve profile
            stage_config_yaml = self._load_stage_config(workspace, stage_name)
            profile = self._resolve_profile_from_config(stage_config_yaml)

            # Prepare launch parameters
            machine_type = "n1-standard-4"
            gpu_type = None
            gpu_count = 0
            zones = None
            use_capacity_search = False

            if profile:
                # Use profile for GCE launch
                machine_type = profile["machine_type"]
                gpu_info = profile.get("gpu", {})
                if gpu_info.get("type") != "none":
                    gpu_type = gpu_info.get("accelerator")
                    gpu_count = gpu_info.get("count", 0)
                zones = profile.get("zones")
                use_capacity_search = True

                # Update GCE launcher with profile as resource
                self.gce_launcher.resources = [profile]

                # Apply runtime preferences from config
                if self.config.gce:
                    # GPU preference ordering for capacity search
                    gpu_preference = self.config.gce.gpu_preference
                    # Note: ResourceLauncher will use these preferences
                    # when searching for capacity

            # Launch on GCE
            self.gce_launcher.launch_instance(
                image_tag=image_tag,
                stage_run_id=stage_run_id,
                entrypoint_script=f"""#!/bin/bash
set -euo pipefail

echo "Running stage: {stage_name}"
cd /app
python -m modules.{stage_name}

echo "Stage completed successfully"
""",
                stage_config={
                    "stage": stage_name,
                    "inputs": inputs,
                    "outputs": {}
                },
                work_dir=self.project_root / ".goldfish" / "runs" / stage_run_id,
                machine_type=machine_type,
                gpu_type=gpu_type,
                gpu_count=gpu_count,
                zones=zones,
                use_capacity_search=use_capacity_search,
            )
        else:
            raise ValueError(f"Unknown backend: {backend}")

    def _finalize_stage_run(self, stage_run_id: str, backend: str, status: str) -> None:
        """Handle terminal status: record outputs, fetch logs, update status."""
        # CAS: only finalize if not already terminal
        with self.db._conn() as conn:
            updated = conn.execute(
                "UPDATE stage_runs SET status=?, completed_at=? WHERE id=? AND status NOT IN ('completed','failed','canceled')",
                (status, datetime.now(timezone.utc).isoformat(), stage_run_id),
            ).rowcount
        if updated == 0:
            return  # already finalized

        stage_run = self.db.get_stage_run(stage_run_id)
        if not stage_run:
            return

        workspace = stage_run["workspace_name"]
        stage_name_from_db = stage_run["stage_name"]

        gcs_base = None
        if backend == "gce" and self.config.gcs and self.config.gcs.bucket:
            bucket = self.config.gcs.bucket
            bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
            gcs_base = f"{bucket_uri.rstrip('/')}/runs/{stage_run_id}/outputs"

        if status == "completed":
            self._record_output_signals(stage_run_id, workspace, stage_name_from_db, gcs_base=gcs_base)

        logs = ""
        try:
            if backend == "local":
                logs = self.local_executor.get_container_logs(stage_run_id, tail_lines=1000)
            elif backend == "gce":
                logs = self.gce_launcher.get_instance_logs(stage_run_id)
                if not logs:
                    logs = "[GCE logs unavailable - instance may have been deleted or logs not synced]"
        except Exception as e:
            logs = f"[Error fetching logs: {e}]"

        log_uri = self._persist_logs(stage_run_id, logs) if logs is not None else None

        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            status=status,
            completed_at=datetime.now(timezone.utc).isoformat(),
            log_uri=log_uri,
            error=(logs[-1000:] if (status == "failed" and logs) else None),
        )

    def wait_for_completion(
        self, stage_run_id: str, poll_interval: int = 5, timeout: int = 3600
    ) -> str:
        """Wait for stage run to complete.

        Polls container status and updates database.

        Args:
            stage_run_id: Stage run identifier
            poll_interval: Seconds between polls (default 5)
            timeout: Maximum seconds to wait (default 3600 = 1 hour)

        Returns:
            Final status: "completed" or "failed"

        Raises:
            GoldfishError: If timeout exceeded or container not found
        """
        import time

        backend = self.config.jobs.backend

        elapsed = 0
        while elapsed < timeout:
            if backend == "local":
                status = self.local_executor.get_container_status(stage_run_id)

                if status == "running":
                    # Still running, update status in db
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id, status="running"
                    )
                    interval = self._poll_interval(elapsed)
                    time.sleep(interval)
                    elapsed += interval
                    continue

                elif status in ("completed", "failed"):
                    self._finalize_stage_run(stage_run_id, backend, status)
                    return status

                elif status == "not_found":
                    raise GoldfishError(f"Container {stage_run_id} not found")

                else:
                    # Unknown status
                    raise GoldfishError(f"Unknown container status: {status}")

            elif backend == "gce":
                status = self.gce_launcher.get_instance_status(stage_run_id)

                if status == "running":
                    # Still running, update status in db
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id, status="running"
                    )
                    interval = self._poll_interval(elapsed)
                    time.sleep(interval)
                    elapsed += interval
                    continue

                elif status in ("completed", "failed"):
                    self._finalize_stage_run(stage_run_id, backend, status)
                    return status

                elif status == "not_found":
                    # GCE API has eventual consistency - instance may take time to appear
                    # Capacity search across multiple zones can take 10+ minutes
                    # Treat "not_found" as transient and continue polling
                    if elapsed % 60 == 0 and elapsed > 0:
                        # Log every minute for visibility
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.info(
                            f"Instance {stage_run_id} not yet visible in GCE API "
                            f"(elapsed: {elapsed}s, may be launching or searching capacity)"
                        )
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    continue

                else:
                    # Unknown status
                    raise GoldfishError(f"Unknown instance status: {status}")

            else:
                raise GoldfishError(f"Backend {backend} not supported for monitoring")

        # Timeout exceeded
        raise GoldfishError(
            f"Stage run {stage_run_id} timed out after {timeout} seconds"
        )

    def refresh_status_once(self, stage_run_id: str) -> Optional[str]:
        """Single backend check to advance status/logs/outputs without blocking."""
        backend = self.config.jobs.backend

        if backend == "local":
            status = self.local_executor.get_container_status(stage_run_id)
            if status == "running":
                # CAS: only update if still running
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE stage_runs SET status='running' WHERE id=? AND status!='completed' AND status!='failed' AND status!='canceled'",
                        (stage_run_id,),
                    )
            elif status in ("completed", "failed"):
                # Guard against double-finalize by only doing it if not terminal
                current = self.db.get_stage_run(stage_run_id)
                if current and current.get("status") not in ("completed", "failed", "canceled"):
                    self._finalize_stage_run(stage_run_id, backend, status)
            return status

        if backend == "gce":
            status = self.gce_launcher.get_instance_status(stage_run_id)
            if status == "running":
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE stage_runs SET status='running' WHERE id=? AND status!='completed' AND status!='failed' AND status!='canceled'",
                        (stage_run_id,),
                    )
            elif status in ("completed", "failed"):
                current = self.db.get_stage_run(stage_run_id)
                if current and current.get("status") not in ("completed", "failed", "canceled"):
                    self._finalize_stage_run(stage_run_id, backend, status)
            return status

        return None
