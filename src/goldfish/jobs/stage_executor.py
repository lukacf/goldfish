"""Stage execution engine for Goldfish."""

import asyncio
import json
import logging
import os
import threading
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from goldfish.config import GoldfishConfig
from goldfish.datasets.registry import DatasetRegistry
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder
from goldfish.infra.gce_launcher import GCELauncher
from goldfish.infra.local_executor import LocalExecutor
from goldfish.infra.profiles import ProfileResolver
from goldfish.models import (
    PipelineDef,
    ReviewSeverity,
    RunReason,
    RunReview,
    StageDef,
    StageRunInfo,
    StageRunProgress,
    StageRunStatus,
)
from goldfish.pipeline.manager import PipelineManager
from goldfish.utils import parse_optional_datetime
from goldfish.utils.config_hash import compute_config_hash
from goldfish.validation import InvalidSourceMetadataError, parse_source_metadata, validate_source_metadata
from goldfish.workspace.manager import WorkspaceManager

logger = logging.getLogger(__name__)

STAGE_LOG_TAIL_FOR_FINALIZE = int(os.getenv("GOLDFISH_FINALIZE_LOG_TAIL", "1000"))


@dataclass
class _MetricsSyncState:
    offset: int = 0
    last_sync: float = 0.0
    metric_names: set[str] = field(default_factory=set)
    validated_names: set[str] = field(default_factory=set)
    step_modes: dict[str, str] = field(default_factory=dict)
    temp_path: Path | None = None
    sync_lock: threading.Lock = field(default_factory=threading.Lock)


class StageExecutor:
    """Execute individual pipeline stages."""

    def __init__(
        self,
        db: Database,
        config: GoldfishConfig,
        workspace_manager: WorkspaceManager,
        pipeline_manager: PipelineManager,
        project_root: Path,
        dataset_registry: DatasetRegistry | None = None,
    ):
        self.db = db
        self.config = config
        self.workspace_manager = workspace_manager
        self.pipeline_manager = pipeline_manager
        self.project_root = project_root
        self.dataset_registry = dataset_registry

        # Dev repo contains all Goldfish runtime artifacts (.goldfish/, runs/, etc.)
        self.dev_repo = config.get_dev_repo_path(project_root)

        # Initialize execution infrastructure
        self.docker_builder = DockerBuilder(config)
        self.local_executor = LocalExecutor()

        # Initialize profile resolver
        profile_overrides = None
        if config.gce:
            profile_overrides = config.gce.effective_profile_overrides
        self.profile_resolver = ProfileResolver(profile_overrides=profile_overrides)

        # Initialize GCE launcher with full config
        gce_bucket = None
        gce_project = None
        gce_zone = "us-central1-a"
        gce_zones = None
        gce_resources: list[dict[str, Any]] = []
        gce_gpu_preference = None

        if config.gcs:
            gce_bucket = config.gcs.bucket

        # Compute artifact_registry for base image resolution and image pushing
        self.artifact_registry: str | None = None

        if config.gce:
            # Use effective_project_id to support both project_id and project aliases
            try:
                gce_project = config.gce.effective_project_id
            except ValueError:
                pass  # Neither project_id nor project set, leave as None for gcloud defaults
            if config.gce.zones:
                gce_zone = config.gce.zones[0]
                gce_zones = config.gce.zones  # Pass all zones for multi-zone lookups
            gce_gpu_preference = config.gce.gpu_preference

            # Resolve artifact_registry from config or auto-generate from project
            self.artifact_registry = config.gce.artifact_registry
            if not self.artifact_registry and gce_project:
                self.artifact_registry = f"us-docker.pkg.dev/{gce_project}/goldfish"
                logger.info(f"Auto-generated artifact_registry: {self.artifact_registry}")

        self.gce_launcher = GCELauncher(
            project_id=gce_project,
            zone=gce_zone,
            bucket=gce_bucket,
            resources=gce_resources,  # Will be set per-stage
            zones=gce_zones,
            gpu_preference=gce_gpu_preference,
        )

        # Live metrics sync state (per run)
        self._metrics_sync_state: dict[str, _MetricsSyncState] = {}
        self._metrics_sync_lock = threading.Lock()
        self._gcs_client = None
        self._gcs_client_lock = threading.Lock()

    def run_stage(
        self,
        workspace: str,
        stage_name: str,
        pipeline_name: str | None = None,
        pipeline_run_id: str | None = None,
        config_override: dict | None = None,
        inputs_override: dict | None = None,
        reason: str | None = None,
        reason_structured: dict | None = None,
        wait: bool = False,
        stage_run_id: str | None = None,
        skip_review: bool = False,
    ) -> StageRunInfo:
        """Run a single pipeline stage.

        Args:
            workspace: Workspace name
            stage_name: Stage to run
            pipeline_name: Pipeline file name
            pipeline_run_id: Parent pipeline run ID
            config_override: Override env vars from config
            inputs_override: Override input sources (for debugging)
            reason: Why this stage is being run (string summary)
            reason_structured: Structured RunReason dict (description, hypothesis, etc.)
            wait: Block until completion
            stage_run_id: Pre-created stage_run_id (from pipeline queue). If provided,
                         updates existing record; if None, creates new one.
            skip_review: Skip pre-run review (default False)

        Flow:
            1. Auto-version workspace (git tag)
            2. Load pipeline and stage definition
            3. Pre-run review (if enabled and not skipped)
            4. Resolve input sources
            5. Build Docker image
            6. Generate entrypoint
            7. Launch container
            8. Monitor and track

        Returns:
            StageRunInfo with status and review (if review blocked the run)
        """
        # 1. Auto-version workspace (returns version and git SHA)
        version, git_sha = self._auto_version(workspace, stage_name, reason)

        # 2. Load pipeline and stage
        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        stage = self._find_stage(pipeline, stage_name)

        # 3. Pre-run review (if enabled and not skipped)
        review: RunReview | None = None
        if self.config.pre_run_review.enabled and not skip_review:
            review = self._perform_pre_run_review(
                workspace=workspace,
                stage_name=stage_name,
                pipeline=pipeline,
                reason_structured=reason_structured,
                git_sha=git_sha,
            )
            if review and review.has_blocking_issues:
                # Create failed stage run record with review
                blocked_stage_run_id = stage_run_id or f"stage-{uuid4().hex[:8]}"
                return self._create_blocked_stage_run(
                    stage_run_id=blocked_stage_run_id,
                    workspace=workspace,
                    version=version,
                    stage_name=stage_name,
                    review=review,
                    reason=reason,
                    reason_structured=reason_structured,
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=pipeline_name,
                )

        # 3b. Load stage config and apply override
        stage_config = self._load_stage_config(workspace, stage_name) or {}
        if config_override:
            # shallow merge override
            stage_config.update(config_override)

        # 2c. Compute config hash and get/create stage version
        config_hash = compute_config_hash(stage_config)
        stage_version_id, stage_version_num, _ = self.db.get_or_create_stage_version(
            workspace=workspace,
            stage=stage_name,
            git_sha=git_sha,
            config_hash=config_hash,
        )

        # 3. Resolve inputs (with source metadata for lineage tracking)
        inputs, input_sources = self._resolve_inputs(workspace, stage, inputs_override)

        # 4. Generate or use provided stage run ID
        if stage_run_id is None:
            stage_run_id = f"stage-{uuid4().hex[:8]}"
            # Create new stage run record
            self._create_stage_run_record(
                stage_run_id=stage_run_id,
                workspace=workspace,
                version=version,
                stage_name=stage_name,
                stage_version_id=stage_version_id,
                inputs=inputs,
                input_sources=input_sources,
                config_override=config_override,
                reason=reason,
                reason_structured=reason_structured,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
                profile=stage_config.get("compute", {}).get("profile") if "compute" in stage_config else None,
                hints=stage_config.get("hints"),
                config=stage_config,
            )
        else:
            # Update existing queued stage run record with resolved values
            self._update_queued_stage_run(
                stage_run_id=stage_run_id,
                version=version,
                stage_version_id=stage_version_id,
                inputs=inputs,
                input_sources=input_sources,
                config=stage_config,
                profile=stage_config.get("compute", {}).get("profile") if "compute" in stage_config else None,
                hints=stage_config.get("hints"),
            )

        try:
            # Emit phase progress: building image
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                status=StageRunStatus.RUNNING,
                progress=StageRunProgress.BUILD,
            )
            # 6. Build Docker image (use profile's base image)
            profile_name = stage_config.get("compute", {}).get("profile")
            image_tag = self._build_docker_image(workspace, version, profile_name=profile_name)

            # Emit phase progress: launching container/instance
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                status=StageRunStatus.RUNNING,
                progress=StageRunProgress.LAUNCH,
            )
            # 7. Launch container
            # Build input config with format info for goldfish.io
            input_configs = {}
            for input_name, input_def in stage.inputs.items():
                input_configs[input_name] = {
                    "location": inputs.get(input_name, ""),
                    "format": input_def.format or input_def.type,  # Use format override or fall back to type
                    "type": input_def.type,
                }

            # Build output config with format info
            output_configs = {}
            for output_name, output_def in stage.outputs.items():
                output_configs[output_name] = {
                    "format": output_def.format or output_def.type,
                    "type": output_def.type,
                }

            self._launch_container(
                stage_run_id,
                workspace,
                stage_name,
                image_tag,
                inputs,
                input_configs,
                output_configs,
                user_config=stage_config,
                git_sha=git_sha,
            )
        except Exception as e:
            # Mark failed immediately with error and re-raise
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                status=StageRunStatus.FAILED,
                completed_at=datetime.now(UTC).isoformat(),
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
            stage_version=stage_version_id,
            stage_version_num=stage_version_num,
            status=StageRunStatus.RUNNING,
            started_at=datetime.now(UTC),
            log_uri=str(self.dev_repo / ".goldfish" / "runs" / stage_run_id / "logs" / "output.log"),
            progress=StageRunProgress.LAUNCH,
            profile=stage_config.get("compute", {}).get("profile") if "compute" in stage_config else None,
            hints=stage_config.get("hints"),
            config=stage_config,
            inputs=inputs,
        )

        if wait:
            self.wait_for_completion(stage_run_id)
            refreshed = self.db.get_stage_run(stage_run_id)
            if refreshed:
                # Exclude fields we're overriding to avoid duplicate keyword args
                base_fields = info.model_dump(
                    exclude={"status", "completed_at", "log_uri", "artifact_uri", "progress", "outputs", "error"}
                )
                return StageRunInfo(
                    **base_fields,
                    status=refreshed.get("status", info.status),
                    completed_at=parse_optional_datetime(refreshed.get("completed_at")),
                    log_uri=refreshed.get("log_uri"),
                    artifact_uri=refreshed.get("artifact_uri"),
                    progress=refreshed.get("progress"),
                    outputs=json.loads(refreshed["outputs_json"]) if refreshed.get("outputs_json") else None,
                    error=refreshed.get("error"),
                )

        return info

    def _resolve_inputs(
        self,
        workspace: str,
        stage: StageDef,
        inputs_override: dict | None = None,
    ) -> tuple[dict[str, str], dict[str, dict]]:
        """Resolve input sources (dataset, signal, or override).

        Returns:
            (inputs, sources) tuple where:
            - inputs: {input_name: source_location}
            - sources: {input_name: {source_stage_run_id, source_stage_version_id, source_type}}
        """
        inputs: dict[str, str] = {}
        sources: dict[str, dict] = {}

        for input_name, input_def in stage.inputs.items():
            # Check for override
            if inputs_override and input_name in inputs_override:
                override_value = inputs_override[input_name]
                # Try to resolve as a registered source name first
                source = self.db.get_source(override_value)
                if source:
                    inputs[input_name] = source["gcs_location"]
                    sources[input_name] = {"source_type": "source", "source_name": override_value}
                else:
                    # Use as literal path
                    inputs[input_name] = override_value
                    sources[input_name] = {"source_type": "override"}
                continue

            # Resolve precedence: from_stage first, then dataset
            if input_def.from_stage:
                # Find output from previous stage
                # Get most recent successful run of source stage
                stage_runs = self.db.list_stage_runs(workspace_name=workspace, stage_name=input_def.from_stage)

                # Find completed run with the signal
                source_run = None
                for run in stage_runs:
                    if run["status"] == StageRunStatus.COMPLETED:
                        source_run = run
                        break

                if not source_run:
                    raise GoldfishError(f"No successful run found for stage '{input_def.from_stage}'")

                source_run_id = source_run["id"]

                # Get signal from that run
                signals = self.db.list_signals(stage_run_id=source_run_id)
                # Use explicit signal name if specified, otherwise default to input name
                signal_name = input_def.signal or input_def.name

                signal = None
                for s in signals:
                    if s["signal_name"] == signal_name:
                        signal = s
                        break

                if not signal:
                    available = [s["signal_name"] for s in signals]
                    raise GoldfishError(
                        f"Signal '{signal_name}' not found in stage '{input_def.from_stage}' "
                        f"(run {source_run_id}). Available signals: {available}"
                    )

                inputs[input_name] = signal["storage_location"]
                sources[input_name] = {
                    "source_type": "stage",
                    "source_stage_run_id": source_run_id,
                    "source_stage_version_id": source_run.get("stage_version_id"),
                }

            elif input_def.type == "dataset":
                # External dataset
                if self.dataset_registry is None:
                    raise GoldfishError("Dataset registry not configured")
                if input_def.dataset is None:
                    raise GoldfishError(f"Input '{input_name}' is type 'dataset' but no dataset specified")
                dataset = self.dataset_registry.get_dataset(input_def.dataset)
                inputs[input_name] = dataset.gcs_location
                sources[input_name] = {
                    "source_type": "dataset",
                    "dataset_name": input_def.dataset,
                }

            else:
                raise GoldfishError(f"Cannot resolve input: {input_name}")

        return inputs, sources

    def _auto_version(self, workspace: str, stage_name: str, reason: str | None) -> tuple[str, str]:
        """Create automatic version for workspace.

        With copy-based mounting, this syncs slot changes to branch before versioning,
        ensuring 100% provenance - every run executes against committed code.

        Returns:
            (version, git_sha) tuple - e.g., ("v1", "abc123def456")
        """
        # Find which slot has this workspace mounted
        slot = None
        for slot_info in self.workspace_manager.get_all_slots():
            if slot_info.workspace == workspace:
                slot = slot_info.slot
                break

        if slot is None:
            raise GoldfishError(
                f"Workspace '{workspace}' is not mounted to any slot. " f"Mount it to a slot first using mount()."
            )

        # Use sync_and_version to sync changes and create version tag
        # This is the provenance guard: all edits are committed before execution
        version, git_sha = self.workspace_manager.sync_and_version(slot, stage_name, reason)

        return version, git_sha

    def _find_stage(self, pipeline: PipelineDef, stage_name: str) -> StageDef:
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
        stage_version_id: int,
        inputs: dict,
        input_sources: dict[str, dict],
        config_override: dict | None,
        reason: str | None,
        reason_structured: dict | None,
        pipeline_run_id: str | None,
        pipeline_name: str | None,
        profile: str | None,
        hints: dict | None,
        config: dict | None,
    ):
        """Create stage run record in database with input lineage tracking."""
        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=version,
            stage_name=stage_name,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            config=config,
            inputs=inputs,
            reason=reason_structured,
            profile=profile,
            hints=hints,
            backend_type=self.config.jobs.backend,
            backend_handle=stage_run_id,  # provisional handle for cancel/logs
        )

        # Link stage run to its stage version
        self.db.update_stage_run_version(stage_run_id, stage_version_id)

        # Record input signals in lineage with source tracking
        for input_name, storage_location in inputs.items():
            source_meta = input_sources.get(input_name, {})
            source_stage_run_id = source_meta.get("source_stage_run_id")
            source_stage_version_id = source_meta.get("source_stage_version_id")

            # Use add_signal_with_source to properly track upstream lineage
            self.db.add_signal_with_source(
                stage_run_id=stage_run_id,
                signal_name=input_name,
                signal_type="input",
                storage_location=storage_location,
                source_stage_run_id=source_stage_run_id,
                source_stage_version_id=source_stage_version_id,
            )

    def _update_queued_stage_run(
        self,
        stage_run_id: str,
        version: str,
        stage_version_id: int,
        inputs: dict,
        input_sources: dict[str, dict],
        config: dict | None,
        profile: str | None,
        hints: dict | None,
    ):
        """Update a queued stage run record with resolved values.

        Called when processing a pre-created stage_run from the pipeline queue.
        Updates version, config, inputs, and records input lineage.
        """
        # Update the stage run with resolved values
        with self.db._conn() as conn:
            conn.execute(
                """
                UPDATE stage_runs
                SET version = ?,
                    config_json = ?,
                    inputs_json = ?,
                    profile = ?,
                    hints_json = ?,
                    backend_type = ?,
                    backend_handle = ?
                WHERE id = ?
                """,
                (
                    version,
                    json.dumps(config) if config else None,
                    json.dumps(inputs) if inputs else None,
                    profile,
                    json.dumps(hints) if hints else None,
                    self.config.jobs.backend,
                    stage_run_id,  # provisional handle for cancel/logs
                    stage_run_id,
                ),
            )

        # Link stage run to its stage version
        self.db.update_stage_run_version(stage_run_id, stage_version_id)

        # Record input signals in lineage with source tracking
        for input_name, storage_location in inputs.items():
            source_meta = input_sources.get(input_name, {})
            source_stage_run_id = source_meta.get("source_stage_run_id")
            source_stage_version_id = source_meta.get("source_stage_version_id")

            self.db.add_signal_with_source(
                stage_run_id=stage_run_id,
                signal_name=input_name,
                signal_type="input",
                storage_location=storage_location,
                source_stage_run_id=source_stage_run_id,
                source_stage_version_id=source_stage_version_id,
            )

    def _record_output_signals(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        gcs_base: str | None = None,
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
        run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id
        outputs_dir = run_dir / "outputs"

        outputs_payload: list[dict[str, Any]] = []

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
                    # Use appropriate suffix based on output type
                    output_type = output_def.type or "directory"
                    if output_type == "npy":
                        storage_location = f"{gcs_base.rstrip('/')}/{output_name}.npy"
                    elif output_type == "csv":
                        storage_location = f"{gcs_base.rstrip('/')}/{output_name}.csv"
                    else:
                        # directory, file, or other types use trailing /
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

                outputs_payload.append(
                    {
                        "name": output_name,
                        "type": output_def.type or "directory",
                        "storage_location": storage_location,
                        "from_stage_ref": f"{stage_name}/{output_name}",
                        "is_artifact": bool(output_def.artifact),
                        "metadata": getattr(output_def, "metadata", None),
                    }
                )

            # Attach outputs JSON to stage_run row (do not override status)
            if outputs_payload:
                conn.execute(
                    "UPDATE stage_runs SET outputs_json=? WHERE id=?",
                    (json.dumps(outputs_payload), stage_run_id),
                )

        # Auto-register artifacts
        for output in outputs_payload:
            if output["is_artifact"]:
                source_id: str = output["name"]  # simplistic name; could namespace later
                source_name = f"{stage_name}_{output['name']}"
                try:
                    existing = self.db.get_source(source_id)
                except Exception:
                    existing = None

                output_meta = output.get("metadata")
                if output_meta is None:
                    logger.warning("Skipping auto-registration for %s: missing metadata", source_id)
                    continue

                try:
                    validate_source_metadata(output_meta)
                except InvalidSourceMetadataError as exc:
                    logger.warning("Skipping auto-registration for %s: %s", source_id, exc.message)
                    continue

                metadata = output_meta
                description = metadata.get("description")
                size_bytes = metadata.get("source", {}).get("size_bytes")

                if existing:
                    existing_meta, status = parse_source_metadata(existing.get("metadata"))
                    if status == "ok" and existing_meta is not None:
                        mismatch_reasons = self._metadata_mismatch_reasons(existing_meta, metadata)
                        if mismatch_reasons:
                            logger.warning(
                                "Skipping auto-registration for %s: metadata mismatch (%s)",
                                source_id,
                                "; ".join(mismatch_reasons),
                            )
                            continue

                try:
                    if existing:
                        with self.db._conn() as conn:
                            conn.execute(
                                """
                                UPDATE sources
                                SET gcs_location=?, created_by=?, status=?, metadata=?, description=?, size_bytes=?
                                WHERE id=?
                                """,
                                (
                                    output["storage_location"],
                                    f"stage:{stage_run_id}",
                                    "available",
                                    json.dumps(metadata),
                                    description,
                                    size_bytes,
                                    source_id,
                                ),
                            )
                    else:
                        gcs_loc: str = output["storage_location"]
                        self.db.create_source(
                            source_id=source_id,
                            name=source_name,
                            gcs_location=gcs_loc,
                            created_by=f"stage:{stage_run_id}",
                            description=description,
                            size_bytes=size_bytes,
                            metadata=metadata,
                        )
                except Exception as e:
                    import logging

                    logging.getLogger(__name__).warning("Failed to auto-register artifact %s: %s", source_id, e)

    def _persist_logs(self, stage_run_id: str, logs: str) -> str:
        """Write logs to local run directory and return path."""
        run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id / "logs"
        run_dir.mkdir(parents=True, exist_ok=True)
        log_path = run_dir / "output.log"
        log_path.write_text(logs or "")
        return str(log_path)

    @staticmethod
    def _metadata_mismatch_reasons(existing: dict[str, Any], incoming: dict[str, Any]) -> list[str]:
        """Return human-readable reasons why metadata is incompatible."""
        reasons: list[str] = []

        def record(path: str, existing_value: Any, incoming_value: Any) -> None:
            if existing_value != incoming_value:
                reasons.append(f"{path} {existing_value!r} != {incoming_value!r}")

        record("schema_version", existing.get("schema_version"), incoming.get("schema_version"))

        existing_source = existing.get("source", {})
        incoming_source = incoming.get("source", {})
        record("source.format", existing_source.get("format"), incoming_source.get("format"))
        existing_size = existing_source.get("size_bytes")
        incoming_size = incoming_source.get("size_bytes")
        if existing_size is not None:
            record("source.size_bytes", existing_size, incoming_size)

        existing_schema = existing.get("schema", {})
        incoming_schema = incoming.get("schema", {})
        existing_kind = existing_schema.get("kind")
        incoming_kind = incoming_schema.get("kind")
        record("schema.kind", existing_kind, incoming_kind)
        if existing_kind != incoming_kind:
            return reasons

        if existing_kind == "tensor":
            record(
                "schema.primary_array",
                existing_schema.get("primary_array"),
                incoming_schema.get("primary_array"),
            )
            existing_arrays = existing_schema.get("arrays", {}) or {}
            incoming_arrays = incoming_schema.get("arrays", {}) or {}
            if set(existing_arrays.keys()) != set(incoming_arrays.keys()):
                reasons.append("schema.arrays keys mismatch")
                return reasons
            for name in existing_arrays:
                existing_arr = existing_arrays.get(name, {})
                incoming_arr = incoming_arrays.get(name, {})
                record(f"schema.arrays.{name}.role", existing_arr.get("role"), incoming_arr.get("role"))
                record(f"schema.arrays.{name}.dtype", existing_arr.get("dtype"), incoming_arr.get("dtype"))
                record(f"schema.arrays.{name}.shape", existing_arr.get("shape"), incoming_arr.get("shape"))
        elif existing_kind == "tabular":
            record("schema.columns", existing_schema.get("columns"), incoming_schema.get("columns"))
            record("schema.dtypes", existing_schema.get("dtypes"), incoming_schema.get("dtypes"))
            record("schema.row_count", existing_schema.get("row_count"), incoming_schema.get("row_count"))
        elif existing_kind == "file":
            record("schema.content_type", existing_schema.get("content_type"), incoming_schema.get("content_type"))

        return reasons

    def _collect_metrics(self, stage_run_id: str, backend: str) -> None:
        """Collect metrics from JSONL and store in database."""
        from goldfish.metrics.collector import MetricsCollector

        collector = MetricsCollector(self.db)

        # Determine metrics file location based on backend
        if backend == "local":
            # For local execution, metrics.jsonl is in the outputs directory
            metrics_file = (
                self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs" / ".goldfish" / "metrics.jsonl"
            )
        elif backend == "gce":
            # For GCE, download metrics.jsonl from GCS to local temp
            if not self.config.gcs or not self.config.gcs.bucket:
                logger.debug(f"No GCS bucket configured, skipping metrics collection for {stage_run_id}")
                return

            bucket = self.config.gcs.bucket
            bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
            gcs_path = f"{bucket_uri.rstrip('/')}/runs/{stage_run_id}/logs/metrics.jsonl"

            # Download to local temp directory
            import tempfile

            temp_dir = Path(tempfile.gettempdir()) / "goldfish_metrics" / stage_run_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = temp_dir / "metrics.jsonl"

            if not self._download_metrics_from_gcs(gcs_path, metrics_file):
                logger.debug(f"No metrics file found in GCS for {stage_run_id}")
                return
        else:
            logger.warning(f"Unknown backend {backend}, skipping metrics collection")
            return

        # Collect metrics from file
        collector.collect_from_file(stage_run_id, metrics_file)

    def _download_metrics_from_gcs(self, gcs_path: str, destination: Path) -> bool:
        """Download metrics.jsonl from GCS using the Python client.

        Returns True if download succeeded, False if the object doesn't exist.
        """
        client = self._get_gcs_client()
        if client is None:
            return False

        try:
            from google.api_core.exceptions import NotFound
        except Exception as exc:
            logger.warning("google-cloud-storage not available for metrics download: %s", exc)
            return False

        if not gcs_path.startswith("gs://"):
            logger.warning("Invalid GCS path: %s", gcs_path)
            return False

        bucket_name, blob_path = gcs_path[5:].split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        try:
            blob.reload()
        except NotFound:
            return False

        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            blob.download_to_filename(str(destination))
        except Exception as exc:
            logger.warning("Failed to download metrics from GCS: %s", exc)
            return False
        return True

    def _get_gcs_client(self):
        if self._gcs_client is not None:
            return self._gcs_client

        with self._gcs_client_lock:
            if self._gcs_client is not None:
                return self._gcs_client
            try:
                from google.cloud import storage
            except Exception as exc:
                logger.warning("google-cloud-storage not available: %s", exc)
                return None
            self._gcs_client = storage.Client()
            return self._gcs_client

    def _metrics_live_sync_enabled(self) -> bool:
        value = os.getenv("GOLDFISH_METRICS_LIVE_SYNC", "1").strip().lower()
        return value not in {"0", "false", "no", "off"}

    def _metrics_live_sync_interval(self) -> int:
        value = os.getenv("GOLDFISH_METRICS_LIVE_SYNC_INTERVAL", "15")
        try:
            parsed = int(value)
        except ValueError:
            return 15
        return max(5, min(300, parsed))

    def _get_metrics_sync_state(self, stage_run_id: str) -> _MetricsSyncState:
        with self._metrics_sync_lock:
            state = self._metrics_sync_state.get(stage_run_id)
            if state is None:
                state = _MetricsSyncState()
                self._metrics_sync_state[stage_run_id] = state
            return state

    def _sync_metrics_file_from_gcs(self, gcs_path: str, state: _MetricsSyncState) -> tuple[Path | None, int]:
        """Append new bytes from GCS metrics.jsonl into a local temp file."""
        client = self._get_gcs_client()
        if client is None:
            return None, state.offset

        try:
            from google.api_core.exceptions import NotFound
        except Exception as exc:
            logger.warning("google-cloud-storage not available for live metrics sync: %s", exc)
            return None, state.offset

        if not gcs_path.startswith("gs://"):
            logger.warning("Invalid GCS path: %s", gcs_path)
            return None, state.offset

        bucket_name, blob_path = gcs_path[5:].split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        try:
            blob.reload()
        except NotFound:
            return None, state.offset

        size = blob.size or 0
        local_path = state.temp_path
        if local_path is None:
            import tempfile

            temp_dir = Path(tempfile.gettempdir()) / "goldfish_metrics_live" / blob_path.replace("/", "_")
            temp_dir.mkdir(parents=True, exist_ok=True)
            local_path = temp_dir / "metrics.jsonl"
            state.temp_path = local_path

        if state.offset == 0 and local_path.exists():
            try:
                local_path.unlink()
            except Exception:
                pass

        if size < state.offset:
            # GCS object reset; start over
            state.offset = 0
            if local_path.exists():
                local_path.unlink()

        if size == state.offset:
            return local_path, state.offset

        try:
            data = blob.download_as_bytes(start=state.offset)
        except Exception as exc:
            logger.warning("Failed to download metrics bytes from GCS: %s", exc)
            return local_path, state.offset

        if data:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "ab") as f:
                f.write(data)

        return local_path, state.offset

    def sync_metrics_if_running(self, stage_run_id: str) -> None:
        """Best-effort incremental metrics sync for running stages."""
        if not self._metrics_live_sync_enabled():
            return

        row = self.db.get_stage_run(stage_run_id)
        if not row or row.get("status") != StageRunStatus.RUNNING:
            with self._metrics_sync_lock:
                self._metrics_sync_state.pop(stage_run_id, None)
            return

        state = self._get_metrics_sync_state(stage_run_id)
        if not state.sync_lock.acquire(blocking=False):
            return
        try:
            interval = self._metrics_live_sync_interval()
            now = time.time()
            if now - state.last_sync < interval:
                return

            backend = row.get("backend_type") or self.config.jobs.backend
            metrics_file: Path | None = None
            start_offset = state.offset

            if backend == "local":
                metrics_file = (
                    self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs" / ".goldfish" / "metrics.jsonl"
                )
            elif backend == "gce":
                if not self.config.gcs or not self.config.gcs.bucket:
                    return
                bucket = self.config.gcs.bucket
                bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
                gcs_path = f"{bucket_uri.rstrip('/')}/runs/{stage_run_id}/logs/metrics.jsonl"
                metrics_file, start_offset = self._sync_metrics_file_from_gcs(gcs_path, state)
            else:
                return

            if metrics_file is None or not metrics_file.exists():
                return

            from goldfish.metrics.collector import MetricsCollector

            collector = MetricsCollector(self.db)
            _, new_offset = collector.collect_from_file_incremental(
                stage_run_id,
                metrics_file,
                start_offset=start_offset,
                step_modes=state.step_modes,
                metric_names=state.metric_names,
                validated_names=state.validated_names,
            )

            state.offset = new_offset
            state.last_sync = now
        finally:
            state.sync_lock.release()

    def _build_docker_image(self, workspace: str, version: str, profile_name: str | None = None) -> str:
        """Build Docker image for this run.

        Args:
            workspace: Workspace name
            version: Version identifier
            profile_name: Optional profile name to determine base image

        Returns image tag (local for local backend, registry for GCE backend).
        """
        from goldfish.infra.profiles import resolve_base_image

        # Get workspace directory
        workspace_dir = self.workspace_manager.get_workspace_path(workspace)

        # Resolve base image from profile using pre-computed artifact_registry
        base_image = None
        if profile_name:
            profile = self.profile_resolver.resolve(profile_name)
            base_image = resolve_base_image(profile, self.artifact_registry)

        # Build image using DockerBuilder
        local_image_tag = self.docker_builder.build_image(
            workspace_dir=workspace_dir,
            workspace_name=workspace,
            version=version,
            use_cache=True,
            base_image=base_image,
        )

        # If using GCE backend, push to Artifact Registry
        backend = self.config.jobs.backend
        if backend == "gce":
            if not self.artifact_registry:
                raise GoldfishError(
                    "GCE backend requires artifact_registry. "
                    "Set gce.artifact_registry in goldfish.yaml or gce.project_id for auto-generation."
                )

            # Use pre-computed artifact_registry
            registry_url = self.artifact_registry

            # Push image to Artifact Registry
            registry_image_tag = self.docker_builder.push_image(
                local_tag=local_image_tag,
                registry_url=registry_url,
                workspace_name=workspace,
                version=version,
            )

            return registry_image_tag

        return local_image_tag

    def _ensure_artifact_registry(self, project_id: str, repo_name: str) -> None:
        """Ensure Artifact Registry repository exists, creating if needed.

        Args:
            project_id: GCP project ID
            repo_name: Repository name (e.g., "goldfish")
        """
        import subprocess

        # Check if repository exists
        check_result = subprocess.run(
            [
                "gcloud",
                "artifacts",
                "repositories",
                "describe",
                repo_name,
                f"--project={project_id}",
                "--location=us",
                "--format=value(name)",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if check_result.returncode == 0:
            logger.debug(f"Artifact Registry repository {repo_name} already exists")
            return

        # Create repository
        logger.info(f"Creating Artifact Registry repository: {repo_name} in project {project_id}")
        create_result = subprocess.run(
            [
                "gcloud",
                "artifacts",
                "repositories",
                "create",
                repo_name,
                f"--project={project_id}",
                "--location=us",
                "--repository-format=docker",
                "--description=Goldfish ML experiment images",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if create_result.returncode != 0:
            # Check if it was a race condition (already exists)
            if "already exists" in create_result.stderr.lower():
                logger.debug(f"Artifact Registry repository {repo_name} created by concurrent process")
                return
            raise GoldfishError(f"Failed to create Artifact Registry repository: {create_result.stderr}")

        logger.info(f"Created Artifact Registry repository: us-docker.pkg.dev/{project_id}/{repo_name}")

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
        except Exception:
            # Log warning but don't fail - config is optional
            return {}

    def _resolve_profile_from_config(self, stage_config: dict) -> dict | None:
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
            raise GoldfishError(f"Failed to resolve profile '{profile_name}': {e}") from e

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
        input_configs: dict | None = None,
        output_configs: dict | None = None,
        user_config: dict | None = None,
        git_sha: str | None = None,
    ):
        """Launch Docker container (local) or GCE instance."""
        backend = self.config.jobs.backend

        # Build stage config for goldfish.io
        # Start with user config (freeze_backbone, epochs, etc.) and add stage/inputs/outputs
        stage_config = dict(user_config) if user_config else {}
        stage_config["stage"] = stage_name
        stage_config["inputs"] = input_configs or inputs
        stage_config["outputs"] = output_configs or {}

        # Build Goldfish environment variables for metrics and provenance
        goldfish_env = {
            "GOLDFISH_PROJECT_NAME": self.config.project_name,
            "GOLDFISH_WORKSPACE": workspace,
            "GOLDFISH_STAGE": stage_name,
            "GOLDFISH_RUN_ID": stage_run_id,
            "GOLDFISH_OUTPUTS_DIR": "/mnt/outputs",
            "GOLDFISH_CONFIG": json.dumps(stage_config),
        }

        # Add git SHA if available
        if git_sha:
            goldfish_env["GOLDFISH_GIT_SHA"] = git_sha

        # Add metrics backend config if configured
        if self.config.metrics.backend:
            goldfish_env["GOLDFISH_METRICS_BACKEND"] = self.config.metrics.backend

            # Add W&B-specific config if backend is wandb
            if self.config.metrics.backend == "wandb" and self.config.metrics.wandb:
                wandb_config = self.config.metrics.wandb
                if wandb_config.get("project"):
                    goldfish_env["GOLDFISH_WANDB_PROJECT"] = wandb_config["project"]
                if wandb_config.get("group"):
                    goldfish_env["GOLDFISH_WANDB_GROUP"] = wandb_config["group"]
                if wandb_config.get("entity"):
                    goldfish_env["GOLDFISH_WANDB_ENTITY"] = wandb_config["entity"]

        # Passthrough WANDB_API_KEY from host environment if set
        import os

        if "WANDB_API_KEY" in os.environ:
            goldfish_env["WANDB_API_KEY"] = os.environ["WANDB_API_KEY"]

        if backend == "local":
            # Create work directory for this run
            run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id
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

            # Launch container using LocalExecutor
            self.local_executor.launch_container(
                image_tag=image_tag,
                stage_run_id=stage_run_id,
                entrypoint_script=entrypoint_script,
                stage_config=stage_config,
                work_dir=run_dir,
                inputs_dir=inputs_dir,
                outputs_dir=outputs_dir,
                goldfish_env=goldfish_env,
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

            # Launch on GCE (gpu_preference from config is passed via GCELauncher init)
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
                stage_config=stage_config,
                work_dir=self.dev_repo / ".goldfish" / "runs" / stage_run_id,
                machine_type=machine_type,
                gpu_type=gpu_type,
                gpu_count=gpu_count,
                zones=zones,
                use_capacity_search=use_capacity_search,
                goldfish_env=goldfish_env,
            )
        else:
            raise GoldfishError(f"Backend {backend} not supported for launch")

    def _finalize_stage_run(self, stage_run_id: str, backend: str, status: str) -> None:
        """Handle terminal status: record outputs, fetch logs, update status."""
        # CAS guard against double-finalize (only finalize if still in non-terminal state)
        terminal_statuses = (StageRunStatus.COMPLETED, StageRunStatus.FAILED, StageRunStatus.CANCELED)
        with self.db._conn() as conn:
            updated = conn.execute(
                "UPDATE stage_runs SET status=?, completed_at=? WHERE id=? AND status NOT IN (?, ?, ?)",
                (status, datetime.now(UTC).isoformat(), stage_run_id, *terminal_statuses),
            ).rowcount
        if updated == 0:
            return

        # Re-read fresh row after CAS
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

        if status == StageRunStatus.COMPLETED:
            try:
                self._record_output_signals(stage_run_id, workspace, stage_name_from_db, gcs_base=gcs_base)
            except Exception as e:
                # If outputs fail to record, mark run failed and surface error
                error_msg = f"Output recording failed: {e}"
                self.db.update_stage_run_status(
                    stage_run_id=stage_run_id,
                    status=StageRunStatus.FAILED,
                    completed_at=datetime.now(UTC).isoformat(),
                    error=error_msg,
                    progress=StageRunProgress.FINALIZING,
                )
                raise

        logs = ""
        try:
            if backend == "local":
                logs = self.local_executor.get_container_logs(stage_run_id, tail_lines=STAGE_LOG_TAIL_FOR_FINALIZE)
            elif backend == "gce":
                logs = self.gce_launcher.get_instance_logs(stage_run_id, tail_lines=STAGE_LOG_TAIL_FOR_FINALIZE)
                if not logs:
                    logs = "[GCE logs unavailable - instance may have been deleted or logs not synced]"
        except Exception as e:
            logs = f"[Error fetching logs: {e}]"

        if backend == "gce" and self.config.gcs and self.config.gcs.bucket:
            bucket = self.config.gcs.bucket
            bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
            log_uri = f"{bucket_uri.rstrip('/')}/runs/{stage_run_id}/logs/train.log"
            # Also persist a local copy for quick access/debugging
            if logs is not None:
                try:
                    self._persist_logs(stage_run_id, logs)
                except Exception:
                    pass
        else:
            log_uri = self._persist_logs(stage_run_id, logs) if logs is not None else None

        # Collect metrics from JSONL and store in database
        try:
            self._collect_metrics(stage_run_id, backend)
        except Exception as e:
            # Log warning but don't fail the run if metrics collection fails
            logger.warning(f"Failed to collect metrics for {stage_run_id}: {e}")

        with self._metrics_sync_lock:
            self._metrics_sync_state.pop(stage_run_id, None)

        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            status=status,
            completed_at=datetime.now(UTC).isoformat(),
            log_uri=log_uri,
            error=(logs[-STAGE_LOG_TAIL_FOR_FINALIZE:] if (status == StageRunStatus.FAILED and logs) else None),
            progress=StageRunProgress.FINALIZING,
        )

    def wait_for_completion(self, stage_run_id: str, poll_interval: int = 5, timeout: int = 3600) -> str:
        """Wait for stage run to complete.

        Polls container status and updates database.

        Args:
            stage_run_id: Stage run identifier
            poll_interval: Seconds between polls (default 5)
            timeout: Maximum seconds to wait (default 3600 = 1 hour)

        Returns:
            Final status: StageRunStatus.COMPLETED or FAILED

        Raises:
            GoldfishError: If timeout exceeded or container not found
        """

        backend = self.config.jobs.backend

        start = time.time()
        last_log: float = 0.0
        not_found_timeout = int(os.getenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "300"))

        while True:
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise GoldfishError(f"Stage run {stage_run_id} timed out after {timeout} seconds")
            if backend == "local":
                status = self.local_executor.get_container_status(stage_run_id)

                if status == StageRunStatus.RUNNING:
                    # Still running, update status in db
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id, status=StageRunStatus.RUNNING, progress=StageRunProgress.RUNNING
                    )
                    interval = self._poll_interval(int(elapsed))
                    time.sleep(interval)
                    continue

                elif status in (StageRunStatus.COMPLETED, StageRunStatus.FAILED):
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id,
                        status=StageRunStatus.RUNNING,
                        progress=StageRunProgress.FINALIZING,
                    )
                    self._finalize_stage_run(stage_run_id, backend, status)
                    return status

                elif status == "not_found":
                    raise GoldfishError(f"Container {stage_run_id} not found")

                else:
                    # Unknown status
                    raise GoldfishError(f"Unknown container status: {status}")

            elif backend == "gce":
                status = self.gce_launcher.get_instance_status(stage_run_id)

                if status == StageRunStatus.RUNNING:
                    # Still running, update status in db
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id, status=StageRunStatus.RUNNING, progress=StageRunProgress.RUNNING
                    )
                    interval = self._poll_interval(int(elapsed))
                    time.sleep(interval)
                    continue

                elif status in (StageRunStatus.COMPLETED, StageRunStatus.FAILED):
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id,
                        status=StageRunStatus.RUNNING,
                        progress=StageRunProgress.FINALIZING,
                    )
                    self._finalize_stage_run(stage_run_id, backend, status)
                    return status

                elif status == "not_found":
                    now = time.time()
                    if now - last_log >= 60:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.info(
                            f"Instance {stage_run_id} not yet visible in GCE API "
                            f"(elapsed: {int(elapsed)}s, may be launching or searching capacity)"
                        )
                        last_log = now
                    if elapsed >= not_found_timeout:
                        raise GoldfishError(
                            f"GCE instance {stage_run_id} not found after {not_found_timeout} seconds; abandoning run"
                        )
                    time.sleep(poll_interval)
                    continue

                else:
                    # Unknown status
                    raise GoldfishError(f"Unknown instance status: {status}")

            else:
                raise GoldfishError(f"Backend {backend} not supported for monitoring")

        # Should not reach here

    def refresh_status_once(self, stage_run_id: str) -> str | None:
        """Single backend check to advance status/logs/outputs without blocking."""
        backend = self.config.jobs.backend
        terminal_statuses = (StageRunStatus.COMPLETED, StageRunStatus.FAILED, StageRunStatus.CANCELED)

        if backend == "local":
            status = self.local_executor.get_container_status(stage_run_id)
            if status == StageRunStatus.RUNNING:
                # CAS: only update if not in terminal state
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE stage_runs SET status=?, progress=? WHERE id=? AND status NOT IN (?, ?, ?)",
                        (StageRunStatus.RUNNING, StageRunProgress.RUNNING, stage_run_id, *terminal_statuses),
                    )
            elif status in (StageRunStatus.COMPLETED, StageRunStatus.FAILED):
                # Guard against double-finalize by only doing it if not terminal
                current = self.db.get_stage_run(stage_run_id)
                if current and current.get("status") not in terminal_statuses:
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id,
                        status=StageRunStatus.RUNNING,
                        progress=StageRunProgress.FINALIZING,
                    )
                    self._finalize_stage_run(stage_run_id, backend, status)
            return status

        if backend == "gce":
            status = self.gce_launcher.get_instance_status(stage_run_id)
            if status == StageRunStatus.RUNNING:
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE stage_runs SET status=?, progress=? WHERE id=? AND status NOT IN (?, ?, ?)",
                        (StageRunStatus.RUNNING, StageRunProgress.RUNNING, stage_run_id, *terminal_statuses),
                    )
            elif status in (StageRunStatus.COMPLETED, StageRunStatus.FAILED):
                current = self.db.get_stage_run(stage_run_id)
                if current and current.get("status") not in terminal_statuses:
                    self.db.update_stage_run_status(
                        stage_run_id=stage_run_id,
                        status=StageRunStatus.RUNNING,
                        progress=StageRunProgress.FINALIZING,
                    )
                    self._finalize_stage_run(stage_run_id, backend, status)
            return status

        return None

    # --- Pre-run Review Methods ---

    def _perform_pre_run_review(
        self,
        workspace: str,
        stage_name: str,
        pipeline: PipelineDef,
        reason_structured: dict | None,
        git_sha: str,
    ) -> RunReview | None:
        """Perform pre-run review using Claude Agent SDK.

        Args:
            workspace: Workspace name
            stage_name: Stage to review
            pipeline: Pipeline definition (for context)
            reason_structured: Structured RunReason dict
            git_sha: Current git SHA for diff calculation

        Returns:
            RunReview with findings, or None if review couldn't be performed
        """
        from goldfish.pre_run_review import review_before_run

        # Get workspace slot path
        try:
            slot_path = self.workspace_manager.get_workspace_path(workspace)
        except GoldfishError:
            logger.warning(f"Cannot review: workspace '{workspace}' not mounted")
            return None

        # Convert reason_structured dict to RunReason model
        run_reason: RunReason | None = None
        if reason_structured:
            try:
                run_reason = RunReason(**reason_structured)
            except Exception as e:
                logger.warning(f"Failed to parse reason_structured: {e}")

        # Get diff from last successful run
        diff_text = self._get_diff_from_last_success(workspace, stage_name, git_sha)

        # Run the review (async -> sync bridge with event loop detection)
        try:
            review = self._run_async_review(
                review_before_run(
                    config=self.config.pre_run_review,
                    workspace_path=slot_path,
                    dev_repo_path=self.dev_repo,
                    stages=[stage_name],  # Review the specific stage
                    reason=run_reason,
                    diff_text=diff_text,
                    db=self.db,
                )
            )
            # Log review result for visibility (not silent)
            if review:
                if review.approved:
                    logger.info(f"Pre-run review passed for {stage_name}: {review.summary}")
                else:
                    logger.warning(f"Pre-run review blocked {stage_name}: {review.summary}")
            return review
        except (KeyboardInterrupt, SystemExit):
            # Let cancellations propagate
            raise
        except Exception as e:
            logger.error(f"Pre-run review failed for {stage_name}: {e}", exc_info=True)
            return None

    def _run_async_review(self, coro: Coroutine[Any, Any, RunReview]) -> RunReview:
        """Run async review coroutine, handling existing event loops.

        This bridges async review code to sync stage execution.

        Design notes:
        - Normal case: StageExecutor.run_stage() is called from sync context
          (CLI or sync MCP server), so asyncio.run() works directly.
        - Edge case: If called from within an async context (e.g., async MCP
          server), we fall back to ThreadPoolExecutor which creates an isolated
          event loop. This is safe because PreRunReviewer.review() is self-contained
          (only does file I/O and HTTP calls, no shared async state).
        - The thread-based fallback adds ~10ms overhead but avoids deadlocks.
        """
        try:
            # Check if there's already a running event loop
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop - safe to use asyncio.run()
            result: RunReview = asyncio.run(coro)
            return result

        # Already in an async context - run in isolated thread to avoid conflicts
        # This is rare (only if MCP server is async) but handled safely
        logger.debug("Running pre-run review in separate thread (async context detected)")
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(asyncio.run, coro)
            thread_result: RunReview = future.result(timeout=self.config.pre_run_review.timeout_seconds + 10)
            return thread_result

    def _get_diff_from_last_success(
        self,
        workspace: str,
        stage_name: str,
        current_sha: str,
    ) -> str:
        """Get diff from the last successful run of this stage.

        Args:
            workspace: Workspace name
            stage_name: Stage name
            current_sha: Current git SHA

        Returns:
            Diff text, or empty string if no previous run
        """
        # Find last successful run
        last_run = self.db.get_latest_completed_stage_run(workspace, stage_name)
        if not last_run:
            return ""

        last_version = last_run.get("version")
        if not last_version:
            return ""

        # Get the git SHA for the last version
        version_info = self.db.get_version(workspace, last_version)
        if not version_info:
            return ""

        last_sha = version_info.get("git_sha")
        if not last_sha or last_sha == current_sha:
            return ""

        # Get diff using git layer
        try:
            diff_result = self.workspace_manager.git.diff_shas(last_sha, current_sha)
            diff_text: str = diff_result.get("diff_text", "")
            return diff_text
        except Exception as e:
            logger.warning(f"Failed to get diff: {e}")
            return ""

    def _create_blocked_stage_run(
        self,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_name: str,
        review: RunReview,
        reason: str | None,
        reason_structured: dict | None,
        pipeline_run_id: str | None,
        pipeline_name: str | None,
    ) -> StageRunInfo:
        """Create a failed stage run record for a blocked review.

        Args:
            stage_run_id: Stage run ID
            workspace: Workspace name
            version: Workspace version
            stage_name: Stage name
            review: The blocking RunReview
            reason: String reason
            reason_structured: Structured RunReason dict
            pipeline_run_id: Parent pipeline run ID
            pipeline_name: Pipeline name

        Returns:
            StageRunInfo with failed status
        """
        now = datetime.now(UTC).isoformat()

        # Build error message with review summary (not full review to keep it readable)
        # Full review is available in the logs
        error_msg = f"Pre-run review blocked: {review.summary}"
        if review.error_count > 0:
            error_details = []
            for issue in review.issues:
                if issue.severity == ReviewSeverity.ERROR:
                    loc = f"{issue.file}:{issue.line}" if issue.file and issue.line else (issue.file or "")
                    error_details.append(f"  - {loc}: {issue.message}" if loc else f"  - {issue.message}")
            if error_details:
                error_msg += "\n\nErrors:\n" + "\n".join(error_details[:5])  # Show first 5

        # Use proper database method for consistency
        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=version,
            stage_name=stage_name,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            reason=reason_structured,
            backend_type=None,  # Not executed - blocked by review
        )

        # Update status to FAILED with error message
        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            status=StageRunStatus.FAILED,
            error=error_msg,
        )

        logger.warning(f"Stage run {stage_run_id} blocked by pre-run review: {review.summary}")

        return StageRunInfo(
            stage_run_id=stage_run_id,
            pipeline_run_id=pipeline_run_id,
            workspace=workspace,
            pipeline=pipeline_name,
            version=version,
            stage=stage_name,
            status=StageRunStatus.FAILED,
            started_at=parse_optional_datetime(now),
            completed_at=parse_optional_datetime(now),
            error=error_msg,
        )
