"""Stage execution engine for Goldfish (implementation).

This module contains the bulk of the stage execution logic. The public entry
point remains `goldfish.jobs.stage_executor`, which provides a thin orchestrator
wrapper for the architecture redesign.
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from goldfish.cloud.protocols import ImageBuilder, ObjectStorage, RunBackend, SignalBus

import yaml

from goldfish.cloud.contracts import BackendStatus, RunHandle, RunSpec, RunStatus, StorageURI
from goldfish.cloud.factory import AdapterFactory, resolve_compute_profile, resolve_profile_base_image
from goldfish.config import GoldfishConfig
from goldfish.datasets.registry import DatasetRegistry
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, NotFoundError
from goldfish.experiment_model.records import ExperimentRecordManager
from goldfish.infra.docker_builder import DockerBuilder, compute_build_context_hash
from goldfish.models import (
    PipelineDef,
    ReviewSeverity,
    RunReason,
    RunReview,
    StageDef,
    StageRunInfo,
)
from goldfish.pipeline.manager import PipelineManager
from goldfish.pipeline.validator import validate_pipeline_run
from goldfish.state_machine import (
    EventContext as SMEventContext,
)
from goldfish.state_machine import (
    FinalizationTracker,
    StageEvent,
    StageState,
    TerminationCause,
)
from goldfish.state_machine import (
    transition as sm_transition,
)
from goldfish.state_machine.transitions import TERMINAL_STATES
from goldfish.svs.contract import resolve_config_params
from goldfish.svs.manifest import read_svs_manifests
from goldfish.svs.post_run import run_post_run_review
from goldfish.utils import parse_optional_datetime
from goldfish.utils.config_hash import compute_config_hash
from goldfish.validation import InvalidSourceMetadataError, parse_source_metadata, validate_source_metadata
from goldfish.workspace.manager import WorkspaceManager

logger = logging.getLogger(__name__)

STAGE_LOG_TAIL_FOR_FINALIZE = int(os.getenv("GOLDFISH_FINALIZE_LOG_TAIL", "1000"))

REDACTION_PATTERNS = [
    (r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s]+", r"\1=[REDACTED]"),
    (r"(?i)bearer\s+[A-Za-z0-9._-]+", "Bearer [REDACTED]"),
    (r"sk-[a-zA-Z0-9]{20,}", "[REDACTED_API_KEY]"),
    (r"ghp_[A-Za-z0-9]{36}", "[REDACTED_GITHUB_TOKEN]"),
]

# Pattern to match stage run IDs in paths (e.g., "stage-abc123" or "stage-deadbeef012345")
_STAGE_RUN_ID_PATTERN = re.compile(r"stage-[a-f0-9]+")


class _ProfileResolverProxy:
    """Compatibility shim for compute profile resolution.

    Keeps `StageExecutor.profile_resolver.resolve(name)` available while preserving
    the Core → cloud.factory boundary (no direct adapter imports).
    """

    def __init__(self, config: GoldfishConfig):
        self._config = config

    def resolve(self, name: str) -> dict[str, Any]:
        return resolve_compute_profile(self._config, name)


def _extract_stage_run_id_from_path(path: str) -> str | None:
    """Extract stage run ID from a storage path if present.

    Looks for patterns like 'stage-abc123' in paths such as:
    - <scheme>://bucket/artifacts/stage-abc123/outputs/model
    - /mnt/outputs/stage-deadbeef/features.npy

    Args:
        path: GCS path or local path that may contain a stage run ID

    Returns:
        The stage run ID if found, None otherwise
    """
    match = _STAGE_RUN_ID_PATTERN.search(path)
    return match.group(0) if match else None


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
        *,
        # Protocol injection for testing and abstraction layer
        storage: "ObjectStorage | None" = None,
        run_backend: "RunBackend | None" = None,
        signal_bus: "SignalBus | None" = None,
        image_builder: "ImageBuilder | None" = None,
    ):
        self.db = db
        self.config = config
        self.workspace_manager = workspace_manager
        self.pipeline_manager = pipeline_manager
        self.project_root = project_root
        self.dataset_registry = dataset_registry
        self.profile_resolver = _ProfileResolverProxy(config)

        # Dev repo contains all Goldfish runtime artifacts (.goldfish/, runs/, etc.)
        self.dev_repo = config.get_dev_repo_path(project_root)

        # Initialize execution infrastructure
        self.docker_builder = DockerBuilder(config, db=db)

        # Compute artifact_registry for base image resolution and image pushing
        self.artifact_registry: str | None = None

        if config.gce:
            # Resolve artifact_registry from config - required for GCE backend
            self.artifact_registry = config.gce.effective_artifact_registry
            if not self.artifact_registry:
                raise GoldfishError(
                    "GCE backend requires artifact_registry configuration. "
                    "Add to goldfish.yaml:\n"
                    "  gce:\n"
                    "    artifact_registry: <region>-docker.pkg.dev/<project>/<repo>\n"
                    "Example regions: us-docker, europe-docker, asia-docker"
                )

            # Zones are required for GCE backend - no US default
            if not config.gce.zones:
                raise GoldfishError(
                    "GCE backend requires zones configuration. "
                    "Add to goldfish.yaml:\n"
                    "  gce:\n"
                    "    zones:\n"
                    "      - <region>-<zone>  # e.g., europe-west4-a\n"
                    "Configure zones in regions where you have GPU quota."
                )

        # Initialize cloud abstraction layer via AdapterFactory
        # This provides protocol-based adapters for storage, compute, and signaling
        self._adapter_factory = AdapterFactory(config)
        # Support protocol injection for testing - if provided, use directly
        # Otherwise, lazily initialize via factory
        self._storage: ObjectStorage | None = storage
        self._run_backend: RunBackend | None = run_backend
        self._signal_bus: SignalBus | None = signal_bus
        self._image_builder: ImageBuilder | None = image_builder

        # Live metrics sync state (per run)
        self._metrics_sync_state: dict[str, _MetricsSyncState] = {}
        self._metrics_sync_lock = threading.Lock()
        self._svs_sync_state: dict[str, float] = {}
        self._svs_sync_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._refreshing_runs: set[str] = set()

    @classmethod
    def create(
        cls,
        db: Database,
        config: GoldfishConfig,
        workspace_manager: WorkspaceManager,
        pipeline_manager: PipelineManager,
        project_root: Path,
        dataset_registry: DatasetRegistry | None = None,
    ) -> "StageExecutor":
        """Factory method that creates StageExecutor with appropriate adapters.

        This method creates the executor with protocol adapters selected based
        on config.jobs.backend (local vs gce). Use this for production code.

        For testing, use the constructor directly with injected adapters.

        Args:
            db: Database instance
            config: Goldfish configuration
            workspace_manager: Workspace manager
            pipeline_manager: Pipeline manager
            project_root: Project root path
            dataset_registry: Optional dataset registry

        Returns:
            StageExecutor with adapters configured based on backend type
        """
        factory = AdapterFactory(config)

        return cls(
            db=db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=project_root,
            dataset_registry=dataset_registry,
            storage=factory.create_storage(root=project_root / ".local_gcs"),
            run_backend=factory.create_run_backend(),
            signal_bus=factory.create_signal_bus(),
        )

    @property
    def storage(self) -> "ObjectStorage":
        """Get the storage adapter (lazily initialized or injected)."""
        if self._storage is None:
            self._storage = self._adapter_factory.create_storage(root=self.project_root / ".local_gcs")
        assert self._storage is not None  # Guaranteed by factory
        return self._storage

    @property
    def run_backend(self) -> "RunBackend":
        """Get the run backend adapter (lazily initialized or injected)."""
        if self._run_backend is None:
            self._run_backend = self._adapter_factory.create_run_backend()
        assert self._run_backend is not None  # Guaranteed by factory
        return self._run_backend

    @property
    def signal_bus(self) -> "SignalBus":
        """Get the signal bus adapter (lazily initialized or injected)."""
        if self._signal_bus is None:
            self._signal_bus = self._adapter_factory.create_signal_bus()
        assert self._signal_bus is not None  # Guaranteed by factory
        return self._signal_bus

    @property
    def image_builder(self) -> "ImageBuilder":
        """Get the image builder adapter (lazily initialized or injected)."""
        if self._image_builder is None:
            self._image_builder = self._adapter_factory.create_image_builder(db=self.db)
        assert self._image_builder is not None  # Guaranteed by factory
        return self._image_builder

    def _get_run_handle(self, stage_run_id: str) -> RunHandle:
        """Get a RunHandle for a stage run from the database.

        Args:
            stage_run_id: Stage run ID to get handle for.

        Returns:
            RunHandle with backend_type, backend_handle, and zone.

        Raises:
            GoldfishError: If stage run not found.
        """
        row = self.db.get_stage_run(stage_run_id)
        if not row:
            raise GoldfishError(f"Stage run '{stage_run_id}' not found")

        backend_type = row.get("backend_type") or self.config.jobs.backend
        backend_handle = row.get("backend_handle") or stage_run_id
        zone = row.get("instance_zone")

        return RunHandle(
            stage_run_id=stage_run_id,
            backend_type=backend_type,
            backend_handle=backend_handle,
            zone=zone,
        )

    def _backend_status_to_stage_state(self, backend_status: BackendStatus) -> StageState | str:
        """Convert BackendStatus to StageState.

        Args:
            backend_status: Status from run_backend.get_status()

        Returns:
            StageState for terminal states, or string status for running/unknown.
        """
        status = backend_status.status
        if status == RunStatus.RUNNING:
            return StageState.RUNNING
        elif status == RunStatus.COMPLETED:
            return StageState.COMPLETED
        elif status == RunStatus.FAILED:
            return StageState.FAILED
        elif status == RunStatus.TERMINATED:
            return StageState.TERMINATED
        elif status == RunStatus.CANCELED:
            return StageState.CANCELED
        elif status == RunStatus.PREPARING:
            return StageState.LAUNCHING
        elif status == RunStatus.PENDING:
            return StageState.PREPARING  # Map PENDING to PREPARING state
        else:
            # Unknown status
            return "unknown"

    def _get_bucket_uri(self) -> StorageURI | None:
        """Get StorageURI for the configured GCS bucket.

        Returns:
            StorageURI pointing to the bucket root, or None if not configured.
        """
        if not self.config.gcs or not self.config.gcs.bucket:
            return None
        bucket = self.config.gcs.bucket
        try:
            bucket_root = StorageURI.parse(bucket)
        except ValueError:
            bucket_root = StorageURI("gs", bucket, "")
        return StorageURI(bucket_root.scheme, bucket_root.bucket, "")

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
        experiment_group: str | None = None,
        results_spec: dict | None = None,
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
            experiment_group: Optional experiment group for filtering
            results_spec: Expected results specification for experiment tracking

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
        # GCS access is validated during storage operations, no pre-check needed

        # 1. Auto-version workspace (returns version and git SHA)
        version, git_sha = self._auto_version(workspace, stage_name, reason)

        # 2. Load pipeline and stage
        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        stage = self._find_stage(pipeline, stage_name)

        # 2a. Establish stage_run_id early for preflight tracking
        if stage_run_id is None:
            stage_run_id = f"stage-{uuid4().hex[:8]}"

        # 2b. SVS preflight validation (always run when SVS enabled)
        preflight_errors: list[str] = []
        preflight_warnings: list[str] = []
        if self.config.svs.enabled:
            workspace_path = self.workspace_manager.get_workspace_path(workspace)
            preflight = validate_pipeline_run(
                workspace_name=workspace,
                workspace_path=workspace_path,
                db=self.db,
                stages=[stage_name],
                pipeline_name=pipeline_name,
                inputs_override=inputs_override or {},
                config=self.config,
                config_override=config_override,
            )
            preflight_errors = list(preflight.get("validation_errors", []))
            preflight_warnings = list(preflight.get("warnings", []))

            if preflight_errors:
                return self._create_preflight_blocked_stage_run(
                    stage_run_id=stage_run_id,
                    workspace=workspace,
                    version=version,
                    stage_name=stage_name,
                    errors=preflight_errors,
                    warnings=preflight_warnings,
                    reason_structured=reason_structured,
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=pipeline_name,
                )

        # 2c. Load stage config and apply override early
        stage_config = self._load_stage_config(workspace, stage_name) or {}
        if config_override:
            stage_config.update(config_override)

        # 2d. Compute stage version
        config_hash = compute_config_hash(stage_config)
        stage_version_id, stage_version_num, _ = self.db.get_or_create_stage_version(
            workspace=workspace,
            stage=stage_name,
            git_sha=git_sha,
            config_hash=config_hash,
        )

        # 2e. Create placeholder record IMMEDIATELY (to satisfy FKs for review/audit)
        # Always create the stage_runs row, even if stage_run_id was pre-generated.
        # The pre-generated ID is just stored in pipeline_stage_queue, not stage_runs.
        record_id = self._create_stage_run_record(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=version,
            stage_name=stage_name,
            stage_version_id=stage_version_id,
            inputs={},  # Resolved later
            input_sources={},
            config_override=config_override,
            reason=reason,
            reason_structured=reason_structured,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            profile=None,
            hints=None,
            config=stage_config,
            preflight_errors=preflight_errors,
            preflight_warnings=preflight_warnings,
            experiment_group=experiment_group,
            results_spec=results_spec,
        )

        # 3. Resolve inputs
        inputs, input_sources, input_context = self._resolve_inputs(
            workspace, stage, inputs_override, pipeline_run_id=pipeline_run_id
        )

        # 4. Pre-run review
        review: RunReview | None = None
        if self.config.pre_run_review.enabled and not skip_review:
            review = self._perform_pre_run_review(
                workspace=workspace,
                stage_name=stage_name,
                pipeline=pipeline,
                reason_structured=reason_structured,
                git_sha=git_sha,
                input_context=input_context,
                config_override=config_override,
            )
            if review:
                self._record_pre_run_review(stage_run_id, review)

            if review and review.has_blocking_issues:
                # Update status to FAILED (record already exists)
                # Build error message with review summary and specific issues
                error_msg = f"Pre-run review blocked: {review.summary}"
                if review.error_count > 0:
                    error_details = []
                    for issue in review.issues:
                        if issue.severity == ReviewSeverity.ERROR:
                            loc = f"{issue.file}:{issue.line}" if issue.file and issue.line else (issue.file or "")
                            error_details.append(f"  - {loc}: {issue.message}" if loc else f"  - {issue.message}")
                    if error_details:
                        error_msg += "\n\nErrors:\n" + "\n".join(error_details[:5])

                # State machine: PREPARING → FAILED (SVS_BLOCK)
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.SVS_BLOCK,
                    SMEventContext(timestamp=datetime.now(UTC), source="executor", error_message=error_msg),
                )

                # Update non-state metadata (state machine handles state via SVS_BLOCK transition above)
                self.db.update_stage_run_status(
                    stage_run_id,
                    completed_at=datetime.now(UTC).isoformat(),
                    error=error_msg,
                )
                return StageRunInfo(
                    stage_run_id=stage_run_id,
                    pipeline_run_id=pipeline_run_id,
                    record_id=record_id,
                    workspace=workspace,
                    pipeline=pipeline_name,
                    version=version,
                    stage=stage_name,
                    status=StageState.FAILED,
                    state=StageState.FAILED.value,
                    started_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                    error=error_msg,
                )

        # 5. Update record with resolved values
        # Note: experiment record was already created in step 2e
        self._update_queued_stage_run(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=version,
            stage_version_id=stage_version_id,
            inputs=inputs,
            input_sources=input_sources,
            config=stage_config,
            profile=stage_config.get("compute", {}).get("profile"),
            hints=stage_config.get("hints"),
            preflight_warnings=preflight_warnings,
            preflight_errors=preflight_errors,
            create_experiment_record=False,  # Already created in step 2e
            experiment_group=experiment_group,
        )

        try:
            # State machine: PREPARING → BUILDING (BUILD_START)
            sm_transition(
                self.db,
                stage_run_id,
                StageEvent.BUILD_START,
                SMEventContext(timestamp=datetime.now(UTC), source="executor"),
            )

            # 6. Build Docker image (use profile's base image)
            profile_name = stage_config.get("compute", {}).get("profile")
            image_tag, build_context_hash = self._build_docker_image(workspace, version, profile_name=profile_name)

            # Record the exact image used for this run (cache key + tag).
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                build_context_hash=build_context_hash,
                image_tag=image_tag,
            )

            # State machine: BUILDING → LAUNCHING (BUILD_OK)
            sm_transition(
                self.db,
                stage_run_id,
                StageEvent.BUILD_OK,
                SMEventContext(timestamp=datetime.now(UTC), source="executor"),
            )

            # 7. Launch container
            # Build input config with format info for goldfish.io
            input_configs = {}
            for input_name, input_def in stage.inputs.items():
                input_configs[input_name] = {
                    "location": inputs.get(input_name, ""),
                    "format": input_def.format or input_def.type,  # Use format override or fall back to type
                    "type": input_def.type,
                    "schema": resolve_config_params(input_def.output_schema, stage_config)
                    if input_def.output_schema is not None
                    else None,
                }

            # Build output config with format info
            output_configs = {}
            for output_name, output_def in stage.outputs.items():
                output_configs[output_name] = {
                    "format": output_def.format or output_def.type,
                    "type": output_def.type,
                    "schema": resolve_config_params(output_def.output_schema, stage_config)
                    if output_def.output_schema is not None
                    else None,
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
                run_reason=reason_structured,
                runtime=stage.runtime,
                entrypoint=stage.entrypoint,
                config_override=config_override,
                inputs_override=inputs_override,
                pipeline_name=pipeline_name,
                results_spec=results_spec,
            )
        except Exception as e:
            error_msg = str(e)
            # Best-effort: if we're still in BUILDING, this is BUILD_FAIL; otherwise LAUNCH_FAIL.
            # We don't want to read the DB state here (avoid TOCTOU); just try both in order.
            ctx = SMEventContext(timestamp=datetime.now(UTC), source="executor", error_message=error_msg)
            result = sm_transition(self.db, stage_run_id, StageEvent.BUILD_FAIL, ctx)
            if not result.success:
                sm_transition(self.db, stage_run_id, StageEvent.LAUNCH_FAIL, ctx)

            # Update non-state metadata (state machine handles state via BUILD_FAIL/LAUNCH_FAIL above)
            self.db.update_stage_run_status(
                stage_run_id=stage_run_id,
                completed_at=datetime.now(UTC).isoformat(),
                error=error_msg,
            )
            raise

        info = StageRunInfo(
            stage_run_id=stage_run_id,
            pipeline_run_id=pipeline_run_id,
            record_id=record_id,
            workspace=workspace,
            pipeline=pipeline_name,
            version=version,
            stage=stage_name,
            stage_version=stage_version_id,
            stage_version_num=stage_version_num,
            status=StageState.RUNNING,
            started_at=datetime.now(UTC),
            log_uri=str(self.dev_repo / ".goldfish" / "runs" / stage_run_id / "logs" / "output.log"),
            state=StageState.RUNNING.value,  # LAUNCH_OK already emitted in _launch_container
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
                    exclude={"status", "completed_at", "log_uri", "artifact_uri", "state", "outputs", "error"}
                )
                return StageRunInfo(
                    **base_fields,
                    status=refreshed.get("status", info.status),
                    completed_at=parse_optional_datetime(refreshed.get("completed_at")),
                    log_uri=refreshed.get("log_uri"),
                    artifact_uri=refreshed.get("artifact_uri"),
                    state=refreshed.get("state"),
                    outputs=json.loads(refreshed["outputs_json"]) if refreshed.get("outputs_json") else None,
                    error=refreshed.get("error"),
                )

        return info

    def _resolve_inputs(
        self,
        workspace: str,
        stage: StageDef,
        inputs_override: dict | None = None,
        pipeline_run_id: str | None = None,
    ) -> tuple[dict[str, str], dict[str, dict], list[dict]]:
        """Resolve all input sources for a stage.

        Args:
            workspace: Workspace name
            stage: Stage definition
            inputs_override: Optional dict of input name -> source/path overrides
            pipeline_run_id: Optional ID of the parent pipeline run

        Returns:
            - inputs: {input_name: source_location}
            - sources: {input_name: {source_stage_run_id, source_stage_version_id, source_type}}
            - input_context: list of resolved input metadata for pre-run review
        """
        inputs: dict[str, str] = {}
        sources: dict[str, dict] = {}
        input_context: list[dict] = []

        for input_name, input_def in stage.inputs.items():
            ctx: dict[str, Any] = {"input": input_name, "consumer_stage": stage.name}
            # Check for override
            if inputs_override and input_name in inputs_override:
                override_value = inputs_override[input_name]

                # 1. Try to resolve as a stage run ID or explicit run/signal dict
                source_run_id = None
                signal_name = input_def.signal or input_def.name

                if isinstance(override_value, str) and override_value.startswith("stage-"):
                    source_run_id = override_value
                elif isinstance(override_value, dict):
                    # Support both 'from_run' (internal) and 'run_id' (user-friendly)
                    source_run_id = override_value.get("from_run") or override_value.get("run_id")
                    if "signal" in override_value:
                        signal_name = override_value["signal"]

                if source_run_id:
                    signals = self.db.list_signals(stage_run_id=source_run_id)
                    signal = next((s for s in signals if s["signal_name"] == signal_name), None)

                    if signal:
                        inputs[input_name] = signal["storage_location"]
                        sources[input_name] = {
                            "source_type": "stage",
                            "source_stage_run_id": source_run_id,
                            "source_stage_version_id": None,
                        }
                        logger.info(
                            "Stage '%s': input '%s' OVERRIDDEN to run '%s' signal '%s'",
                            stage.name,
                            input_name,
                            source_run_id,
                            signal_name,
                        )
                        ctx.update(
                            {
                                "source_type": "stage",
                                "override": override_value,
                                "run_id": source_run_id,
                                "storage_location": signal["storage_location"],
                                "contents": self._list_storage_contents(signal["storage_location"]),
                            }
                        )
                        input_context.append(ctx)
                        continue

                # 2. Try to resolve as a registered source name (only if string)
                if isinstance(override_value, str | int):
                    source = self.db.get_source(str(override_value))
                    if source:
                        inputs[input_name] = source["gcs_location"]
                        sources[input_name] = {"source_type": "source", "source_name": str(override_value)}
                        logger.info(
                            "Stage '%s': input '%s' OVERRIDDEN to source '%s' (%s)",
                            stage.name,
                            input_name,
                            override_value,
                            source["gcs_location"],
                        )
                        ctx.update(
                            {
                                "source_type": "source",
                                "override": override_value,
                                "storage_location": source["gcs_location"],
                                "contents": self._list_storage_contents(source["gcs_location"]),
                            }
                        )
                        input_context.append(ctx)
                        continue

                # 3. Use as literal path (fallback)
                inputs[input_name] = str(override_value)
                # Try to extract source_stage_run_id from the path for lineage tracking
                source_run_id = _extract_stage_run_id_from_path(str(override_value))
                sources[input_name] = {
                    "source_type": "override",
                    "source_stage_run_id": source_run_id,  # May be None if not extractable
                }
                logger.info("Stage '%s': input '%s' OVERRIDDEN to path '%s'", stage.name, input_name, override_value)
                ctx.update(
                    {
                        "source_type": "override",
                        "override": override_value,
                        "storage_location": str(override_value),
                        "contents": self._list_storage_contents(str(override_value)),
                    }
                )
                input_context.append(ctx)
                continue

            # Resolve precedence: from_stage first, then dataset
            if input_def.from_stage:
                ctx.update(
                    {
                        "source_type": "stage",
                        "from_stage": input_def.from_stage,
                        "signal": input_def.signal or input_def.name,
                    }
                )

                source_run = None

                # Priority 1: Use the run from the SAME pipeline invocation if available
                if pipeline_run_id:
                    p_runs = self.db.list_stage_runs(
                        pipeline_run_id=pipeline_run_id,
                        stage_name=input_def.from_stage,
                        state=StageState.COMPLETED.value,
                    )
                    if p_runs:
                        source_run = p_runs[0]  # Most recent in this pipeline

                # Priority 2: Use most recent globally successful or unreviewed run
                if not source_run:
                    # Find output from previous stage
                    # Priority: Most recent COMPLETED run that is NOT 'bad_results'
                    stage_runs = self.db.list_stage_runs(
                        workspace_name=workspace, stage_name=input_def.from_stage, state=StageState.COMPLETED.value
                    )

                    for run in stage_runs:
                        outcome = run.get("outcome")
                        if outcome == "bad_results":
                            continue

                        # Found the most recent valid run (success or None/unreviewed)
                        source_run = run
                        break

                    skipped_bad = sum(1 for r in stage_runs if r.get("outcome") == "bad_results")
                    if skipped_bad > 0:
                        logger.warning(
                            "Stage '%s': skipped %d COMPLETED runs with bad_results outcome for input '%s'",
                            stage.name,
                            skipped_bad,
                            input_name,
                        )

                if not source_run:
                    raise GoldfishError(
                        f"No successful or unreviewed COMPLETED run found for stage '{input_def.from_stage}'"
                    )

                source_run_id = source_run["id"]
                ctx.update(
                    {
                        "selected_run_id": source_run_id,
                        "selected_run_state": source_run.get("state"),
                        "selected_run_started_at": source_run.get("started_at"),
                        "selected_run_outcome": source_run.get("outcome"),
                    }
                )

                # Get signal from that run
                signals = self.db.list_signals(stage_run_id=source_run_id)
                signal_name = input_def.signal or input_def.name

                signal = next((s for s in signals if s["signal_name"] == signal_name), None)
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
                ctx.update(
                    {
                        "storage_location": signal["storage_location"],
                        "contents": self._list_storage_contents(signal["storage_location"]),
                    }
                )
                logger.info(
                    "Stage '%s': input '%s' resolved to run %s (%s)",
                    stage.name,
                    input_name,
                    source_run_id,
                    signal["storage_location"],
                )

                latest_runs = self.db.list_stage_runs(
                    workspace_name=workspace, stage_name=input_def.from_stage, limit=1
                )
                if latest_runs:
                    latest = latest_runs[0]
                    ctx.update(
                        {
                            "latest_run_id": latest.get("id"),
                            "latest_run_state": latest.get("state"),
                            "latest_run_started_at": latest.get("started_at"),
                            "latest_run_outcome": latest.get("outcome"),
                        }
                    )
                input_context.append(ctx)

            elif input_def.type == "dataset":
                # External dataset
                if self.dataset_registry is None:
                    raise GoldfishError("Dataset registry not configured")
                if input_def.dataset is None:
                    raise GoldfishError(f"Input '{input_name}' is type 'dataset' but no dataset specified")
                dataset = self.dataset_registry.get_dataset(input_def.dataset)
                ctx.update({"source_type": "dataset", "dataset": input_def.dataset})
                if input_def.output_schema:
                    from goldfish.svs.contract import validate_input_schema_against_metadata

                    if getattr(dataset, "metadata_status", None) == "ok" and getattr(dataset, "metadata", None):
                        metadata = dataset.metadata
                        assert metadata is not None  # Checked above
                        schema_errors = validate_input_schema_against_metadata(
                            input_name=input_name,
                            input_schema=input_def.output_schema,
                            metadata=metadata,
                        )
                        if schema_errors:
                            raise GoldfishError(f"Input '{input_name}' schema mismatch: " + "; ".join(schema_errors))
                    else:
                        logger.warning(
                            "Skipping input schema check for '%s': metadata %s",
                            input_name,
                            getattr(dataset, "metadata_status", "missing"),
                        )
                inputs[input_name] = dataset.gcs_location
                sources[input_name] = {
                    "source_type": "dataset",
                    "dataset_name": input_def.dataset,
                }
                ctx.update(
                    {
                        "storage_location": dataset.gcs_location,
                        "contents": self._list_storage_contents(dataset.gcs_location),
                    }
                )
                logger.info(
                    "Stage '%s': input '%s' resolved to dataset '%s' (%s)",
                    stage.name,
                    input_name,
                    input_def.dataset,
                    dataset.gcs_location,
                )
                input_context.append(ctx)

            else:
                raise GoldfishError(f"Cannot resolve input: {input_name}")

        return inputs, sources, input_context

    def _get_svs_agent(self):
        """Get the configured SVS agent provider."""
        from goldfish.svs.agent import get_agent_provider

        return get_agent_provider(self.config.svs.agent_provider)

    def _run_post_run_svs_review(self, stage_run_id: str) -> None:
        """Run AI semantic review of stage outputs after completion."""
        # Only run if SVS and AI post-run are enabled
        if not self.config.svs.enabled or not self.config.svs.ai_post_run_enabled:
            return

        # AI review needs direct disk access to outputs. For backends with launch delay
        # (GCE), outputs are in cloud storage and require downloading first.
        caps = self.run_backend.capabilities
        if caps.has_launch_delay:
            logger.debug(f"Skipping post-run AI review for {stage_run_id}: backend requires downloading outputs first")
            return

        outputs_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs"

        # If container-side review already wrote findings, skip host-side review
        findings_path = outputs_dir / ".goldfish" / "svs_findings.json"
        if findings_path.exists():
            logger.debug("Skipping host post-run review for %s: findings already present", stage_run_id)
            return

        # Read intermediate stats manifest to pass to agent
        # (svs_findings.json hasn't been written yet)
        manifest_data = read_svs_manifests(outputs_dir)
        stats = manifest_data.get("stats", {})

        # Run review (this writes svs_findings.json)
        try:
            agent = self._get_svs_agent()
            run_post_run_review(
                outputs_dir=outputs_dir,
                stats=stats,
                config=self.config.svs,
                agent=agent,
            )
        except Exception as e:
            logger.warning(f"SVS post-run AI review failed for {stage_run_id}: {e}")

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
        preflight_errors: list[str] | None = None,
        preflight_warnings: list[str] | None = None,
        experiment_group: str | None = None,
        results_spec: dict | None = None,
    ) -> str:
        """Create stage run record in database with input lineage tracking.

        Returns:
            The generated experiment record_id (ULID)
        """
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
            preflight_errors=preflight_errors,
            preflight_warnings=preflight_warnings,
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

        # Create experiment record for this run
        exp_manager = ExperimentRecordManager(self.db)
        record_id = exp_manager.create_run_record(
            workspace_name=workspace,
            version=version,
            stage_run_id=stage_run_id,
            experiment_group=experiment_group,
        )

        # Save results_spec immediately after experiment record creation
        # This ensures results_spec is persisted even if later steps fail (e.g., Docker build)
        if results_spec and record_id:
            try:
                exp_manager.save_results_spec(stage_run_id, record_id, results_spec)
            except Exception as e:
                logger.warning(f"Failed to save results_spec for {stage_run_id}: {e}")

        return record_id

    def _update_queued_stage_run(
        self,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_version_id: int,
        inputs: dict,
        input_sources: dict[str, dict],
        config: dict | None,
        profile: str | None,
        hints: dict | None,
        preflight_warnings: list[str] | None = None,
        preflight_errors: list[str] | None = None,
        create_experiment_record: bool = True,
        experiment_group: str | None = None,
        results_spec: dict | None = None,
    ) -> str | None:
        """Update a queued stage run record with resolved values.

        Called when processing a pre-created stage_run from the pipeline queue.
        Updates version, config, inputs, records input lineage.

        Args:
            stage_run_id: The stage run ID to update
            workspace: Workspace name
            version: Workspace version
            stage_version_id: Stage version ID
            inputs: Resolved input paths
            input_sources: Input source metadata
            config: Stage config
            profile: Compute profile
            hints: Stage hints
            preflight_warnings: Validation warnings
            preflight_errors: Validation errors
            create_experiment_record: Whether to create an experiment record (True for
                pipeline-queued runs that don't already have one)
            experiment_group: Optional experiment group for filtering
            results_spec: Expected results specification for experiment tracking

        Returns:
            The generated experiment record_id (ULID) if created, None otherwise
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

        # Persist preflight results if provided
        if preflight_warnings is not None or preflight_errors is not None:
            self.db.update_stage_run_preflight(
                stage_run_id=stage_run_id,
                errors=preflight_errors,
                warnings=preflight_warnings,
            )

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

        # Create experiment record only for pipeline-queued runs that don't already have one
        if create_experiment_record:
            exp_manager = ExperimentRecordManager(self.db)
            record_id = exp_manager.create_run_record(
                workspace_name=workspace,
                version=version,
                stage_run_id=stage_run_id,
                experiment_group=experiment_group,
            )

            # Save results_spec immediately after experiment record creation
            # This ensures results_spec is persisted even if later steps fail
            if results_spec and record_id:
                try:
                    exp_manager.save_results_spec(stage_run_id, record_id, results_spec)
                except Exception as e:
                    logger.warning(f"Failed to save results_spec for {stage_run_id}: {e}")

            return record_id

        return None

    def _record_output_signals(
        self,
        stage_run_id: str,
        workspace: str,
        stage_name: str,
        output_base_uri: StorageURI | None = None,
    ):
        """Record output signals after stage completion.

        Reads output definitions from the pipeline and records them in the database
        so subsequent stages can resolve inputs. When running on a backend with
        remote object storage, outputs are assumed to be written under output_base_uri
        unless an explicit *.storage_location (or legacy *.gcs_location) marker is present.
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

                # Check if storage location was written by the stage
                storage_marker = outputs_dir / f"{output_name}.storage_location"
                gcs_marker = outputs_dir / f"{output_name}.gcs_location"  # legacy
                if storage_marker.exists():
                    storage_location = storage_marker.read_text().strip()
                elif gcs_marker.exists():
                    storage_location = gcs_marker.read_text().strip()
                elif output_base_uri is not None:
                    # Default storage location for remote backends
                    # Use appropriate suffix based on output type
                    output_type = output_def.type or "directory"
                    if output_type == "npy":
                        storage_location = str(output_base_uri.join(f"{output_name}.npy"))
                    elif output_type == "csv":
                        storage_location = str(output_base_uri.join(f"{output_name}.csv"))
                    else:
                        # directory, file, or other types use trailing /
                        storage_location = f"{str(output_base_uri.join(output_name)).rstrip('/')}/"

                # Calculate fingerprint for local outputs
                stats_json = None
                if output_base_uri is None:
                    from goldfish.utils.fingerprint import calculate_fingerprint

                    local_path = outputs_dir / output_name
                    if output_def.type == "npy":
                        local_path = local_path.with_suffix(".npy")
                    elif output_def.type == "csv":
                        local_path = local_path.with_suffix(".csv")

                    stats = calculate_fingerprint(local_path)
                    if stats:
                        stats_json = json.dumps(stats)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO signal_lineage
                    (stage_run_id, signal_name, signal_type, storage_location, is_artifact, stats_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stage_run_id,
                        output_name,
                        output_def.type or "directory",
                        storage_location,
                        int(bool(output_def.artifact)),
                        stats_json,
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
                # size_bytes may be None for stage outputs (unknown at authoring time).
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

    def _redact_logs(self, logs: str) -> str:
        """Apply redaction patterns to logs to protect sensitive information."""
        if not logs:
            return ""

        redacted = logs
        for pattern, replacement in REDACTION_PATTERNS:
            redacted = re.sub(pattern, replacement, redacted)
        return redacted

    def _persist_logs(self, stage_run_id: str, logs: str) -> str:
        """Write logs to local run directory and return path."""
        run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id / "logs"
        run_dir.mkdir(parents=True, exist_ok=True)
        log_path = run_dir / "output.log"

        # Redact logs before persisting
        redacted_logs = self._redact_logs(logs)
        log_path.write_text(redacted_logs or "")
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

    def _collect_svs_manifests(self, stage_run_id: str, backend: str) -> None:
        """Read SVS manifests from container output and sync to database."""
        # Only collect if SVS is enabled
        if not self.config.svs.enabled:
            return

        # Determine outputs directory based on backend capabilities
        # Backends with launch delay (GCE) store outputs in cloud storage
        # Backends without launch delay (local) store outputs in local filesystem
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend(backend)
        if caps.has_launch_delay:
            # Remote backend - download from cloud storage
            bucket_uri = self._get_bucket_uri()
            if bucket_uri is None:
                return

            import tempfile

            temp_dir = Path(tempfile.gettempdir()) / "goldfish_svs" / stage_run_id
            temp_dir.mkdir(parents=True, exist_ok=True)

            # Goldfish manifests are in outputs/.goldfish/
            gcs_prefix_uri = bucket_uri.join("runs", stage_run_id, "outputs", ".goldfish")

            # We use a simplified approach: just try to download the known manifest files
            for filename in ["svs_stats.json", "svs_findings.json", "svs_findings_during.json"]:
                dest = temp_dir / ".goldfish" / filename
                file_uri = gcs_prefix_uri.join(filename)
                self._download_from_storage(file_uri, dest)

            outputs_dir = temp_dir
        else:
            # Local backend - outputs are in local filesystem
            outputs_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs"

        # Read manifests using shared logic
        manifest_data = read_svs_manifests(outputs_dir)

        # 1. Update signal_lineage with stats
        for signal_name, stats in manifest_data.get("stats", {}).items():
            self.db.update_signal_lineage_stats(
                stage_run_id=stage_run_id,
                signal_name=signal_name,
                stats_json=json.dumps(stats),
            )

        # 2. Update stage_run with findings (stats + AI review + during-run history)
        if manifest_data.get("ai_review") or manifest_data.get("stats") or manifest_data.get("during_run"):
            findings = {
                "stats": manifest_data.get("stats"),
                "ai_review": manifest_data.get("ai_review"),
                "during_run": manifest_data.get("during_run"),
            }
            self.db.update_stage_run_svs_findings(
                stage_run_id=stage_run_id,
                svs_findings_json=json.dumps(findings),
            )

        # 3. Insert post-run review into svs_reviews table for dashboard visibility
        ai_review = manifest_data.get("ai_review")
        if ai_review and isinstance(ai_review, dict):
            import hashlib
            from datetime import datetime

            review_findings = ai_review.get("findings", [])
            decision = ai_review.get("decision", "approved")
            duration_ms = ai_review.get("duration_ms", 0)
            model = ai_review.get("model", self.config.svs.agent_model)
            response_text = ai_review.get("response_text", "")

            try:
                self.db.create_svs_review(
                    stage_run_id=stage_run_id,
                    review_type="post_run",
                    model_used=model,
                    prompt_hash=hashlib.sha256(f"post_run_{stage_run_id}".encode()).hexdigest()[:16],
                    decision=decision,
                    parsed_findings=json.dumps(review_findings) if review_findings else None,
                    reviewed_at=datetime.now().isoformat(),
                    duration_ms=duration_ms,
                    response_text=response_text if response_text else None,
                )
            except Exception as e:
                logger.debug(f"Failed to create post-run svs_review record: {e}")

        # 4. Insert during-run review into svs_reviews table for dashboard visibility
        during_run = manifest_data.get("during_run")
        if during_run and isinstance(during_run, dict):
            import hashlib
            from datetime import datetime

            during_history = during_run.get("history", [])
            during_decision = during_run.get("decision", "approved")

            try:
                self.db.create_svs_review(
                    stage_run_id=stage_run_id,
                    review_type="during_run",
                    model_used=self.config.svs.agent_model or "unknown",
                    prompt_hash=hashlib.sha256(f"during_run_{stage_run_id}".encode()).hexdigest()[:16],
                    decision=during_decision,
                    parsed_findings=json.dumps(during_history) if during_history else None,
                    reviewed_at=datetime.now().isoformat(),
                    duration_ms=0,
                )
            except Exception as e:
                logger.debug(f"Failed to create during-run svs_review record: {e}")

    def _collect_metrics(self, stage_run_id: str, backend: str) -> None:
        """Collect metrics from JSONL and store in database."""
        from goldfish.cloud.factory import get_capabilities_for_backend
        from goldfish.metrics.collector import MetricsCollector

        collector = MetricsCollector(self.db)

        # Determine metrics file location based on backend capabilities
        # Backends with launch delay (GCE) store metrics in cloud storage
        # Backends without launch delay (local) store metrics in local filesystem
        caps = get_capabilities_for_backend(backend)
        if caps.has_launch_delay:
            # Remote backend - download from cloud storage
            bucket_uri = self._get_bucket_uri()
            if bucket_uri is None:
                logger.debug(f"No GCS bucket configured, skipping metrics collection for {stage_run_id}")
                return

            metrics_uri = bucket_uri.join("runs", stage_run_id, "logs", "metrics.jsonl")

            # Download to local temp directory
            import tempfile

            temp_dir = Path(tempfile.gettempdir()) / "goldfish_metrics" / stage_run_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = temp_dir / "metrics.jsonl"

            if not self._download_from_storage(metrics_uri, metrics_file):
                logger.debug(f"No metrics file found in cloud storage for {stage_run_id}")
                return
        else:
            # Local backend - metrics.jsonl is in the outputs directory
            metrics_file = (
                self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs" / ".goldfish" / "metrics.jsonl"
            )

        # Collect metrics from file
        collector.collect_from_file(stage_run_id, metrics_file)

    def _download_from_storage(self, uri: StorageURI, destination: Path) -> bool:
        """Download a file from storage using the storage adapter.

        Args:
            uri: StorageURI of the file to download
            destination: Local path to write to

        Returns:
            True if download succeeded, False if the object doesn't exist.
        """
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if self.storage.download_to_file(uri, destination):
                return True
            return False
        except Exception as exc:
            logger.warning("Failed to download from storage %s: %s", uri, exc)
            return False

    def _ensure_storage_access(self, operation: str) -> None:
        """Ensure storage is accessible for cloud backend operations.

        For backends with launch delay (GCE), validates that storage is
        configured and reachable. Uses the storage adapter to check connectivity.
        """
        caps = self.run_backend.capabilities
        if not caps.has_launch_delay:
            return  # Local backend uses disk, no storage access needed
        bucket_uri = self._get_bucket_uri()
        if bucket_uri is None:
            raise GoldfishError(
                f"GCE backend requires gcs.bucket for {operation}. "
                "Set gcs.bucket in goldfish.yaml or GOLDFISH_GCS_BUCKET."
            )
        # Use the storage adapter to verify connectivity
        try:
            # Check if we can access the bucket by listing (with empty prefix)
            # Use a non-existent prefix to minimize data transfer
            health_check_uri = bucket_uri.join("_goldfish_health_check_")
            list(self.storage.list_prefix(health_check_uri))
        except Exception as exc:
            raise GoldfishError(f"Storage access check failed for {operation}: {exc}") from exc

    def _metrics_live_sync_enabled(self) -> bool:
        value = os.getenv("GOLDFISH_METRICS_LIVE_SYNC", "1").strip().lower()
        return value not in {"0", "false", "no", "off"}

    def _metrics_live_sync_interval(self) -> int:
        """Get metrics live sync interval in seconds.

        Priority: env var > config defaults > hardcoded default (15s)
        """
        env_value = os.getenv("GOLDFISH_METRICS_LIVE_SYNC_INTERVAL")
        if env_value:
            try:
                parsed = int(env_value)
                return max(5, min(300, parsed))
            except ValueError:
                pass
        # Fall back to config defaults
        return max(5, min(300, self.config.defaults.log_sync_interval))

    def _get_metrics_sync_state(self, stage_run_id: str) -> _MetricsSyncState:
        with self._metrics_sync_lock:
            state = self._metrics_sync_state.get(stage_run_id)
            if state is None:
                state = _MetricsSyncState()
                self._metrics_sync_state[stage_run_id] = state
            return state

    def _sync_metrics_file_from_storage_uri(
        self, uri: StorageURI, state: _MetricsSyncState
    ) -> tuple[Path | None, int, str | None]:
        """Download metrics.jsonl from storage into a local temp file.

        Uses the storage adapter to download the complete file.
        The offset tracking is maintained for compatibility but we download
        the full file each time (simpler, works across all storage backends).

        Args:
            uri: StorageURI pointing to the metrics file
            state: Sync state object to track offset and temp path

        Returns:
            Tuple of (local_path, new_offset, warning_message)
        """
        import tempfile

        # Set up local temp path if not already done
        local_path = state.temp_path
        if local_path is None:
            # Create a safe temp directory name from the URI
            safe_name = f"{uri.bucket}_{uri.path}".replace("/", "_")
            temp_dir = Path(tempfile.gettempdir()) / "goldfish_metrics_live" / safe_name
            temp_dir.mkdir(parents=True, exist_ok=True)
            local_path = temp_dir / "metrics.jsonl"
            state.temp_path = local_path

        try:
            # Check if file exists and get its size
            size = self.storage.get_size(uri)
            if size is None:
                return None, state.offset, None  # File doesn't exist yet

            # If file hasn't changed, return existing local copy
            if size == state.offset and local_path.exists():
                return local_path, state.offset, None

            # If file got smaller (reset), start over
            if size < state.offset:
                state.offset = 0
                if local_path.exists():
                    local_path.unlink()

            # Download the full file (simpler than range downloads, works everywhere)
            if self.storage.download_to_file(uri, local_path):
                state.offset = size  # Update offset to current size
                return local_path, state.offset, None

            return None, state.offset, "Live metrics sync failed: download failed"

        except Exception as exc:
            logger.warning("Failed to sync metrics from storage: %s", exc)
            return local_path, state.offset, f"Live metrics sync failed: {exc}"

    def sync_metrics_if_running(self, stage_run_id: str) -> list[str]:
        """Best-effort incremental metrics sync for running stages."""
        warnings: list[str] = []
        if not self._metrics_live_sync_enabled():
            return warnings

        row = self.db.get_stage_run(stage_run_id)
        # Check state (source of truth), not legacy status
        if not row or row.get("state") != StageState.RUNNING.value:
            with self._metrics_sync_lock:
                self._metrics_sync_state.pop(stage_run_id, None)
            return warnings

        state = self._get_metrics_sync_state(stage_run_id)
        if not state.sync_lock.acquire(blocking=False):
            return warnings
        try:
            interval = self._metrics_live_sync_interval()
            now = time.time()
            if now - state.last_sync < interval:
                return warnings

            backend = row.get("backend_type") or self.config.jobs.backend
            metrics_file: Path | None = None
            start_offset = state.offset

            # Use capability-based check for storage location
            from goldfish.cloud.factory import get_capabilities_for_backend

            caps = get_capabilities_for_backend(backend)
            if caps.has_launch_delay:
                # Remote backend - download from cloud storage
                bucket_uri = self._get_bucket_uri()
                if bucket_uri is None:
                    warnings.append(
                        "Live metrics sync skipped: gcs.bucket not configured for remote backend.",
                    )
                    return warnings
                metrics_uri = bucket_uri.join("runs", stage_run_id, "logs", "metrics.jsonl")
                metrics_file, start_offset, sync_warning = self._sync_metrics_file_from_storage_uri(metrics_uri, state)
                if sync_warning:
                    warnings.append(sync_warning)
            else:
                # Local backend - metrics.jsonl is in the outputs directory
                metrics_file = (
                    self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs" / ".goldfish" / "metrics.jsonl"
                )

            if metrics_file is None or not metrics_file.exists():
                return warnings

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
        return warnings

    def sync_svs_if_running(self, stage_run_id: str) -> None:
        """Best-effort SVS findings sync for running stages."""
        if not self.config.svs.enabled:
            return

        row = self.db.get_stage_run(stage_run_id)
        # Check state (source of truth), not legacy status
        if not row or row.get("state") != StageState.RUNNING.value:
            with self._svs_sync_lock:
                self._svs_sync_state.pop(stage_run_id, None)
            return

        now = time.time()
        # SVS sync interval: env var > config defaults
        env_interval = os.environ.get("GOLDFISH_SVS_LIVE_SYNC_INTERVAL")
        if env_interval:
            try:
                interval = float(env_interval)
            except ValueError:
                interval = float(self.config.defaults.log_sync_interval)
        else:
            interval = float(self.config.defaults.log_sync_interval)
        with self._svs_sync_lock:
            last_sync = self._svs_sync_state.get(stage_run_id, 0.0)
            if now - last_sync < interval:
                return
            self._svs_sync_state[stage_run_id] = now

        backend = row.get("backend_type") or self.config.jobs.backend

        # Use capability-based check for storage location
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend(backend)
        if caps.has_launch_delay:
            # Remote backend - download from cloud storage
            bucket_uri = self._get_bucket_uri()
            if bucket_uri is None:
                return

            import tempfile

            temp_dir = Path(tempfile.gettempdir()) / "goldfish_svs_live" / stage_run_id
            temp_dir.mkdir(parents=True, exist_ok=True)

            gcs_prefix_uri = bucket_uri.join("runs", stage_run_id, "outputs", ".goldfish")

            findings_dest = temp_dir / ".goldfish" / "svs_findings.json"
            stats_dest = temp_dir / ".goldfish" / "svs_stats.json"
            during_dest = temp_dir / ".goldfish" / "svs_findings_during.json"
            self._download_from_storage(gcs_prefix_uri.join("svs_findings.json"), findings_dest)
            self._download_from_storage(gcs_prefix_uri.join("svs_stats.json"), stats_dest)
            self._download_from_storage(gcs_prefix_uri.join("svs_findings_during.json"), during_dest)
            outputs_dir = temp_dir
        else:
            # Local backend - outputs are in local filesystem
            outputs_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id / "outputs"
            if not outputs_dir.exists():
                return

        manifest = read_svs_manifests(outputs_dir)
        if not manifest.get("during_run") and not manifest.get("ai_review") and not manifest.get("stats"):
            return

        merged: dict[str, Any] = {}
        existing_raw = row.get("svs_findings_json")
        if existing_raw:
            try:
                merged = json.loads(existing_raw)
            except json.JSONDecodeError:
                merged = {}

        if manifest.get("stats") is not None:
            merged["stats"] = manifest.get("stats")
        if manifest.get("ai_review") is not None:
            merged["ai_review"] = manifest.get("ai_review")
        if manifest.get("during_run") is not None:
            merged["during_run"] = manifest.get("during_run")

        if merged:
            self.db.update_stage_run_svs_findings(stage_run_id, json.dumps(merged))

        # Also insert during-run findings into svs_reviews table for live dashboard visibility
        during_run = manifest.get("during_run")
        if during_run and isinstance(during_run, dict):
            self._sync_during_run_to_svs_reviews(stage_run_id, during_run)

    def _sync_during_run_to_svs_reviews(self, stage_run_id: str, during_run: dict) -> None:
        """Sync during-run findings to svs_reviews table for dashboard visibility.

        Only inserts new findings (based on timestamp) to avoid duplicates.
        """
        import hashlib

        history = during_run.get("history", [])
        if not history:
            return

        # Get existing during-run review timestamps for this run
        existing_reviews = self.db.get_svs_reviews(stage_run_id=stage_run_id, review_type="during_run")
        existing_hashes = set()
        for r in existing_reviews:
            # Use prompt_hash as the unique identifier
            if r.get("prompt_hash"):
                existing_hashes.add(r["prompt_hash"])

        # Insert new findings
        decision = during_run.get("decision", "approved")
        for entry in history:
            if not isinstance(entry, dict):
                continue

            # Create a unique hash for this entry based on timestamp + content
            timestamp = entry.get("timestamp", "")
            check_name = entry.get("check", "")
            summary = entry.get("summary", "")[:100]  # First 100 chars
            unique_key = f"during_run_{stage_run_id}_{timestamp}_{check_name}_{summary}"
            entry_hash = hashlib.sha256(unique_key.encode()).hexdigest()[:16]

            if entry_hash in existing_hashes:
                continue

            try:
                # Store the single finding as parsed_findings
                self.db.create_svs_review(
                    stage_run_id=stage_run_id,
                    review_type="during_run",
                    model_used=self.config.svs.agent_model or "unknown",
                    prompt_hash=entry_hash,
                    decision=decision,
                    parsed_findings=json.dumps([entry]),
                    response_text=entry.get("summary"),
                    reviewed_at=timestamp or datetime.now().isoformat(),
                    duration_ms=0,
                )
            except Exception as e:
                logger.debug(f"Failed to sync during-run finding to svs_reviews: {e}")

    def _build_docker_image(self, workspace: str, version: str, profile_name: str | None = None) -> tuple[str, str]:
        """Build Docker image for this run.

        Args:
            workspace: Workspace name
            version: Version identifier
            profile_name: Optional profile name to determine base image

        Returns:
            Tuple of (image_tag, build_context_hash).
        """
        # Import constants from image_versions (single source of truth)
        from goldfish.cloud.image_versions import BASE_IMAGE_CPU, BASE_IMAGE_GPU

        # Get workspace directory
        workspace_dir = self.workspace_manager.get_workspace_path(workspace)

        # Resolve base image from profile using pre-computed artifact_registry
        # Get version from database if available (per-project tracking)
        base_image = None
        if profile_name:
            profile = resolve_compute_profile(self.config, profile_name)
            # Determine image type from profile to get version from DB
            # Use max(DB version, shipped default) so Goldfish upgrades
            # (e.g., glibc bumps) aren't blocked by stale DB entries.
            profile_base_image = profile.get("base_image")
            base_image_version = None
            if profile_base_image in (BASE_IMAGE_GPU, BASE_IMAGE_CPU):
                image_type = "gpu" if profile_base_image == BASE_IMAGE_GPU else "cpu"
                version_info = self.db.get_current_base_image_version(image_type)
                if version_info:
                    from goldfish.cloud.image_versions import BASE_IMAGE_VERSION_DEFAULT, _version_gte

                    db_ver = str(version_info["version"])
                    base_image_version = (
                        db_ver if _version_gte(db_ver, BASE_IMAGE_VERSION_DEFAULT) else BASE_IMAGE_VERSION_DEFAULT
                    )
            base_image = resolve_profile_base_image(profile, self.artifact_registry, base_image_version)

        # Determine build backend based on capabilities
        # Backends with launch delay (GCE) require cloud build + artifact registry
        # Backends without launch delay (local) use local Docker build
        caps = self.run_backend.capabilities
        if caps.has_launch_delay:
            # Remote backend: Use ImageBuilder protocol (CloudBuildImageBuilder for GCE)
            # This ensures linux-native wheels (flash-attn, etc.) install correctly
            if not self.artifact_registry:
                raise GoldfishError(
                    "Remote backend requires artifact_registry. "
                    "Set gce.artifact_registry in goldfish.yaml or gce.project_id for auto-generation."
                )

            # Use docker_builder to prepare context, then ImageBuilder for actual build
            # This delegates Cloud Build logic to the CloudBuildImageBuilder adapter
            with self.docker_builder.prepare_build_context(workspace_dir, workspace, version, base_image) as (
                build_ctx,
                context_path,
                dockerfile_path,
                local_tag,
            ):
                build_context_hash = compute_build_context_hash(build_ctx)
                cached = self.db.get_docker_build_by_content_hash(workspace, build_context_hash)
                if cached and cached.get("registry_tag"):
                    logger.info("Reusing cached workspace image (build_context_hash=%s)", build_context_hash[:16])
                    return str(cached["registry_tag"]), build_context_hash

                registry_tag = f"{self.artifact_registry}/{local_tag}"
                started_at = datetime.now(UTC).isoformat()
                build_id = f"build-{uuid4().hex[:8]}"
                build_args_json = json.dumps(build_ctx.build_args, sort_keys=True, separators=(",", ":"))
                build_context_json = json.dumps(
                    {
                        "dockerfile_hash": build_ctx.dockerfile_hash,
                        "git_sha": build_ctx.git_sha,
                        "goldfish_runtime_hash": build_ctx.goldfish_runtime_hash,
                        "base_image": build_ctx.base_image,
                        "base_image_digest": build_ctx.base_image_digest,
                        "requirements_hash": build_ctx.requirements_hash,
                        "build_args": build_ctx.build_args,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                image_type = "gpu" if "gpu" in build_ctx.base_image.lower() else "cpu"
                self.db.insert_docker_build(
                    build_id=build_id,
                    image_type=image_type,
                    target="workspace",
                    backend="cloud",
                    started_at=started_at,
                    registry_tag=registry_tag,
                    cloud_build_id=None,
                    workspace_name=workspace,
                    version=version,
                    content_hash=build_context_hash,
                    dockerfile_hash=build_ctx.dockerfile_hash,
                    git_sha=build_ctx.git_sha,
                    goldfish_runtime_hash=build_ctx.goldfish_runtime_hash,
                    base_image=build_ctx.base_image,
                    base_image_digest=build_ctx.base_image_digest,
                    requirements_hash=build_ctx.requirements_hash,
                    build_args_json=build_args_json,
                    build_context_json=build_context_json,
                )

                # ImageBuilder.build() returns registry tag for cloud builds
                try:
                    registry_image_tag = self.image_builder.build(
                        context_path=context_path,
                        dockerfile_path=dockerfile_path,
                        image_tag=registry_tag,
                        build_args=build_ctx.build_args,
                        no_cache=False,  # Use cache for faster builds
                    )
                except Exception as e:
                    self.db.update_docker_build_status(
                        build_id,
                        status="failed",
                        error=str(e),
                        completed_at=datetime.now(UTC).isoformat(),
                    )
                    raise

                pip_freeze = self.docker_builder.capture_pip_freeze_from_image(registry_image_tag)
                if pip_freeze is not None:
                    try:
                        payload = json.loads(build_context_json)
                        if isinstance(payload, dict):
                            payload["pip_freeze"] = pip_freeze
                            build_context_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
                    except Exception as e:
                        logger.debug("Failed to store pip freeze output for build %s: %s", build_id, e)

                self.db.update_docker_build_status(
                    build_id,
                    status="completed",
                    completed_at=datetime.now(UTC).isoformat(),
                    registry_tag=registry_image_tag,
                    build_context_json=build_context_json,
                )
                return registry_image_tag, build_context_hash

        # Local backend: Build locally using DockerBuilder
        local_image_tag, build_context_hash = self.docker_builder.build_image_with_context_hash(
            workspace_dir=workspace_dir,
            workspace_name=workspace,
            version=version,
            use_cache=True,
            base_image=base_image,
        )

        return local_image_tag, build_context_hash

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
            logger.warning(
                "Stage '%s': config file not found at %s (default config will be used)",
                stage_name,
                config_path,
            )
            return {}

        try:
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            # Log warning but don't fail - config is optional
            logger.warning("Stage '%s': failed to parse config %s: %s", stage_name, config_path, e)
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

        try:
            return resolve_compute_profile(self.config, profile_name)
        except Exception as e:
            raise GoldfishError(f"Failed to resolve profile '{profile_name}': {e}") from e

    def _validate_capabilities_for_stage(self, workspace: str, stage_name: str, backend: str) -> None:
        """Validate that backend capabilities match profile requirements.

        This prevents misconfigurations like GPU profiles on local backend.
        Part of the cloud abstraction layer capability contract.

        Args:
            workspace: Workspace name
            stage_name: Stage name
            backend: Backend type ("local" or "gce")

        Raises:
            GoldfishError: If profile requires capabilities the backend doesn't support
        """
        # Get backend capabilities from the abstraction layer
        capabilities = self.run_backend.capabilities

        # Load and resolve the profile for this stage
        stage_config = self._load_stage_config(workspace, stage_name)
        profile = self._resolve_profile_from_config(stage_config)

        if not profile:
            return  # No profile, no capability requirements to check

        # Check GPU capability
        gpu_info = profile.get("gpu", {})
        if gpu_info.get("type") != "none" and gpu_info.get("count", 0) > 0:
            if not capabilities.supports_gpu:
                raise GoldfishError(
                    f"Stage '{stage_name}' requires GPU (profile specifies "
                    f"gpu.type={gpu_info.get('type')}, count={gpu_info.get('count')}), "
                    f"but backend '{backend}' does not support GPU. "
                    "Use a GPU-capable backend (e.g., 'gce') or select a CPU profile."
                )

        # Check spot/preemptible capability
        # Profile indicates spot preference via preemptible_allowed
        if profile.get("preemptible_allowed", False):
            # Only warn if spot is preferred but not supported - it's a preference, not requirement
            if not capabilities.supports_spot:
                logger.debug(
                    "Stage '%s' profile allows preemptible instances, but backend '%s' "
                    "does not support spot/preemptible. Will use on-demand instances.",
                    stage_name,
                    backend,
                )

    @staticmethod
    def _poll_interval(elapsed: int) -> int:
        if elapsed < 60:
            return 5
        if elapsed < 600:
            return 10
        if elapsed < 3600:
            return 30
        return 60

    def _build_entrypoint_script(self, stage_name: str, runtime: str, entrypoint: str | None) -> str:
        """Build the entrypoint script for a stage.

        For Rust stages: Compiles modules/{stage_name}.rs using cargo, then executes the binary.
        For Python stages: Runs modules/{stage_name}.py via python -m.

        The Rust compilation happens inside the container at runtime, enabling
        the goldfish-rust crate to be linked against the container's environment.
        """
        if runtime == "rust":
            entrypoint_rel = entrypoint or f"entrypoints/{stage_name}"
            entrypoint_path = f"/app/{entrypoint_rel}"
            module_path = f"/app/modules/{stage_name}.rs"
            cargo_override = f"/app/modules/{stage_name}.Cargo.toml"
            build_dir = f"/app/.goldfish_rust_build/{stage_name}"
            return f"""#!/bin/bash
set -euo pipefail

echo "Running stage: {stage_name} (rust)"
cd /app

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found in image. Use a goldfish-base image or install Rust in your base image." >&2
  exit 1
fi

if [ ! -d "/app/goldfish-rust" ]; then
  echo "goldfish-rust crate not found at /app/goldfish-rust. Ensure it is included in the build context." >&2
  exit 1
fi

if [ ! -f "{module_path}" ]; then
  echo "Rust module not found: {module_path}" >&2
  exit 1
fi

mkdir -p "{build_dir}/src" "/app/entrypoints"
mkdir -p "$(dirname "{entrypoint_path}")"
cp "{module_path}" "{build_dir}/src/main.rs"

if [ -f "{cargo_override}" ]; then
  cp "{cargo_override}" "{build_dir}/Cargo.toml"
else
  cat > "{build_dir}/Cargo.toml" <<'CARGO_EOF'
[package]
name = "{stage_name}"
version = "0.1.0"
edition = "2021"

[dependencies]
goldfish-rust = {{ path = "/app/goldfish-rust" }}
CARGO_EOF
fi

cargo build --release --manifest-path "{build_dir}/Cargo.toml"
cp "{build_dir}/target/release/{stage_name}" "{entrypoint_path}"
chmod +x "{entrypoint_path}"

exec "{entrypoint_path}"
"""
        if runtime != "python":
            raise GoldfishError(f"Unsupported runtime '{runtime}' for stage '{stage_name}'")

        return f"""#!/bin/bash
set -euo pipefail

echo "Running stage: {stage_name}"
cd /app
python - <<'PY'
from goldfish.io.bootstrap import run_module_with_svs
import sys
sys.exit(run_module_with_svs("modules.{stage_name}"))
PY

echo "Stage completed successfully"
"""

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
        run_reason: dict | None = None,
        runtime: str = "python",
        entrypoint: str | None = None,
        config_override: dict | None = None,
        inputs_override: dict | None = None,
        pipeline_name: str | None = None,
        results_spec: dict | None = None,
    ):
        """Launch Docker container (local) or GCE instance."""
        backend = self.config.jobs.backend

        # Capability validation: ensure backend supports requested features
        # This is critical for catching misconfigurations early (e.g., GPU profile on local backend)
        self._validate_capabilities_for_stage(workspace, stage_name, backend)

        # Build stage config for goldfish.io
        # Start with user config (freeze_backbone, epochs, etc.) and add stage/inputs/outputs
        stage_config = dict(user_config) if user_config else {}
        stage_config["stage"] = stage_name
        stage_config["inputs"] = input_configs or inputs
        stage_config["outputs"] = output_configs or {}

        # Build SVS context for during-run AI monitoring
        # Include full run command so AI reviewer sees runtime overrides
        svs_context = {
            "run_reason": run_reason or {},
            "stage_name": stage_name,
            "workspace": workspace,
            "pipeline_name": pipeline_name,
            "config_override": config_override,
            "inputs_override": inputs_override,
            "outputs": {
                name: {
                    "type": cfg.get("type"),
                    "schema": cfg.get("schema"),
                }
                for name, cfg in (output_configs or {}).items()
            },
            # Include results_spec for post-run ML assessment
            # Contains expected metrics: primary_metric, direction, min_value, goal_value, etc.
            "results_spec": results_spec,
        }

        # Build Goldfish environment variables for metrics and provenance
        goldfish_env = {
            "GOLDFISH_PROJECT_NAME": self.config.project_name,
            "GOLDFISH_WORKSPACE": workspace,
            "GOLDFISH_STAGE": stage_name,
            "GOLDFISH_RUN_ID": stage_run_id,
            "GOLDFISH_OUTPUTS_DIR": "/mnt/outputs",
            "GOLDFISH_STAGE_CONFIG": json.dumps(stage_config),
            "GOLDFISH_SVS_CONFIG": json.dumps(self.config.svs.model_dump()),
            "GOLDFISH_SVS_CONTEXT": json.dumps(svs_context),
            "GOLDFISH_SVS_STATS_ENABLED": "true"
            if self.config.svs.enabled and self.config.svs.stats_enabled
            else "false",
            # Ensure agent CLIs write config in a writable home directory
            "HOME": "/app",
            "XDG_CONFIG_HOME": "/app/.config",
            "XDG_CACHE_HOME": "/app/.cache",
        }

        # Pass GCS bucket for checkpoint API (immediate upload for preemption recovery)
        bucket_uri = self._get_bucket_uri()
        if bucket_uri is not None:
            goldfish_env["GOLDFISH_GCS_BUCKET"] = str(bucket_uri)

        # Overdrive defaults: unbuffered stdout + faster metrics flush
        goldfish_env.setdefault("PYTHONUNBUFFERED", os.environ.get("PYTHONUNBUFFERED", "1"))
        goldfish_env.setdefault(
            "GOLDFISH_METRICS_FLUSH_INTERVAL",
            os.environ.get("GOLDFISH_METRICS_FLUSH_INTERVAL", "5"),
        )

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
        if "WANDB_API_KEY" in os.environ:
            goldfish_env["WANDB_API_KEY"] = os.environ["WANDB_API_KEY"]

        # Passthrough ANTHROPIC_API_KEY for SVS AI reviews (during-run and post-run)
        if "ANTHROPIC_API_KEY" in os.environ:
            goldfish_env["ANTHROPIC_API_KEY"] = os.environ["ANTHROPIC_API_KEY"]

        # Passthrough agent provider override for SVS (useful for testing/debugging)
        if "GOLDFISH_SVS_AGENT_PROVIDER" in os.environ:
            goldfish_env["GOLDFISH_SVS_AGENT_PROVIDER"] = os.environ["GOLDFISH_SVS_AGENT_PROVIDER"]

        # Load stage config and resolve profile for resource allocation
        stage_config_yaml = self._load_stage_config(workspace, stage_name)

        # Apply config_override to stage config for profile resolution
        if config_override:
            # Deep merge: config_override takes precedence
            merged_config = dict(stage_config_yaml)
            for key, value in config_override.items():
                if key in merged_config and isinstance(merged_config[key], dict) and isinstance(value, dict):
                    merged_config[key] = {**merged_config[key], **value}
                else:
                    merged_config[key] = value
            stage_config_yaml = merged_config

        profile = self._resolve_profile_from_config(stage_config_yaml)

        # Merge user-defined environment variables from stage config
        # Security: GOLDFISH_* vars are set by us, user vars should not override them
        user_env = stage_config_yaml.get("environment", {})
        if user_env and isinstance(user_env, dict):
            for key, value in user_env.items():
                # Skip Goldfish internal vars - don't let users override them
                if key.startswith("GOLDFISH_"):
                    logger.warning(
                        f"Ignoring user-defined environment variable '{key}' - " "GOLDFISH_* variables are reserved"
                    )
                    continue
                goldfish_env[key] = str(value)

        # Extract resource settings from profile
        gpu_count = 0
        gpu_type: str | None = None
        memory_gb = 4.0
        cpu_count = 2.0
        spot = False
        profile_name = "cpu-small"
        machine_type: str | None = None

        if profile:
            profile_name = profile.get("name", "cpu-small")
            machine_type = profile.get("machine_type")  # e.g., "a3-highgpu-1g" for H100
            gpu_info = profile.get("gpu", {})
            if gpu_info.get("type") != "none":
                gpu_type = gpu_info.get("accelerator")  # Use accelerator (e.g., nvidia-h100-80gb)
                gpu_count = gpu_info.get("count", 0)
            memory_gb = profile.get("memory_gb", 4.0)
            cpu_count = profile.get("vcpus", 2.0)
            # Profile uses preemptible_allowed (GCE terminology), map to spot
            spot = profile.get("preemptible_allowed", False)

        # Extract timeout from stage config (compute.max_runtime_seconds)
        # Falls back to defaults.timeout_seconds if not specified
        timeout_seconds: int | None = None
        capacity_wait_seconds: int | None = None
        compute_config = stage_config_yaml.get("compute", {})
        if compute_config and isinstance(compute_config, dict):
            max_runtime = compute_config.get("max_runtime_seconds")
            if max_runtime is not None:
                timeout_seconds = int(max_runtime)
            capacity_wait = compute_config.get("capacity_wait_seconds")
            if capacity_wait is not None:
                capacity_wait_seconds = int(capacity_wait)
        if timeout_seconds is None:
            timeout_seconds = self.config.defaults.timeout_seconds
        if capacity_wait_seconds is None:
            capacity_wait_seconds = self.config.defaults.capacity_wait_seconds

        # Build command from entrypoint
        entrypoint_script = self._build_entrypoint_script(stage_name, runtime, entrypoint)
        command = ["bash", "-c", entrypoint_script] if entrypoint_script else None

        # Convert inputs dict to StorageURI dict
        input_uris: dict[str, StorageURI] = {}
        for signal_name, path in inputs.items():
            if isinstance(path, str):
                try:
                    input_uris[signal_name] = StorageURI.parse(path)
                except ValueError:
                    # Local path - convert to file:// URI
                    input_uris[signal_name] = StorageURI("file", "", str(Path(path).resolve()))
            elif isinstance(path, StorageURI):
                input_uris[signal_name] = path

        # Build output URI
        output_uri: StorageURI | None = None
        bucket_uri = self._get_bucket_uri()
        if bucket_uri:
            output_uri = bucket_uri.join("runs", stage_run_id, "outputs")
        else:
            # Local backend - use dev_repo path
            run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir = run_dir / "outputs"
            outputs_dir.mkdir(exist_ok=True)
            output_uri = StorageURI("file", "", str(outputs_dir.resolve()))

        # Build RunSpec
        run_spec = RunSpec(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            stage_name=stage_name,
            image=image_tag,
            command=command,
            env=goldfish_env,
            profile=profile_name,
            machine_type=machine_type,
            gpu_count=gpu_count,
            gpu_type=gpu_type,
            memory_gb=memory_gb,
            cpu_count=cpu_count,
            inputs=input_uris,
            output_uri=output_uri,
            spot=spot,
            timeout_seconds=timeout_seconds,
            capacity_wait_seconds=capacity_wait_seconds,
        )

        # Launch via run_backend protocol
        handle = self.run_backend.launch(run_spec)

        # Store backend handle for monitoring/cleanup
        # Required for all backends so wait_for_completion can resolve the handle
        self.db.set_stage_run_backend(
            stage_run_id=stage_run_id,
            backend_type=backend,
            backend_handle=handle.backend_handle,
            instance_zone=handle.zone,
        )

        # State machine: LAUNCHING → RUNNING (LAUNCH_OK)
        sm_transition(
            self.db,
            stage_run_id,
            StageEvent.LAUNCH_OK,
            SMEventContext(timestamp=datetime.now(UTC), source="executor"),
        )

    def _finalize_stage_run(self, stage_run_id: str, backend: str, status: str) -> None:
        """Handle terminal status: record outputs, fetch logs, transition state."""
        # CAS guard against double-finalize (only finalize if still in non-terminal state)
        # Use state column (source of truth) for the check
        terminal_state_values = tuple(s.value for s in TERMINAL_STATES)
        placeholders = ", ".join("?" for _ in terminal_state_values)
        with self.db._conn() as conn:
            # Check if already terminal - use CAS pattern on state column
            row = conn.execute(
                f"SELECT state FROM stage_runs WHERE id = ? AND state NOT IN ({placeholders})",
                (stage_run_id, *terminal_state_values),
            ).fetchone()
        if not row:
            return  # Already in terminal state, skip finalization

        # Re-read fresh row after CAS
        stage_run = self.db.get_stage_run(stage_run_id)
        if not stage_run:
            return

        workspace = stage_run["workspace_name"]
        stage_name_from_db = stage_run["stage_name"]

        # State machine: successful execution enters POST_RUN via EXIT_SUCCESS (v1.2).
        # (Failure paths transition to FAILED/TERMINATED and do not enter POST_RUN per spec.)
        warnings: list[str] = []
        tracker: FinalizationTracker | None = None
        if status == StageState.COMPLETED:
            # Best-effort: if we never observed the instance in RUNNING, still mark launch OK.
            sm_transition(
                self.db,
                stage_run_id,
                StageEvent.LAUNCH_OK,
                SMEventContext(timestamp=datetime.now(UTC), source="executor"),
            )
            sm_transition(
                self.db,
                stage_run_id,
                StageEvent.EXIT_SUCCESS,
                SMEventContext(
                    timestamp=datetime.now(UTC),
                    source="executor",
                    exit_code=0,
                    exit_code_exists=True,
                ),
            )
            # FinalizationTracker expects validated stage_run_id format; when calling
            # _finalize_stage_run in tests, the ID may not match. Treat as best-effort.
            try:
                tracker = FinalizationTracker(self.db, stage_run_id)
                tracker.mark_output_sync_done()
            except Exception:
                tracker = None

        # Use capability-based check for remote storage (GCS)
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend(backend)
        output_base_uri: StorageURI | None = None
        bucket_uri = self._get_bucket_uri()
        if caps.has_launch_delay and bucket_uri is not None:
            output_base_uri = bucket_uri.join("runs", stage_run_id, "outputs")

        if status == StageState.COMPLETED:
            try:
                self._record_output_signals(
                    stage_run_id,
                    workspace,
                    stage_name_from_db,
                    output_base_uri=output_base_uri,
                )
                if tracker is not None:
                    tracker.mark_output_recording_done()
            except Exception as e:
                # If outputs fail to record, mark run failed and surface error
                error_msg = f"Output recording failed: {e}"
                # State machine: POST_RUN → FAILED (POST_RUN_FAIL, critical=True) (v1.2)
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.POST_RUN_FAIL,
                    SMEventContext(
                        timestamp=datetime.now(UTC),
                        source="executor",
                        critical=True,
                        error_message=error_msg,
                    ),
                )

                # Update non-state metadata (state machine handles state via POST_RUN_FAIL above)
                self.db.update_stage_run_status(
                    stage_run_id=stage_run_id,
                    completed_at=datetime.now(UTC).isoformat(),
                    error=error_msg,
                )
                return

        logs = ""
        try:
            handle = self._get_run_handle(stage_run_id)
            logs = self.run_backend.get_logs(handle, tail=STAGE_LOG_TAIL_FOR_FINALIZE)
            # Use capability-based message when logs unavailable for remote backends
            if caps.has_launch_delay and not logs:
                logs = caps.logs_unavailable_message
        except Exception as e:
            warnings.append(f"LOG_FETCH failed: {e}")
            logs = f"[Error fetching logs: {e}]"

        # Remote backends store logs in cloud storage
        if caps.has_launch_delay and bucket_uri is not None:
            log_uri = str(bucket_uri.join("runs", stage_run_id, "logs", "train.log"))
            # Also persist a local copy for quick access/debugging
            if logs is not None:
                try:
                    self._persist_logs(stage_run_id, logs)
                except Exception:
                    warnings.append("LOG_PERSIST failed")
                    pass
        else:
            # Local backends persist logs locally
            log_uri = self._persist_logs(stage_run_id, logs) if logs is not None else None

        # Collect metrics from JSONL and store in database
        try:
            self._collect_metrics(stage_run_id, backend)
        except Exception as e:
            # Log warning but don't fail the run if metrics collection fails
            warnings.append(f"METRICS_COLLECTION failed: {e}")
            logger.warning(f"Failed to collect metrics for {stage_run_id}: {e}")

        # Extract auto-results from metrics and update run_results
        try:
            exp_manager = ExperimentRecordManager(self.db)
            auto_results = exp_manager.extract_auto_results(stage_run_id)
            if auto_results is not None:
                exp_manager.update_auto_results(stage_run_id, auto_results, status)
        except Exception as e:
            warnings.append(f"AUTO_RESULTS failed: {e}")
            logger.warning(f"Failed to extract auto-results for {stage_run_id}: {e}")

        # Run AI semantic review of outputs
        try:
            self._run_post_run_svs_review(stage_run_id)
        except Exception as e:
            warnings.append(f"POST_RUN_REVIEW failed: {e}")
            logger.warning(f"Failed AI post-run review for {stage_run_id}: {e}")

        # Collect SVS manifests (stats + AI findings)
        try:
            self._collect_svs_manifests(stage_run_id, backend)
        except Exception as e:
            warnings.append(f"SVS_MANIFESTS failed: {e}")
            logger.warning(f"Failed to collect SVS manifests for {stage_run_id}: {e}")

        # Extract failure pattern for self-learning (only on failure)
        if status == StageState.FAILED and self.config.svs.enabled and self.config.svs.auto_learn_failures:
            import threading

            from goldfish.svs.agent import get_agent_provider
            from goldfish.svs.patterns.extractor import extract_failure_pattern

            error_msg = logs[-500:] if logs else "Unknown error"
            agent = get_agent_provider(self.config.svs.agent_provider)

            def _extract_pattern() -> None:
                try:
                    logger.info(f"Starting pattern extraction for {stage_run_id}")
                    pattern = extract_failure_pattern(
                        db=self.db,
                        stage_run_id=stage_run_id,
                        error=error_msg,
                        logs=logs,
                        agent=agent,
                    )
                    if pattern:
                        logger.info(f"Created pattern {pattern.id}: {pattern.symptom[:50]}...")
                    else:
                        logger.info(f"No pattern extracted for {stage_run_id}")
                except Exception as e:
                    logger.warning(f"Pattern extraction failed for {stage_run_id}: {e}")

            thread = threading.Thread(target=_extract_pattern, daemon=True)
            thread.start()
            logger.info(f"Started background pattern extraction for {stage_run_id}")

        with self._metrics_sync_lock:
            self._metrics_sync_state.pop(stage_run_id, None)

        # Preserve meaningful error messages set before finalize (e.g., "Instance preempted")
        # Only use logs as error if no meaningful error was already set
        final_error = None
        if status == StageState.FAILED:
            # Check for existing meaningful error (set by monitor/refresh before finalize)
            existing_error = stage_run.get("error")
            if existing_error and not existing_error.startswith("[GCE logs unavailable"):
                # Preserve meaningful error, optionally append log snippet
                final_error = existing_error
                if logs and logs != "[GCE logs unavailable - instance may have been deleted or logs not synced]":
                    # Append last 200 chars of logs for context
                    log_snippet = self._redact_logs(logs[-200:])
                    final_error = f"{existing_error}\n\nLast logs:\n{log_snippet}"
            elif logs:
                # No meaningful error - use logs as error
                final_error = self._redact_logs(logs[-STAGE_LOG_TAIL_FOR_FINALIZE:])

        # Update non-state metadata (state machine handles state via POST_RUN_OK/POST_RUN_FAIL below)
        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            completed_at=datetime.now(UTC).isoformat(),
            log_uri=log_uri,
            error=final_error,
        )

        # State machine: post-run success path → AWAITING_USER_FINALIZATION (v1.2).
        # If user already called finalize_run while RUNNING, skip AWAITING and go
        # straight to COMPLETED (early finalization).
        if status == StageState.COMPLETED:
            if warnings:
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.POST_RUN_FAIL,
                    SMEventContext(
                        timestamp=datetime.now(UTC),
                        source="executor",
                        critical=False,
                        error_message="; ".join(warnings[:5]),
                    ),
                )
            else:
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.POST_RUN_OK,
                    SMEventContext(timestamp=datetime.now(UTC), source="executor"),
                )

            # Check for early finalization — user already called finalize_run
            # while the run was still RUNNING. results_status='finalized' means
            # results are already recorded; skip AWAITING and go to COMPLETED.
            with self.db._conn() as conn:
                row = conn.execute(
                    "SELECT results_status FROM run_results WHERE stage_run_id = ?",
                    (stage_run_id,),
                ).fetchone()
            if row and row[0] == "finalized":
                logger.info("Early finalization detected for %s, auto-completing", stage_run_id)
                sm_transition(
                    self.db,
                    stage_run_id,
                    StageEvent.USER_FINALIZE,
                    SMEventContext(timestamp=datetime.now(UTC), source="executor"),
                )

        # Clean up container after finalization
        try:
            handle = self._get_run_handle(stage_run_id)
            self.run_backend.cleanup(handle)
        except Exception:
            warnings.append("CLEANUP failed")
            pass  # Container may already be removed

    def wait_for_completion(self, stage_run_id: str, poll_interval: int = 5, timeout: int = 3600) -> str:
        """Wait for stage run to complete.

        Polls container status and updates database.

        Args:
            stage_run_id: Stage run identifier
            poll_interval: Seconds between polls (default 5)
            timeout: Maximum seconds to wait (default 3600 = 1 hour)

        Returns:
            Final state: StageState.COMPLETED or FAILED

        Raises:
            GoldfishError: If timeout exceeded or container not found
        """
        backend = self.config.jobs.backend
        caps = self.run_backend.capabilities

        start = time.time()
        last_log: float = 0.0
        not_found_timeout = int(os.getenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "600"))

        # Get handle for status checks
        handle = self._get_run_handle(stage_run_id)

        while True:
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise GoldfishError(f"Stage run {stage_run_id} timed out after {timeout} seconds")

            # Get status via run_backend protocol
            backend_status_message: str | None = None
            try:
                backend_status = self.run_backend.get_status(handle)
                status = self._backend_status_to_stage_state(backend_status)
                backend_status_message = backend_status.message
            except NotFoundError:
                status = "not_found"

            # Handle common statuses (same for all backends)
            if status == StageState.RUNNING:
                interval = self._poll_interval(int(elapsed))
                time.sleep(interval)
                continue

            if status in (StageState.COMPLETED, StageState.FAILED):
                self._finalize_stage_run(stage_run_id, backend, status)
                return status

            if status == "unknown":
                # UNKNOWN status - log with details and continue polling
                # This handles transient API errors, GCS issues, etc.
                now = time.time()
                if now - last_log >= 60:
                    logger.warning(
                        "Status UNKNOWN for %s (elapsed: %ds): %s",
                        stage_run_id,
                        int(elapsed),
                        backend_status_message or "no details available",
                    )
                    last_log = now
                time.sleep(poll_interval)
                continue

            if status == "not_found":
                # Handle based on backend capabilities
                if not caps.has_launch_delay:
                    # Fast backend (local): short grace period for startup
                    if elapsed < 10:
                        time.sleep(0.5)
                        continue
                    raise GoldfishError(f"Container {stage_run_id} not found")
                else:
                    # Slow backend (GCE): longer timeouts with preemption handling
                    now = time.time()
                    if now - last_log >= 60:
                        logger.info(
                            f"Instance {stage_run_id} not yet visible "
                            f"(elapsed: {int(elapsed)}s, may be launching or searching capacity)"
                        )
                        last_log = now

                    row = self.db.get_stage_run(stage_run_id)
                    state_val = row.get("state") if row else None
                    instance_ran = state_val == StageState.RUNNING.value

                    # Longer timeout for BUILD/LAUNCH phases.
                    # not_found_timeout (600s) covers CPU VMs with data_disk provisioning.
                    launch_timeout = int(os.getenv("GOLDFISH_GCE_LAUNCH_TIMEOUT", "1200"))
                    if not_found_timeout <= 0:
                        effective_timeout = 0
                    else:
                        in_pre_run_state = state_val in (StageState.BUILDING.value, StageState.LAUNCHING.value)
                        effective_timeout = (
                            launch_timeout if in_pre_run_state and not instance_ran else not_found_timeout
                        )

                    if elapsed >= effective_timeout:
                        if instance_ran:
                            logger.warning(
                                f"Instance {stage_run_id} disappeared after running "
                                f"(likely preemption, no exit code found)"
                            )
                            error_msg = "Instance preempted/terminated unexpectedly (no exit code found)"
                            self.db.update_stage_run_status(stage_run_id=stage_run_id, error=error_msg)
                            self._finalize_stage_run(stage_run_id, backend, StageState.FAILED)
                            return StageState.FAILED
                        else:
                            logger.error(
                                f"Instance {stage_run_id} not found after {not_found_timeout}s "
                                f"(state={state_val}), marking as terminated"
                            )
                            error_msg = f"Instance not found after {not_found_timeout}s (may have failed to launch)"
                            sm_transition(
                                self.db,
                                stage_run_id,
                                StageEvent.INSTANCE_LOST,
                                SMEventContext(
                                    timestamp=datetime.now(UTC),
                                    source="executor",
                                    termination_cause=TerminationCause.ORPHANED,
                                    error_message=error_msg,
                                ),
                            )
                            self.db.update_stage_run_status(
                                stage_run_id=stage_run_id,
                                completed_at=datetime.now(UTC).isoformat(),
                                error=error_msg,
                            )
                            return StageState.TERMINATED
                    time.sleep(poll_interval)
                    continue

            # Unknown status
            raise GoldfishError(f"Unknown status: {status}")

    def refresh_status_once(self, stage_run_id: str) -> str | None:
        """Single backend check to advance state/logs/outputs without blocking."""
        with self._refresh_lock:
            if stage_run_id in self._refreshing_runs:
                # Already being refreshed by another thread
                row = self.db.get_stage_run(stage_run_id)
                return row.get("state") if row else None  # state is source of truth
            self._refreshing_runs.add(stage_run_id)

        try:
            return self._refresh_status_once_unlocked(stage_run_id)
        finally:
            with self._refresh_lock:
                self._refreshing_runs.remove(stage_run_id)

    def _refresh_status_once_unlocked(self, stage_run_id: str) -> str | None:
        """Internal implementation of refresh_status_once without locking."""
        backend = self.config.jobs.backend
        caps = self.run_backend.capabilities

        # Get status via run_backend protocol
        backend_status_message: str | None = None
        try:
            handle = self._get_run_handle(stage_run_id)
            backend_status = self.run_backend.get_status(handle)
            status = self._backend_status_to_stage_state(backend_status)
            backend_status_message = backend_status.message
        except NotFoundError:
            status = "not_found"

        # Handle common statuses (same for all backends)
        if status == "unknown":
            # UNKNOWN status - log with details but don't change state
            logger.warning(
                "Status UNKNOWN for %s: %s",
                stage_run_id,
                backend_status_message or "no details available",
            )
            return status

        if status == StageState.RUNNING:
            # State machine already has state=RUNNING, no update needed
            pass
        elif status in (StageState.COMPLETED, StageState.FAILED):
            # Guard against double-finalize by checking state (source of truth)
            current = self.db.get_stage_run(stage_run_id)
            current_state = current.get("state") if current else None
            if current and current_state not in {s.value for s in TERMINAL_STATES}:
                self._finalize_stage_run(stage_run_id, backend, status)
        elif status == "not_found" and caps.has_launch_delay:
            # Slow backend: handle not_found with timeout-based recovery
            row = self.db.get_stage_run(stage_run_id)
            if not row:
                return status

            state_val = row.get("state")
            not_found_timeout = int(os.getenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "600"))
            started_at = row.get("started_at")
            elapsed = float(not_found_timeout)
            if started_at:
                try:
                    started_dt = datetime.fromisoformat(started_at)
                    elapsed = (datetime.now(UTC) - started_dt).total_seconds()
                except ValueError:
                    elapsed = float(not_found_timeout)

            instance_ran = state_val == StageState.RUNNING.value

            # If in build/launch state and no evidence of running, skip recovery
            in_pre_run_state = state_val in (StageState.BUILDING.value, StageState.LAUNCHING.value)
            if in_pre_run_state and not instance_ran:
                return status

            if elapsed < not_found_timeout:
                return status

            # Instance disappeared after timeout - finalize as FAILED
            current = self.db.get_stage_run(stage_run_id)
            current_state = current.get("state") if current else None
            if current and current_state not in {s.value for s in TERMINAL_STATES}:
                if instance_ran:
                    error_msg = "Instance preempted/terminated unexpectedly (no exit code found)"
                else:
                    error_msg = f"Instance not found after {not_found_timeout}s (may have failed to launch)"
                self.db.update_stage_run_status(stage_run_id=stage_run_id, error=error_msg)
                self._finalize_stage_run(stage_run_id, backend, StageState.FAILED)
            return StageState.FAILED

        return status

    # --- Pre-run Review Methods ---

    def _perform_pre_run_review(
        self,
        workspace: str,
        stage_name: str,
        pipeline: PipelineDef,
        reason_structured: dict | None,
        git_sha: str,
        input_context: list[dict] | None = None,
        config_override: dict | None = None,
    ) -> RunReview | None:
        """Perform pre-run review using Claude Agent SDK.

        Args:
            workspace: Workspace name
            stage_name: Stage to review
            pipeline: Pipeline definition (for context)
            reason_structured: Structured RunReason dict
            git_sha: Current git SHA for diff calculation
            input_context: Resolved input metadata
            config_override: Runtime config overrides

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
                    svs_config=self.config.svs,
                    workspace_path=slot_path,
                    dev_repo_path=self.dev_repo,
                    stages=[stage_name],  # Review the specific stage
                    reason=run_reason,
                    diff_text=diff_text,
                    input_context=input_context,
                    db=self.db,
                    config_override=config_override,
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

    def _record_pre_run_review(self, stage_run_id: str, review: RunReview) -> None:
        """Record a pre-run review in the SVS reviews table.

        Args:
            stage_run_id: Stage run ID
            review: The RunReview to record
        """
        import hashlib

        now = datetime.now(UTC).isoformat()
        # Create hash of the full review text as prompt hash for deduplication/tracking
        prompt_hash = hashlib.sha256(review.full_review.encode()).hexdigest()

        decision = "approved"
        if not review.approved:
            decision = "blocked"
        elif any(i.severity == ReviewSeverity.WARNING for i in review.issues):
            decision = "warned"

        try:
            review_id = self.db.create_svs_review(
                stage_run_id=stage_run_id,
                review_type="pre_run",
                model_used=self.config.pre_run_review.model,
                prompt_hash=prompt_hash,
                decision=decision,
                reviewed_at=now,
                response_text=review.full_review,
                parsed_findings=json.dumps([i.model_dump(mode="json") for i in review.issues]),
                duration_ms=review.review_time_ms,
            )
            logger.info(
                "Recorded pre-run review for %s: id=%s decision=%s issues=%d",
                stage_run_id,
                review_id,
                decision,
                len(review.issues),
            )
        except Exception as e:
            logger.warning(f"Failed to record pre-run review for {stage_run_id}: {e}")

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
        preflight_warnings: list[str] | None = None,
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
            preflight_warnings=preflight_warnings,
            backend_type=None,  # Not executed - blocked by review
        )

        # Create experiment record for this run (even if blocked by review)
        exp_manager = ExperimentRecordManager(self.db)
        record_id = exp_manager.create_run_record(
            workspace_name=workspace,
            version=version,
            stage_run_id=stage_run_id,
        )

        # State machine: PREPARING → FAILED (SVS_BLOCK)
        sm_transition(
            self.db,
            stage_run_id,
            StageEvent.SVS_BLOCK,
            SMEventContext(timestamp=datetime.now(UTC), source="executor", error_message=error_msg),
        )

        # Update non-state metadata (error message)
        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            error=error_msg,
        )

        logger.warning(f"Stage run {stage_run_id} blocked by pre-run review: {review.summary}")

        return StageRunInfo(
            stage_run_id=stage_run_id,
            pipeline_run_id=pipeline_run_id,
            record_id=record_id,
            workspace=workspace,
            pipeline=pipeline_name,
            version=version,
            stage=stage_name,
            status=StageState.FAILED,
            state=StageState.FAILED.value,
            started_at=parse_optional_datetime(now),
            completed_at=parse_optional_datetime(now),
            error=error_msg,
        )

    def _list_storage_contents(self, path: str, limit: int = 100) -> list[str]:
        """List contents (files and folders) of a storage location.

        Args:
            path: Storage location (cloud URI or local path)
            limit: Maximum number of items to return

        Returns:
            List of paths relative to the input root
        """
        if not path:
            return []

        # 1. Cloud Storage Path (parse any supported scheme)
        if "://" in path and not path.startswith("file://"):
            try:
                uri = StorageURI.parse(path)
                uris = self.storage.list_prefix(uri)

                # Make paths relative to the prefix
                results = []
                prefix_len = len(uri.path)
                for item_uri in uris[:limit]:
                    rel_path = item_uri.path[prefix_len:].lstrip("/")
                    if rel_path:
                        results.append(rel_path)
                return sorted(results)
            except Exception as e:
                logger.debug(f"Failed to list storage contents for {path}: {e}")
                return [f"[Error listing storage contents: {e}]"]

        # 2. Local Path
        try:
            local_path = Path(path)
            if not local_path.exists():
                return ["[Path not found]"]

            if local_path.is_file():
                return [local_path.name]

            if local_path.is_dir():
                results = []
                for p in local_path.rglob("*"):
                    # Limit depth or count if needed, but rglob is usually fine for small dirs
                    rel = str(p.relative_to(local_path))
                    if p.is_dir():
                        rel += "/"
                    results.append(rel)
                    if len(results) >= limit:
                        break
                return sorted(results)
        except Exception as e:
            logger.debug(f"Failed to list local contents for {path}: {e}")
            return [f"[Error listing local contents: {e}]"]

        return []

    def _create_preflight_blocked_stage_run(
        self,
        stage_run_id: str,
        workspace: str,
        version: str,
        stage_name: str,
        errors: list[str],
        warnings: list[str],
        reason_structured: dict | None,
        pipeline_run_id: str | None,
        pipeline_name: str | None,
    ) -> StageRunInfo:
        """Create a failed stage run record for preflight validation errors."""
        now = datetime.now(UTC).isoformat()
        error_msg = "Preflight validation failed: " + "; ".join(errors[:5])

        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=version,
            stage_name=stage_name,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            reason=reason_structured,
            preflight_errors=errors,
            preflight_warnings=warnings,
            backend_type=None,  # Not executed - blocked by preflight
        )

        # Create experiment record for this run (even if blocked by preflight)
        exp_manager = ExperimentRecordManager(self.db)
        record_id = exp_manager.create_run_record(
            workspace_name=workspace,
            version=version,
            stage_run_id=stage_run_id,
        )

        # State machine: PREPARING → FAILED (PREPARE_FAIL)
        sm_transition(
            self.db,
            stage_run_id,
            StageEvent.PREPARE_FAIL,
            SMEventContext(timestamp=datetime.now(UTC), source="executor", error_message=error_msg),
        )

        # Update non-state metadata (state machine handles state via PREPARE_FAIL above)
        self.db.update_stage_run_status(
            stage_run_id=stage_run_id,
            completed_at=now,
            error=error_msg,
        )

        logger.warning("Stage run %s blocked by preflight validation: %s", stage_run_id, error_msg)

        return StageRunInfo(
            stage_run_id=stage_run_id,
            pipeline_run_id=pipeline_run_id,
            record_id=record_id,
            workspace=workspace,
            pipeline=pipeline_name,
            version=version,
            stage=stage_name,
            status=StageState.FAILED,
            state=StageState.FAILED.value,
            started_at=parse_optional_datetime(now),
            completed_at=parse_optional_datetime(now),
            error=error_msg,
        )
