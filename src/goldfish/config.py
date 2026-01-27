"""Configuration loading for Goldfish."""

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from goldfish.svs.config import SVSConfig

logger = logging.getLogger(__name__)


class StateMdConfig(BaseModel):
    """STATE.md configuration."""

    model_config = ConfigDict(extra="forbid")

    path: str = "STATE.md"
    max_recent_actions: int = 15


class AuditConfig(BaseModel):
    """Audit trail configuration."""

    model_config = ConfigDict(extra="forbid")

    min_reason_length: int = 15


class LocalStorageConfig(BaseModel):
    """Local storage simulation configuration per LOCAL_PARITY_SPEC."""

    model_config = ConfigDict(extra="forbid")

    root: str = ".local_gcs"  # Directory for emulated buckets
    consistency_delay_ms: int = 0  # Delay reads after writes (0 = immediate)
    size_limit_mb: int | None = None  # Optional: simulate bucket quota


class LocalComputeConfig(BaseModel):
    """Local compute simulation configuration per LOCAL_PARITY_SPEC."""

    model_config = ConfigDict(extra="forbid")

    docker_socket: str = "/var/run/docker.sock"
    simulate_preemption_after_seconds: int | None = None  # null = no preemption
    preemption_grace_period_seconds: int = 30  # Match GCP behavior
    zone_availability: dict[str, bool] = Field(default_factory=lambda: {"local-zone-1": True})


class LocalSignalingConfig(BaseModel):
    """Local signaling simulation configuration per LOCAL_PARITY_SPEC."""

    model_config = ConfigDict(extra="forbid")

    metadata_file: str = ".local_metadata.json"
    size_limit_bytes: int = 262144  # 256KB per value (GCP limit)
    latency_ms: int = 0  # Simulated latency


class LocalConfig(BaseModel):
    """Local backend simulation configuration per LOCAL_PARITY_SPEC.

    Controls how the local backend emulates GCP semantics for testing.
    """

    model_config = ConfigDict(extra="forbid")

    storage: LocalStorageConfig = Field(default_factory=LocalStorageConfig)
    compute: LocalComputeConfig = Field(default_factory=LocalComputeConfig)
    signaling: LocalSignalingConfig = Field(default_factory=LocalSignalingConfig)


class JobsConfig(BaseModel):
    """Job execution configuration."""

    model_config = ConfigDict(extra="forbid")

    backend: str = "gce"
    infra_path: str | None = None  # Path to infra scripts (e.g., "../goldfish/infra")
    experiments_dir: str = "experiments"  # Where to export experiments

    # Local Docker container resource limits (for backend="local")
    container_memory: str | None = None  # e.g., "4g", "8g" - Docker memory limit
    container_cpus: str | None = None  # e.g., "2.0", "4.0" - Docker CPU limit
    container_pids: int | None = None  # e.g., 100, 200 - Docker pids limit


class GCSConfig(BaseModel):
    """GCS storage configuration."""

    model_config = ConfigDict(extra="forbid")

    bucket: str
    sources_prefix: str = "sources/"
    artifacts_prefix: str = "artifacts/"
    snapshots_prefix: str = "snapshots/"
    datasets_prefix: str = "datasets/"


class PreRunReviewConfig(BaseModel):
    """Pre-run review configuration using Claude Agent SDK."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    model: str = "claude-opus-4-5-20251101"
    timeout_seconds: int = 60
    max_turns: int = 30  # Max agent turns for exploring code


class MetricsConfig(BaseModel):
    """Metrics collection configuration."""

    model_config = ConfigDict(extra="forbid")

    backend: str | None = None  # "wandb", "mlflow", or None for local-only
    wandb: dict[str, str] | None = None  # W&B-specific config (project, entity)


class CloudBuildConfig(BaseModel):
    """Cloud Build settings for remote Docker image builds.

    Cloud Build allows building images on GCP instead of locally,
    which is faster and doesn't tie up the local machine.

    Machine type notes:
    - Default pools max out at E2_HIGHCPU_32 (32 vCPUs)
    - N1_HIGHCPU_32 also available at same price
    - Private pools support up to N2_HIGHCPU_96 but require additional setup
    """

    model_config = ConfigDict(extra="forbid")

    machine_type: str = "E2_HIGHCPU_32"  # Max for default pools (32 vCPUs)
    timeout_minutes: int = 60  # Build timeout
    disk_size_gb: int = 200  # GPU images are large

    # FlashAttention-3 wheel from GCS (for GPU base image builds)
    # Pre-built wheel avoids 2+ hour compile time during Docker build
    # Example: "gs://bucket/wheels/flash_attn_3-3.0.0b1-cp39-abi3-linux_x86_64.whl"
    fa3_wheel_gcs: str | None = None


class DockerConfig(BaseModel):
    """Docker image customization configuration.

    Allows projects to customize Docker base images without editing Goldfish source.
    Three approaches:
    1. extra_packages: Add pip packages on top of goldfish-base-{cpu,gpu} images
    2. base_images: Override base image names/URLs per type (cpu, gpu)
    3. Custom Dockerfiles: Place Dockerfile.base-cpu/Dockerfile.base-gpu in project root

    Example goldfish.yaml:
        docker:
          # Override base images (optional)
          base_images:
            gpu: "us-docker.pkg.dev/my-project/repo/my-custom-gpu:v1"
            cpu: "goldfish-base-cpu"  # Use default with custom version below
          base_image_version: "v10"  # Override default version for short names

          # Add packages on top of base images
          extra_packages:
            gpu:
              - flash-attn --no-build-isolation
              - triton
            cpu:
              - lightgbm

          cloud_build:
            machine_type: E2_HIGHCPU_32
            timeout_minutes: 60
    """

    model_config = ConfigDict(extra="forbid")

    # Base image overrides per type (cpu, gpu)
    # Values can be:
    # - Short name: "goldfish-base-gpu" (uses artifact_registry + base_image_version)
    # - Full URL: "us-docker.pkg.dev/project/repo/image:tag" (used as-is)
    base_images: dict[str, str] = Field(default_factory=dict)

    # Override the default base image version (default: from profiles.py)
    # Only applies to short names, not full URLs
    base_image_version: str | None = None

    extra_packages: dict[str, list[str]] = Field(default_factory=dict)
    # Keys: "gpu", "cpu"
    # Values: list of pip install args (supports flags like --no-build-isolation)

    cloud_build: CloudBuildConfig = Field(default_factory=CloudBuildConfig)


class GCEConfig(BaseModel):
    """GCE (Google Compute Engine) configuration."""

    model_config = ConfigDict(extra="forbid")

    # Project ID - accepts both "project_id" and "project" for convenience
    project_id: str | None = Field(default=None)
    project: str | None = Field(default=None)  # Alias for project_id

    # Optional: Artifact Registry URL for Docker images
    # Example: "us-docker.pkg.dev/{project_id}/goldfish"
    artifact_registry: str | None = Field(default=None)
    image_uri: str | None = Field(default=None)  # Alias for artifact_registry

    # Optional: global zone preferences (applies to all profiles)
    zones: list[str] | None = None
    region: str | None = None  # Alternative to zones

    # Optional: profile overrides and custom profiles - accepts both names
    # Example:
    # profile_overrides:
    #   h100-spot:
    #     zones: ["us-west1-a"]  # Override zones for h100-spot
    #   my-custom:
    #     machine_type: "n2-standard-16"
    #     zones: ["us-east1-b"]
    #     ...
    profile_overrides: dict[str, dict] | None = None
    profiles: dict[str, dict] | None = None  # Alias for profile_overrides

    # Service account (optional)
    service_account: str | None = None

    # Runtime preferences
    gpu_preference: list[str] = Field(default_factory=lambda: ["h100", "a100", "none"])
    preemptible_preference: str = "on_demand_first"  # or "spot_first"
    search_timeout_sec: int = 900
    initial_backoff_sec: int = 10
    backoff_multiplier: float = 1.5
    max_attempts: int = 150

    @property
    def effective_project_id(self) -> str:
        """Get the project ID from either field."""
        if self.project_id:
            return self.project_id
        if self.project:
            return self.project
        raise ValueError("GCE config requires project_id or project")

    @property
    def effective_artifact_registry(self) -> str | None:
        """Get artifact registry URL from either field, or auto-generate from project_id."""
        if self.artifact_registry:
            return self.artifact_registry
        if self.image_uri:
            return self.image_uri
        # Auto-generate from project_id if available
        try:
            project_id = self.effective_project_id
            return f"us-docker.pkg.dev/{project_id}/goldfish"
        except ValueError:
            return None

    @property
    def effective_profile_overrides(self) -> dict[str, dict] | None:
        """Get profile overrides from either field."""
        return self.profile_overrides or self.profiles


def _get_valid_fields_for_path(loc: tuple | list) -> list[str]:
    """Get valid field names for a given error location path.

    Args:
        loc: Location tuple from Pydantic error, e.g., ('gce', 'projeect')
            For top-level errors: ('projeect_name',)
            For nested errors: ('gce', 'projeect')

    Returns:
        List of valid field names for that section
    """
    # Map section names to their valid fields
    field_maps = {
        "state_md": list(StateMdConfig.model_fields.keys()),
        "audit": list(AuditConfig.model_fields.keys()),
        "jobs": list(JobsConfig.model_fields.keys()),
        "gcs": list(GCSConfig.model_fields.keys()),
        "gce": list(GCEConfig.model_fields.keys()),
        "local": list(LocalConfig.model_fields.keys()),
        "pre_run_review": list(PreRunReviewConfig.model_fields.keys()),
        "metrics": list(MetricsConfig.model_fields.keys()),
        "docker": list(DockerConfig.model_fields.keys()),
        "svs": list(SVSConfig.model_fields.keys()),
    }

    top_level_fields = [
        "project_name",
        "dev_repo_path",
        "workspaces_dir",
        "slots",
        "state_md",
        "audit",
        "jobs",
        "gcs",
        "gce",
        "local",
        "pre_run_review",
        "metrics",
        "docker",
        "svs",
        "invariants",
    ]

    if not loc:
        return top_level_fields

    # If only one element, it's a top-level field error
    if len(loc) == 1:
        return top_level_fields

    # Check if first element is a known section (for nested errors)
    first = str(loc[0])
    if first in field_maps:
        return field_maps[first]

    # Default to top-level fields
    return top_level_fields


class GoldfishConfig(BaseModel):
    """Main Goldfish configuration."""

    model_config = ConfigDict(extra="forbid")

    project_name: str
    dev_repo_path: str  # Relative path to the -dev repo
    workspaces_dir: str = "workspaces"
    slots: list[str] = Field(default_factory=lambda: ["w1", "w2", "w3"])
    state_md: StateMdConfig = Field(default_factory=StateMdConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    jobs: JobsConfig = Field(default_factory=JobsConfig)
    gcs: GCSConfig | None = None
    gce: GCEConfig | None = None
    local: LocalConfig = Field(default_factory=LocalConfig)  # Local simulation config
    pre_run_review: PreRunReviewConfig = Field(default_factory=PreRunReviewConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)
    svs: SVSConfig = Field(default_factory=SVSConfig)
    invariants: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_gce_config(self) -> "GoldfishConfig":
        """Validate GCE configuration completeness.

        Warns if GCE backend is enabled but zones aren't configured,
        which will cause runtime errors when launching instances.
        """
        if self.jobs.backend == "gce":
            if self.gce is None:
                logger.warning(
                    "jobs.backend is 'gce' but no gce config provided. "
                    "GCE launches will fail. Add gce section to goldfish.yaml."
                )
            elif not self.gce.zones and not self.gce.region:
                logger.warning(
                    "GCE backend enabled but no zones configured. "
                    "Add 'zones' to gce config in goldfish.yaml to specify "
                    "which GCP zones to use for instance launches."
                )
            if self.gce and not self.gce.effective_artifact_registry:
                logger.warning(
                    "GCE backend enabled but no artifact_registry configured. "
                    "GPU profiles require artifact_registry for Docker images. "
                    "Add 'artifact_registry' to gce config in goldfish.yaml."
                )
        return self

    @classmethod
    def load(cls, project_root: Path) -> "GoldfishConfig":
        """Load configuration from goldfish.yaml in project root."""
        from pydantic import ValidationError

        from goldfish.errors import GoldfishError, ProjectNotInitializedError

        config_path = project_root / "goldfish.yaml"
        if not config_path.exists():
            raise ProjectNotInitializedError(f"No goldfish.yaml found in {project_root}. Run 'goldfish init' first.")

        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)
        except (OSError, PermissionError) as e:
            raise GoldfishError(f"Cannot read configuration file: {type(e).__name__}") from e
        except yaml.YAMLError as e:
            raise GoldfishError("Failed to parse configuration file: invalid YAML syntax") from e

        if data is None:
            raise GoldfishError("Configuration file is empty")

        # Handle common misconfigurations: gce nested inside jobs
        if "gce" not in data and "jobs" in data and isinstance(data["jobs"], dict):
            if "gce" in data["jobs"]:
                data["gce"] = data["jobs"].pop("gce")

        # Handle convenience: gcs_bucket inside gce section -> create gcs config
        if "gcs" not in data and "gce" in data and isinstance(data["gce"], dict):
            gcs_bucket = data["gce"].pop("gcs_bucket", None)
            if gcs_bucket:
                data["gcs"] = {"bucket": gcs_bucket}

        # Migrate old profile_overrides format
        if "gce" in data and isinstance(data["gce"], dict):
            profile_overrides = data["gce"].get("profile_overrides") or data["gce"].get("profiles")
            if profile_overrides and isinstance(profile_overrides, dict):
                for _profile_name, profile in profile_overrides.items():
                    if isinstance(profile, dict):
                        # Migrate preemptible -> preemptible_allowed/on_demand_allowed
                        if "preemptible" in profile and "preemptible_allowed" not in profile:
                            is_preemptible = profile.pop("preemptible")
                            profile["preemptible_allowed"] = bool(is_preemptible)
                            # If preemptible=true, assume on_demand is also allowed unless explicitly set
                            if "on_demand_allowed" not in profile:
                                profile["on_demand_allowed"] = True

                        # Migrate gpu.type to include gpu.accelerator if missing
                        if "gpu" in profile and isinstance(profile["gpu"], dict):
                            gpu = profile["gpu"]
                            if "type" in gpu and "accelerator" not in gpu:
                                # Map common GPU types to GCE accelerator names
                                gpu_type_map = {
                                    "nvidia-h100-80gb": "nvidia-h100-80gb",
                                    "nvidia-tesla-a100": "nvidia-tesla-a100",
                                    "nvidia-tesla-t4": "nvidia-tesla-t4",
                                    "nvidia-tesla-v100": "nvidia-tesla-v100",
                                    "h100": "nvidia-h100-80gb",
                                    "a100": "nvidia-tesla-a100",
                                    "t4": "nvidia-tesla-t4",
                                    "v100": "nvidia-tesla-v100",
                                    "none": None,
                                }
                                gpu_type = gpu["type"]
                                gpu["accelerator"] = gpu_type_map.get(gpu_type, gpu_type)
                            if "count" not in gpu:
                                gpu["count"] = 1 if gpu.get("accelerator") else 0

        # Migrate legacy extra_packages at root level to docker.extra_packages.base
        # Old format: extra_packages: [list] at root
        # New format: docker: {extra_packages: {base: [list]}}
        if "extra_packages" in data:
            extra_packages = data.pop("extra_packages")
            if isinstance(extra_packages, list):
                # Root-level list -> docker.extra_packages.base
                if "docker" not in data:
                    data["docker"] = {}
                if "extra_packages" not in data["docker"]:
                    data["docker"]["extra_packages"] = {}
                # Put in "base" key - applies to both cpu and gpu
                data["docker"]["extra_packages"]["base"] = extra_packages
                logger.info(
                    "Migrated legacy extra_packages to docker.extra_packages.base. "
                    "Consider updating goldfish.yaml to use the new format."
                )

        try:
            config = cls(**data)
        except ValidationError as e:
            # Extract the most useful error info with suggestions for typos
            from goldfish.validation import format_unknown_field_error

            errors = e.errors()
            if errors:
                first_error = errors[0]
                error_type = first_error.get("type", "")
                field = ".".join(str(loc) for loc in first_error.get("loc", []))
                msg = first_error.get("msg", "validation error")

                # Handle extra_forbidden (unknown field) with suggestions
                if error_type == "extra_forbidden":
                    # Get valid fields for the context
                    valid_fields = _get_valid_fields_for_path(first_error.get("loc", []))
                    error_msg = format_unknown_field_error(field, valid_fields)
                    raise GoldfishError(f"Invalid configuration: {error_msg}") from e

                raise GoldfishError(f"Invalid configuration: {field} - {msg}") from e
            raise GoldfishError("Invalid configuration: validation failed") from e

        return config

    def save(self, project_root: Path) -> None:
        """Save configuration to goldfish.yaml."""
        from goldfish.errors import GoldfishError

        config_path = project_root / "goldfish.yaml"
        try:
            with open(config_path, "w") as f:
                yaml.safe_dump(
                    self.model_dump(exclude_none=True),
                    f,
                    sort_keys=False,
                    default_flow_style=False,
                )
        except (OSError, PermissionError) as e:
            raise GoldfishError(f"Cannot write configuration file: {type(e).__name__}") from e

    @property
    def db_path(self) -> str:
        """Path to the SQLite database (relative to dev repo)."""
        return ".goldfish/goldfish.db"

    def get_dev_repo_path(self, project_root: Path) -> Path:
        """Resolve the dev repo path to an absolute path.

        Args:
            project_root: The user's project root directory.

        Returns:
            Absolute path to the dev repository.
        """
        # dev_repo_path is stored relative to project_root's parent
        # e.g., if project is /home/user/mlm, dev_repo_path might be "mlm-dev"
        # which resolves to /home/user/mlm-dev
        # NOTE: Must resolve project_root first, otherwise Path('.').parent is '.'
        return (project_root.resolve().parent / self.dev_repo_path).resolve()


def generate_default_config(project_name: str, dev_repo_path: str = "../{project}-dev") -> GoldfishConfig:
    """Generate a default configuration for a new project."""
    return GoldfishConfig(
        project_name=project_name,
        dev_repo_path=dev_repo_path.format(project=project_name),
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(),
        audit=AuditConfig(),
        jobs=JobsConfig(),
        svs=SVSConfig(),
        invariants=[],
    )
