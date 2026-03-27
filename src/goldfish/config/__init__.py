"""Configuration models and loading for Goldfish."""

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Self

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


class DefaultsConfig(BaseModel):
    """Global defaults configuration for stage execution.

    These defaults apply to all stages unless overridden at the stage level.

    Example goldfish.yaml:
        defaults:
          timeout_seconds: 7200    # 2 hours
          log_sync_interval: 15    # Sync logs every 15 seconds
          backend: gce             # Default compute backend
          capacity_wait_seconds: 3600  # Keep trying for 1 hour
          launch_timeout_seconds: 2700  # 45 min for large GPU VMs
    """

    model_config = ConfigDict(extra="forbid")

    timeout_seconds: int = Field(default=3600, gt=0)  # 1 hour default
    log_sync_interval: int = Field(default=10, gt=0)  # 10 seconds default
    backend: Literal["local", "gce", "kubernetes"] = "local"
    capacity_wait_seconds: int = Field(default=600, gt=0)  # 10 min default
    launch_timeout_seconds: int = Field(default=2700, gt=0)  # Total LAUNCHING state budget (capacity search + boot).
    # Must exceed capacity_wait_seconds. Per-attempt gcloud timeout is separate
    # (600s GPU default, configurable per profile via gce.profile_overrides.*.launch_timeout_seconds).

    @model_validator(mode="after")
    def _validate_launch_timeout_exceeds_capacity_wait(self) -> Self:
        if self.launch_timeout_seconds < self.capacity_wait_seconds:
            raise ValueError(
                f"launch_timeout_seconds ({self.launch_timeout_seconds}) must be >= "
                f"capacity_wait_seconds ({self.capacity_wait_seconds}); "
                f"the launch timeout is the total budget including capacity search + boot"
            )
        return self


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


class S3StorageConfig(BaseModel):
    """S3 storage configuration.

    For AWS S3 or S3-compatible storage (MinIO, etc).

    Security: endpoint_url is validated to prevent SSRF attacks.
    Only public DNS names are allowed - localhost, internal IPs, and
    cloud metadata endpoints are rejected.
    """

    model_config = ConfigDict(extra="forbid")

    bucket: str
    region: str | None = None
    endpoint_url: str | None = None  # For S3-compatible (MinIO, etc)
    sources_prefix: str = "sources/"
    artifacts_prefix: str = "artifacts/"
    snapshots_prefix: str = "snapshots/"
    datasets_prefix: str = "datasets/"

    @model_validator(mode="after")
    def validate_endpoint_url_security(self) -> "S3StorageConfig":
        """Validate endpoint_url to prevent SSRF attacks.

        Rejects:
        - localhost and 127.0.0.1
        - Internal IP ranges (10.x, 172.16-31.x, 192.168.x)
        - Cloud metadata endpoints (169.254.169.254)
        - Link-local addresses (169.254.x.x)
        """
        if self.endpoint_url is None:
            return self

        import ipaddress
        from urllib.parse import urlparse

        try:
            parsed = urlparse(self.endpoint_url)
            host = parsed.hostname
        except Exception:
            raise ValueError(f"Invalid endpoint_url format: {self.endpoint_url}") from None

        if not host:
            raise ValueError("endpoint_url must include a hostname")

        # Check for localhost
        if host.lower() in ("localhost", "localhost.localdomain"):
            raise ValueError(
                "endpoint_url cannot be localhost (SSRF protection). "
                "Use a public DNS name for S3-compatible storage."
            )

        # Check if host is an IP address (IPv4 or IPv6)
        def check_ip_security(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
            """Check IP address for SSRF vulnerabilities."""
            # Reject loopback (127.x.x.x for IPv4, ::1 for IPv6)
            if ip.is_loopback:
                raise ValueError(
                    f"endpoint_url cannot use loopback address {host} (SSRF protection). "
                    "Use a public DNS name for S3-compatible storage."
                )

            # Reject private IP ranges (10.x, 172.16-31.x, 192.168.x for IPv4; fc00::/7 for IPv6)
            if ip.is_private:
                raise ValueError(
                    f"endpoint_url cannot use internal IP address {host} (SSRF protection). "
                    "Use a public DNS name for S3-compatible storage."
                )

            # Reject link-local (169.254.x.x for IPv4, fe80::/10 for IPv6)
            if ip.is_link_local:
                raise ValueError(
                    f"endpoint_url cannot use link-local address {host} (SSRF protection). "
                    "This includes cloud metadata endpoints."
                )

        # Try to parse as IP address (IPv4 or IPv6)
        ip: ipaddress.IPv4Address | ipaddress.IPv6Address | None = None
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            # Not a valid IP address - could be a hostname (which is fine)
            pass

        if ip is not None:
            check_ip_security(ip)

        # Check for IPv4-mapped IPv6 addresses (::ffff:x.x.x.x)
        if host.lower().startswith("::ffff:"):
            ipv4_part = host[7:]  # Strip ::ffff: prefix
            try:
                ipv4 = ipaddress.ip_address(ipv4_part)
                check_ip_security(ipv4)
            except ValueError:
                pass

        return self


class AzureStorageConfig(BaseModel):
    """Azure Blob storage configuration.

    Azure storage account names must be:
    - 3-24 characters long
    - Alphanumeric only (lowercase letters and numbers)
    """

    model_config = ConfigDict(extra="forbid")

    container: str
    account: str
    sources_prefix: str = "sources/"
    artifacts_prefix: str = "artifacts/"
    snapshots_prefix: str = "snapshots/"
    datasets_prefix: str = "datasets/"

    @model_validator(mode="after")
    def validate_account_name(self) -> "AzureStorageConfig":
        """Validate Azure storage account name format.

        Azure requires: 3-24 characters, alphanumeric only (lowercase).
        """
        import re

        account = self.account

        if len(account) < 3:
            raise ValueError(
                f"Azure storage account name must be at least 3 characters, got {len(account)}. "
                "Account names must be 3-24 characters, alphanumeric only."
            )

        if len(account) > 24:
            raise ValueError(
                f"Azure storage account name must be at most 24 characters, got {len(account)}. "
                "Account names must be 3-24 characters, alphanumeric only."
            )

        # Azure requires lowercase alphanumeric only
        if not re.match(r"^[a-z0-9]+$", account):
            raise ValueError(
                f"Azure storage account name must be alphanumeric only (lowercase letters and numbers). "
                f"Got: '{account}'. Remove hyphens, underscores, and uppercase letters."
            )

        return self


StorageBackend = Literal["gcs", "s3", "azure", "local"]


class StorageConfig(BaseModel):
    """Unified storage backend configuration.

    Allows selecting between multiple storage backends:
    - gcs: Google Cloud Storage (default)
    - s3: AWS S3 or S3-compatible
    - azure: Azure Blob Storage
    - local: Local filesystem (for development/testing)

    The selected backend must have its corresponding configuration section present.
    For example, backend='s3' requires the s3: section to be defined.

    Example goldfish.yaml:
        storage:
          backend: s3
          s3:
            bucket: my-bucket
            region: us-east-1
    """

    model_config = ConfigDict(extra="forbid")

    backend: StorageBackend = "gcs"
    gcs: GCSConfig | None = None
    s3: S3StorageConfig | None = None
    azure: AzureStorageConfig | None = None

    @model_validator(mode="after")
    def validate_backend_config_consistency(self) -> "StorageConfig":
        """Validate that the selected backend has its config section.

        Raises:
            ValueError: If backend is set but its config section is missing.
        """
        if self.backend == "gcs" and self.gcs is None:
            raise ValueError(
                "storage.backend='gcs' requires storage.gcs section with bucket configuration. "
                "Add gcs: {bucket: 'your-bucket'} to storage config."
            )
        if self.backend == "s3" and self.s3 is None:
            raise ValueError(
                "storage.backend='s3' requires storage.s3 section with bucket configuration. "
                "Add s3: {bucket: 'your-bucket'} to storage config."
            )
        if self.backend == "azure" and self.azure is None:
            raise ValueError(
                "storage.backend='azure' requires storage.azure section with container and account. "
                "Add azure: {container: 'your-container', account: 'your-account'} to storage config."
            )
        # backend='local' doesn't require any additional config
        return self

    @property
    def effective_bucket(self) -> str | None:
        """Get the bucket/container name for the active backend.

        Returns:
            The bucket name for GCS/S3, container for Azure, or None for local.
        """
        if self.backend == "gcs" and self.gcs:
            return self.gcs.bucket
        if self.backend == "s3" and self.s3:
            return self.s3.bucket
        if self.backend == "azure" and self.azure:
            return self.azure.container
        return None


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
    # Example: "<scheme>://bucket/wheels/flash_attn_3-3.0.0b1-cp39-abi3-linux_x86_64.whl"
    fa3_wheel_gcs: str | None = None


class DockerConfig(BaseModel):
    """Docker image customization configuration.

    Allows projects to customize Docker base images without editing Goldfish source.
    Three approaches:
    1. extra_packages: Add pip packages on top of goldfish-base-{cpu,gpu} images
    2. base_images: Override base image names/URLs per type (cpu, gpu)
    3. Custom Dockerfiles: Place Dockerfile.base-cpu/Dockerfile.base-gpu in project root

    Version resolution precedence:
    - BASE images: config -> DB -> default constant (v10)
    - PROJECT images: config -> DB -> None (no default - user-built)

    Note: Project images have NO default version because they're user-built,
    not Goldfish-shipped. If no version exists, builds use the next version
    from the database (v1 for first build, vN+1 for subsequent).

    Example goldfish.yaml:
        docker:
          # Override base images (optional)
          base_images:
            gpu: "us-docker.pkg.dev/my-project/repo/my-custom-gpu:v1"
            cpu: "goldfish-base-cpu"  # Use default with custom version below
          base_image_version: "v10"    # Override base image version
          project_image_version: "v3"  # Override project image version

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

    # Override the default base image version (default: v10 from image_versions.py)
    # Only applies to short names, not full URLs
    base_image_version: str | None = None

    # Override the project image version (no default - project images are user-built)
    # Project images are {project_name}-{cpu,gpu}:{version}
    # If not set, uses current version from DB (or next version for builds)
    project_image_version: str | None = None

    extra_packages: dict[str, list[str]] = Field(default_factory=dict)
    # Keys: "gpu", "cpu"
    # Values: list of pip install args (supports flags like --no-build-isolation)

    cloud_build: CloudBuildConfig = Field(default_factory=CloudBuildConfig)


class WarmPoolConfig(BaseModel):
    """Warm VM pool configuration (optional, off by default)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    max_instances: int = Field(default=2, ge=1, le=10)
    idle_timeout_minutes: int = Field(default=30, ge=5, le=120)
    profiles: list[str] = Field(default_factory=list)
    watchdog_seconds: int = Field(default=21600, ge=3600)
    preserve_paths: list[str] = Field(default_factory=list)


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
    profile_overrides: dict[str, dict[str, object]] | None = None
    profiles: dict[str, dict[str, object]] | None = None  # Alias for profile_overrides

    # Service account (optional)
    service_account: str | None = None

    # Runtime preferences
    gpu_preference: list[str] = Field(default_factory=lambda: ["h100", "a100", "none"])
    preemptible_preference: str = "on_demand_first"  # or "spot_first"
    search_timeout_sec: int = 900
    initial_backoff_sec: int = 10
    backoff_multiplier: float = 1.5
    max_attempts: int = 150

    # Warm pool (optional, off by default)
    warm_pool: WarmPoolConfig = Field(default_factory=WarmPoolConfig)

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
    def effective_profile_overrides(self) -> dict[str, dict[str, object]] | None:
        """Get profile overrides from either field."""
        return self.profile_overrides or self.profiles


def _get_valid_fields_for_path(loc: Sequence[object]) -> list[str]:
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
        "defaults": list(DefaultsConfig.model_fields.keys()),
        "jobs": list(JobsConfig.model_fields.keys()),
        "gcs": list(GCSConfig.model_fields.keys()),
        "storage": list(StorageConfig.model_fields.keys()),
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
        "defaults",
        "jobs",
        "gcs",
        "storage",
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
    slots: list[str] = Field(default_factory=lambda: ["w1", "w2", "w3"])  # Also accepts int in YAML
    state_md: StateMdConfig = Field(default_factory=StateMdConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    jobs: JobsConfig = Field(default_factory=JobsConfig)
    gcs: GCSConfig | None = None
    storage: StorageConfig | None = None
    gce: GCEConfig | None = None
    local: LocalConfig = Field(default_factory=LocalConfig)  # Local simulation config
    pre_run_review: PreRunReviewConfig = Field(default_factory=PreRunReviewConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)
    svs: SVSConfig = Field(default_factory=SVSConfig)
    invariants: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_slots(cls, data: dict[str, object]) -> dict[str, object]:
        """Convert integer slots to list: slots: 5 → [w1, w2, w3, w4, w5]."""
        if isinstance(data, dict):
            slots = data.get("slots")
            if isinstance(slots, bool):
                pass  # Reject: let Pydantic raise validation error
            elif isinstance(slots, int):
                if slots < 1:
                    raise ValueError(f"slots must be >= 1, got {slots}")
                data["slots"] = [f"w{i}" for i in range(1, slots + 1)]
        return data

    @model_validator(mode="after")
    def validate_gce_config(self) -> "GoldfishConfig":
        """Validate GCE configuration completeness.

        Validates/warns about the GCE config section when present.

        Note: Backend selection is handled behind adapter boundaries. Core config
        should not branch on backend strings (e.g., jobs.backend set to "gce").
        """
        if self.gce is None:
            return self

        if not self.gce.zones and not self.gce.region:
            logger.warning(
                "gce config provided but no zones configured. "
                "Add 'zones' or 'region' in goldfish.yaml to specify "
                "which GCP zones to use for instance launches."
            )

        if not self.gce.effective_artifact_registry:
            logger.warning(
                "gce config provided but no artifact_registry configured. "
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
                logger.warning(
                    "Migrated 'gce' from inside 'jobs' section to top level. "
                    "Consider updating goldfish.yaml: move 'gce:' to be a sibling of 'jobs:', not nested inside it."
                )

        # Handle convenience: gcs_bucket inside gce section -> create gcs config
        if "gcs" not in data and "gce" in data and isinstance(data["gce"], dict):
            gcs_bucket = data["gce"].pop("gcs_bucket", None)
            if gcs_bucket:
                data["gcs"] = {"bucket": gcs_bucket}
                logger.warning(
                    "Migrated 'gcs_bucket' from 'gce' section to 'gcs.bucket'. "
                    "Consider updating goldfish.yaml: use 'gcs: {bucket: %s}' instead of 'gce.gcs_bucket'.",
                    gcs_bucket,
                )

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
