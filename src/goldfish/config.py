"""Configuration loading for Goldfish."""

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class StateMdConfig(BaseModel):
    """STATE.md configuration."""

    path: str = "STATE.md"
    max_recent_actions: int = 15


class AuditConfig(BaseModel):
    """Audit trail configuration."""

    min_reason_length: int = 15


class JobsConfig(BaseModel):
    """Job execution configuration."""

    backend: str = "gce"
    infra_path: str | None = None  # Path to infra scripts (e.g., "../goldfish/infra")
    experiments_dir: str = "experiments"  # Where to export experiments


class GCSConfig(BaseModel):
    """GCS storage configuration."""

    bucket: str
    sources_prefix: str = "sources/"
    artifacts_prefix: str = "artifacts/"
    snapshots_prefix: str = "snapshots/"
    datasets_prefix: str = "datasets/"


class GCEConfig(BaseModel):
    """GCE (Google Compute Engine) configuration."""

    project_id: str

    # Optional: Artifact Registry URL for Docker images
    # Example: "us-docker.pkg.dev/{project_id}/goldfish"
    artifact_registry: str | None = None

    # Optional: global zone preferences (applies to all profiles)
    zones: list[str] | None = None

    # Optional: profile overrides and custom profiles
    # Example:
    # profile_overrides:
    #   h100-spot:
    #     zones: ["us-west1-a"]  # Override zones for h100-spot
    #   my-custom:
    #     machine_type: "n2-standard-16"
    #     zones: ["us-east1-b"]
    #     ...
    profile_overrides: dict[str, dict] | None = None

    # Runtime preferences
    gpu_preference: list[str] = Field(default_factory=lambda: ["h100", "a100", "none"])
    preemptible_preference: str = "on_demand_first"  # or "spot_first"
    search_timeout_sec: int = 900
    initial_backoff_sec: int = 10
    backoff_multiplier: float = 1.5
    max_attempts: int = 150


class GoldfishConfig(BaseModel):
    """Main Goldfish configuration."""

    project_name: str
    dev_repo_path: str  # Relative path to the -dev repo
    workspaces_dir: str = "workspaces"
    slots: list[str] = Field(default_factory=lambda: ["w1", "w2", "w3"])
    state_md: StateMdConfig = Field(default_factory=StateMdConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    jobs: JobsConfig = Field(default_factory=JobsConfig)
    gcs: GCSConfig | None = None
    gce: GCEConfig | None = None
    invariants: list[str] = Field(default_factory=list)

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

        try:
            return cls(**data)
        except ValidationError as e:
            # Extract the most useful error info without leaking internal details
            errors = e.errors()
            if errors:
                first_error = errors[0]
                field = ".".join(str(loc) for loc in first_error.get("loc", []))
                msg = first_error.get("msg", "validation error")
                raise GoldfishError(f"Invalid configuration: {field} - {msg}") from e
            raise GoldfishError("Invalid configuration: validation failed") from e

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
        """Path to the SQLite database (relative to project root)."""
        return ".goldfish/goldfish.db"


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
        invariants=[],
    )
