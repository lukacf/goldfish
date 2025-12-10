"""Goldfish project initialization.

Creates the project structure and dev repository.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional

import yaml

# Timeout for git operations during init (60 seconds)
INIT_GIT_TIMEOUT = 60

from goldfish.config import (
    AuditConfig,
    GCSConfig,
    GCEConfig,
    GoldfishConfig,
    JobsConfig,
    StateMdConfig,
)
from goldfish.state.state_md import StateManager


def init_project(
    project_name: str,
    project_path: Path,
    dev_repo_path: Optional[Path] = None,
) -> GoldfishConfig:
    """Initialize a new Goldfish project.

    Creates:
    - {project}-dev/ git repo (sibling directory)
    - {project}/goldfish.yaml config file
    - {project}/STATE.md initial state
    - {project}/workspaces/ directory
    - {project}/.goldfish/ directory for database

    Args:
        project_name: Name of the project (e.g., "mlm")
        project_path: Path to the project directory
        dev_repo_path: Optional custom path for dev repo (default: sibling)

    Returns:
        GoldfishConfig for the initialized project
    """
    project_path = project_path.resolve()

    # Default dev repo is sibling: ../mlm-dev
    if dev_repo_path is None:
        dev_repo_path = project_path.parent / f"{project_name}-dev"

    dev_repo_path = dev_repo_path.resolve()

    # Create project directory if needed
    project_path.mkdir(parents=True, exist_ok=True)

    # Create dev repository
    _create_dev_repo(dev_repo_path, project_name)

    # Create workspaces directory
    workspaces_dir = project_path / "workspaces"
    workspaces_dir.mkdir(exist_ok=True)

    # Create .goldfish directory for database
    goldfish_dir = project_path / ".goldfish"
    goldfish_dir.mkdir(exist_ok=True)

    # Read GCS/GCE config from environment variables (optional)
    gcs_config = None
    gce_config = None

    gcs_bucket = os.getenv("GOLDFISH_GCS_BUCKET")
    if gcs_bucket:
        gcs_config = GCSConfig(bucket=gcs_bucket)

    gce_project = os.getenv("GOLDFISH_GCE_PROJECT")
    if gce_project:
        # Prefer explicit registry env, otherwise default to the project-scoped "goldfish" repo
        artifact_registry = os.getenv(
            "GOLDFISH_ARTIFACT_REGISTRY",
            f"us-docker.pkg.dev/{gce_project}/goldfish",
        )
        gce_config = GCEConfig(
            project_id=gce_project,
            artifact_registry=artifact_registry,
        )

    # Create config
    config = GoldfishConfig(
        project_name=project_name,
        dev_repo_path=str(dev_repo_path.relative_to(project_path.parent)),
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(
            path="STATE.md",
            max_recent_actions=15,
        ),
        audit=AuditConfig(
            min_reason_length=15,
        ),
        db_path=".goldfish/goldfish.db",
        jobs=JobsConfig(
            backend="gce",
            experiments_dir="experiments",
        ),
        gcs=gcs_config,
        gce=gce_config,
        invariants=[],
    )

    # Write config file
    config_path = project_path / "goldfish.yaml"
    _write_config(config, config_path)

    # Create initial STATE.md
    state_path = project_path / config.state_md.path
    StateManager.create_initial(state_path, config)

    return config


def _create_dev_repo(dev_repo_path: Path, project_name: str) -> None:
    """Create the dev git repository with initial structure."""
    if dev_repo_path.exists():
        # Check if it's already a git repo
        if (dev_repo_path / ".git").exists():
            return  # Already initialized
        raise ValueError("Dev repo path exists but is not a git repository")

    dev_repo_path.mkdir(parents=True)

    # Initialize git repo
    subprocess.run(
        ["git", "init"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )

    # Create initial workspace structure
    code_dir = dev_repo_path / "code"
    code_dir.mkdir()
    (code_dir / ".gitkeep").touch()

    scripts_dir = dev_repo_path / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / ".gitkeep").touch()

    entrypoints_dir = dev_repo_path / "entrypoints"
    entrypoints_dir.mkdir()
    (entrypoints_dir / ".gitkeep").touch()

    # Create .gitignore
    gitignore = dev_repo_path / ".gitignore"
    gitignore.write_text(
        """# Python
__pycache__/
*.py[cod]
*.egg-info/
.eggs/
dist/
build/

# Virtual environments
.venv/
venv/
env/

# IDE
.idea/
.vscode/
*.swp
*.swo

# Project specific
*.log
.DS_Store
"""
    )

    # Initial commit
    subprocess.run(
        ["git", "add", "-A"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )
    subprocess.run(
        ["git", "commit", "-m", f"Initialize {project_name} workspace structure"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )

    # Ensure the branch is named 'main' (git may default to 'master' or other names)
    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )


def _write_config(config: GoldfishConfig, config_path: Path) -> None:
    """Write config to YAML file."""
    config_dict = {
        "project_name": config.project_name,
        "dev_repo_path": config.dev_repo_path,
        "workspaces_dir": config.workspaces_dir,
        "slots": config.slots,
        "state_md": {
            "path": config.state_md.path,
            "max_recent_actions": config.state_md.max_recent_actions,
        },
        "audit": {
            "min_reason_length": config.audit.min_reason_length,
        },
        "db_path": config.db_path,
        "jobs": {
            "backend": config.jobs.backend,
            "experiments_dir": config.jobs.experiments_dir,
        },
    }

    if config.gcs:
        config_dict["gcs"] = {
            "bucket": config.gcs.bucket,
            "sources_prefix": config.gcs.sources_prefix,
            "artifacts_prefix": config.gcs.artifacts_prefix,
            "snapshots_prefix": config.gcs.snapshots_prefix,
            "datasets_prefix": config.gcs.datasets_prefix,
        }

    if config.gce:
        gce_dict = {"project_id": config.gce.project_id}
        if config.gce.artifact_registry:
            gce_dict["artifact_registry"] = config.gce.artifact_registry
        if config.gce.zones:
            gce_dict["zones"] = config.gce.zones
        if config.gce.profile_overrides:
            gce_dict["profile_overrides"] = config.gce.profile_overrides
        config_dict["gce"] = gce_dict

    if config.invariants:
        config_dict["invariants"] = config.invariants

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def init_from_existing(
    project_path: Path,
    source_dir: Path,
) -> GoldfishConfig:
    """Initialize Goldfish for an existing codebase.

    Copies source code into the dev repo and creates initial workspace.

    Args:
        project_path: Path to the project directory
        source_dir: Path to existing code to import

    Returns:
        GoldfishConfig for the initialized project
    """
    import shutil

    project_name = project_path.name
    config = init_project(project_name, project_path)

    # Get dev repo path
    dev_repo_path = (project_path.parent / config.dev_repo_path).resolve()

    # Copy source code to dev repo
    code_dir = dev_repo_path / "code"

    # Remove placeholder .gitkeep
    gitkeep = code_dir / ".gitkeep"
    if gitkeep.exists():
        gitkeep.unlink()

    # Copy source files
    for item in source_dir.iterdir():
        if item.name.startswith("."):
            continue  # Skip hidden files
        dest = code_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)

    # Commit the imported code
    subprocess.run(
        ["git", "add", "-A"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )
    subprocess.run(
        ["git", "commit", "-m", f"Import existing code from {source_dir.name}"],
        cwd=dev_repo_path,
        capture_output=True,
        check=True,
        timeout=INIT_GIT_TIMEOUT,
    )

    return config
