"""Fixtures for deluxe E2E tests with real GCE execution."""

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Dict, Any
from uuid import uuid4

import pytest

from goldfish.config import GoldfishConfig, GCEConfig, GCSConfig, JobsConfig
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager
from goldfish.pipeline.manager import PipelineManager
from goldfish.datasets.registry import DatasetRegistry
from goldfish.jobs.stage_executor import StageExecutor


def skip_if_not_enabled():
    """Skip test if deluxe tests not explicitly enabled."""
    if os.getenv("GOLDFISH_DELUXE_TEST_ENABLED") != "1":
        pytest.skip("Deluxe GCE tests not enabled. Set GOLDFISH_DELUXE_TEST_ENABLED=1 to run")

    if not os.getenv("GOLDFISH_GCE_PROJECT"):
        pytest.skip("GOLDFISH_GCE_PROJECT not set")

    if not os.getenv("GOLDFISH_GCS_BUCKET"):
        pytest.skip("GOLDFISH_GCS_BUCKET not set")


@pytest.fixture(scope="session")
def gce_config():
    """Load GCE configuration from environment."""
    skip_if_not_enabled()

    project_id = os.getenv("GOLDFISH_GCE_PROJECT")
    bucket = os.getenv("GOLDFISH_GCS_BUCKET")
    zone = os.getenv("GOLDFISH_DELUXE_ZONE", "us-central1-a")

    # Verify GCP authentication
    try:
        result = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            check=True,
        )
        if not result.stdout.strip():
            pytest.skip("No active GCP authentication. Run 'gcloud auth login'")
    except Exception as e:
        pytest.skip(f"Failed to verify GCP authentication: {e}")

    return {
        "project_id": project_id,
        "bucket": bucket,
        "zone": zone,
    }


@pytest.fixture
def gce_cleanup():
    """Track GCE instances and ensure cleanup on test exit.

    Usage:
        def test_something(gce_cleanup):
            instance_name = "test-instance"
            zone = "us-central1-a"

            # Register cleanup handler
            gce_cleanup.append(lambda: delete_instance(instance_name, zone))

            # ... test code that creates instance ...
    """
    cleanup_handlers: List[callable] = []

    yield cleanup_handlers

    # Run all cleanup handlers
    for handler in cleanup_handlers:
        try:
            handler()
        except Exception as e:
            print(f"Cleanup handler failed: {e}")


@pytest.fixture
def deluxe_project(gce_config, tmp_path):
    """Create a complete Goldfish project for deluxe testing.

    Returns dict with:
        - project_root: Path to project
        - config: GoldfishConfig
        - db: Database
        - workspace_manager: WorkspaceManager
        - pipeline_manager: PipelineManager
        - dataset_registry: DatasetRegistry
        - stage_executor: StageExecutor
    """
    skip_if_not_enabled()

    # Create project directory
    project_root = tmp_path / f"deluxe-test-{uuid4().hex[:8]}"
    project_root.mkdir()

    # Create project structure
    (project_root / "workspaces").mkdir()
    (project_root / ".goldfish").mkdir()
    (project_root / "experiments").mkdir()

    # Initialize dev repo
    dev_repo = project_root / ".goldfish" / "dev"
    dev_repo.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "deluxe-test@goldfish.ai"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Deluxe Test"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create initial commit
    (dev_repo / "README.md").write_text("# Deluxe Test Project")
    subprocess.run(["git", "add", "-A"], cwd=dev_repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=dev_repo,
        capture_output=True,
        check=True,
    )

    # Create config with GCE settings
    config = GoldfishConfig(
        project_name="deluxe-ml-test",
        dev_repo_path=".goldfish/dev",
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        jobs=JobsConfig(backend="gce", experiments_dir="experiments"),
        gcs=GCSConfig(
            bucket=gce_config["bucket"],
            sources_prefix="deluxe-tests/sources/",
            artifacts_prefix="deluxe-tests/artifacts/",
        ),
        gce=GCEConfig(
            project_id=gce_config["project_id"],
            zones=[gce_config["zone"]],
            gpu_preference=["none"],  # No GPU for cost control
            preemptible_preference="on_demand_first",
        ),
    )

    # Create database
    db = Database(project_root / ".goldfish" / "goldfish.db")

    # Create managers
    workspace_manager = WorkspaceManager(
        config=config,
        project_root=project_root,
        db=db,
    )

    pipeline_manager = PipelineManager(
        db=db,
        workspace_manager=workspace_manager,
    )

    dataset_registry = DatasetRegistry(
        db=db,
        config=config,
    )

    stage_executor = StageExecutor(
        db=db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=project_root,
        dataset_registry=dataset_registry,
    )

    # Copy ML project template to workspace
    # (Will be done in the test after creating workspace)

    return {
        "project_root": project_root,
        "config": config,
        "db": db,
        "workspace_manager": workspace_manager,
        "pipeline_manager": pipeline_manager,
        "dataset_registry": dataset_registry,
        "stage_executor": stage_executor,
        "gce_config": gce_config,
    }


@pytest.fixture
def ml_project_template():
    """Return path to ML project template."""
    return Path(__file__).parent / "fixtures" / "ml_project_template"


def is_dry_run():
    """Check if running in dry-run mode."""
    return os.getenv("GOLDFISH_DELUXE_DRY_RUN") == "1"
