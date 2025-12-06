"""Shared test fixtures for Goldfish tests."""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from goldfish.config import GoldfishConfig, StateMdConfig, AuditConfig, JobsConfig, GCSConfig
from goldfish.db.database import Database


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory that's cleaned up after the test."""
    tmp = tempfile.mkdtemp(prefix="goldfish_test_")
    yield Path(tmp)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def temp_git_repo(temp_dir: Path) -> Generator[Path, None, None]:
    """Create a temporary git repository."""
    repo_path = temp_dir / "test-repo"
    repo_path.mkdir()

    subprocess.run(["git", "init"], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_path, capture_output=True, check=True
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repo_path, capture_output=True, check=True
    )

    # Create initial commit
    (repo_path / "README.md").write_text("# Test")
    subprocess.run(["git", "add", "-A"], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=repo_path, capture_output=True, check=True
    )

    yield repo_path


@pytest.fixture
def test_config(temp_dir: Path) -> GoldfishConfig:
    """Create a test configuration."""
    return GoldfishConfig(
        project_name="test-project",
        dev_repo_path=str(temp_dir / "test-dev"),
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        gcs=GCSConfig(
            bucket="test-bucket",
            sources_prefix="sources/",
            artifacts_prefix="artifacts/",
        ),
        invariants=["Test invariant"],
    )


@pytest.fixture
def test_db(temp_dir: Path) -> Generator[Database, None, None]:
    """Create a test database."""
    db_path = temp_dir / "test.db"
    db = Database(db_path)
    yield db


@pytest.fixture
def initialized_project(temp_dir: Path, temp_git_repo: Path) -> dict:
    """Create a fully initialized test project structure."""
    project_root = temp_dir / "project"
    project_root.mkdir()

    # Create directory structure
    (project_root / "workspaces").mkdir()
    (project_root / ".goldfish").mkdir()
    (project_root / "experiments").mkdir()

    # Create config
    config = GoldfishConfig(
        project_name="test-project",
        dev_repo_path=str(temp_git_repo.relative_to(temp_dir)),
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )

    # Write config file
    import yaml
    config_path = project_root / "goldfish.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config.model_dump(exclude_none=True), f)

    # Create database
    db = Database(project_root / ".goldfish" / "goldfish.db")

    return {
        "project_root": project_root,
        "dev_repo": temp_git_repo,
        "config": config,
        "db": db,
    }
