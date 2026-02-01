"""Shared test fixtures for Goldfish tests."""

import os
import shutil
import subprocess
import sys
import tempfile
import types
from collections.abc import Generator
from pathlib import Path

import pytest

from goldfish.config import (
    AuditConfig,
    GCSConfig,
    GoldfishConfig,
    JobsConfig,
    PreRunReviewConfig,
    StateMdConfig,
)
from goldfish.db.database import Database
from goldfish.svs.config import SVSConfig

# Git environment variables that can leak from git hooks (e.g., pre-push)
_GIT_ENV_VARS = [
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_REFLOG_ACTION",
    "GIT_QUARANTINE_PATH",
]


@pytest.fixture(autouse=True)
def _clean_stale_server_tools_submodules() -> Generator[None, None, None]:
    """Prevent stale `goldfish.server_tools.<submodule>` package attributes.

    Some unit tests remove `goldfish.server_tools.*` entries from `sys.modules`
    to force fresh imports under mocks. If the corresponding attribute on the
    `goldfish.server_tools` package is not removed, later imports can return a
    stale module object that is no longer in `sys.modules`, making patching
    brittle and order-dependent.
    """
    try:
        import goldfish.server_tools as server_tools_pkg

        for attr_name, attr_value in list(vars(server_tools_pkg).items()):
            if not isinstance(attr_value, types.ModuleType):
                continue

            mod_name = attr_value.__name__
            if not mod_name.startswith("goldfish.server_tools."):
                continue

            mod = sys.modules.get(mod_name)
            if mod is None:
                delattr(server_tools_pkg, attr_name)
            elif mod is not attr_value:
                setattr(server_tools_pkg, attr_name, mod)
    except Exception:
        pass

    yield


def pytest_configure(config: pytest.Config) -> None:
    """Clear git environment variables to prevent hook interference.

    When tests run as part of git pre-push hook, git environment variables
    like GIT_DIR leak into test subprocesses, causing git operations in
    temporary test directories to fail.
    """
    for var in _GIT_ENV_VARS:
        os.environ.pop(var, None)


def _docker_available() -> bool:
    """Return True when Docker is installed and the daemon is responsive."""
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _deluxe_enabled() -> bool:
    """Return True when deluxe tests are enabled via environment variable."""
    return os.environ.get("GOLDFISH_DELUXE_TEST_ENABLED", "0") == "1"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Deselect opt-in tests unless their requirements are met."""
    docker_available = _docker_available()
    deluxe_enabled = _deluxe_enabled()

    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        is_deluxe = item.get_closest_marker("deluxe_gce") is not None or "tests/e2e/deluxe/" in item.nodeid
        requires_docker = item.get_closest_marker("requires_docker") is not None

        if is_deluxe and not deluxe_enabled:
            deselected.append(item)
        elif requires_docker and not docker_available:
            deselected.append(item)
        else:
            selected.append(item)

    if not deselected:
        return

    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


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
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_path, capture_output=True, check=True)

    # Create initial commit
    (repo_path / "README.md").write_text("# Test")
    subprocess.run(["git", "add", "-A"], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=repo_path, capture_output=True, check=True)

    # Ensure branch is named 'main' (git default may vary by system/version)
    subprocess.run(["git", "branch", "-M", "main"], cwd=repo_path, capture_output=True, check=True)

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
        pre_run_review=PreRunReviewConfig(enabled=False),  # Disable pre-run review for tests
        svs=SVSConfig(enabled=False),  # Disable SVS for tests by default
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
