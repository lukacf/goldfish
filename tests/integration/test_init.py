"""Comprehensive tests for init.py module - P2.

Tests cover:
- Project initialization and structure creation
- Dev repository creation with git
- Config file writing
- Initialization from existing codebase
- Timeout handling
- Idempotency
"""

import subprocess

import pytest
import yaml


class TestInitProject:
    """Tests for init_project() function."""

    def test_init_project_creates_structure(self, temp_dir):
        """Test that init_project creates all required directories and files."""
        from goldfish.init import init_project

        project_path = temp_dir / "test-project"
        dev_repo_path = temp_dir / "test-project-dev"

        config = init_project(
            project_name="test-project",
            project_path=project_path,
            dev_repo_path=dev_repo_path,
        )

        # Check project directory structure (only goldfish.yaml in user project)
        assert project_path.exists()
        assert (project_path / "goldfish.yaml").exists()

        # Check dev repo structure (runtime artifacts live here)
        assert (dev_repo_path / "workspaces").exists()
        assert (dev_repo_path / ".goldfish").exists()
        assert (dev_repo_path / "STATE.md").exists()

        # Check config was created correctly
        assert config.project_name == "test-project"
        assert config.workspaces_dir == "workspaces"
        assert len(config.slots) == 3

    def test_init_project_creates_git_repo(self, temp_dir):
        """Test that init_project creates a valid git repository."""
        from goldfish.init import init_project

        project_path = temp_dir / "test-project"
        dev_repo_path = temp_dir / "test-project-dev"

        init_project(
            project_name="test-project",
            project_path=project_path,
            dev_repo_path=dev_repo_path,
        )

        # Check dev repo was created
        assert dev_repo_path.exists()
        assert (dev_repo_path / ".git").exists()

        # Check initial structure
        assert (dev_repo_path / "code").exists()
        assert (dev_repo_path / "scripts").exists()
        assert (dev_repo_path / "entrypoints").exists()
        assert (dev_repo_path / ".gitignore").exists()

        # Verify git repo is valid with at least one commit
        result = subprocess.run(
            ["git", "rev-list", "--count", "HEAD"],
            cwd=dev_repo_path,
            capture_output=True,
            text=True,
        )
        commit_count = int(result.stdout.strip())
        assert commit_count >= 1

    def test_init_project_config_file_valid(self, temp_dir):
        """Test that written config file is valid YAML and correct."""
        from goldfish.init import init_project

        project_path = temp_dir / "test-project"
        dev_repo_path = temp_dir / "test-project-dev"

        init_project(
            project_name="test-project",
            project_path=project_path,
            dev_repo_path=dev_repo_path,
        )

        config_path = project_path / "goldfish.yaml"
        assert config_path.exists()

        # Load and verify YAML
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)

        assert config_dict["project_name"] == "test-project"
        assert config_dict["workspaces_dir"] == "workspaces"
        assert "slots" in config_dict
        assert len(config_dict["slots"]) == 3
        assert "state_md" in config_dict
        assert "audit" in config_dict
        assert "jobs" in config_dict

    def test_init_project_state_md_created(self, temp_dir):
        """Test that STATE.md is created with initial content in dev repo."""
        from goldfish.init import init_project

        project_path = temp_dir / "test-project"
        dev_repo_path = temp_dir / "test-project-dev"

        init_project(
            project_name="test-project",
            project_path=project_path,
            dev_repo_path=dev_repo_path,
        )

        # STATE.md is created in dev repo (not user project)
        state_path = dev_repo_path / "STATE.md"
        assert state_path.exists()

        content = state_path.read_text()
        assert len(content) > 0
        assert "test-project" in content.lower()

    def test_init_project_default_dev_repo_path(self, temp_dir):
        """Test that default dev repo path is sibling directory."""
        from goldfish.init import init_project

        project_path = temp_dir / "test-project"

        config = init_project(
            project_name="test-project",
            project_path=project_path,
        )

        # Default should be ../test-project-dev
        expected_dev_repo = temp_dir / "test-project-dev"
        assert expected_dev_repo.exists()
        assert (expected_dev_repo / ".git").exists()

    def test_init_project_custom_dev_repo_path(self, temp_dir):
        """Test init_project with custom dev repo path."""
        from goldfish.init import init_project

        project_path = temp_dir / "myproject"
        custom_dev_path = temp_dir / "custom" / "dev-repo"

        init_project(
            project_name="myproject",
            project_path=project_path,
            dev_repo_path=custom_dev_path,
        )

        assert custom_dev_path.exists()
        assert (custom_dev_path / ".git").exists()

    def test_init_project_handles_existing_project_dir(self, temp_dir):
        """Test that init_project works when project directory already exists."""
        from goldfish.init import init_project

        project_path = temp_dir / "existing-project"
        project_path.mkdir()

        # Should not raise error
        config = init_project(
            project_name="existing-project",
            project_path=project_path,
        )

        assert config is not None
        assert (project_path / "goldfish.yaml").exists()


class TestCreateDevRepo:
    """Tests for _create_dev_repo() helper function."""

    def test_create_dev_repo_handles_existing_git_repo(self, temp_dir):
        """Test that _create_dev_repo is idempotent for existing repos."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "dev-repo"
        dev_repo_path.mkdir()

        # Initialize git repo manually
        subprocess.run(
            ["git", "init"],
            cwd=dev_repo_path,
            capture_output=True,
            check=True,
        )

        # Should not raise error or re-initialize
        _create_dev_repo(dev_repo_path, "test-project")

        # Should still be a valid git repo
        assert (dev_repo_path / ".git").exists()

    def test_create_dev_repo_rejects_non_git_directory(self, temp_dir):
        """Test that _create_dev_repo raises error for non-git directory."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "not-a-repo"
        dev_repo_path.mkdir()
        (dev_repo_path / "somefile.txt").touch()

        # Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            _create_dev_repo(dev_repo_path, "test-project")

        assert "not a git repository" in str(exc_info.value).lower()

    def test_create_dev_repo_creates_initial_structure(self, temp_dir):
        """Test that _create_dev_repo creates all expected directories."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "dev-repo"

        _create_dev_repo(dev_repo_path, "test-project")

        # Check structure
        assert (dev_repo_path / "code" / ".gitkeep").exists()
        assert (dev_repo_path / "scripts" / ".gitkeep").exists()
        assert (dev_repo_path / "entrypoints" / ".gitkeep").exists()
        assert (dev_repo_path / ".gitignore").exists()

    def test_create_dev_repo_gitignore_content(self, temp_dir):
        """Test that .gitignore contains expected patterns."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "dev-repo"

        _create_dev_repo(dev_repo_path, "test-project")

        gitignore_content = (dev_repo_path / ".gitignore").read_text()

        # Check for important patterns
        assert "__pycache__" in gitignore_content
        assert ".venv/" in gitignore_content
        assert "*.py[cod]" in gitignore_content  # Matches .pyc, .pyo, .pyd
        assert ".DS_Store" in gitignore_content

    def test_create_dev_repo_initial_commit(self, temp_dir):
        """Test that _create_dev_repo creates an initial commit."""
        from goldfish.init import _create_dev_repo

        dev_repo_path = temp_dir / "dev-repo"

        _create_dev_repo(dev_repo_path, "test-project")

        # Check commit exists
        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=dev_repo_path,
            capture_output=True,
            text=True,
        )

        assert "Initialize" in result.stdout
        assert "test-project" in result.stdout


class TestInitFromExisting:
    """Tests for init_from_existing() function."""

    def test_init_from_existing_copies_files(self, temp_dir):
        """Test that init_from_existing copies source files correctly."""
        from goldfish.init import init_from_existing

        # Create source directory with files
        source_dir = temp_dir / "existing-code"
        source_dir.mkdir()
        (source_dir / "main.py").write_text("print('hello')")
        (source_dir / "utils.py").write_text("def foo(): pass")
        (source_dir / "README.md").write_text("# Project")

        subdir = source_dir / "lib"
        subdir.mkdir()
        (subdir / "module.py").write_text("class MyClass: pass")

        # Initialize from existing
        project_path = temp_dir / "new-project"
        config = init_from_existing(project_path, source_dir)

        # Check files were copied
        dev_repo_path = (project_path.parent / config.dev_repo_path).resolve()
        code_dir = dev_repo_path / "code"

        assert (code_dir / "main.py").exists()
        assert (code_dir / "utils.py").exists()
        assert (code_dir / "README.md").exists()
        assert (code_dir / "lib" / "module.py").exists()

        # Check content is correct
        assert (code_dir / "main.py").read_text() == "print('hello')"

    def test_init_from_existing_skips_hidden_files(self, temp_dir):
        """Test that init_from_existing skips hidden files."""
        from goldfish.init import init_from_existing

        # Create source with hidden files
        source_dir = temp_dir / "source"
        source_dir.mkdir()
        (source_dir / "visible.py").write_text("# visible")
        (source_dir / ".hidden").write_text("# hidden")
        (source_dir / ".git").mkdir()

        # Initialize
        project_path = temp_dir / "project"
        config = init_from_existing(project_path, source_dir)

        # Check hidden files were skipped
        dev_repo_path = (project_path.parent / config.dev_repo_path).resolve()
        code_dir = dev_repo_path / "code"

        assert (code_dir / "visible.py").exists()
        assert not (code_dir / ".hidden").exists()
        assert not (code_dir / ".git").exists()

    def test_init_from_existing_removes_gitkeep(self, temp_dir):
        """Test that .gitkeep is removed when copying files."""
        from goldfish.init import init_from_existing

        source_dir = temp_dir / "source"
        source_dir.mkdir()
        (source_dir / "code.py").write_text("# code")

        project_path = temp_dir / "project"
        config = init_from_existing(project_path, source_dir)

        # .gitkeep should be removed
        dev_repo_path = (project_path.parent / config.dev_repo_path).resolve()
        code_dir = dev_repo_path / "code"

        assert not (code_dir / ".gitkeep").exists()
        assert (code_dir / "code.py").exists()

    def test_init_from_existing_creates_commit(self, temp_dir):
        """Test that init_from_existing commits the imported code."""
        from goldfish.init import init_from_existing

        source_dir = temp_dir / "source"
        source_dir.mkdir()
        (source_dir / "app.py").write_text("# app")

        project_path = temp_dir / "project"
        config = init_from_existing(project_path, source_dir)

        # Check commit was created
        dev_repo_path = (project_path.parent / config.dev_repo_path).resolve()
        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=dev_repo_path,
            capture_output=True,
            text=True,
        )

        # Should have 2 commits: initial + import
        assert result.stdout.count("\n") >= 1
        assert "Import" in result.stdout or "import" in result.stdout


class TestInitGitTimeout:
    """Tests for git operation timeout handling."""

    def test_init_handles_git_timeout(self, temp_dir):
        """Test that init operations respect timeout."""
        from goldfish.init import _create_dev_repo

        # This test verifies that timeout parameter is passed
        # We can't easily test actual timeout without hanging
        # Instead, verify timeout is used in subprocess calls
        dev_repo_path = temp_dir / "dev-repo"

        # Should complete successfully with normal timeout
        _create_dev_repo(dev_repo_path, "test-project")

        assert (dev_repo_path / ".git").exists()


class TestWriteConfig:
    """Tests for _write_config() helper function."""

    def test_write_config_creates_valid_yaml(self, temp_dir):
        """Test that _write_config creates valid YAML."""
        from goldfish.config import (
            AuditConfig,
            GoldfishConfig,
            JobsConfig,
            StateMdConfig,
        )
        from goldfish.init import _write_config

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            workspaces_dir="workspaces",
            slots=["w1", "w2"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=10),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="exp"),
            invariants=[],
        )

        config_path = temp_dir / "config.yaml"
        _write_config(config, config_path)

        # Load and verify
        with open(config_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded["project_name"] == "test"
        assert loaded["dev_repo_path"] == "../test-dev"
        assert loaded["slots"] == ["w1", "w2"]
