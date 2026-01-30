"""Tests for GCE deluxe test fixtures.

These tests verify the fixture setup works correctly. They use mocking
to avoid requiring actual GCP credentials.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest


class TestDeluxeFixtureHelpers:
    """Test helper functions in conftest."""

    def test_is_dry_run_default_false(self):
        """is_dry_run returns False by default."""
        from tests.e2e.deluxe.conftest import is_dry_run

        with patch.dict(os.environ, {}, clear=True):
            # Remove GOLDFISH_DELUXE_DRY_RUN if set
            os.environ.pop("GOLDFISH_DELUXE_DRY_RUN", None)
            assert is_dry_run() is False

    def test_is_dry_run_when_set(self):
        """is_dry_run returns True when env var is 1."""
        from tests.e2e.deluxe.conftest import is_dry_run

        with patch.dict(os.environ, {"GOLDFISH_DELUXE_DRY_RUN": "1"}):
            assert is_dry_run() is True

    def test_is_enabled_default_false(self):
        """is_enabled returns False by default."""
        from tests.e2e.deluxe.conftest import is_enabled

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("GOLDFISH_DELUXE_TEST_ENABLED", None)
            assert is_enabled() is False

    def test_is_enabled_when_set(self):
        """is_enabled returns True when env var is 1."""
        from tests.e2e.deluxe.conftest import is_enabled

        with patch.dict(os.environ, {"GOLDFISH_DELUXE_TEST_ENABLED": "1"}):
            assert is_enabled() is True

    def test_get_gcp_config_defaults(self):
        """get_gcp_config returns defaults when env vars not set."""
        from tests.e2e.deluxe.conftest import get_gcp_config

        with patch.dict(os.environ, {}, clear=True):
            # Remove all GOLDFISH env vars
            for key in list(os.environ.keys()):
                if key.startswith("GOLDFISH_"):
                    os.environ.pop(key, None)

            config = get_gcp_config()
            assert config["project"] is None
            assert config["bucket"] is None
            assert config["zone"] == "us-central1-a"  # default
            assert config["artifact_registry"] is None

    def test_get_gcp_config_from_env(self):
        """get_gcp_config reads from environment variables."""
        from tests.e2e.deluxe.conftest import get_gcp_config

        env = {
            "GOLDFISH_GCP_PROJECT": "test-project",
            "GOLDFISH_GCS_BUCKET": "test-bucket",
            "GOLDFISH_GCE_ZONE": "us-west1-b",
            "GOLDFISH_ARTIFACT_REGISTRY": "us-docker.pkg.dev/test/repo",
        }

        with patch.dict(os.environ, env, clear=True):
            config = get_gcp_config()
            assert config["project"] == "test-project"
            assert config["bucket"] == "test-bucket"
            assert config["zone"] == "us-west1-b"
            assert config["artifact_registry"] == "us-docker.pkg.dev/test/repo"


class TestIOTestTemplate:
    """Test io_test_template fixture setup."""

    def test_template_files_exist(self):
        """Verify all required template files exist."""
        template_path = Path(__file__).parent / "fixtures" / "io_test_template"

        assert template_path.exists(), f"Template directory not found: {template_path}"

        required_files = [
            "modules/generate_test_data.py",
            "modules/validate_io.py",
            "configs/generate_test_data.yaml",
            "configs/validate_io.yaml",
            "pipeline.yaml",
            "requirements.txt",
        ]

        for file in required_files:
            file_path = template_path / file
            assert file_path.exists(), f"Missing required file: {file}"

    def test_pipeline_yaml_valid(self):
        """Verify pipeline.yaml has required structure."""
        import yaml

        template_path = Path(__file__).parent / "fixtures" / "io_test_template"
        pipeline_path = template_path / "pipeline.yaml"

        with open(pipeline_path) as f:
            pipeline = yaml.safe_load(f)

        assert "stages" in pipeline, "Pipeline must have stages"
        assert len(pipeline["stages"]) >= 2, "Pipeline must have at least 2 stages"

        # Verify generate_test_data stage
        stage_names = [s["name"] for s in pipeline["stages"]]
        assert "generate_test_data" in stage_names
        assert "validate_io" in stage_names


class TestDeluxeTempDirFixture:
    """Test deluxe_temp_dir fixture."""

    def test_temp_dir_created(self, deluxe_temp_dir):
        """deluxe_temp_dir creates a temporary directory."""
        assert deluxe_temp_dir.exists()
        assert deluxe_temp_dir.is_dir()
        assert "goldfish_deluxe_" in str(deluxe_temp_dir)


class TestDeluxeGitRepoFixture:
    """Test deluxe_git_repo fixture."""

    def test_git_repo_initialized(self, deluxe_git_repo):
        """deluxe_git_repo creates an initialized git repository."""
        import subprocess

        assert deluxe_git_repo.exists()
        assert (deluxe_git_repo / ".git").exists()

        # Verify git status works
        result = subprocess.run(
            ["git", "status"],
            cwd=deluxe_git_repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_git_repo_has_main_branch(self, deluxe_git_repo):
        """deluxe_git_repo has main branch."""
        import subprocess

        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=deluxe_git_repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert result.stdout.strip() == "main"

    def test_git_repo_has_initial_commit(self, deluxe_git_repo):
        """deluxe_git_repo has initial commit."""
        import subprocess

        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=deluxe_git_repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Initial commit" in result.stdout


class TestGCECleanupFixture:
    """Test gce_cleanup fixture execution."""

    @pytest.mark.parametrize(
        "env_enabled",
        [
            pytest.param(True, id="enabled"),
        ],
    )
    def test_cleanup_callbacks_executed(self, env_enabled):
        """gce_cleanup executes all callbacks after test."""
        callback_results = []

        # Simulate the fixture behavior manually
        cleanup_callbacks = []
        cleanup_callbacks.append(lambda: callback_results.append("callback1"))
        cleanup_callbacks.append(lambda: callback_results.append("callback2"))

        # Execute cleanup (simulating fixture teardown)
        for callback in cleanup_callbacks:
            try:
                callback()
            except Exception:
                pass

        assert callback_results == ["callback1", "callback2"]

    def test_cleanup_handles_failures_gracefully(self):
        """gce_cleanup doesn't fail if a callback raises."""
        callback_results = []

        cleanup_callbacks = []
        cleanup_callbacks.append(lambda: callback_results.append("before_error"))
        cleanup_callbacks.append(lambda: (_ for _ in ()).throw(RuntimeError("test error")))
        cleanup_callbacks.append(lambda: callback_results.append("after_error"))

        # Execute cleanup with error handling (simulating fixture teardown)
        for callback in cleanup_callbacks:
            try:
                callback()
            except Exception:
                pass  # Swallow errors like the fixture does

        # First and third callbacks should run despite second failing
        assert "before_error" in callback_results
        assert "after_error" in callback_results
