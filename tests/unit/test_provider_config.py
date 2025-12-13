"""Unit tests for provider configuration and backward compatibility."""

import pytest
from pydantic import ValidationError

from goldfish.config import GoldfishConfig, JobsConfig


class TestJobsConfig:
    """Test JobsConfig provider fields."""

    def test_default_execution_provider(self):
        """Test default execution provider mapping."""
        # Default backend is "gce"
        config = JobsConfig()
        assert config.effective_execution_provider == "gce"

    def test_explicit_execution_provider(self):
        """Test explicit execution provider."""
        config = JobsConfig(execution_provider="local")
        assert config.effective_execution_provider == "local"

    def test_execution_provider_overrides_backend(self):
        """Test that execution_provider takes precedence over backend."""
        config = JobsConfig(backend="gce", execution_provider="local")
        assert config.effective_execution_provider == "local"

    def test_backend_fallback_gce(self):
        """Test backend='gce' maps to execution_provider='gce'."""
        config = JobsConfig(backend="gce")
        assert config.effective_execution_provider == "gce"

    def test_backend_fallback_local(self):
        """Test backend='local' maps to execution_provider='local'."""
        config = JobsConfig(backend="local")
        assert config.effective_execution_provider == "local"

    def test_default_storage_provider(self):
        """Test default storage provider inference."""
        # Default backend gce → storage gcs
        config = JobsConfig()
        assert config.effective_storage_provider == "gcs"

    def test_explicit_storage_provider(self):
        """Test explicit storage provider."""
        config = JobsConfig(storage_provider="s3")
        assert config.effective_storage_provider == "s3"

    def test_storage_provider_inferred_from_execution_gce(self):
        """Test storage provider inferred from execution provider (gce → gcs)."""
        config = JobsConfig(execution_provider="gce")
        assert config.effective_storage_provider == "gcs"

    def test_storage_provider_inferred_from_execution_local(self):
        """Test storage provider inferred from execution provider (local → local)."""
        config = JobsConfig(execution_provider="local")
        assert config.effective_storage_provider == "local"

    def test_storage_provider_overrides_inference(self):
        """Test explicit storage provider overrides inference."""
        config = JobsConfig(execution_provider="local", storage_provider="gcs")
        assert config.effective_execution_provider == "local"
        assert config.effective_storage_provider == "gcs"


class TestGoldfishConfigProviders:
    """Test GoldfishConfig provider configuration methods."""

    def test_get_execution_provider_config_from_providers_dict(self):
        """Test getting provider config from new-style providers dict."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(execution_provider="gce"),
            providers={
                "gce": {
                    "project_id": "my-project",
                    "zone": "us-central1-a",
                    "bucket": "my-bucket",
                }
            },
        )

        provider_config = config.get_execution_provider_config()

        assert provider_config["project_id"] == "my-project"
        assert provider_config["zone"] == "us-central1-a"
        assert provider_config["bucket"] == "my-bucket"

    def test_get_execution_provider_config_from_legacy_gce(self):
        """Test backward compatibility with legacy gce config."""
        from goldfish.config import GCEConfig, GCSConfig

        # Note: GCEConfig uses "image_uri" as alias for "artifact_registry"
        # So we must construct with image_uri parameter name
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(
                project_id="legacy-project",
                zones=["us-west1-a", "us-west1-b"],
                image_uri="us-docker.pkg.dev/legacy-project/goldfish",  # Use alias name
            ),
            gcs=GCSConfig(bucket="legacy-bucket"),
        )

        provider_config = config.get_execution_provider_config()

        assert provider_config["project_id"] == "legacy-project"
        assert provider_config["zone"] == "us-west1-a"
        assert provider_config["zones"] == ["us-west1-a", "us-west1-b"]
        assert provider_config["bucket"] == "legacy-bucket"
        assert provider_config["artifact_registry"] == "us-docker.pkg.dev/legacy-project/goldfish"

    def test_get_execution_provider_config_local_default(self):
        """Test local provider gets default config."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(execution_provider="local"),
        )

        provider_config = config.get_execution_provider_config()

        assert provider_config["work_dir"] == "/tmp/goldfish"

    def test_get_storage_provider_config_from_providers_dict(self):
        """Test getting storage provider config from new-style providers dict."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(storage_provider="gcs"),
            providers={
                "gcs": {
                    "bucket": "new-bucket",
                    "datasets_prefix": "data",
                    "project_id": "my-project",
                }
            },
        )

        provider_config = config.get_storage_provider_config()

        assert provider_config["bucket"] == "new-bucket"
        assert provider_config["datasets_prefix"] == "data"
        assert provider_config["project_id"] == "my-project"

    def test_get_storage_provider_config_from_legacy_gcs(self):
        """Test backward compatibility with legacy gcs config."""
        from goldfish.config import GCEConfig, GCSConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(storage_provider="gcs"),
            gce=GCEConfig(project_id="legacy-project"),
            gcs=GCSConfig(
                bucket="legacy-gcs-bucket",
                datasets_prefix="datasets",
                artifacts_prefix="artifacts",
            ),
        )

        provider_config = config.get_storage_provider_config()

        assert provider_config["bucket"] == "legacy-gcs-bucket"
        assert provider_config["datasets_prefix"] == "datasets"
        assert provider_config["artifacts_prefix"] == "artifacts"
        assert provider_config["project_id"] == "legacy-project"

    def test_get_storage_provider_config_local_default(self):
        """Test local storage provider gets default config."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(storage_provider="local"),
        )

        provider_config = config.get_storage_provider_config()

        assert provider_config["base_path"] == ".goldfish/storage"
        assert provider_config["datasets_prefix"] == "datasets"

    def test_new_style_config_takes_precedence(self):
        """Test that new-style provider config takes precedence over legacy."""
        from goldfish.config import GCEConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(execution_provider="gce"),
            gce=GCEConfig(project_id="legacy-project"),
            providers={
                "gce": {
                    "project_id": "new-project",
                    "zone": "us-east1-a",
                }
            },
        )

        provider_config = config.get_execution_provider_config()

        # New-style config should win
        assert provider_config["project_id"] == "new-project"
        assert provider_config["zone"] == "us-east1-a"

    def test_mixed_execution_and_storage_providers(self):
        """Test configuration with mixed providers."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(
                execution_provider="local",
                storage_provider="gcs",
            ),
            providers={
                "gcs": {"bucket": "my-gcs-bucket"},
                "local": {"work_dir": "/custom/work"},
            },
        )

        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["work_dir"] == "/custom/work"
        assert storage_config["bucket"] == "my-gcs-bucket"


class TestBackwardCompatibility:
    """Test backward compatibility scenarios."""

    def test_legacy_gce_backend_still_works(self):
        """Test that legacy backend='gce' configuration still works."""
        from goldfish.config import GCEConfig, GCSConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(project_id="test-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        # Should auto-map to providers
        assert config.jobs.effective_execution_provider == "gce"
        assert config.jobs.effective_storage_provider == "gcs"

        # Provider configs should be derived from legacy config
        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["project_id"] == "test-project"
        assert storage_config["bucket"] == "test-bucket"

    def test_legacy_local_backend_still_works(self):
        """Test that legacy backend='local' configuration still works."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="local"),
        )

        # Should auto-map to providers
        assert config.jobs.effective_execution_provider == "local"
        assert config.jobs.effective_storage_provider == "local"

        # Provider configs should use defaults
        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["work_dir"] == "/tmp/goldfish"
        assert storage_config["base_path"] == ".goldfish/storage"

    def test_no_providers_dict_uses_defaults(self):
        """Test that omitting providers dict uses sensible defaults."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(execution_provider="local"),
        )

        # Should still work with default configs
        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config == {"work_dir": "/tmp/goldfish"}
        assert storage_config == {
            "base_path": ".goldfish/storage",
            "datasets_prefix": "datasets",
            "artifacts_prefix": "artifacts",
            "snapshots_prefix": "snapshots",
        }
