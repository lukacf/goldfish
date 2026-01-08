"""Tests for GCE configuration loading."""

import tempfile
from pathlib import Path

from goldfish.config import GCEConfig, GoldfishConfig


class TestGCEConfig:
    """Test GCE configuration."""

    def test_gce_config_optional(self):
        """GCE config should be optional."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
        )
        assert config.gce is None

    def test_gce_config_with_project_id(self):
        """Should load GCE config with project ID."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project_id="my-gcp-project"),
        )
        assert config.gce.project_id == "my-gcp-project"
        # Check defaults
        assert config.gce.gpu_preference == ["h100", "a100", "none"]
        assert config.gce.preemptible_preference == "on_demand_first"

    def test_gce_config_with_profile_overrides(self):
        """Should support profile overrides."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            gce=GCEConfig(
                project_id="my-gcp-project",
                profile_overrides={"h100-spot": {"zones": ["us-west1-a"]}},
            ),
        )
        assert config.gce.profile_overrides["h100-spot"]["zones"] == ["us-west1-a"]

    def test_gce_config_with_custom_profile(self):
        """Should support custom profiles."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            gce=GCEConfig(
                project_id="my-gcp-project",
                profile_overrides={
                    "my-custom": {
                        "machine_type": "n2-standard-16",
                        "zones": ["us-east1-b"],
                    }
                },
            ),
        )
        assert config.gce.profile_overrides["my-custom"]["machine_type"] == "n2-standard-16"

    def test_load_gce_config_from_yaml(self):
        """Should load GCE config from goldfish.yaml."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create goldfish.yaml with GCE config
            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "jobs": {"backend": "local"},
                "gce": {
                    "project_id": "my-gcp-project",
                    "gpu_preference": ["h100", "a100"],
                    "profile_overrides": {"h100-spot": {"zones": ["us-west1-a", "us-west1-b"]}},
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            # Load config
            config = GoldfishConfig.load(project_root)

            assert config.gce is not None
            assert config.gce.project_id == "my-gcp-project"
            assert config.gce.gpu_preference == ["h100", "a100"]
            assert config.gce.profile_overrides["h100-spot"]["zones"] == ["us-west1-a", "us-west1-b"]


class TestGCEConfigMigration:
    """Test GCE configuration migration for backwards compatibility.

    REGRESSION TESTS: These ensure old config formats continue to work.
    """

    def test_migrate_preemptible_to_preemptible_allowed(self):
        """REGRESSION: Old 'preemptible' field should migrate to 'preemptible_allowed'.

        Old format:
            profile_overrides:
              h100-spot:
                preemptible: true

        New format:
            profile_overrides:
              h100-spot:
                preemptible_allowed: true
                on_demand_allowed: true
        """
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Old format with 'preemptible' instead of 'preemptible_allowed'
            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "gce": {
                    "project_id": "my-gcp-project",
                    "profile_overrides": {
                        "h100-spot": {
                            "machine_type": "a3-highgpu-1g",
                            "preemptible": True,  # OLD FORMAT
                            "boot_disk": {"type": "pd-ssd", "size_gb": 200},
                        }
                    },
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = GoldfishConfig.load(project_root)

            # Should migrate preemptible -> preemptible_allowed
            profile = config.gce.effective_profile_overrides["h100-spot"]
            assert profile.get("preemptible_allowed") is True
            assert profile.get("on_demand_allowed") is True
            # Old key should be removed
            assert "preemptible" not in profile

    def test_migrate_preemptible_false_sets_on_demand_only(self):
        """REGRESSION: preemptible=false should set preemptible_allowed=false."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "gce": {
                    "project_id": "my-gcp-project",
                    "profile_overrides": {
                        "my-ondemand": {
                            "machine_type": "n2-standard-16",
                            "preemptible": False,  # OLD FORMAT - on-demand only
                        }
                    },
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = GoldfishConfig.load(project_root)

            profile = config.gce.effective_profile_overrides["my-ondemand"]
            assert profile.get("preemptible_allowed") is False
            assert profile.get("on_demand_allowed") is True

    def test_migrate_gpu_type_to_accelerator(self):
        """REGRESSION: Old GPU type without accelerator should auto-map.

        Old format:
            gpu:
              type: nvidia-h100-80gb
              count: 1

        Should add:
            gpu:
              type: nvidia-h100-80gb
              accelerator: nvidia-h100-80gb
              count: 1
        """
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "gce": {
                    "project_id": "my-gcp-project",
                    "profile_overrides": {
                        "gpu-profile": {
                            "machine_type": "a3-highgpu-1g",
                            "gpu": {
                                "type": "nvidia-h100-80gb",
                                "count": 1,
                                # Missing 'accelerator' field
                            },
                        }
                    },
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = GoldfishConfig.load(project_root)

            profile = config.gce.effective_profile_overrides["gpu-profile"]
            assert profile["gpu"]["accelerator"] == "nvidia-h100-80gb"
            assert profile["gpu"]["count"] == 1

    def test_migrate_short_gpu_type_names(self):
        """REGRESSION: Short GPU type names should map to full accelerator names."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "gce": {
                    "project_id": "my-gcp-project",
                    "profile_overrides": {
                        "h100-short": {"gpu": {"type": "h100"}},  # Short name
                        "a100-short": {"gpu": {"type": "a100"}},  # Short name
                        "t4-short": {"gpu": {"type": "t4"}},  # Short name
                    },
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = GoldfishConfig.load(project_root)

            # Short names should map to full accelerator names
            assert config.gce.effective_profile_overrides["h100-short"]["gpu"]["accelerator"] == "nvidia-h100-80gb"
            assert config.gce.effective_profile_overrides["a100-short"]["gpu"]["accelerator"] == "nvidia-tesla-a100"
            assert config.gce.effective_profile_overrides["t4-short"]["gpu"]["accelerator"] == "nvidia-tesla-t4"

    def test_artifact_registry_auto_generated_from_project_id(self):
        """REGRESSION: artifact_registry should auto-generate from project_id.

        When artifact_registry is not specified but project_id is, it should
        auto-generate as: us-docker.pkg.dev/{project_id}/goldfish
        """
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project_id="my-gcp-project"),
        )

        # Should auto-generate from project_id
        assert config.gce.effective_artifact_registry == "us-docker.pkg.dev/my-gcp-project/goldfish"

    def test_artifact_registry_explicit_overrides_auto_generation(self):
        """Explicit artifact_registry should not be overridden by auto-generation."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            gce=GCEConfig(
                project_id="my-gcp-project",
                artifact_registry="europe-docker.pkg.dev/my-gcp-project/custom-repo",
            ),
        )

        # Explicit value should be used
        assert config.gce.effective_artifact_registry == "europe-docker.pkg.dev/my-gcp-project/custom-repo"

    def test_gcs_bucket_migration_from_gce_section(self):
        """REGRESSION: gcs_bucket inside gce section should migrate to gcs.bucket."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Old format with gcs_bucket inside gce section
            config_data = {
                "project_name": "test-project",
                "dev_repo_path": "../test-dev",
                "gce": {
                    "project_id": "my-gcp-project",
                    "gcs_bucket": "my-artifacts-bucket",  # OLD FORMAT
                },
            }

            config_path = project_root / "goldfish.yaml"
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = GoldfishConfig.load(project_root)

            # Should migrate to gcs.bucket
            assert config.gcs is not None
            assert config.gcs.bucket == "my-artifacts-bucket"
