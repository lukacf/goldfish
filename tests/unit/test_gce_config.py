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
