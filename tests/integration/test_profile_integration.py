"""Tests for profile integration in stage configs and GCE launcher."""

from goldfish.config import GCEConfig
from goldfish.infra.profiles import ProfileResolver


class TestStageConfigProfiles:
    """Test profile field in stage configs."""

    def test_parse_stage_config_with_profile(self):
        """Stage config should support profile field."""
        import yaml

        config_yaml = """
compute:
  profile: "h100-spot"

env:
  EPOCHS: "100"
"""
        config = yaml.safe_load(config_yaml)

        # Should have profile in compute section
        assert "compute" in config
        assert "profile" in config["compute"]
        assert config["compute"]["profile"] == "h100-spot"

    def test_parse_stage_config_backward_compat(self):
        """Stage config should still support old format (cpu/memory/gpu)."""
        import yaml

        config_yaml = """
compute:
  cpu: 4
  memory: 8GB
  gpu: none
  disk: 20GB

env:
  EPOCHS: "10"
"""
        config = yaml.safe_load(config_yaml)

        # Should have old fields
        assert config["compute"]["cpu"] == 4
        assert config["compute"]["memory"] == "8GB"


class TestProfileResolutionInExecutor:
    """Test profile resolution in stage executor."""

    def test_resolve_profile_from_stage_config(self):
        """Should resolve profile name to full specs."""

        resolver = ProfileResolver(profile_overrides=None)

        # Resolve h100-spot profile
        profile = resolver.resolve("h100-spot")

        # Should have full GCE specs
        assert profile["machine_type"] == "a3-highgpu-1g"
        assert profile["gpu"]["type"] == "h100"
        assert profile["gpu"]["accelerator"] == "nvidia-h100-80gb"
        assert profile["zones"] == [
            "us-central1-a",
            "us-central1-b",
            "us-central1-c",
            "us-central1-f",
            "us-east1-b",
            "us-east1-c",
            "us-west1-a",
            "us-west1-b",
            "us-west4-a",
            "europe-west4-a",
            "europe-west4-b",
        ]

    def test_resolve_profile_with_override(self):
        """Should apply goldfish.yaml overrides."""

        overrides = {
            "h100-spot": {
                "zones": ["us-west1-a"]  # Restrict zones
            }
        }
        resolver = ProfileResolver(profile_overrides=overrides)

        profile = resolver.resolve("h100-spot")

        # Should use overridden zones
        assert profile["zones"] == ["us-west1-a"]
        # Other fields should remain from built-in
        assert profile["machine_type"] == "a3-highgpu-1g"


class TestGCELauncherWithProfiles:
    """Test GCE launcher integration with profiles."""

    def test_launch_with_profile_dict(self):
        """GCE launcher should accept resolved profile dict."""
        from goldfish.infra.gce_launcher import GCELauncher
        from goldfish.infra.profiles import get_builtin_profile

        launcher = GCELauncher(
            project_id="test-project",
            bucket="gs://test-bucket",
        )

        # Get resolved profile
        profile = get_builtin_profile("cpu-small")

        # Should be able to extract machine specs from profile
        machine_type = profile["machine_type"]
        zones = profile["zones"]
        gpu = profile.get("gpu", {})

        assert machine_type == "n2-standard-4"
        assert len(zones) > 0
        assert gpu.get("type") == "none"

    def test_convert_profile_to_resource_list(self):
        """Should convert profile to resource list for ResourceLauncher."""
        from goldfish.infra.profiles import get_builtin_profile

        profile = get_builtin_profile("h100-spot")

        # Convert profile to resource format expected by ResourceLauncher
        # ResourceLauncher expects a list of resource dicts
        resources = [profile]

        assert len(resources) == 1
        assert resources[0]["machine_type"] == "a3-highgpu-1g"
        assert resources[0]["gpu"]["type"] == "h100"


class TestEndToEndProfileWorkflow:
    """Test complete workflow from stage config to GCE launch."""

    def test_profile_resolution_workflow(self):
        """Test full workflow: stage config → profile → GCE specs."""
        import yaml

        # 1. Stage config (what Claude writes)
        stage_config_yaml = """
compute:
  profile: "h100-spot"

env:
  EPOCHS: "100"
"""
        stage_config = yaml.safe_load(stage_config_yaml)

        # 2. Extract profile name
        profile_name = stage_config["compute"]["profile"]
        assert profile_name == "h100-spot"

        # 3. Resolve profile (with optional GCE config overrides)
        gce_config = GCEConfig(
            project_id="test-project",
            profile_overrides={"h100-spot": {"zones": ["us-west1-a", "us-west1-b"]}},
        )
        resolver = ProfileResolver(profile_overrides=gce_config.profile_overrides)
        profile = resolver.resolve(profile_name)

        # 4. Verify resolved profile has everything needed for GCE launch
        assert profile["machine_type"] == "a3-highgpu-1g"
        assert profile["gpu"]["accelerator"] == "nvidia-h100-80gb"
        assert profile["zones"] == ["us-west1-a", "us-west1-b"]  # Overridden
        assert profile["boot_disk"]["type"] == "hyperdisk-balanced"
        assert profile["boot_disk"]["size_gb"] == 600
        assert profile["data_disk"]["size_gb"] == 600

        # 5. Create resource list for ResourceLauncher
        resources = [profile]

        # 6. Pass to ResourceLauncher with GCE config runtime preferences
        # (This would be done in stage executor)
        launcher_params = {
            "resources": resources,
            "gpu_preference": gce_config.gpu_preference,
            "preemptible_preference": gce_config.preemptible_preference,
            "search_timeout_sec": gce_config.search_timeout_sec,
            "project_id": gce_config.project_id,
        }

        assert launcher_params["gpu_preference"] == ["h100", "a100", "none"]
        assert launcher_params["preemptible_preference"] == "on_demand_first"
