"""Tests for GCE resource profiles system.

Tests the built-in profile system and customization capabilities.
"""

import pytest

from goldfish.infra.profiles import (
    BUILTIN_PROFILES,
    ProfileNotFoundError,
    ProfileResolver,
    get_builtin_profile,
)


class TestBuiltinProfiles:
    """Test built-in GCE resource profiles."""

    def test_builtin_profiles_exist(self):
        """Should have standard built-in profiles."""
        # Essential profiles that should always exist
        required_profiles = [
            "cpu-small",
            "cpu-large",
            "h100-spot",
            "h100-on-demand",
            "a100-spot",
            "a100-on-demand",
        ]

        for profile_name in required_profiles:
            assert profile_name in BUILTIN_PROFILES, f"Missing required profile: {profile_name}"

    def test_cpu_small_profile_structure(self):
        """CPU small profile should have correct structure."""
        profile = BUILTIN_PROFILES["cpu-small"]

        assert "machine_type" in profile
        assert "zones" in profile
        assert isinstance(profile["zones"], list)
        assert len(profile["zones"]) > 0
        assert "boot_disk" in profile
        assert "data_disk" in profile

        # CPU profiles should not have GPU
        gpu = profile.get("gpu", {})
        assert gpu.get("type") == "none" or gpu.get("count") == 0

    def test_h100_spot_profile_structure(self):
        """H100 spot profile should have GPU configuration."""
        profile = BUILTIN_PROFILES["h100-spot"]

        assert "machine_type" in profile
        assert "gpu" in profile

        gpu = profile["gpu"]
        assert gpu["type"] == "h100"
        assert "accelerator" in gpu
        assert gpu["count"] >= 1

        # Should prefer spot/preemptible
        assert profile.get("preemptible_allowed") is True

    def test_get_builtin_profile(self):
        """Should retrieve built-in profile by name."""
        profile = get_builtin_profile("cpu-small")
        assert profile["machine_type"] is not None

    def test_get_builtin_profile_not_found(self):
        """Should raise error for unknown profile."""
        with pytest.raises(ProfileNotFoundError, match="nonexistent-profile"):
            get_builtin_profile("nonexistent-profile")


class TestProfileResolver:
    """Test profile resolution with overrides."""

    def test_resolve_builtin_profile_no_overrides(self):
        """Should return built-in profile when no overrides."""
        resolver = ProfileResolver(profile_overrides=None)

        profile = resolver.resolve("cpu-small")

        # Should match built-in
        assert profile["machine_type"] == BUILTIN_PROFILES["cpu-small"]["machine_type"]

    def test_resolve_with_zone_override(self):
        """Should override zones while keeping other fields."""
        overrides = {"cpu-small": {"zones": ["us-west1-a"]}}
        resolver = ProfileResolver(profile_overrides=overrides)

        profile = resolver.resolve("cpu-small")

        # Zones should be overridden
        assert profile["zones"] == ["us-west1-a"]
        # Other fields should remain from built-in
        assert profile["machine_type"] == BUILTIN_PROFILES["cpu-small"]["machine_type"]

    def test_resolve_custom_profile(self):
        """Should support completely custom profiles."""
        custom_profiles = {
            "my-custom": {
                "machine_type": "n2-standard-16",
                "zones": ["us-east1-b"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 500},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)

        profile = resolver.resolve("my-custom")

        assert profile["machine_type"] == "n2-standard-16"
        assert profile["zones"] == ["us-east1-b"]

    def test_resolve_nonexistent_profile(self):
        """Should raise error for unknown profile with no custom definition."""
        resolver = ProfileResolver(profile_overrides=None)

        with pytest.raises(ProfileNotFoundError, match="unknown-profile"):
            resolver.resolve("unknown-profile")

    def test_list_available_profiles(self):
        """Should list all available profiles (built-in + custom)."""
        custom_profiles = {"my-custom": {"machine_type": "n2-standard-16"}}
        resolver = ProfileResolver(profile_overrides=custom_profiles)

        available = resolver.list_profiles()

        # Should include built-ins
        assert "cpu-small" in available
        assert "h100-spot" in available
        # Should include custom
        assert "my-custom" in available

    def test_deep_merge_nested_overrides(self):
        """Should deep merge nested structures like boot_disk."""
        overrides = {
            "cpu-small": {
                "boot_disk": {
                    "size_gb": 1000  # Override size but keep type
                }
            }
        }
        resolver = ProfileResolver(profile_overrides=overrides)

        profile = resolver.resolve("cpu-small")

        # Size should be overridden
        assert profile["boot_disk"]["size_gb"] == 1000
        # Type should remain from built-in
        assert profile["boot_disk"]["type"] == BUILTIN_PROFILES["cpu-small"]["boot_disk"]["type"]


class TestProfileValidation:
    """Test profile validation."""

    def test_validate_profile_structure(self):
        """Should validate required fields in profile."""
        from goldfish.infra.profiles import validate_profile

        valid_profile = {
            "machine_type": "n2-standard-4",
            "zones": ["us-central1-a"],
            "boot_disk": {"type": "pd-ssd", "size_gb": 100},
            "data_disk": {"type": "pd-ssd", "size_gb": 200},
            "gpu": {"type": "none", "count": 0},
        }

        # Should not raise
        validate_profile(valid_profile)

    def test_validate_missing_machine_type(self):
        """Should raise error if machine_type missing."""
        from goldfish.infra.profiles import ProfileValidationError, validate_profile

        invalid_profile = {
            "zones": ["us-central1-a"],
        }

        with pytest.raises(ProfileValidationError, match="machine_type"):
            validate_profile(invalid_profile)

    def test_validate_empty_zones(self):
        """Should raise error if zones list is empty."""
        from goldfish.infra.profiles import ProfileValidationError, validate_profile

        invalid_profile = {
            "machine_type": "n2-standard-4",
            "zones": [],  # Empty!
            "boot_disk": {"type": "pd-ssd", "size_gb": 100},
            "data_disk": {"type": "pd-ssd", "size_gb": 200},
            "gpu": {"type": "none", "count": 0},
        }

        with pytest.raises(ProfileValidationError, match="zones"):
            validate_profile(invalid_profile)
