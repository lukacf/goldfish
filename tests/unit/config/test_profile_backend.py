"""Tests for per-profile backend selection.

TDD tests for allowing profiles to specify their compute backend.
"""

from goldfish.cloud.adapters.gcp.profiles import (
    BUILTIN_PROFILES,
    ProfileResolver,
    validate_profile,
)


class TestProfileBackendField:
    """Tests for optional backend field in profiles."""

    def test_builtin_profiles_have_no_backend_by_default(self):
        """Built-in profiles should not have a backend field (backwards compat)."""
        for profile_name, profile in BUILTIN_PROFILES.items():
            # Backend field should not be in built-in profiles
            # This ensures backwards compatibility
            assert "backend" not in profile, (
                f"Built-in profile '{profile_name}' should not have 'backend' field "
                "to maintain backwards compatibility"
            )

    def test_profile_with_backend_field_is_valid(self):
        """Profile with backend field should pass validation."""
        profile_with_backend = {
            "backend": "kubernetes",
            "machine_type": "n2-standard-4",
            "zones": ["us-central1-a"],
            "boot_disk": {"type": "pd-ssd", "size_gb": 100},
            "data_disk": {"type": "pd-ssd", "size_gb": 200},
            "gpu": {"type": "none", "count": 0},
        }
        # Should not raise
        validate_profile(profile_with_backend)

    def test_profile_without_backend_field_is_valid(self):
        """Profile without backend field should pass validation (backwards compat)."""
        profile_without_backend = {
            "machine_type": "n2-standard-4",
            "zones": ["us-central1-a"],
            "boot_disk": {"type": "pd-ssd", "size_gb": 100},
            "data_disk": {"type": "pd-ssd", "size_gb": 200},
            "gpu": {"type": "none", "count": 0},
        }
        # Should not raise
        validate_profile(profile_without_backend)


class TestProfileResolverBackend:
    """Tests for ProfileResolver handling of backend field."""

    def test_resolve_builtin_profile_returns_no_backend(self):
        """Resolving built-in profile should not add backend field."""
        resolver = ProfileResolver(profile_overrides=None)
        profile = resolver.resolve("cpu-small")

        # Backend should not be present (backwards compat)
        assert "backend" not in profile

    def test_resolve_custom_profile_with_backend(self):
        """Custom profile with backend field should be preserved."""
        custom_profiles = {
            "team-gpu": {
                "backend": "kubernetes",
                "machine_type": "n2-standard-16",
                "zones": ["us-east1-b"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 500},
                "gpu": {"type": "nvidia-tesla-t4", "accelerator": "nvidia-tesla-t4", "count": 1},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("team-gpu")

        assert profile["backend"] == "kubernetes"
        assert profile["machine_type"] == "n2-standard-16"

    def test_resolve_builtin_override_with_backend(self):
        """Overriding built-in profile can add backend field."""
        overrides = {
            "cpu-small": {
                "backend": "gce",
                "zones": ["us-west1-a"],
            }
        }
        resolver = ProfileResolver(profile_overrides=overrides)
        profile = resolver.resolve("cpu-small")

        # Backend should be added via override
        assert profile["backend"] == "gce"
        # Zones should be overridden
        assert profile["zones"] == ["us-west1-a"]
        # Other fields should remain from built-in
        assert profile["machine_type"] == BUILTIN_PROFILES["cpu-small"]["machine_type"]

    def test_resolve_custom_profile_without_backend(self):
        """Custom profile without backend field should work (defaults to config)."""
        custom_profiles = {
            "cpu-highmem": {
                "machine_type": "n2-highmem-8",
                "zones": ["us-east1-b"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 500},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("cpu-highmem")

        # No backend field - runtime will use default from config
        assert "backend" not in profile
        assert profile["machine_type"] == "n2-highmem-8"


class TestProfileBackendRetrieval:
    """Tests for retrieving backend from resolved profile."""

    def test_get_backend_from_profile_with_backend(self):
        """Should be able to get backend from profile that has it."""
        custom_profiles = {
            "k8s-gpu": {
                "backend": "kubernetes",
                "machine_type": "n2-standard-16",
                "zones": ["us-east1-b"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 500},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("k8s-gpu")

        backend = profile.get("backend")
        assert backend == "kubernetes"

    def test_get_backend_from_profile_without_backend_returns_none(self):
        """Getting backend from profile without it should return None."""
        resolver = ProfileResolver(profile_overrides=None)
        profile = resolver.resolve("cpu-small")

        backend = profile.get("backend")
        assert backend is None


class TestBackendFieldValues:
    """Tests for valid backend field values."""

    def test_backend_can_be_gce(self):
        """Backend field can be 'gce'."""
        custom_profiles = {
            "gce-profile": {
                "backend": "gce",
                "machine_type": "n2-standard-4",
                "zones": ["us-central1-a"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 200},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("gce-profile")
        assert profile["backend"] == "gce"

    def test_backend_can_be_local(self):
        """Backend field can be 'local'."""
        custom_profiles = {
            "local-profile": {
                "backend": "local",
                "machine_type": "n2-standard-4",
                "zones": ["local-zone-1"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 200},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("local-profile")
        assert profile["backend"] == "local"

    def test_backend_can_be_kubernetes(self):
        """Backend field can be 'kubernetes' (future backend)."""
        custom_profiles = {
            "k8s-profile": {
                "backend": "kubernetes",
                "machine_type": "n2-standard-4",
                "zones": ["us-central1-a"],
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 200},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(profile_overrides=custom_profiles)
        profile = resolver.resolve("k8s-profile")
        assert profile["backend"] == "kubernetes"


class TestBackendWithGlobalZones:
    """Tests for backend field interaction with global_zones."""

    def test_backend_preserved_with_global_zones(self):
        """Backend field should be preserved when global_zones are applied."""
        custom_profiles = {
            "team-gpu": {
                "backend": "kubernetes",
                "machine_type": "n2-standard-16",
                # No zones - will use global_zones
                "boot_disk": {"type": "pd-ssd", "size_gb": 100},
                "data_disk": {"type": "pd-ssd", "size_gb": 500},
                "gpu": {"type": "none", "count": 0},
            }
        }
        resolver = ProfileResolver(
            profile_overrides=custom_profiles,
            global_zones=["europe-west4-a", "europe-west4-b"],
        )
        profile = resolver.resolve("team-gpu")

        # Backend should be preserved
        assert profile["backend"] == "kubernetes"
        # Global zones should be applied
        assert profile["zones"] == ["europe-west4-a", "europe-west4-b"]
