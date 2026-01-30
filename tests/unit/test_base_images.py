"""Unit tests for base image module."""

from goldfish.cloud.adapters.gcp.profiles import (
    BASE_IMAGE_CPU,
    BASE_IMAGE_GPU,
    FALLBACK_BASE_IMAGE,
    PUBLIC_BASE_IMAGE_CPU,
    PUBLIC_BASE_IMAGE_GPU,
    get_base_image_names,
    resolve_base_image,
)


class TestResolveBaseImage:
    """Tests for resolve_base_image function."""

    def test_public_cpu_image_used_directly(self) -> None:
        """Public CPU image (Jupyter) should be used as-is, no registry needed."""
        profile = {"base_image": PUBLIC_BASE_IMAGE_CPU}

        result = resolve_base_image(profile)

        assert result == PUBLIC_BASE_IMAGE_CPU
        assert "quay.io/jupyter" in result

    def test_public_gpu_image_used_directly(self) -> None:
        """Public GPU image (NVIDIA NGC PyTorch) should be used as-is."""
        profile = {"base_image": PUBLIC_BASE_IMAGE_GPU}

        result = resolve_base_image(profile)

        assert result == PUBLIC_BASE_IMAGE_GPU
        # NVIDIA NGC image has nvrtc for Flash Attention
        assert "nvidia" in result

    def test_fallback_when_no_base_image(self) -> None:
        """Should fallback to python:3.11-slim when profile has no base_image."""
        profile = {}

        result = resolve_base_image(profile)

        assert result == FALLBACK_BASE_IMAGE

    def test_short_name_with_registry(self) -> None:
        """Short image names should use registry when provided."""
        profile = {"base_image": "my-custom-image"}
        registry = "us-docker.pkg.dev/myproject/goldfish"

        result = resolve_base_image(profile, registry)

        assert result.startswith(registry)
        assert "my-custom-image" in result

    def test_short_name_without_registry_falls_back(self) -> None:
        """Short image names without registry should fallback."""
        profile = {"base_image": "my-custom-image"}

        result = resolve_base_image(profile, None)

        assert result == FALLBACK_BASE_IMAGE

    def test_gpu_short_name_without_registry_falls_back_to_public_gpu(self) -> None:
        """GPU short name without registry should fallback to public GPU image (with nvrtc)."""
        profile = {"base_image": BASE_IMAGE_GPU}

        result = resolve_base_image(profile, None)

        # Should fall back to NGC image which has nvrtc, not python:3.11-slim
        assert result == PUBLIC_BASE_IMAGE_GPU
        assert "nvidia" in result

    def test_cpu_short_name_without_registry_falls_back_to_public_cpu(self) -> None:
        """CPU short name without registry should fallback to public CPU image."""
        profile = {"base_image": BASE_IMAGE_CPU}

        result = resolve_base_image(profile, None)

        assert result == PUBLIC_BASE_IMAGE_CPU
        assert "jupyter" in result


class TestGetBaseImageNames:
    """Tests for get_base_image_names function."""

    def test_returns_images(self) -> None:
        """Should return base image names."""
        names = get_base_image_names()

        assert len(names) >= 2

    def test_has_descriptions(self) -> None:
        """Should have descriptions for each image."""
        names = get_base_image_names()

        for _name, desc in names.items():
            assert isinstance(desc, str)
            assert len(desc) > 10  # Meaningful description


class TestProfilesHaveBaseImage:
    """Tests that all built-in profiles have base images."""

    def test_cpu_small_has_custom_image(self) -> None:
        """cpu-small profile should use custom CPU base image."""
        from goldfish.cloud.adapters.gcp.profiles import BUILTIN_PROFILES

        assert BUILTIN_PROFILES["cpu-small"]["base_image"] == BASE_IMAGE_CPU

    def test_cpu_large_has_custom_image(self) -> None:
        """cpu-large profile should use custom CPU base image."""
        from goldfish.cloud.adapters.gcp.profiles import BUILTIN_PROFILES

        assert BUILTIN_PROFILES["cpu-large"]["base_image"] == BASE_IMAGE_CPU

    def test_h100_spot_has_custom_gpu_image(self) -> None:
        """h100-spot profile should use custom GPU base image (with nvrtc for Flash Attention)."""
        from goldfish.cloud.adapters.gcp.profiles import BUILTIN_PROFILES

        assert BUILTIN_PROFILES["h100-spot"]["base_image"] == BASE_IMAGE_GPU

    def test_a100_spot_has_custom_gpu_image(self) -> None:
        """a100-spot profile should use custom GPU base image (with nvrtc for Flash Attention)."""
        from goldfish.cloud.adapters.gcp.profiles import BUILTIN_PROFILES

        assert BUILTIN_PROFILES["a100-spot"]["base_image"] == BASE_IMAGE_GPU

    def test_all_profiles_have_base_image(self) -> None:
        """All built-in profiles should have a base_image set."""
        from goldfish.cloud.adapters.gcp.profiles import BUILTIN_PROFILES

        for name, profile in BUILTIN_PROFILES.items():
            assert "base_image" in profile, f"Profile {name} missing base_image"
            assert profile["base_image"], f"Profile {name} has empty base_image"
