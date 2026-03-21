"""Built-in GCE resource profiles and profile resolution.

Provides standard resource profiles for common ML workloads, abstracting away
GCE machine types, zones, and disk configurations from Claude.
"""

from copy import deepcopy
from typing import Any

# ===========================================================================
# Image Constants - imported from cloud.image_versions (single source of truth)
# ===========================================================================
# NOTE: These are re-exported here for backward compatibility only.
# New code should import directly from goldfish.cloud.image_versions
from goldfish.cloud.image_versions import (
    BASE_IMAGE_CPU,
    BASE_IMAGE_GPU,
    BASE_IMAGE_VERSION_DEFAULT,
    FALLBACK_BASE_IMAGE,
    PUBLIC_BASE_IMAGE_CPU,
    PUBLIC_BASE_IMAGE_GPU,
)


class ProfileNotFoundError(Exception):
    """Raised when a profile name is not found."""

    pass


class ProfileValidationError(Exception):
    """Raised when a profile has invalid structure."""

    pass


# Pre-built base images with ML libraries
#
# CPU: Public Jupyter image (no setup needed)
# GPU: Custom goldfish-base-gpu from Artifact Registry (includes nvrtc for Flash Attention)
#
# When artifact_registry is configured in goldfish.yaml, GPU profiles use the custom
# image which includes the full CUDA toolkit. Without AR configured, falls back to
# the public Jupyter CUDA image (which lacks nvrtc for Flash Attention JIT).

# ===========================================================================
# Backward compat alias - new code should use BASE_IMAGE_VERSION_DEFAULT
BASE_IMAGE_VERSION = BASE_IMAGE_VERSION_DEFAULT


# Built-in resource profiles optimized for ML workloads
#
# IMPORTANT: The zones listed in these profiles are EXAMPLES based on common
# GPU availability patterns. Users MUST configure their own zones in goldfish.yaml:
#
#   gce:
#     zones:
#       - europe-west4-a
#       - asia-southeast1-b
#
# The global zones setting overrides all profile defaults, allowing you to
# target regions where YOU have GPU quota.
#
# See ProfileResolver.resolve() for zone priority:
# 1. Profile-specific override in goldfish.yaml gce.profile_overrides
# 2. Global zones from goldfish.yaml gce.zones
# 3. Built-in defaults (below) - ONLY used if no config provided
BUILTIN_PROFILES: dict[str, dict[str, Any]] = {
    # CPU-only profiles
    "cpu-small": {
        "base_image": BASE_IMAGE_CPU,  # Custom image with ML libs and Rust
        "machine_type": "n2-standard-4",
        "gpu": {
            "type": "none",
            "accelerator": None,
            "count": 0,
        },
        "preemptible_allowed": True,
        "on_demand_allowed": True,
        "zones": [
            "us-central1-a",
            "us-central1-b",
            "us-central1-f",
        ],
        "boot_disk": {
            "type": "pd-balanced",
            "size_gb": 200,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "pd-balanced",
            "size_gb": 100,
            "mode": "rw",
        },
    },
    "cpu-large": {
        "base_image": BASE_IMAGE_CPU,  # Custom image with ML libs and Rust
        "machine_type": "c4-highcpu-192",
        "gpu": {
            "type": "none",
            "accelerator": None,
            "count": 0,
        },
        "preemptible_allowed": True,
        "on_demand_allowed": True,
        "zones": [
            "us-central1-f",
            "us-central1-a",
        ],
        "boot_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    # H100 GPU profiles
    "h100-spot": {
        "base_image": BASE_IMAGE_GPU,  # Custom image with nvrtc for Flash Attention
        "machine_type": "a3-highgpu-1g",
        "gpu": {
            "type": "h100",
            "accelerator": "nvidia-h100-80gb",
            "count": 1,
        },
        "preemptible_allowed": True,
        "on_demand_allowed": False,
        "zones": [
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
        ],
        "boot_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    "h100-on-demand": {
        "base_image": BASE_IMAGE_GPU,
        # a3-highgpu-8g is the smallest on-demand H100 machine.
        # a3-highgpu-1g/2g/4g are spot/flex-start ONLY.
        "machine_type": "a3-highgpu-8g",
        "gpu": {
            "type": "h100",
            "accelerator": "nvidia-h100-80gb",
            "count": 8,
        },
        "preemptible_allowed": False,
        "on_demand_allowed": True,
        "zones": [
            "us-central1-a",
            "us-central1-b",
            "us-central1-c",
            "us-west4-a",
        ],
        "boot_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    # A100 GPU profiles
    "a100-spot": {
        "base_image": BASE_IMAGE_GPU,  # Custom image with nvrtc for Flash Attention
        "machine_type": "a2-highgpu-1g",
        "gpu": {
            "type": "a100",
            "accelerator": "nvidia-tesla-a100",
            "count": 1,
        },
        "preemptible_allowed": True,
        "on_demand_allowed": False,
        "zones": [
            "us-central1-f",
            "us-central1-b",
            "us-west4-b",
            "europe-west4-a",
        ],
        "boot_disk": {
            "type": "pd-ssd",
            "size_gb": 600,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "pd-ssd",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    "a100-on-demand": {
        "base_image": BASE_IMAGE_GPU,  # Custom image with nvrtc for Flash Attention
        "machine_type": "a2-highgpu-1g",
        "gpu": {
            "type": "a100",
            "accelerator": "nvidia-tesla-a100",
            "count": 1,
        },
        "preemptible_allowed": True,
        "on_demand_allowed": True,
        "zones": [
            "us-central1-f",
            "us-central1-b",
            "us-west4-b",
            "europe-west4-a",
        ],
        "boot_disk": {
            "type": "pd-ssd",
            "size_gb": 600,
            "image_family": "debian-12",
            "image_project": "debian-cloud",
        },
        "data_disk": {
            "type": "pd-ssd",
            "size_gb": 600,
            "mode": "rw",
        },
    },
}


def get_builtin_profile(name: str) -> dict[str, Any]:
    """Get a built-in profile by name.

    Args:
        name: Profile name (e.g., "h100-spot", "cpu-large")

    Returns:
        Profile dictionary

    Raises:
        ProfileNotFoundError: If profile doesn't exist
    """
    if name not in BUILTIN_PROFILES:
        available = ", ".join(sorted(BUILTIN_PROFILES.keys()))
        raise ProfileNotFoundError(f"Profile '{name}' not found. Available profiles: {available}")
    return deepcopy(BUILTIN_PROFILES[name])


def validate_profile(profile: dict[str, Any]) -> None:
    """Validate profile structure.

    Args:
        profile: Profile dictionary to validate

    Raises:
        ProfileValidationError: If profile is invalid
    """
    required_fields = ["machine_type", "zones", "boot_disk"]

    for field in required_fields:
        if field not in profile:
            raise ProfileValidationError(
                f"Profile missing required field: '{field}'. "
                f"Required fields: machine_type, zones, boot_disk. "
                f"Optional: gpu (default: none), data_disk."
            )

    if not isinstance(profile["zones"], list) or len(profile["zones"]) == 0:
        raise ProfileValidationError("Profile 'zones' must be a non-empty list")

    # Normalize gpu: null/missing → CPU-only default
    if "gpu" not in profile or profile["gpu"] is None:
        profile["gpu"] = {"type": "none", "accelerator": None, "count": 0}
    elif not isinstance(profile["gpu"], dict):
        raise ProfileValidationError(
            "Profile 'gpu' must be a dictionary (e.g., {type: none, count: 0} for CPU) "
            "or null/omitted for CPU-only profiles"
        )


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    result = deepcopy(base)

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dicts
            result[key] = deep_merge(result[key], value)
        else:
            # Override value
            result[key] = deepcopy(value)

    return result


class ProfileResolver:
    """Resolves resource profiles with optional custom overrides.

    Handles merging built-in profiles with user-defined overrides from goldfish.yaml.
    Global zones can be applied to all profiles for region customization.
    """

    def __init__(
        self,
        profile_overrides: dict[str, dict[str, Any]] | None = None,
        global_zones: list[str] | None = None,
    ):
        """Initialize profile resolver.

        Args:
            profile_overrides: Optional dict of profile overrides from goldfish.yaml
            global_zones: Optional global zone list to apply to all profiles.
                         This allows users to customize regions without overriding
                         every profile individually.
        """
        self.profile_overrides = profile_overrides or {}
        self.global_zones = global_zones

    def resolve(self, name: str) -> dict[str, Any]:
        """Resolve a profile by name, applying any overrides.

        Priority for zones (highest to lowest):
        1. Profile-specific override in profile_overrides
        2. Global zones from goldfish.yaml gce.zones
        3. Built-in profile defaults

        Args:
            name: Profile name (e.g., "h100-spot")

        Returns:
            Resolved profile dictionary with 'name' field included

        Raises:
            ProfileNotFoundError: If profile doesn't exist in built-ins or overrides
        """
        # Check if this is a completely custom profile
        if name in self.profile_overrides and name not in BUILTIN_PROFILES:
            # Custom profile - use as-is
            profile = deepcopy(self.profile_overrides[name])
            # Apply global zones if custom profile doesn't specify zones
            if self.global_zones and "zones" not in profile:
                profile["zones"] = list(self.global_zones)
            validate_profile(profile)
            profile["name"] = name  # Ensure name is included for ResourceLauncher
            return profile

        # Get built-in profile as base
        if name not in BUILTIN_PROFILES:
            available = ", ".join(sorted(self.list_profiles()))
            raise ProfileNotFoundError(f"Profile '{name}' not found. Available profiles: {available}")

        profile = get_builtin_profile(name)

        # Apply overrides if they exist
        if name in self.profile_overrides:
            profile = deep_merge(profile, self.profile_overrides[name])

        # Apply global zones if not overridden (either by profile_overrides or custom profile)
        # This allows users to set zones once in goldfish.yaml instead of per-profile
        profile_has_zone_override = name in self.profile_overrides and "zones" in self.profile_overrides[name]
        if self.global_zones and not profile_has_zone_override:
            profile["zones"] = list(self.global_zones)

        validate_profile(profile)
        profile["name"] = name  # Ensure name is included for ResourceLauncher
        return profile

    def list_profiles(self) -> list[str]:
        """List all available profile names (built-in + custom).

        Returns:
            Sorted list of profile names
        """
        all_profiles = set(BUILTIN_PROFILES.keys()) | set(self.profile_overrides.keys())
        return sorted(all_profiles)


def resolve_base_image(
    profile: dict[str, Any],
    artifact_registry: str | None = None,
    version: str | None = None,
) -> str:
    """Resolve the base image for a profile.

    Built-in profiles use public images (PyTorch official, GHCR) that require
    NO setup - they just work. Custom profiles can use private registry images.

    Args:
        profile: Resolved profile dictionary
        artifact_registry: Optional registry URL for custom images
        version: Optional version override (e.g., "v10"). If not provided,
                 uses the hardcoded BASE_IMAGE_VERSION constant.

    Returns:
        Full image URL ready to use in FROM directive
    """
    base_image: str | None = profile.get("base_image")
    effective_version = version if version else BASE_IMAGE_VERSION

    # No base image specified - use fallback
    if not base_image:
        return FALLBACK_BASE_IMAGE

    # If it's already a full image reference (contains / or :), use as-is
    # This handles: pytorch/pytorch:..., ghcr.io/..., us-docker.pkg.dev/...
    if "/" in base_image or ":" in base_image:
        return str(base_image)

    # Short name (e.g., "goldfish-base-cpu") - needs registry
    if artifact_registry:
        return f"{artifact_registry}/{base_image}:{effective_version}"

    # No registry for short name - fall back to appropriate public image
    # This ensures GPU profiles still get a GPU-capable image even without AR configured
    if base_image == BASE_IMAGE_GPU:
        return PUBLIC_BASE_IMAGE_GPU
    elif base_image == BASE_IMAGE_CPU:
        return PUBLIC_BASE_IMAGE_CPU

    return FALLBACK_BASE_IMAGE


def get_base_image_names() -> dict[str, str]:
    """Get all base image names and their descriptions.

    Returns:
        Dict of base image name -> description
    """
    return {
        BASE_IMAGE_CPU: "CPU image with numpy, pandas, scikit-learn, and common ML libraries",
        BASE_IMAGE_GPU: "GPU image with CUDA, PyTorch, numpy, pandas, and common ML libraries",
    }
