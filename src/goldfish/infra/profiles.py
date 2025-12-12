"""Built-in GCE resource profiles and profile resolution.

Provides standard resource profiles for common ML workloads, abstracting away
GCE machine types, zones, and disk configurations from Claude.
"""

from copy import deepcopy
from typing import Any


class ProfileNotFoundError(Exception):
    """Raised when a profile name is not found."""

    pass


class ProfileValidationError(Exception):
    """Raised when a profile has invalid structure."""

    pass


# Pre-built base images with ML libraries
# Uses well-maintained PUBLIC images - NO setup required by users
#
# CPU: Jupyter pytorch-notebook (numpy, pandas, scikit-learn, torch, matplotlib)
# GPU: Jupyter pytorch-notebook with CUDA (same + CUDA support)
#
# Source: https://jupyter-docker-stacks.readthedocs.io/

# Public base images (no registry setup needed)
# CPU: Jupyter pytorch-notebook - has numpy, pandas, scikit-learn, torch, matplotlib, seaborn
PUBLIC_BASE_IMAGE_CPU = "quay.io/jupyter/pytorch-notebook:python-3.11"

# GPU: Jupyter pytorch-notebook with CUDA 12 - same libraries + CUDA support
PUBLIC_BASE_IMAGE_GPU = "quay.io/jupyter/pytorch-notebook:cuda12-python-3.11"

# Fallback (bare Python - requires requirements.txt)
FALLBACK_BASE_IMAGE = "python:3.11-slim"

# For custom registry images (optional override in goldfish.yaml)
BASE_IMAGE_CPU = "goldfish-base-cpu"
BASE_IMAGE_GPU = "goldfish-base-gpu"
BASE_IMAGE_VERSION = "v1"


# Built-in resource profiles optimized for ML workloads
# Based on legacy infra/gcp.yaml
BUILTIN_PROFILES: dict[str, dict[str, Any]] = {
    # CPU-only profiles
    "cpu-small": {
        "base_image": PUBLIC_BASE_IMAGE_CPU,  # GHCR public image with ML libs
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
        },
        "data_disk": {
            "type": "pd-balanced",
            "size_gb": 100,
            "mode": "rw",
        },
    },
    "cpu-large": {
        "base_image": PUBLIC_BASE_IMAGE_CPU,  # GHCR public image with ML libs
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
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    # H100 GPU profiles
    "h100-spot": {
        "base_image": PUBLIC_BASE_IMAGE_GPU,  # PyTorch official with CUDA
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
            "us-west4-a",
        ],
        "boot_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    "h100-on-demand": {
        "base_image": PUBLIC_BASE_IMAGE_GPU,  # PyTorch official with CUDA
        "machine_type": "a3-highgpu-1g",
        "gpu": {
            "type": "h100",
            "accelerator": "nvidia-h100-80gb",
            "count": 1,
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
        },
        "data_disk": {
            "type": "hyperdisk-balanced",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    # A100 GPU profiles
    "a100-spot": {
        "base_image": PUBLIC_BASE_IMAGE_GPU,  # PyTorch official with CUDA
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
        },
        "data_disk": {
            "type": "pd-ssd",
            "size_gb": 600,
            "mode": "rw",
        },
    },
    "a100-on-demand": {
        "base_image": PUBLIC_BASE_IMAGE_GPU,  # PyTorch official with CUDA
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
    required_fields = ["machine_type", "zones", "boot_disk", "data_disk", "gpu"]

    for field in required_fields:
        if field not in profile:
            raise ProfileValidationError(f"Profile missing required field: {field}")

    if not isinstance(profile["zones"], list) or len(profile["zones"]) == 0:
        raise ProfileValidationError("Profile 'zones' must be a non-empty list")

    if not isinstance(profile["gpu"], dict):
        raise ProfileValidationError("Profile 'gpu' must be a dictionary")


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
    """

    def __init__(self, profile_overrides: dict[str, dict[str, Any]] | None = None):
        """Initialize profile resolver.

        Args:
            profile_overrides: Optional dict of profile overrides from goldfish.yaml
        """
        self.profile_overrides = profile_overrides or {}

    def resolve(self, name: str) -> dict[str, Any]:
        """Resolve a profile by name, applying any overrides.

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


def resolve_base_image(profile: dict[str, Any], artifact_registry: str | None = None) -> str:
    """Resolve the base image for a profile.

    Built-in profiles use public images (PyTorch official, GHCR) that require
    NO setup - they just work. Custom profiles can use private registry images.

    Args:
        profile: Resolved profile dictionary
        artifact_registry: Optional registry URL for custom images

    Returns:
        Full image URL ready to use in FROM directive
    """
    base_image: str | None = profile.get("base_image")

    # No base image specified - use fallback
    if not base_image:
        return FALLBACK_BASE_IMAGE

    # If it's already a full image reference (contains / or :), use as-is
    # This handles: pytorch/pytorch:..., ghcr.io/..., us-docker.pkg.dev/...
    if "/" in base_image or ":" in base_image:
        return str(base_image)

    # Short name (e.g., "goldfish-base-cpu") - needs registry
    if artifact_registry:
        return f"{artifact_registry}/{base_image}:{BASE_IMAGE_VERSION}"

    # No registry for short name - use fallback
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
