"""Built-in GCE resource profiles and profile resolution.

Provides standard resource profiles for common ML workloads, abstracting away
GCE machine types, zones, and disk configurations from Claude.
"""

from typing import Any, Dict, List, Optional
from copy import deepcopy


class ProfileNotFoundError(Exception):
    """Raised when a profile name is not found."""
    pass


class ProfileValidationError(Exception):
    """Raised when a profile has invalid structure."""
    pass


# Built-in resource profiles optimized for ML workloads
# Based on legacy infra/gcp.yaml
BUILTIN_PROFILES: Dict[str, Dict[str, Any]] = {
    # CPU-only profiles
    "cpu-small": {
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


def get_builtin_profile(name: str) -> Dict[str, Any]:
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
        raise ProfileNotFoundError(
            f"Profile '{name}' not found. Available profiles: {available}"
        )
    return deepcopy(BUILTIN_PROFILES[name])


def validate_profile(profile: Dict[str, Any]) -> None:
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


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
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

    def __init__(self, profile_overrides: Optional[Dict[str, Dict[str, Any]]] = None):
        """Initialize profile resolver.

        Args:
            profile_overrides: Optional dict of profile overrides from goldfish.yaml
        """
        self.profile_overrides = profile_overrides or {}

    def resolve(self, name: str) -> Dict[str, Any]:
        """Resolve a profile by name, applying any overrides.

        Args:
            name: Profile name (e.g., "h100-spot")

        Returns:
            Resolved profile dictionary

        Raises:
            ProfileNotFoundError: If profile doesn't exist in built-ins or overrides
        """
        # Check if this is a completely custom profile
        if name in self.profile_overrides and name not in BUILTIN_PROFILES:
            # Custom profile - use as-is
            profile = deepcopy(self.profile_overrides[name])
            validate_profile(profile)
            return profile

        # Get built-in profile as base
        if name not in BUILTIN_PROFILES:
            available = ", ".join(sorted(self.list_profiles()))
            raise ProfileNotFoundError(
                f"Profile '{name}' not found. Available profiles: {available}"
            )

        profile = get_builtin_profile(name)

        # Apply overrides if they exist
        if name in self.profile_overrides:
            profile = deep_merge(profile, self.profile_overrides[name])

        validate_profile(profile)
        return profile

    def list_profiles(self) -> List[str]:
        """List all available profile names (built-in + custom).

        Returns:
            Sorted list of profile names
        """
        all_profiles = set(BUILTIN_PROFILES.keys()) | set(self.profile_overrides.keys())
        return sorted(all_profiles)
