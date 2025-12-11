"""Config hashing utilities for stage versioning.

Provides deterministic hashing of stage configurations to track
unique (code + config) combinations for stage versioning.
"""

from __future__ import annotations

import hashlib
import json


def compute_config_hash(config: dict | None) -> str:
    """Compute deterministic SHA256 hash of config.

    Creates a canonical JSON representation with sorted keys,
    then hashes it. This ensures the same config always produces
    the same hash regardless of key insertion order.

    Args:
        config: Configuration dictionary, or None for empty config

    Returns:
        Full 64-character SHA256 hex string

    Examples:
        >>> compute_config_hash({"b": 2, "a": 1})
        >>> compute_config_hash({"a": 1, "b": 2})  # Same hash
        >>> compute_config_hash(None)  # Same as {}
    """
    if config is None:
        config = {}

    # Create canonical JSON: sorted keys, minimal separators
    canonical = json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        default=str,  # Handle non-JSON types (Path, etc.)
    )

    return hashlib.sha256(canonical.encode()).hexdigest()


def short_hash(full_hash: str) -> str:
    """Truncate hash for display purposes.

    Args:
        full_hash: Full SHA256 hex string (64 chars)

    Returns:
        First 12 characters of the hash
    """
    return full_hash[:12]
