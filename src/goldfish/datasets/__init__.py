"""Dataset management for Goldfish.

This module provides tools for:
- Registering project-level datasets
- Uploading local data to GCS
- Tracking dataset locations and metadata
"""

from goldfish.datasets.registry import DatasetRegistry

__all__ = ["DatasetRegistry"]
