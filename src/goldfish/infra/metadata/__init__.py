"""Metadata Infrastructure - Unified interface for cross-cloud signaling."""

from goldfish.infra.metadata.base import MetadataBus, MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus

__all__ = ["MetadataBus", "MetadataSignal", "LocalMetadataBus"]
