"""Metadata Signal Bus - Base Protocol and Models.

Defines the interface for cross-cloud metadata signaling used for
low-latency 'Overdrive' synchronization.
"""

from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field


class MetadataSignal(BaseModel):
    """A signal sent via instance metadata."""

    command: str  # e.g., "sync"
    request_id: str
    payload: dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)


@runtime_checkable
class MetadataBus(Protocol):
    """Protocol for interacting with Cloud Instance Metadata."""

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        """Set a metadata signal for the instance.

        Args:
            key: Metadata key/topic
            signal: The signal object
            target: Optional target instance/resource identifier
        """
        ...

    def get_signal(self, key: str) -> MetadataSignal | None:
        """Get the current metadata signal."""
        ...

    def clear_signal(self, key: str) -> None:
        """Clear a metadata signal."""
        ...

    def set_ack(self, key: str, request_id: str) -> None:
        """Acknowledge a signal (used by the container)."""
        ...

    def get_ack(self, key: str) -> str | None:
        """Get the last acknowledged request ID (used by the server)."""
        ...
