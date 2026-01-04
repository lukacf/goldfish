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

    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None:
        """Get the current signal for a key.

        Args:
            key: Metadata key/topic
            target: Optional target instance identifier
        """
        ...

    def clear_signal(self, key: str, target: str | None = None) -> None:
        """Clear the signal for a key.

        Args:
            key: Metadata key/topic
            target: Optional target instance identifier
        """
        ...

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        """Acknowledge a signal.

        Args:
            key: Metadata key/topic
            request_id: ID of the signal being acknowledged
            target: Optional target instance identifier
        """
        ...

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        """Get the current acknowledgment.

        Args:
            key: Metadata key/topic
            target: Optional target instance identifier
        """
        ...
