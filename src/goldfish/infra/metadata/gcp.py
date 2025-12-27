"""GCP Metadata Bus - Driver for Google Cloud Platform.

Uses Instance Metadata for low-latency signaling.
"""

import logging

from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)


class GCPMetadataBus(MetadataBus):
    """Placeholder for GCP Metadata Bus."""

    def set_signal(self, key: str, signal: MetadataSignal) -> None:
        logger.warning("GCPMetadataBus.set_signal not implemented")

    def get_signal(self, key: str) -> MetadataSignal | None:
        return None

    def clear_signal(self, key: str) -> None:
        pass

    def set_ack(self, key: str, request_id: str) -> None:
        pass

    def get_ack(self, key: str) -> str | None:
        return None
