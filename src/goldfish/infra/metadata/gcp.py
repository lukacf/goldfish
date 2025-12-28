"""GCP Metadata Bus - Driver for Google Cloud Platform.

Uses Instance Metadata for low-latency signaling.
"""

import logging
import subprocess

from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)


class GCPMetadataBus(MetadataBus):
    """GCP Metadata Bus implementation using gcloud CLI."""

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        """Set instance metadata using gcloud.

        Requires 'target' to be the instance name.
        """
        if not target:
            logger.warning("GCPMetadataBus.set_signal: target instance name required")
            return

        try:
            # Format the signal as a string
            value = signal.model_dump_json()

            # Use gcloud to update metadata
            # gcloud compute instances add-metadata INSTANCE --metadata KEY=VALUE
            cmd = ["gcloud", "compute", "instances", "add-metadata", target, "--metadata", f"{key}={value}", "--quiet"]

            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.debug(f"GCPMetadataBus set_signal: {key}={signal.request_id} target={target}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to set metadata on {target}: {e.stderr}")
            raise  # Re-raise to alert caller
        except Exception as e:
            logger.error(f"Error in set_signal: {e}")
            raise

    def get_signal(self, key: str) -> MetadataSignal | None:
        return None

    def clear_signal(self, key: str) -> None:
        pass

    def set_ack(self, key: str, request_id: str) -> None:
        pass

    def get_ack(self, key: str) -> str | None:
        return None
