"""GCP Metadata Bus - Driver for Google Cloud Platform.

Uses Instance Metadata for low-latency signaling.
"""

import logging
import subprocess
from typing import cast

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

    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None:
        val = self._get_metadata_value(target, key)
        if not val:
            return None
        try:
            return cast(MetadataSignal, MetadataSignal.model_validate_json(val))
        except Exception:
            return None

    def clear_signal(self, key: str, target: str | None = None) -> None:
        if not target:
            return
        try:
            cmd = ["gcloud", "compute", "instances", "remove-metadata", target, "--keys", key, "--quiet"]
            subprocess.run(cmd, check=True, capture_output=True)
        except Exception as e:
            logger.debug(f"Failed to clear signal on {target}: {e}")

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        if not target:
            return
        try:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "add-metadata",
                target,
                "--metadata",
                f"{key}_ack={request_id}",
                "--quiet",
            ]
            subprocess.run(cmd, check=True, capture_output=True)
        except Exception as e:
            logger.error(f"Failed to set ACK on {target}: {e}")

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        return self._get_metadata_value(target, f"{key}_ack")

    def _get_metadata_value(self, target: str | None, key: str) -> str | None:
        if not target:
            return None
        try:
            # gcloud compute instances describe INSTANCE --format="value(metadata.items.KEY)"
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "describe",
                target,
                "--format",
                f"value(metadata.items.{key})",
                "--quiet",
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            return result.stdout.strip() or None
        except Exception:
            return None
