"""GCP Metadata Bus - Driver for Google Cloud Platform.

Uses Instance Metadata for low-latency signaling.
"""

import logging
import re
import subprocess

from goldfish.errors import GoldfishError
from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)


def validate_instance_name(name: str) -> None:
    """Validate GCP instance name to prevent command injection.

    Rules: 1-63 chars, lowercase letters, numbers, hyphens.
    Must start with a letter, end with a letter or number.
    """
    if not re.match(r"^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$", name):
        raise GoldfishError(f"Invalid GCP instance name: {name}")


class GCPMetadataBus(MetadataBus):
    """GCP Metadata Bus implementation using gcloud CLI."""

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        """Set instance metadata using gcloud.

        Requires 'target' to be the instance name.
        """
        if not target:
            logger.warning("GCPMetadataBus.set_signal: target instance name required")
            return

        validate_instance_name(target)

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
            raise GoldfishError(f"Failed to set metadata signal: {e.stderr}") from e
        except Exception as e:
            logger.error(f"Error in set_signal: {e}")
            if isinstance(e, GoldfishError):
                raise
            raise GoldfishError(f"GCP Metadata Bus error: {e}") from e

    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None:
        if target:
            validate_instance_name(target)
        val = self._get_metadata_value(target, key)
        if not val:
            return None
        try:
            return MetadataSignal.model_validate_json(val)  # type: ignore[no-any-return]
        except Exception:
            return None

    def clear_signal(self, key: str, target: str | None = None) -> None:
        if not target:
            return
        validate_instance_name(target)
        try:
            cmd = ["gcloud", "compute", "instances", "remove-metadata", target, "--keys", key, "--quiet"]
            subprocess.run(cmd, check=True, capture_output=True)
        except Exception as e:
            logger.debug(f"Failed to clear signal on {target}: {e}")

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        if not target:
            return
        validate_instance_name(target)
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
            raise GoldfishError(f"Failed to set metadata ACK: {e}") from e

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        if target:
            validate_instance_name(target)
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
