"""GCP Metadata Bus - Driver for Google Cloud Platform.

Uses Instance Metadata for low-latency signaling.
"""

import logging
import re
import subprocess
import tempfile

from goldfish.errors import GoldfishError
from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)

_SIMPLE_INSTANCE_RE = re.compile(r"^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$")
_TARGET_ALLOWED_RE = re.compile(r"^[a-zA-Z0-9\-\/\.\:]+$")
_TARGET_RE = re.compile(r"(?:projects/(?P<project>[^/]+)/)?zones/(?P<zone>[^/]+)/instances/(?P<instance>[^/]+)$")
_TARGET_PREFIXES = (
    "https://www.googleapis.com/compute/v1/",
    "https://compute.googleapis.com/compute/v1/",
)


def validate_instance_name(name: str) -> None:
    """Validate GCP instance name or URI to prevent command injection."""
    if _SIMPLE_INSTANCE_RE.match(name):
        return

    # Only allow safe URI characters and require the instances path to exist.
    if _TARGET_ALLOWED_RE.match(name) and "instances/" in name:
        return

    raise GoldfishError(f"Invalid GCP instance name or URI: {name}")


def _normalize_target(target: str) -> tuple[str, str | None, str | None]:
    """Normalize instance targets to (name, zone, project) for gcloud commands."""
    validate_instance_name(target)

    normalized = target
    for prefix in _TARGET_PREFIXES:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break

    match = _TARGET_RE.search(normalized)
    if match:
        instance = match.group("instance")
        zone = match.group("zone")
        project = match.group("project")
        if not _SIMPLE_INSTANCE_RE.match(instance):
            raise GoldfishError(f"Invalid GCP instance name: {instance}")
        return instance, zone, project

    if not _SIMPLE_INSTANCE_RE.match(target):
        raise GoldfishError(f"Invalid GCP instance name: {target}")

    return target, None, None


class GCPMetadataBus(MetadataBus):
    """GCP Metadata Bus implementation using gcloud CLI."""

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        """Set instance metadata using gcloud.

        Requires 'target' to be the instance name.
        """
        if not target:
            logger.warning("GCPMetadataBus.set_signal: target instance name required")
            return

        instance_name, zone, project = _normalize_target(target)

        try:
            # Format the signal as a string
            value = signal.model_dump_json()

            # Use --metadata-from-file to avoid shell/gcloud escaping issues with JSON
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as tmp:
                tmp.write(value)
                tmp.flush()

                # gcloud compute instances add-metadata INSTANCE --metadata-from-file KEY=FILE
                cmd = [
                    "gcloud",
                    "compute",
                    "instances",
                    "add-metadata",
                    instance_name,
                    "--metadata-from-file",
                    f"{key}={tmp.name}",
                    "--quiet",
                ]
                if zone:
                    cmd.append(f"--zone={zone}")
                if project:
                    cmd.append(f"--project={project}")

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
            instance_name, zone, project = _normalize_target(target)
        else:
            instance_name, zone, project = None, None, None
        val = self._get_metadata_value(instance_name, key, zone=zone, project=project)
        if not val:
            return None
        try:
            return MetadataSignal.model_validate_json(val)  # type: ignore[no-any-return]
        except Exception:
            return None

    def clear_signal(self, key: str, target: str | None = None) -> None:
        if not target:
            return
        instance_name, zone, project = _normalize_target(target)
        try:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "remove-metadata",
                instance_name,
                "--keys",
                key,
                "--quiet",
            ]
            if zone:
                cmd.append(f"--zone={zone}")
            if project:
                cmd.append(f"--project={project}")
            subprocess.run(cmd, check=True, capture_output=True)
        except Exception as e:
            logger.debug(f"Failed to clear signal on {target}: {e}")

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        if not target:
            return
        instance_name, zone, project = _normalize_target(target)
        try:
            # Simple strings are safe for --metadata flag
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "add-metadata",
                instance_name,
                "--metadata",
                f"{key}_ack={request_id}",
                "--quiet",
            ]
            if zone:
                cmd.append(f"--zone={zone}")
            if project:
                cmd.append(f"--project={project}")
            subprocess.run(cmd, check=True, capture_output=True)
        except Exception as e:
            logger.error(f"Failed to set ACK on {target}: {e}")
            raise GoldfishError(f"Failed to set metadata ACK: {e}") from e

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        if target:
            instance_name, zone, project = _normalize_target(target)
        else:
            instance_name, zone, project = None, None, None
        return self._get_metadata_value(instance_name, f"{key}_ack", zone=zone, project=project)

    def _get_metadata_value(
        self,
        target: str | None,
        key: str,
        zone: str | None = None,
        project: str | None = None,
    ) -> str | None:
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
            if zone:
                cmd.append(f"--zone={zone}")
            if project:
                cmd.append(f"--project={project}")
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            return result.stdout.strip() or None
        except Exception:
            return None
