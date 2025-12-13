"""GCS storage provider implementation."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from goldfish.errors import GoldfishError
from goldfish.providers.base import StorageLocation, StorageProvider

logger = logging.getLogger(__name__)


class GCSStorageProvider(StorageProvider):
    """Storage provider for Google Cloud Storage.

    Wraps gsutil commands with provider interface.
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize GCS storage provider.

        Expected config keys:
            - bucket: GCS bucket name (required)
            - sources_prefix: Prefix for external sources (default: "sources/")
            - artifacts_prefix: Prefix for stage artifacts (default: "artifacts/")
            - snapshots_prefix: Prefix for snapshots (default: "snapshots/")
            - datasets_prefix: Prefix for datasets (default: "datasets/")
            - project_id: Optional GCP project ID for generating hyperlinks
        """
        super().__init__(config)

        # Validate config is dict
        if not isinstance(config, dict):
            raise GoldfishError(f"GCS provider config must be dict, got {type(config).__name__}")

        # Validate and extract required bucket
        self.bucket = config.get("bucket")
        if not self.bucket:
            raise GoldfishError("GCS provider requires 'bucket' configuration")
        if not isinstance(self.bucket, str):
            raise GoldfishError(f"GCS provider 'bucket' must be string, got {type(self.bucket).__name__}")

        # Extract and validate optional string fields
        self.sources_prefix = config.get("sources_prefix", "sources/")
        if not isinstance(self.sources_prefix, str):
            raise GoldfishError(
                f"GCS provider 'sources_prefix' must be string, got {type(self.sources_prefix).__name__}"
            )
        self.sources_prefix = self.sources_prefix.rstrip("/")

        self.artifacts_prefix = config.get("artifacts_prefix", "artifacts/")
        if not isinstance(self.artifacts_prefix, str):
            raise GoldfishError(
                f"GCS provider 'artifacts_prefix' must be string, got {type(self.artifacts_prefix).__name__}"
            )
        self.artifacts_prefix = self.artifacts_prefix.rstrip("/")

        self.snapshots_prefix = config.get("snapshots_prefix", "snapshots/")
        if not isinstance(self.snapshots_prefix, str):
            raise GoldfishError(
                f"GCS provider 'snapshots_prefix' must be string, got {type(self.snapshots_prefix).__name__}"
            )
        self.snapshots_prefix = self.snapshots_prefix.rstrip("/")

        self.datasets_prefix = config.get("datasets_prefix", "datasets/")
        if not isinstance(self.datasets_prefix, str):
            raise GoldfishError(
                f"GCS provider 'datasets_prefix' must be string, got {type(self.datasets_prefix).__name__}"
            )
        self.datasets_prefix = self.datasets_prefix.rstrip("/")

        self.project_id = config.get("project_id")
        if self.project_id is not None and not isinstance(self.project_id, str):
            raise GoldfishError(f"GCS provider 'project_id' must be string, got {type(self.project_id).__name__}")

    def _normalize_remote_path(self, remote_path: str) -> str:
        """Convert remote_path to full gs:// URI if needed.

        Args:
            remote_path: Either "dataset_name" or "gs://bucket/path"

        Returns:
            Full gs:// URI
        """
        if remote_path.startswith("gs://"):
            return remote_path

        # Treat as dataset name, add prefix
        return f"gs://{self.bucket}/{self.datasets_prefix}/{remote_path}"

    def upload(
        self,
        local_path: Path,
        remote_path: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation:
        """Upload local file or directory to GCS.

        Args:
            local_path: Local file or directory path
            remote_path: Remote path (dataset name or gs:// URI)
            metadata: Optional metadata (currently unused by gsutil)

        Returns:
            StorageLocation with GCS URI
        """
        if not local_path.exists():
            raise GoldfishError(f"Local path not found: {local_path}")

        gcs_uri = self._normalize_remote_path(remote_path)

        # Build gsutil command
        if local_path.is_dir():
            # Upload directory recursively
            cmd = ["gsutil", "-m", "cp", "-r", str(local_path), gcs_uri]
        else:
            # Upload single file
            cmd = ["gsutil", "cp", str(local_path), gcs_uri]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
            )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                raise GoldfishError(f"Failed to upload to GCS: {error_msg}")

            # Get size if local file
            size_bytes = None
            if local_path.is_file():
                size_bytes = local_path.stat().st_size

            hyperlink = self.get_hyperlink(gcs_uri)

            return StorageLocation(
                uri=gcs_uri,
                size_bytes=size_bytes,
                metadata=metadata,
                hyperlink=hyperlink,
            )

        except FileNotFoundError as err:
            raise GoldfishError(
                "gsutil command not found. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
            ) from err

    def download(
        self,
        remote_path: str,
        local_path: Path,
    ) -> Path:
        """Download from GCS to local filesystem.

        Args:
            remote_path: Remote path (dataset name or gs:// URI)
            local_path: Local destination path

        Returns:
            Path to downloaded file/directory
        """
        gcs_uri = self._normalize_remote_path(remote_path)

        # Create parent directory if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Build gsutil command
        cmd = ["gsutil", "-m", "cp", "-r", gcs_uri, str(local_path)]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
            )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                raise GoldfishError(f"Failed to download from GCS: {error_msg}")

            return local_path

        except FileNotFoundError as err:
            raise GoldfishError(
                "gsutil command not found. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
            ) from err

    def exists(self, remote_path: str) -> bool:
        """Check if path exists in GCS.

        Args:
            remote_path: Remote path (dataset name or gs:// URI)

        Returns:
            True if exists
        """
        gcs_uri = self._normalize_remote_path(remote_path)

        try:
            result = subprocess.run(
                ["gsutil", "stat", gcs_uri],
                capture_output=True,
                check=False,
            )
            return result.returncode == 0

        except FileNotFoundError as e:
            # gsutil not installed - log warning and return False
            logger.warning(f"gsutil not found when checking existence of {gcs_uri}: {e}")
            return False
        except (OSError, PermissionError) as e:
            # Permission or system error
            logger.error(f"Error checking existence of {gcs_uri}: {e}")
            return False

    def get_size(self, remote_path: str) -> int | None:
        """Get size of stored object.

        Args:
            remote_path: Remote path (dataset name or gs:// URI)

        Returns:
            Size in bytes, or None if not available
        """
        gcs_uri = self._normalize_remote_path(remote_path)

        try:
            result = subprocess.run(
                ["gsutil", "du", "-s", gcs_uri],
                capture_output=True,
                check=False,
            )

            if result.returncode == 0:
                # Parse output: "12345  gs://bucket/path"
                output = result.stdout.decode().strip()
                parts = output.split()
                if parts:
                    return int(parts[0])

            return None

        except FileNotFoundError as e:
            logger.warning(f"gsutil not found when getting size of {gcs_uri}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Failed to parse size from gsutil output for {gcs_uri}: {e}")
            return None
        except (OSError, PermissionError) as e:
            logger.error(f"Error getting size of {gcs_uri}: {e}")
            return None

    def presign(self, remote_path: str, expiration_seconds: int = 3600) -> str | None:
        """Generate signed URL for temporary access.

        Args:
            remote_path: Remote path (dataset name or gs:// URI)
            expiration_seconds: URL validity duration

        Returns:
            Signed URL
        """
        gcs_uri = self._normalize_remote_path(remote_path)

        try:
            result = subprocess.run(
                ["gsutil", "signurl", "-d", f"{expiration_seconds}s", gcs_uri],
                capture_output=True,
                check=False,
            )

            if result.returncode == 0:
                # Parse output to extract URL
                output = result.stdout.decode().strip()
                lines = output.split("\n")
                if len(lines) > 1:
                    # Second line contains the URL
                    parts = lines[1].split()
                    if len(parts) > 1:
                        return parts[1]

            return None

        except FileNotFoundError as e:
            logger.warning(f"gsutil not found when generating presigned URL for {gcs_uri}: {e}")
            return None
        except (OSError, PermissionError) as e:
            logger.error(f"Error generating presigned URL for {gcs_uri}: {e}")
            return None

    def get_hyperlink(self, remote_path: str) -> str | None:
        """Get GCS console hyperlink.

        Args:
            remote_path: Remote path (dataset name or gs:// URI)

        Returns:
            Console URL for viewing in browser
        """
        gcs_uri = self._normalize_remote_path(remote_path)

        # Extract bucket and path from gs://bucket/path
        if not gcs_uri.startswith("gs://"):
            return None

        parts = gcs_uri[5:].split("/", 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""

        # Build console URL
        if path:
            return f"https://console.cloud.google.com/storage/browser/{bucket}/{path}"
        else:
            return f"https://console.cloud.google.com/storage/browser/{bucket}"

    def snapshot(
        self,
        remote_path: str,
        snapshot_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation | None:
        """Create a snapshot by copying to snapshots prefix.

        Args:
            remote_path: Remote path to snapshot
            snapshot_id: Identifier for snapshot
            metadata: Optional metadata

        Returns:
            StorageLocation for snapshot
        """
        source_uri = self._normalize_remote_path(remote_path)
        snapshot_uri = f"gs://{self.bucket}/{self.snapshots_prefix}/{snapshot_id}"

        try:
            result = subprocess.run(
                ["gsutil", "-m", "cp", "-r", source_uri, snapshot_uri],
                capture_output=True,
                check=False,
            )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                raise GoldfishError(f"Failed to create snapshot: {error_msg}")

            hyperlink = self.get_hyperlink(snapshot_uri)

            return StorageLocation(
                uri=snapshot_uri,
                size_bytes=self.get_size(snapshot_uri),
                metadata=metadata,
                hyperlink=hyperlink,
            )

        except FileNotFoundError as err:
            raise GoldfishError("gsutil command not found") from err

    def store_handle(
        self,
        remote_path: str,
        handle: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Store opaque handle as metadata.

        For GCS, we could store this in object metadata,
        but gsutil doesn't make this easy. Return False for now.

        Args:
            remote_path: Remote path
            handle: Opaque handle string
            metadata: Optional metadata

        Returns:
            False (not supported via gsutil)
        """
        # Would require using gcloud storage or Python client library
        return False

    def retrieve_handle(self, remote_path: str) -> str | None:
        """Retrieve stored handle.

        Args:
            remote_path: Remote path

        Returns:
            None (not supported via gsutil)
        """
        return None
