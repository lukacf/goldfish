"""GCS implementation of ObjectStorage protocol.

Wraps google-cloud-storage SDK to implement the ObjectStorage protocol.
All GCS-specific code is contained in this adapter.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from google.cloud import storage
from google.cloud.exceptions import NotFound

from goldfish.cloud.contracts import StorageURI
from goldfish.errors import NotFoundError, StorageError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class GCSStorage:
    """GCS implementation of ObjectStorage protocol.

    Uses the google-cloud-storage SDK for all operations.
    All URIs must use the gs:// scheme.
    """

    def __init__(self, project: str | None = None) -> None:
        """Initialize GCS storage client.

        Args:
            project: GCP project ID. If None, uses default credentials project.
        """
        self._client = storage.Client(project=project)
        self._project = project

    def put(self, uri: StorageURI, data: bytes) -> None:
        """Write bytes to GCS.

        Args:
            uri: Target GCS location (gs://bucket/path)
            data: Bytes to write

        Raises:
            StorageError: If write fails
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            blob.upload_from_string(data)
            logger.debug("Uploaded %d bytes to %s", len(data), uri)
        except Exception as e:
            raise StorageError(f"Failed to upload to {uri}: {e}") from e

    def get(self, uri: StorageURI) -> bytes:
        """Read bytes from GCS.

        Args:
            uri: Source GCS location

        Returns:
            File contents as bytes

        Raises:
            NotFoundError: If object doesn't exist
            StorageError: If read fails
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            data: bytes = blob.download_as_bytes()
            logger.debug("Downloaded %d bytes from %s", len(data), uri)
            return data
        except NotFound as e:
            raise NotFoundError(str(uri)) from e
        except Exception as e:
            raise StorageError(f"Failed to download from {uri}: {e}") from e

    def exists(self, uri: StorageURI) -> bool:
        """Check if object exists in GCS.

        Args:
            uri: Location to check

        Returns:
            True if object exists
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            exists: bool = blob.exists()
            return exists
        except Exception as e:
            logger.warning("Error checking existence of %s: %s", uri, e)
            return False

    def list_prefix(self, prefix: StorageURI) -> list[StorageURI]:
        """List objects with given prefix in GCS.

        Based on RCT-GCS-2: Returns all objects matching prefix in lexicographic order.

        Args:
            prefix: URI prefix to match

        Returns:
            List of matching URIs (may be empty), sorted lexicographically
        """
        if prefix.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {prefix.scheme}://")

        try:
            bucket = self._client.bucket(prefix.bucket)
            blobs = bucket.list_blobs(prefix=prefix.path)

            # Build URIs and sort lexicographically (matching GCS behavior per RCT-GCS-2)
            uris = [StorageURI(scheme="gs", bucket=prefix.bucket, path=blob.name) for blob in blobs]
            return sorted(uris, key=lambda u: u.path)
        except Exception as e:
            logger.warning("Error listing prefix %s: %s", prefix, e)
            return []

    def delete(self, uri: StorageURI) -> None:
        """Delete object from GCS.

        No-op if object doesn't exist (idempotent).

        Args:
            uri: Object to delete
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            blob.delete()
            logger.debug("Deleted %s", uri)
        except NotFound:
            # Idempotent - no error if already gone
            pass
        except Exception as e:
            raise StorageError(f"Failed to delete {uri}: {e}") from e

    def get_local_path(self, uri: StorageURI) -> Path | None:
        """Get local filesystem path for URI.

        GCS doesn't provide direct local paths unless mounted via gcsfuse.
        Returns None - callers should use get() to download data.

        Args:
            uri: Storage URI

        Returns:
            None (GCS objects don't have local paths without explicit mounting)
        """
        # GCS objects don't have local paths by default
        # A future enhancement could detect gcsfuse mounts
        return None

    def download_to_file(self, uri: StorageURI, destination: Path) -> bool:
        """Download object to a local file.

        Args:
            uri: Source GCS location
            destination: Local path to write to

        Returns:
            True if download succeeded, False if object doesn't exist
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            # Check existence first to avoid creating empty file on NotFound
            if not blob.exists():
                return False
            destination.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(str(destination))
            logger.debug("Downloaded %s to %s", uri, destination)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.warning("Failed to download %s: %s", uri, e)
            return False

    def get_size(self, uri: StorageURI) -> int | None:
        """Get size of object in bytes.

        Args:
            uri: Location to check

        Returns:
            Size in bytes, or None if object doesn't exist
        """
        if uri.scheme != "gs":
            raise StorageError(f"GCSStorage only supports gs:// URIs, got {uri.scheme}://")

        try:
            bucket = self._client.bucket(uri.bucket)
            blob = bucket.blob(uri.path)
            blob.reload()  # Fetch metadata including size
            size: int | None = blob.size
            return size
        except NotFound:
            return None
        except Exception as e:
            logger.warning("Error getting size of %s: %s", uri, e)
            return None
