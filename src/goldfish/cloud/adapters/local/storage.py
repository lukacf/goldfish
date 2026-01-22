"""Local filesystem implementation of ObjectStorage protocol.

Maps gs:// URIs to local filesystem paths for development and testing.
Supports configurable simulation controls per LOCAL_PARITY_SPEC.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.cloud.contracts import StorageURI
from goldfish.errors import NotFoundError, StorageError

if TYPE_CHECKING:
    from goldfish.config import LocalStorageConfig

# Default size limit (matches GCS default object size limit)
DEFAULT_SIZE_LIMIT_MB = 5 * 1024  # 5TB, effectively unlimited for local


class LocalObjectStorage:
    """Local filesystem storage that emulates GCS semantics.

    Maps gs://bucket/path to {root}/bucket/path on the local filesystem.
    Supports configurable consistency delay and size limits per LOCAL_PARITY_SPEC.

    Consistency delay simulates GCS eventual consistency: reads within N ms
    of a write to the SAME path will be delayed. This matches real GCS behavior
    where recently written objects may not be immediately visible.
    """

    def __init__(
        self,
        root: Path,
        config: LocalStorageConfig | None = None,
    ) -> None:
        """Initialize local storage with root directory.

        Args:
            root: Base directory for all storage operations.
            config: Optional storage config for simulation controls.
        """
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._consistency_delay_ms = config.consistency_delay_ms if config else 0
        self._size_limit_bytes = config.size_limit_mb * 1024 * 1024 if config and config.size_limit_mb else None
        # Track write timestamps per path for consistency simulation
        self._write_timestamps: dict[str, float] = {}

    def _resolve_path(self, uri: StorageURI) -> Path:
        """Convert StorageURI to local filesystem path.

        Raises:
            StorageError: If resolved path escapes root (path traversal).
        """
        path = (self.root / uri.bucket / uri.path).resolve()
        # Defense-in-depth: verify resolved path is within root
        if not path.is_relative_to(self.root):
            raise StorageError("Path traversal detected", str(uri))
        return path

    def put(self, uri: StorageURI, data: bytes) -> None:
        """Write data to storage location.

        Args:
            uri: Target storage URI.
            data: Bytes to write.

        Raises:
            StorageError: If write fails or data exceeds size limit.
        """
        # Enforce size limit if configured
        if self._size_limit_bytes is not None and len(data) > self._size_limit_bytes:
            raise StorageError(
                f"Object size {len(data)} bytes exceeds limit {self._size_limit_bytes} bytes",
                str(uri),
            )

        path = self._resolve_path(uri)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            # Track write timestamp for consistency simulation
            if self._consistency_delay_ms > 0:
                self._write_timestamps[str(uri)] = time.time()
        except OSError as e:
            raise StorageError(f"Failed to write: {e}", str(uri)) from e

    def _apply_consistency_delay(self, uri: StorageURI) -> None:
        """Apply consistency delay if reading within window of recent write.

        Only delays if the specific URI was written within the consistency window.
        This simulates GCS eventual consistency behavior.
        """
        if self._consistency_delay_ms <= 0:
            return

        uri_key = str(uri)
        if uri_key in self._write_timestamps:
            write_time = self._write_timestamps[uri_key]
            elapsed_ms = (time.time() - write_time) * 1000
            if elapsed_ms < self._consistency_delay_ms:
                # Still within consistency window - apply remaining delay
                remaining_ms = self._consistency_delay_ms - elapsed_ms
                time.sleep(remaining_ms / 1000.0)
            # Clean up old timestamp
            del self._write_timestamps[uri_key]

    def get(self, uri: StorageURI) -> bytes:
        """Read data from storage location.

        Args:
            uri: Source storage URI.

        Returns:
            File contents as bytes.

        Raises:
            NotFoundError: If the URI does not exist.
            StorageError: If read fails for other reasons.
        """
        # Simulate consistency delay only if this path was recently written
        self._apply_consistency_delay(uri)

        path = self._resolve_path(uri)
        if not path.exists():
            raise NotFoundError(str(uri))
        try:
            return path.read_bytes()
        except OSError as e:
            raise StorageError(f"Failed to read: {e}", str(uri)) from e

    def exists(self, uri: StorageURI) -> bool:
        """Check if storage location exists.

        Args:
            uri: Storage URI to check.

        Returns:
            True if the object exists.
        """
        # Simulate consistency delay only if this path was recently written
        self._apply_consistency_delay(uri)

        return self._resolve_path(uri).exists()

    def list_prefix(self, prefix: StorageURI) -> list[StorageURI]:
        """List all objects under a prefix.

        Args:
            prefix: Storage URI prefix to list under.

        Returns:
            List of StorageURIs for all objects under the prefix,
            sorted lexicographically by path (matching GCS behavior per RCT-GCS-2).
        """
        base_path = self._resolve_path(prefix)
        if not base_path.exists():
            return []

        results = []
        for path in base_path.rglob("*"):
            if path.is_file():
                # Reconstruct URI from path
                relative = path.relative_to(self.root / prefix.bucket)
                results.append(StorageURI(prefix.scheme, prefix.bucket, str(relative)))

        # Sort lexicographically by full URI string (matches GCS behavior)
        results.sort(key=lambda uri: str(uri))
        return results

    def delete(self, uri: StorageURI) -> None:
        """Delete object at storage location.

        Args:
            uri: Storage URI to delete.
        """
        path = self._resolve_path(uri)
        if path.exists():
            path.unlink()

    def get_local_path(self, uri: StorageURI) -> Path | None:
        """Get local filesystem path for a URI.

        For local storage, this returns the actual path.
        For remote storage, this would return None or a cached path.

        Args:
            uri: Storage URI.

        Returns:
            Local filesystem path, or None if not locally available.
        """
        path = self._resolve_path(uri)
        return path if path.exists() else None

    def download_to_file(self, uri: StorageURI, destination: Path) -> bool:
        """Download object to a local file.

        For local storage, this copies the file to the destination.

        Args:
            uri: Source storage URI.
            destination: Local path to write to.

        Returns:
            True if download succeeded, False if object doesn't exist.
        """
        self._apply_consistency_delay(uri)
        path = self._resolve_path(uri)
        if not path.exists():
            return False
        try:
            import shutil

            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            return True
        except OSError:
            return False

    def get_size(self, uri: StorageURI) -> int | None:
        """Get size of object in bytes.

        Args:
            uri: Storage URI to check.

        Returns:
            Size in bytes, or None if object doesn't exist.
        """
        self._apply_consistency_delay(uri)
        path = self._resolve_path(uri)
        if not path.exists():
            return None
        return path.stat().st_size
