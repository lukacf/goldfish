"""Local Metadata Bus - File-based simulation for development.

Uses a local JSON file to simulate Instance Metadata, allowing for TDD
and local testing of the Overdrive sync logic.
"""

import contextlib
import fcntl
import json
import logging
from collections.abc import Generator
from pathlib import Path

from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)


class LocalMetadataBus(MetadataBus):
    """Simulates Instance Metadata using a local JSON file with file locking."""

    def __init__(self, metadata_path: Path):
        self.path = metadata_path
        self._ensure_exists()

    def _ensure_exists(self) -> None:
        """Create metadata file if it doesn't exist, with proper locking.

        Uses O_CREAT to atomically create or open the file, then initializes
        with empty JSON under exclusive lock to prevent races with other
        processes that may be trying to read/write simultaneously.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Use a+ mode to create if not exists, open for read+write if exists
        # The lock ensures only one process initializes the empty file
        with open(self.path, "a+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0)
                content = f.read()
                if not content:
                    # File is empty, initialize with empty JSON
                    f.seek(0)
                    f.write("{}")
                    f.truncate()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    @contextlib.contextmanager
    def _atomic_update(self) -> Generator[dict, None, None]:
        """Context manager for atomic read-modify-write operations."""
        # Use r+ to read/write without truncating immediately
        try:
            with open(self.path, "r+") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    content = f.read()
                    try:
                        data = json.loads(content) if content else {}
                    except json.JSONDecodeError:
                        # File corrupted, reset to empty dict
                        logger.warning("Metadata file corrupted, resetting: %s", self.path)
                        data = {}
                    yield data
                    f.seek(0)
                    f.write(json.dumps(data, indent=2))
                    f.truncate()
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except FileNotFoundError:
            # Re-initialize if deleted during run
            self._ensure_exists()
            with self._atomic_update() as data:
                yield data

    def _read(self) -> dict:
        """Read data with shared lock to prevent torn reads."""
        try:
            with open(self.path) as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    content = f.read()
                    return json.loads(content) if content else {}  # type: ignore[no-any-return]
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        with self._atomic_update() as data:
            data[f"{key}_signal"] = signal.model_dump(mode="json")
        logger.debug(f"LocalMetadata set_signal: {key}={signal.request_id} target={target}")

    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None:
        data = self._read()
        sig_data = data.get(f"{key}_signal")
        if not sig_data or not isinstance(sig_data, dict):
            return None
        return MetadataSignal(**sig_data)

    def clear_signal(self, key: str, target: str | None = None) -> None:
        with self._atomic_update() as data:
            data.pop(f"{key}_signal", None)

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        with self._atomic_update() as data:
            data[f"{key}_ack"] = request_id
        logger.debug(f"LocalMetadata set_ack: {key}={request_id}")

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        data = self._read()
        return data.get(f"{key}_ack")
