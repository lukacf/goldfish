"""Local Metadata Bus - File-based simulation for development.

Uses a local JSON file to simulate Instance Metadata, allowing for TDD
and local testing of the Overdrive sync logic.
"""

import contextlib
import fcntl
import json
import logging
import os
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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Atomic creation with 600 permissions
            fd = os.open(str(self.path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write("{}")
        except FileExistsError:
            pass

    @contextlib.contextmanager
    def _atomic_update(self) -> Generator[dict, None, None]:
        """Context manager for atomic read-modify-write operations."""
        # Use r+ to read/write without truncating immediately
        with open(self.path, "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                content = f.read()
                data = json.loads(content) if content else {}
                yield data
                f.seek(0)
                f.write(json.dumps(data, indent=2))
                f.truncate()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

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
