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
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text("{}")
            self.path.chmod(0o600)

    @contextlib.contextmanager
    def _atomic_update(self) -> Generator[dict, None, None]:
        """Context manager for atomic read-modify-write operations."""
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
        """Read data (no lock required for simple read, but safer with shared lock)."""
        # For simplicity in local dev, we skip shared read locks,
        # focusing on fixing the critical write race condition.
        try:
            return json.loads(self.path.read_text())  # type: ignore[no-any-return]
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        with self._atomic_update() as data:
            data[f"{key}_signal"] = signal.model_dump(mode="json")
        logger.debug(f"LocalMetadata set_signal: {key}={signal.request_id} target={target}")

    def get_signal(self, key: str) -> MetadataSignal | None:
        data = self._read()
        sig_data = data.get(f"{key}_signal")
        if not sig_data:
            return None
        return MetadataSignal(**sig_data)

    def clear_signal(self, key: str) -> None:
        with self._atomic_update() as data:
            data.pop(f"{key}_signal", None)

    def set_ack(self, key: str, request_id: str) -> None:
        with self._atomic_update() as data:
            data[f"{key}_ack"] = request_id
        logger.debug(f"LocalMetadata set_ack: {key}={request_id}")

    def get_ack(self, key: str) -> str | None:
        data = self._read()
        return data.get(f"{key}_ack")
