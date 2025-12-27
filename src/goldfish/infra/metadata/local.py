"""Local Metadata Bus - File-based simulation for development.

Uses a local JSON file to simulate Instance Metadata, allowing for TDD
and local testing of the Overdrive sync logic.
"""

import json
import logging
from pathlib import Path

from goldfish.infra.metadata.base import MetadataBus, MetadataSignal

logger = logging.getLogger(__name__)


class LocalMetadataBus(MetadataBus):
    """Simulates Instance Metadata using a local JSON file."""

    def __init__(self, metadata_path: Path):
        self.path = metadata_path
        self._ensure_exists()

    def _ensure_exists(self) -> None:
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text("{}")

    def _read(self) -> dict:
        try:
            return json.loads(self.path.read_text())  # type: ignore[no-any-return]
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def _write(self, data: dict) -> None:
        self.path.write_text(json.dumps(data, indent=2))

    def set_signal(self, key: str, signal: MetadataSignal) -> None:
        data = self._read()
        data[f"{key}_signal"] = signal.model_dump(mode="json")
        self._write(data)
        logger.debug(f"LocalMetadata set_signal: {key}={signal.request_id}")

    def get_signal(self, key: str) -> MetadataSignal | None:
        data = self._read()
        sig_data = data.get(f"{key}_signal")
        if not sig_data:
            return None
        return MetadataSignal(**sig_data)

    def clear_signal(self, key: str) -> None:
        data = self._read()
        data.pop(f"{key}_signal", None)
        self._write(data)

    def set_ack(self, key: str, request_id: str) -> None:
        data = self._read()
        data[f"{key}_ack"] = request_id
        self._write(data)
        logger.debug(f"LocalMetadata set_ack: {key}={request_id}")

    def get_ack(self, key: str) -> str | None:
        data = self._read()
        return data.get(f"{key}_ack")
