"""Unit tests for MetadataSignalBus."""

from pathlib import Path

from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus


def test_local_metadata_bus_signaling(tmp_path: Path):
    """Test that we can send and acknowledge signals locally."""
    metadata_file = tmp_path / "metadata.json"
    bus = LocalMetadataBus(metadata_file)

    # 1. Server sends a signal
    sig = MetadataSignal(command="sync", request_id="req-123", payload={"mode": "overdrive"})
    bus.set_signal("goldfish", sig)

    # 2. Container (simulated) reads the signal
    read_sig = bus.get_signal("goldfish")
    assert read_sig is not None
    assert read_sig.command == "sync"
    assert read_sig.request_id == "req-123"
    assert read_sig.payload["mode"] == "overdrive"

    # 3. Container acknowledges
    bus.set_ack("goldfish", "req-123")

    # 4. Server checks for ACK
    assert bus.get_ack("goldfish") == "req-123"

    # 5. Clear signal
    bus.clear_signal("goldfish")
    assert bus.get_signal("goldfish") is None
    # Ack should remain until overwritten
    assert bus.get_ack("goldfish") == "req-123"


def test_local_metadata_persistence(tmp_path: Path):
    """Test that metadata survives across bus instances (file-based)."""
    metadata_file = tmp_path / "metadata.json"
    bus1 = LocalMetadataBus(metadata_file)
    bus1.set_ack("test", "id-456")

    bus2 = LocalMetadataBus(metadata_file)
    assert bus2.get_ack("test") == "id-456"
