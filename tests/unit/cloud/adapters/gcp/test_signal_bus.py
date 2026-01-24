"""Unit tests for GCPSignalBus adapter.

Tests that GCPSignalBus is correctly re-exported from GCPMetadataBus.
The detailed tests for GCPMetadataBus are in test_gcp_metadata_bus.py.
"""

from __future__ import annotations


class TestGCPSignalBusExport:
    """Tests for GCPSignalBus re-export."""

    def test_gcp_signal_bus_is_gcp_metadata_bus(self):
        """GCPSignalBus is the same class as GCPMetadataBus."""
        from goldfish.cloud.adapters.gcp.signal_bus import GCPSignalBus
        from goldfish.infra.metadata.gcp import GCPMetadataBus

        assert GCPSignalBus is GCPMetadataBus

    def test_gcp_signal_bus_can_be_imported_from_gcp_adapters(self):
        """GCPSignalBus can be imported from the GCP adapters package."""
        from goldfish.cloud.adapters.gcp import GCPSignalBus

        assert GCPSignalBus is not None

    def test_gcp_signal_bus_in_all_exports(self):
        """GCPSignalBus is in __all__ exports."""
        from goldfish.cloud.adapters.gcp import signal_bus

        assert "GCPSignalBus" in signal_bus.__all__
