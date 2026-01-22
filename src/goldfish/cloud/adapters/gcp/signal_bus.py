"""GCP SignalBus implementation.

Re-exports the existing GCPMetadataBus which already implements
the SignalBus protocol (via MetadataBus base).
"""

from goldfish.infra.metadata.gcp import GCPMetadataBus

# GCPMetadataBus already implements all SignalBus protocol methods:
# - set_signal(key, signal, target)
# - get_signal(key, target)
# - clear_signal(key, target)
# - set_ack(key, request_id, target)
# - get_ack(key, target)
#
# We re-export it with a consistent name for the adapter pattern.

GCPSignalBus = GCPMetadataBus

__all__ = ["GCPSignalBus"]
