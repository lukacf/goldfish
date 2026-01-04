"""Unit tests for LocalMetadataSyncer."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus
from goldfish.infra.metadata.local_syncer import LocalMetadataSyncer


def test_local_metadata_syncer_acknowledges_sync(tmp_path) -> None:
    """Syncer should ack new sync signals and trigger metric sync."""
    bus = LocalMetadataBus(tmp_path / "metadata.json")
    stage_executor = MagicMock()

    syncer = LocalMetadataSyncer(bus=bus, stage_executor=stage_executor, poll_interval=0.01)
    syncer.start()

    try:
        sig = MetadataSignal(command="sync", request_id="req-123", payload={"run_id": "stage-abc"})
        bus.set_signal("goldfish", sig)

        for _ in range(100):
            if bus.get_ack("goldfish") == "req-123":
                break
            time.sleep(0.01)

        assert bus.get_ack("goldfish") == "req-123"
        stage_executor.sync_metrics_if_running.assert_called_once_with("stage-abc")
    finally:
        syncer.stop()
