"""Local metadata syncer for Overdrive parity in non-GCE environments."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus

if TYPE_CHECKING:
    from goldfish.jobs.stage_executor import StageExecutor


class LocalMetadataSyncer:
    """Background poller that processes LocalMetadataBus sync signals."""

    def __init__(
        self,
        bus: LocalMetadataBus,
        stage_executor: StageExecutor,
        poll_interval: float = 1.0,
    ) -> None:
        self._bus = bus
        self._stage_executor = stage_executor
        self._poll_interval = max(0.1, poll_interval)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the background poller."""
        self._thread.start()

    def stop(self) -> None:
        """Stop the background poller."""
        self._stop.set()
        self._thread.join(timeout=2.0)

    def _run(self) -> None:
        last_ack: str | None = None
        while not self._stop.is_set():
            signal = self._bus.get_signal("goldfish")
            if isinstance(signal, MetadataSignal) and signal.command == "sync":
                req_id = signal.request_id
                if req_id and req_id != last_ack:
                    run_id = signal.payload.get("run_id") if isinstance(signal.payload, dict) else None
                    if isinstance(run_id, str):
                        try:
                            self._stage_executor.sync_metrics_if_running(run_id)
                            self._stage_executor.sync_svs_if_running(run_id)
                        except Exception:
                            pass
                    self._bus.set_ack("goldfish", req_id)
                    last_ack = req_id
            time.sleep(self._poll_interval)
