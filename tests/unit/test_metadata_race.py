"""Stress tests for LocalMetadataBus concurrency."""

import multiprocessing
from pathlib import Path

from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.local import LocalMetadataBus


def worker_task(metadata_path, worker_id, iterations):
    """Increment a counter in metadata multiple times."""
    bus = LocalMetadataBus(Path(metadata_path))
    for i in range(iterations):
        # We use atomic_update internally in set_signal/get_signal
        # but here we'll test overlapping updates by using a unique key per worker
        # and also a shared key to test locks.

        # 1. Update shared counter
        with bus._atomic_update() as data:
            count = data.get("shared_counter", 0)
            data["shared_counter"] = count + 1

        # 2. Set worker-specific signal
        bus.set_signal(f"worker_{worker_id}", MetadataSignal(command="test", request_id=f"req_{i}"))


def test_metadata_concurrency(tmp_path):
    """Test LocalMetadataBus with multiple processes to verify locking."""
    metadata_path = tmp_path / "metadata.json"
    num_workers = 10
    iterations = 50

    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_task, args=(str(metadata_path), i, iterations))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Verify final state
    bus = LocalMetadataBus(metadata_path)
    data = bus._read()

    # Shared counter should be exactly num_workers * iterations
    assert data.get("shared_counter") == num_workers * iterations

    # Each worker should have its last signal recorded
    for i in range(num_workers):
        assert f"worker_{i}_signal" in data
        assert data[f"worker_{i}_signal"]["request_id"] == f"req_{iterations-1}"
