"""Unit tests for StageExecutor live metrics sync."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunStatus


class _DummyBlob:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.size = len(data)

    def reload(self) -> None:
        return None

    def download_as_bytes(self, start: int = 0) -> bytes:
        return self._data[start:]


class _DummyBucket:
    def __init__(self, blob: _DummyBlob) -> None:
        self._blob = blob

    def blob(self, _path: str) -> _DummyBlob:
        return self._blob


class _DummyClient:
    def __init__(self, blob: _DummyBlob) -> None:
        self._blob = blob

    def bucket(self, _name: str) -> _DummyBucket:
        return _DummyBucket(self._blob)


def _create_running_stage_run(test_db) -> str:
    test_db.create_workspace_lineage("ws", description="test")
    test_db.create_version("ws", "v1", "tag-v1", "sha123", "manual")
    run_id = "stage-acde123"
    test_db.create_stage_run(
        stage_run_id=run_id,
        workspace_name="ws",
        version="v1",
        stage_name="train",
        pipeline_run_id=None,
        pipeline_name=None,
        config={},
        inputs={},
        profile=None,
        hints=None,
        backend_type="gce",
        backend_handle="instance-123",
    )
    test_db.update_stage_run_status(run_id, StageRunStatus.RUNNING)
    return run_id


def test_sync_metrics_if_running_skips_when_locked(test_db, test_config, tmp_path):
    """Per-run lock should prevent concurrent syncs."""
    run_id = _create_running_stage_run(test_db)
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    state = executor._get_metrics_sync_state(run_id)
    state.sync_lock.acquire()
    try:
        executor._sync_metrics_file_from_gcs = MagicMock()
        executor.sync_metrics_if_running(run_id)
        executor._sync_metrics_file_from_gcs.assert_not_called()
    finally:
        state.sync_lock.release()


def test_sync_metrics_file_resets_tempfile_on_zero_offset(test_db, test_config, tmp_path, monkeypatch):
    """Zero-offset sync should reset any stale temp file before appending."""
    run_id = _create_running_stage_run(test_db)
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    data = b'{"type": "metric", "name": "loss", "value": 0.1, "timestamp": "2024-01-01T00:00:00Z"}\n'
    blob = _DummyBlob(data)
    executor._get_gcs_client = MagicMock(return_value=_DummyClient(blob))

    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    state = executor._get_metrics_sync_state(run_id)
    state.offset = 0

    gcs_path = "gs://bucket/runs/stage-acde123/logs/metrics.jsonl"
    blob_path = "runs/stage-acde123/logs/metrics.jsonl"
    local_path = Path(tmp_path) / "goldfish_metrics_live" / blob_path.replace("/", "_") / "metrics.jsonl"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text("old\n")

    path, _ = executor._sync_metrics_file_from_gcs(gcs_path, state)
    assert path == local_path
    assert local_path.read_bytes() == data
