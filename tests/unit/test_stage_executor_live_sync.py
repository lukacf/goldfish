"""Unit tests for StageExecutor live metrics sync."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.state_machine.types import StageState


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
    test_db.update_stage_run_status(run_id, StageState.RUNNING)
    # Also set state column (source of truth)
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET state = ? WHERE id = ?", (StageState.RUNNING.value, run_id))
    return run_id


def _create_running_stage_run_local(test_db) -> str:
    test_db.create_workspace_lineage("ws", description="test")
    test_db.create_version("ws", "v1", "tag-v1", "sha123", "manual")
    run_id = "stage-local123"
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
        backend_type="local",
        backend_handle=None,
    )
    test_db.update_stage_run_status(run_id, StageState.RUNNING)
    # Also set state column (source of truth)
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET state = ? WHERE id = ?", (StageState.RUNNING.value, run_id))
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
    executor._get_gcs_client = MagicMock(return_value=(_DummyClient(blob), None))

    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    state = executor._get_metrics_sync_state(run_id)
    state.offset = 0

    gcs_path = "gs://bucket/runs/stage-acde123/logs/metrics.jsonl"
    blob_path = "runs/stage-acde123/logs/metrics.jsonl"
    local_path = Path(tmp_path) / "goldfish_metrics_live" / blob_path.replace("/", "_") / "metrics.jsonl"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text("old\n")

    path, _, _ = executor._sync_metrics_file_from_gcs(gcs_path, state)
    assert path == local_path
    assert local_path.read_bytes() == data


def test_sync_metrics_file_cli_fallback(test_db, test_config, tmp_path, monkeypatch):
    """Fallback CLI download should populate local metrics file when GCS client unavailable."""
    run_id = _create_running_stage_run(test_db)
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    executor._get_gcs_client = MagicMock(return_value=(None, "no creds"))
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    data = b'{"type": "metric", "name": "loss", "value": 0.2, "timestamp": "2024-01-01T00:00:00Z"}\n'
    gcs_path = "gs://bucket/runs/stage-acde123/logs/metrics.jsonl"
    # CLI fallback uses gcs_path.replace("gs://", "").replace("/", "_") for the dir name
    local_path = (
        Path(tmp_path) / "goldfish_metrics_live" / gcs_path.replace("gs://", "").replace("/", "_") / "metrics.jsonl"
    )

    def _fake_cli_download(_gcs: str, destination: Path) -> bool:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return True

    monkeypatch.setattr(executor, "_download_metrics_from_gcs_cli", _fake_cli_download)

    state = executor._get_metrics_sync_state(run_id)
    path, _, _ = executor._sync_metrics_file_from_gcs(gcs_path, state)
    assert path == local_path
    assert path.read_bytes() == data


def test_sync_metrics_if_running_warns_missing_gcs_bucket(test_db, test_config, tmp_path):
    """Missing GCS bucket should surface a warning for live sync."""
    run_id = _create_running_stage_run(test_db)
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"
    config.gcs = None
    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    warnings = executor.sync_metrics_if_running(run_id)
    assert any("gcs.bucket" in warning for warning in warnings)


def test_sync_metrics_if_running_collects_local_metrics(test_db, test_config, tmp_path):
    """Local backend should ingest metrics into DB during live sync."""
    run_id = _create_running_stage_run_local(test_db)
    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    metrics_file = executor.dev_repo / ".goldfish" / "runs" / run_id / "outputs" / ".goldfish" / "metrics.jsonl"
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    metrics_file.write_text('{"type":"metric","name":"loss","value":0.5,"timestamp":"2024-01-01T00:00:00Z"}\n')

    executor.sync_metrics_if_running(run_id)

    assert test_db.count_run_metrics(run_id) == 1
