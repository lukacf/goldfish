"""Unit tests for StageExecutor live metrics sync."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from goldfish.cloud.contracts import BackendCapabilities, StorageURI
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.state_machine.types import StageState


def _make_mock_run_backend():
    """Create a mock RunBackend with local-like capabilities (no Docker calls)."""
    mock_backend = MagicMock()
    mock_backend.capabilities = BackendCapabilities(
        ack_timeout_seconds=1.0,
        has_launch_delay=False,
        timeout_becomes_pending=False,
        logs_unavailable_message="Logs not available",
        zone_resolution_method="config",
    )
    return mock_backend


def _create_mock_storage(size: int = 100, data: bytes = b"test data"):
    """Create a mock storage adapter."""
    mock_storage = MagicMock()
    mock_storage.put = MagicMock()
    mock_storage.get = MagicMock(return_value=data)
    mock_storage.exists = MagicMock(return_value=True)
    mock_storage.delete = MagicMock()
    mock_storage.list_prefix = MagicMock(return_value=[])
    mock_storage.get_size = MagicMock(return_value=size)

    def _download_to_file(uri, local_path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        return True

    mock_storage.download_to_file = MagicMock(side_effect=_download_to_file)
    return mock_storage


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
        run_backend=_make_mock_run_backend(),
    )

    state = executor._get_metrics_sync_state(run_id)
    state.sync_lock.acquire()
    try:
        executor._sync_metrics_file_from_storage_uri = MagicMock()
        executor.sync_metrics_if_running(run_id)
        executor._sync_metrics_file_from_storage_uri.assert_not_called()
    finally:
        state.sync_lock.release()


def test_sync_metrics_file_resets_tempfile_on_zero_offset(test_db, test_config, tmp_path, monkeypatch):
    """Zero-offset sync should reset any stale temp file before appending."""
    run_id = _create_running_stage_run(test_db)
    mock_storage = MagicMock()
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        storage=mock_storage,
        run_backend=_make_mock_run_backend(),
    )

    data = b'{"type": "metric", "name": "loss", "value": 0.1, "timestamp": "2024-01-01T00:00:00Z"}\n'

    # Configure mock storage behavior
    mock_storage.get_size = MagicMock(return_value=len(data))

    def _download_to_file(uri, local_path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        return True

    mock_storage.download_to_file = MagicMock(side_effect=_download_to_file)

    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    state = executor._get_metrics_sync_state(run_id)
    state.offset = 0

    uri = StorageURI("gs", "bucket", "runs/stage-acde123/logs/metrics.jsonl")
    # Create a stale temp file with old data
    safe_name = f"{uri.bucket}_{uri.path}".replace("/", "_")
    local_path = Path(tmp_path) / "goldfish_metrics_live" / safe_name / "metrics.jsonl"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text("old\n")

    path, _, _ = executor._sync_metrics_file_from_storage_uri(uri, state)
    assert path == local_path
    assert local_path.read_bytes() == data


def test_sync_metrics_file_storage_error_returns_warning(test_db, test_config, tmp_path, monkeypatch):
    """Storage errors should return warning instead of crashing."""
    run_id = _create_running_stage_run(test_db)
    mock_storage = MagicMock()
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        storage=mock_storage,
        run_backend=_make_mock_run_backend(),
    )

    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    # Mock storage that fails
    mock_storage.get_size = MagicMock(side_effect=Exception("Storage unavailable"))

    state = executor._get_metrics_sync_state(run_id)
    uri = StorageURI("gs", "bucket", "runs/stage-acde123/logs/metrics.jsonl")

    path, offset, warning = executor._sync_metrics_file_from_storage_uri(uri, state)
    # Should return warning, not crash
    assert warning is not None
    assert "Storage unavailable" in warning


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
        run_backend=_make_mock_run_backend(),
    )

    warnings = executor.sync_metrics_if_running(run_id)
    # Should warn about missing GCS bucket
    assert any("bucket" in w.lower() for w in warnings)


def test_sync_metrics_local_runs_skipped(test_db, test_config, tmp_path):
    """Local backend runs should skip GCS-based live sync (local logs are direct)."""
    run_id = _create_running_stage_run_local(test_db)
    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=_make_mock_run_backend(),
    )

    warnings = executor.sync_metrics_if_running(run_id)
    # Should complete without attempting GCS sync (no warnings)
    assert warnings == []
