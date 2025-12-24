"""Unit tests for incremental metrics collection."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from goldfish.metrics.collector import MetricsCollector


def _write_lines(path, lines: list[dict]) -> None:
    with open(path, "a") as f:
        for entry in lines:
            f.write(json.dumps(entry) + "\n")


def test_collect_from_file_incremental_reads_only_new_lines(test_db, temp_dir):
    """Incremental collection should only ingest newly appended lines."""
    stage_run_id = "stage-live-collector"
    now = datetime.now(UTC)
    t1 = now.isoformat()
    t2 = (now - timedelta(seconds=5)).isoformat()

    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version(
        workspace_name="test_ws",
        version="v1",
        git_tag="v1",
        git_sha="deadbeef",
        created_by="manual",
    )
    test_db.create_stage_run(
        stage_run_id=stage_run_id,
        workspace_name="test_ws",
        version="v1",
        stage_name="train",
        pipeline_run_id=None,
        pipeline_name=None,
        config={},
        inputs={},
        profile=None,
        hints=None,
        backend_type="local",
        backend_handle="container-123",
    )

    metrics_file = temp_dir / "metrics.jsonl"

    collector = MetricsCollector(test_db)

    # First batch
    _write_lines(
        metrics_file,
        [
            {"type": "metric", "name": "loss", "value": 0.5, "step": 0, "timestamp": t1},
            {"type": "metric", "name": "accuracy", "value": 0.8, "step": 0, "timestamp": t1},
        ],
    )

    result1, offset1 = collector.collect_from_file_incremental(stage_run_id, metrics_file, start_offset=0)
    assert result1.metrics_count == 2
    assert offset1 > 0

    # Second batch with an older timestamp for loss (should NOT change last_value)
    _write_lines(
        metrics_file,
        [
            {"type": "metric", "name": "loss", "value": 0.6, "step": 1, "timestamp": t2},
            {"type": "metric", "name": "accuracy", "value": 0.85, "step": 1, "timestamp": t2},
        ],
    )

    result2, offset2 = collector.collect_from_file_incremental(stage_run_id, metrics_file, start_offset=offset1)
    assert result2.metrics_count == 2
    assert offset2 > offset1

    summary = test_db.get_metrics_summary(stage_run_id)
    loss_summary = next(s for s in summary if s["name"] == "loss")
    assert loss_summary["count"] == 2
    assert loss_summary["last_value"] == 0.5
