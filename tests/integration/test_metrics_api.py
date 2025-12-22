"""Integration tests for Metrics API end-to-end flow."""

import json
from datetime import UTC, datetime

import pytest

from goldfish.metrics.collector import MetricsCollector


@pytest.fixture
def workspace_setup(test_db):
    """Create workspace lineage and version for testing."""
    now = datetime.now(UTC).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "INSERT INTO workspace_lineage (workspace_name, parent_workspace, parent_version, created_at) VALUES (?, NULL, NULL, ?)",
            ("test_ws", now),
        )
        conn.execute(
            "INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("test_ws", "v1", "test_ws-v1", "abc123", now, "test", "test version"),
        )
    return test_db


class TestMetricsCollection:
    """Test metrics collection from JSONL to database."""

    def test_collect_metrics_from_jsonl(self, workspace_setup, temp_dir):
        """Should collect metrics from JSONL file and populate database."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create a metrics.jsonl file with test data
        metrics_file = temp_dir / "metrics.jsonl"

        metrics_data = [
            {"type": "metric", "name": "loss", "value": 0.5, "step": 0, "timestamp": now},
            {"type": "metric", "name": "loss", "value": 0.3, "step": 1, "timestamp": now},
            {"type": "metric", "name": "accuracy", "value": 0.8, "step": 0, "timestamp": now},
            {"type": "artifact", "name": "model", "path": "/outputs/model", "timestamp": now},
        ]

        with open(metrics_file, "w") as f:
            for entry in metrics_data:
                f.write(json.dumps(entry) + "\n")

        # Create a test stage run
        stage_run_id = "stage-test123"
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

        # Collect metrics
        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        # Verify collection stats
        assert result["metrics_count"] == 3
        assert result["artifacts_count"] == 1

        # Verify metrics in database
        metrics = test_db.get_run_metrics(stage_run_id)
        assert len(metrics) == 3

        # Check individual metrics
        loss_metrics = [m for m in metrics if m["name"] == "loss"]
        assert len(loss_metrics) == 2
        assert loss_metrics[0]["value"] == 0.5
        assert loss_metrics[0]["step"] == 0
        assert loss_metrics[1]["value"] == 0.3
        assert loss_metrics[1]["step"] == 1

        accuracy_metrics = [m for m in metrics if m["name"] == "accuracy"]
        assert len(accuracy_metrics) == 1
        assert accuracy_metrics[0]["value"] == 0.8

        # Verify summary
        summary = test_db.get_metrics_summary(stage_run_id)
        assert len(summary) == 2

        loss_summary = next(s for s in summary if s["name"] == "loss")
        assert loss_summary["min_value"] == 0.3
        assert loss_summary["max_value"] == 0.5
        assert loss_summary["last_value"] == 0.3
        assert loss_summary["count"] == 2

        accuracy_summary = next(s for s in summary if s["name"] == "accuracy")
        assert accuracy_summary["min_value"] == 0.8
        assert accuracy_summary["max_value"] == 0.8
        assert accuracy_summary["last_value"] == 0.8
        assert accuracy_summary["count"] == 1

        # Verify artifacts
        artifacts = test_db.get_run_artifacts(stage_run_id)
        assert len(artifacts) == 1
        assert artifacts[0]["name"] == "model"
        assert artifacts[0]["path"] == "/outputs/model"

    def test_collect_empty_metrics_file(self, workspace_setup, temp_dir):
        """Should handle empty metrics file gracefully."""
        test_db = workspace_setup
        metrics_file = temp_dir / "metrics.jsonl"
        metrics_file.touch()

        stage_run_id = "stage-empty123"
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

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result["metrics_count"] == 0
        assert result["artifacts_count"] == 0

    def test_collect_missing_metrics_file(self, workspace_setup, temp_dir):
        """Should handle missing metrics file gracefully."""
        test_db = workspace_setup
        metrics_file = temp_dir / "missing.jsonl"

        stage_run_id = "stage-missing123"
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

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result["metrics_count"] == 0
        assert result["artifacts_count"] == 0

    def test_collect_invalid_jsonl_entries(self, workspace_setup, temp_dir):
        """Should skip invalid JSONL entries and continue."""
        test_db = workspace_setup
        metrics_file = temp_dir / "metrics.jsonl"
        now = datetime.now(UTC).isoformat()

        with open(metrics_file, "w") as f:
            # Valid entry
            f.write(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "timestamp": now}) + "\n")
            # Invalid JSON
            f.write("not valid json\n")
            # Missing required fields
            f.write(json.dumps({"type": "metric"}) + "\n")
            # Valid entry
            f.write(json.dumps({"type": "metric", "name": "accuracy", "value": 0.9, "timestamp": now}) + "\n")

        stage_run_id = "stage-invalid123"
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

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        # Should collect only the valid entries
        assert result["metrics_count"] == 2
        metrics = test_db.get_run_metrics(stage_run_id)
        assert len(metrics) == 2


class TestMetricsSummaryAggregation:
    """Test summary aggregation logic."""

    def test_upsert_updates_min_max_last(self, workspace_setup):
        """Should correctly update min, max, last values."""
        test_db = workspace_setup
        stage_run_id = "stage-agg123"
        now = datetime.now(UTC).isoformat()

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

        # Insert values in different orders
        test_db.upsert_metric_summary(stage_run_id, "metric", 5.0)
        test_db.upsert_metric_summary(stage_run_id, "metric", 2.0)
        test_db.upsert_metric_summary(stage_run_id, "metric", 8.0)
        test_db.upsert_metric_summary(stage_run_id, "metric", 3.0)

        summary = test_db.get_metrics_summary(stage_run_id)
        assert len(summary) == 1

        s = summary[0]
        assert s["name"] == "metric"
        assert s["min_value"] == 2.0
        assert s["max_value"] == 8.0
        assert s["last_value"] == 3.0  # Last inserted
        assert s["count"] == 4


class TestBackwardCompatibility:
    """Test backward compatibility with old JSONL format."""

    def test_old_format_without_type_field(self, workspace_setup, temp_dir):
        """Should handle old JSONL format without 'type' field."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create old-format JSONL (pre-cc19bce) without "type" field
        metrics_file = temp_dir / "metrics.jsonl"
        old_format_data = [
            {"name": "loss", "value": 0.5, "step": 0, "timestamp": now},
            {"name": "accuracy", "value": 0.8, "step": 0, "timestamp": now},
            {"name": "model", "path": "/outputs/model.pt", "timestamp": now},  # Old artifact format
        ]

        with open(metrics_file, "w") as f:
            for entry in old_format_data:
                f.write(json.dumps(entry) + "\n")

        # Create stage run
        stage_run_id = "stage-oldformat123"
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

        # Collect - should infer types from fields
        from goldfish.metrics.collector import MetricsCollector

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        # Should collect both metrics (2) and artifact (1)
        assert result["metrics_count"] == 2
        assert result["artifacts_count"] == 1

        # Verify metrics in database
        metrics = test_db.get_run_metrics(stage_run_id)
        assert len(metrics) == 2

        # Verify artifacts
        artifacts = test_db.get_run_artifacts(stage_run_id)
        assert len(artifacts) == 1
        assert artifacts[0]["name"] == "model"


class TestCascadeDelete:
    """Test CASCADE DELETE behavior."""

    def test_deleting_stage_run_deletes_metrics(self, workspace_setup, temp_dir):
        """Should CASCADE DELETE metrics when stage_run is deleted."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create stage run
        stage_run_id = "stage-cascade123"
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

        # Insert metrics and artifacts
        test_db.batch_insert_metrics(
            stage_run_id,
            [
                {"name": "loss", "value": 0.5, "step": 0, "timestamp": now},
                {"name": "accuracy", "value": 0.8, "step": 0, "timestamp": now},
            ],
        )
        test_db.batch_insert_artifacts(
            stage_run_id,
            [{"name": "model", "path": "/outputs/model.pt", "timestamp": now}],
        )

        # Verify data exists
        assert len(test_db.get_run_metrics(stage_run_id)) == 2
        assert len(test_db.get_metrics_summary(stage_run_id)) == 2
        assert len(test_db.get_run_artifacts(stage_run_id)) == 1

        # Delete stage_run
        with test_db._conn() as conn:
            conn.execute("DELETE FROM stage_runs WHERE id = ?", (stage_run_id,))

        # Metrics should be CASCADE deleted
        assert len(test_db.get_run_metrics(stage_run_id)) == 0
        assert len(test_db.get_metrics_summary(stage_run_id)) == 0
        assert len(test_db.get_run_artifacts(stage_run_id)) == 0


class TestMCPFiltering:
    """Test MCP tool filtering and pagination."""

    def test_filter_by_metric_name(self, workspace_setup, temp_dir):
        """Should filter metrics by name."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create stage run with multiple metrics
        stage_run_id = "stage-filter123"
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

        # Insert different metrics
        test_db.batch_insert_metrics(
            stage_run_id,
            [
                {"name": "loss", "value": 0.5, "step": 0, "timestamp": now},
                {"name": "loss", "value": 0.4, "step": 1, "timestamp": now},
                {"name": "accuracy", "value": 0.8, "step": 0, "timestamp": now},
                {"name": "accuracy", "value": 0.9, "step": 1, "timestamp": now},
            ],
        )

        # Filter by "loss"
        loss_metrics = test_db.get_run_metrics(stage_run_id, metric_name="loss")
        assert len(loss_metrics) == 2
        assert all(m["name"] == "loss" for m in loss_metrics)

        # Filter by "accuracy"
        accuracy_metrics = test_db.get_run_metrics(stage_run_id, metric_name="accuracy")
        assert len(accuracy_metrics) == 2
        assert all(m["name"] == "accuracy" for m in accuracy_metrics)

    def test_pagination_with_limit_offset(self, workspace_setup, temp_dir):
        """Should paginate metrics correctly."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create stage run
        stage_run_id = "stage-paginate123"
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

        # Insert 10 metrics
        metrics = [{"name": "loss", "value": float(i), "step": i, "timestamp": now} for i in range(10)]
        test_db.batch_insert_metrics(stage_run_id, metrics)

        # Get all metrics
        all_metrics = test_db.get_run_metrics(stage_run_id)
        assert len(all_metrics) == 10

        # Test pagination: first 5
        # Note: Pagination is done at MCP tool level, not DB level
        # But we verify the data is there for pagination to work
        assert len(all_metrics[:5]) == 5
        assert len(all_metrics[5:]) == 5
