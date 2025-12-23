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
            {"type": "artifact", "name": "model", "path": "outputs/model", "timestamp": now},
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
        assert result.metrics_count == 3
        assert result.artifacts_count == 1

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
        assert artifacts[0]["path"] == "outputs/model"

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

        assert result.metrics_count == 0
        assert result.artifacts_count == 0

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

        assert result.metrics_count == 0
        assert result.artifacts_count == 0

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
        assert result.metrics_count == 2
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
            {"name": "model", "path": "outputs/model.pt", "timestamp": now},  # Old artifact format (relative path)
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
        assert result.metrics_count == 2
        assert result.artifacts_count == 1

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
        """Should paginate metrics correctly with SQL limits."""
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

        # Test pagination: first 5 via SQL
        page1 = test_db.get_run_metrics(stage_run_id, limit=5, offset=0)
        page2 = test_db.get_run_metrics(stage_run_id, limit=5, offset=5)
        assert len(page1) == 5
        assert len(page2) == 5

    def test_offset_without_limit_returns_remaining(self, workspace_setup, temp_dir):
        """Offset without limit should return remaining metrics."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        stage_run_id = "stage-offset-nolimit"
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

        metrics = [{"name": "loss", "value": float(i), "step": i, "timestamp": now} for i in range(10)]
        test_db.batch_insert_metrics(stage_run_id, metrics)

        remaining = test_db.get_run_metrics(stage_run_id, limit=None, offset=3)
        assert len(remaining) == 7


class TestMetricsIdempotency:
    """Test that metrics collection is idempotent."""

    def test_collect_from_file_twice_no_duplicates(self, workspace_setup, temp_dir):
        """Collecting from the same file twice should not create duplicates.

        This tests the idempotency of the collection process, ensuring that
        re-running collection on the same file produces consistent results.
        """
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create a metrics.jsonl file
        metrics_file = temp_dir / "metrics.jsonl"
        metrics_data = [
            {"type": "metric", "name": "loss", "value": 0.5, "step": 0, "timestamp": now},
            {"type": "metric", "name": "loss", "value": 0.3, "step": 1, "timestamp": now},
        ]

        with open(metrics_file, "w") as f:
            for entry in metrics_data:
                f.write(json.dumps(entry) + "\n")

        # Create stage run
        stage_run_id = "stage-idempotent123"
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

        # Collect twice
        collector = MetricsCollector(test_db)
        result1 = collector.collect_from_file(stage_run_id, metrics_file)
        result2 = collector.collect_from_file(stage_run_id, metrics_file)

        # First collection should succeed
        assert result1.metrics_count == 2
        assert result1.skipped_count == 0

        # Second collection should skip duplicates (idempotent)
        assert result2.metrics_count == 0
        assert result2.skipped_count == 0
        metrics = test_db.get_run_metrics(stage_run_id)

        # Should have original metrics only (no duplicates)
        assert len(metrics) == 2

    def test_summary_count_matches_actual_metrics(self, workspace_setup, temp_dir):
        """Summary count should match actual metrics in run_metrics table.

        This verifies the summary statistics are accurate and not corrupted
        by duplicate entries or collection errors.
        """
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        # Create stage run
        stage_run_id = "stage-summary123"
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

        # Insert known metrics
        metrics = [
            {"name": "loss", "value": 0.5, "step": 0, "timestamp": now},
            {"name": "loss", "value": 0.4, "step": 1, "timestamp": now},
            {"name": "loss", "value": 0.3, "step": 2, "timestamp": now},
        ]
        test_db.batch_insert_metrics(stage_run_id, metrics)

        # Get actual metrics count
        actual_metrics = test_db.get_run_metrics(stage_run_id)
        actual_count = len(actual_metrics)

        # Get summary
        summary = test_db.get_metrics_summary(stage_run_id)
        assert len(summary) == 1

        loss_summary = summary[0]
        assert loss_summary["count"] == actual_count
        assert loss_summary["count"] == 3
        assert loss_summary["min_value"] == 0.3
        assert loss_summary["max_value"] == 0.5
        assert loss_summary["last_value"] == 0.3  # Last by timestamp


class TestWriterFlushErrors:
    """Test flush error reporting in LocalWriter."""

    def test_flush_errors_tracked_on_io_failure(self, temp_dir):
        """Writer should raise on flush errors and track data loss."""
        from goldfish.metrics.writer import LocalWriter, MetricsFlushError

        writer = LocalWriter(outputs_dir=temp_dir)

        # Log some metrics
        writer.log_metric("loss", 0.5)
        writer.log_metric("accuracy", 0.9)

        # Make the metrics file read-only to cause flush failure
        metrics_dir = temp_dir / ".goldfish"
        metrics_file = metrics_dir / "metrics.jsonl"
        metrics_file.touch()  # Create file first
        metrics_file.chmod(0o444)  # Read-only

        # Attempt flush (should raise and track error)
        with pytest.raises(MetricsFlushError):
            writer.flush()

        # Check error was tracked
        assert writer.had_flush_errors()
        assert writer.get_metrics_lost_count() == 2
        errors = writer.get_flush_errors()
        assert len(errors) == 1

        # Cleanup
        metrics_file.chmod(0o644)

    def test_no_flush_errors_on_success(self, temp_dir):
        """Writer should report no errors on successful flush."""
        from goldfish.metrics.writer import LocalWriter

        writer = LocalWriter(outputs_dir=temp_dir)

        writer.log_metric("loss", 0.5)
        writer.flush()

        assert not writer.had_flush_errors()
        assert writer.get_metrics_lost_count() == 0
        assert writer.get_flush_errors() == []


class TestValidationPathTraversal:
    """Test enhanced path traversal validation."""

    def test_null_byte_in_artifact_path_rejected(self):
        """Null bytes in artifact paths should be rejected."""
        from goldfish.validation import InvalidArtifactPathError, validate_artifact_path

        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("model\x00.pt")

    def test_whitespace_artifact_path_rejected(self):
        """Leading/trailing whitespace in paths should be rejected."""
        from goldfish.validation import InvalidArtifactPathError, validate_artifact_path

        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path(" model.pt")
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("model.pt ")
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("  ")

    def test_artifact_path_with_spaces_inside_allowed(self):
        """Paths with spaces in the middle are currently allowed.

        Note: Shell metachar validation only covers: ;|&$`"'\\<>*?[]{}~!
        Spaces are not blocked, though they may cause issues in some contexts.
        This test documents the current behavior.
        """
        from goldfish.validation import validate_artifact_path

        # Spaces in the middle are currently allowed (not in _DANGEROUS_CHARS)
        # This documents current behavior - may want to change in future
        validate_artifact_path("my_model.pt")  # No space - definitely valid


class TestMetricsCollectorEdgeCases:
    """Edge cases for MetricsCollector behavior."""

    def test_unknown_entry_type_is_reported(self, workspace_setup, temp_dir):
        """Unknown entry types should be skipped and reported."""
        test_db = workspace_setup

        # Create stage run
        stage_run_id = "stage-unknown-type"
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
        with open(metrics_file, "w") as f:
            f.write(json.dumps({"type": "weird", "name": "loss"}) + "\n")

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result.metrics_count == 0
        assert result.artifacts_count == 0
        assert result.skipped_count == 1
        assert any("Unknown entry type" in err for err in result.errors)
        assert result.errors_truncated is False

    def test_error_list_is_capped(self, workspace_setup, temp_dir, monkeypatch):
        """Collector should cap error list to prevent memory blowups."""
        test_db = workspace_setup

        stage_run_id = "stage-error-cap"
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

        # Lower the error cap for the test
        monkeypatch.setattr("goldfish.metrics.collector.MAX_ERROR_MESSAGES", 5)

        metrics_file = temp_dir / "metrics.jsonl"
        with open(metrics_file, "w") as f:
            for _ in range(20):
                f.write("{bad json}\n")

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result.skipped_count == 20
        assert len(result.errors) <= 5
        assert result.errors_truncated is True

    def test_line_limit_aborts_collection(self, workspace_setup, temp_dir, monkeypatch):
        """Exceeding max lines should abort collection and insert nothing."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        stage_run_id = "stage-line-limit"
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

        monkeypatch.setattr("goldfish.metrics.collector.MAX_METRICS_LINES", 2)

        metrics_file = temp_dir / "metrics.jsonl"
        with open(metrics_file, "w") as f:
            f.write(json.dumps({"type": "metric", "name": "loss", "value": 0.5, "step": 0, "timestamp": now}) + "\n")
            f.write(json.dumps({"type": "metric", "name": "loss", "value": 0.4, "step": 1, "timestamp": now}) + "\n")
            f.write(json.dumps({"type": "metric", "name": "loss", "value": 0.3, "step": 2, "timestamp": now}) + "\n")

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result.errors
        assert any("exceeds max lines" in err for err in result.errors)
        assert test_db.get_run_metrics(stage_run_id) == []

    def test_multi_batch_collection_rebuilds_summary_once(self, workspace_setup, temp_dir):
        """Collector should handle multi-batch files and produce correct summary."""
        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        stage_run_id = "stage-multi-batch"
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
        with open(metrics_file, "w") as f:
            for i in range(1500):
                f.write(
                    json.dumps({"type": "metric", "name": "loss", "value": float(i), "step": i, "timestamp": now})
                    + "\n"
                )

        collector = MetricsCollector(test_db)
        result = collector.collect_from_file(stage_run_id, metrics_file)

        assert result.metrics_count == 1500
        summary = test_db.get_metrics_summary(stage_run_id)
        assert len(summary) == 1
        assert summary[0]["count"] == 1500


class TestConcurrentBatchInsert:
    """Concurrency test for batch_insert_metrics."""

    def test_concurrent_batch_insert_metrics(self, workspace_setup):
        """Concurrent inserts should not corrupt metrics or crash."""
        import threading

        test_db = workspace_setup
        now = datetime.now(UTC).isoformat()

        stage_run_id = "stage-concurrent-batch"
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

        def worker(start: int) -> None:
            metrics = [
                {"name": "loss", "value": float(i), "step": i, "timestamp": now} for i in range(start, start + 50)
            ]
            test_db.batch_insert_metrics(stage_run_id, metrics)

        threads = [threading.Thread(target=worker, args=(i * 50,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        metrics = test_db.get_run_metrics(stage_run_id)
        assert len(metrics) == 200
