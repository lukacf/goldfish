"""Unit tests for LocalWriter (JSONL metrics writer)."""

import json

import pytest

from goldfish.metrics.writer import LocalWriter


class TestLocalWriter:
    """Tests for LocalWriter."""

    def test_init_creates_directory(self, tmp_path):
        """Test that LocalWriter creates the metrics directory."""
        metrics_dir = tmp_path / ".goldfish"
        writer = LocalWriter(outputs_dir=tmp_path)
        assert metrics_dir.exists()

    def test_log_metric_writes_jsonl(self, tmp_path):
        """Test logging a single metric writes to JSONL."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5, step=10)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

        with open(metrics_file) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["type"] == "metric"
            assert data["name"] == "loss"
            assert data["value"] == 0.5
            assert data["step"] == 10
            assert "timestamp" in data

    def test_log_metrics_batch(self, tmp_path):
        """Test logging multiple metrics at once."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 2

            data1 = json.loads(lines[0])
            data2 = json.loads(lines[1])

            assert data1["step"] == 10
            assert data2["step"] == 10

    def test_log_artifact_records_path(self, tmp_path):
        """Test that log_artifact records the artifact path."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_artifact("model", "model.pt")
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

        with open(metrics_file) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["type"] == "artifact"
            assert data["name"] == "model"
            assert data["path"] == "model.pt"
            assert "timestamp" in data

    def test_multiple_metrics(self, tmp_path):
        """Test logging multiple metrics sequentially."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5, step=1)
        writer.log_metric("loss", 0.4, step=2)
        writer.log_metric("loss", 0.3, step=3)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 3

            for i, line in enumerate(lines, start=1):
                data = json.loads(line)
                assert data["name"] == "loss"
                assert data["step"] == i

    def test_metric_without_step(self, tmp_path):
        """Test logging a metric without a step."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("accuracy", 0.95)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["step"] is None

    def test_custom_timestamp(self, tmp_path):
        """Test logging with a custom timestamp."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5, timestamp=1700000000.0)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            # Float timestamps are converted to ISO 8601 format
            assert data["timestamp"] == "2023-11-14T22:13:20+00:00"

    def test_flush_creates_file(self, tmp_path):
        """Test that flush creates the file."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5)

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        # File might not exist yet
        writer.flush()
        # After flush, file should exist
        assert metrics_file.exists()

    def test_append_mode(self, tmp_path):
        """Test that metrics are appended, not overwritten."""
        writer = LocalWriter(outputs_dir=tmp_path)
        writer.log_metric("loss", 0.5)
        writer.flush()

        writer.log_metric("accuracy", 0.9)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 2

    def test_auto_flush_at_threshold(self, tmp_path):
        """Test that auto-flush triggers at 100 metrics."""
        writer = LocalWriter(outputs_dir=tmp_path)

        # Log 99 metrics - should not flush
        for i in range(99):
            writer.log_metric("loss", float(i))

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert not metrics_file.exists()  # Not flushed yet

        # 100th metric should trigger auto-flush
        writer.log_metric("loss", 99.0)

        assert metrics_file.exists()
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 100

    def test_flush_error_clears_buffer(self, tmp_path):
        """Test that buffer is cleared even on flush error."""
        from goldfish.metrics.writer import MetricsFlushError

        writer = LocalWriter(outputs_dir=tmp_path)

        # Log some metrics
        writer.log_metric("loss", 0.5)
        writer.log_metric("accuracy", 0.9)

        # Make metrics file read-only to cause write error
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        metrics_file.touch()
        metrics_file.chmod(0o444)

        # Flush should fail and raise
        with pytest.raises(MetricsFlushError):
            writer.flush()

        # Buffer should be cleared to prevent infinite loop
        assert len(writer._metrics_buffer) == 0

        # Cleanup
        metrics_file.chmod(0o644)
