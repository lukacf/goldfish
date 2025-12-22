"""Unit tests for LocalWriter (JSONL metrics writer)."""

import json

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
        writer.log_metric("loss", 0.5, timestamp=1234567890.0)
        writer.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["timestamp"] == 1234567890.0

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
