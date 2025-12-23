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

    def test_flush_error_retains_buffer(self, tmp_path):
        """Flush errors should retain the buffer for retry."""
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

        # Buffer should remain so a later flush can retry
        assert len(writer._metrics_buffer) == 2

        # Cleanup
        metrics_file.chmod(0o644)

    def test_step_consistency_warns_and_skips(self, tmp_path):
        """Mixing step=None and step=int for the same metric should be skipped (no crash)."""
        writer = LocalWriter(outputs_dir=tmp_path)

        writer.log_metric("loss", 0.5)
        # Should not raise; inconsistent metric should be skipped
        writer.log_metric("loss", 0.4, step=1)

        assert len(writer._metrics_buffer) == 1
        assert writer.get_metrics_lost_count() == 1

    def test_metric_name_limit_enforced(self, tmp_path, monkeypatch):
        """Too many unique metric names should raise an error."""
        monkeypatch.setenv("GOLDFISH_METRICS_MAX_NAMES", "2")
        writer = LocalWriter(outputs_dir=tmp_path)

        writer.log_metric("loss", 0.5)
        writer.log_metric("accuracy", 0.9)

        with pytest.raises(Exception) as exc_info:
            writer.log_metric("precision", 0.8)

        assert "metric names" in str(exc_info.value).lower()

    def test_outputs_dir_requires_absolute_path(self):
        """Relative outputs_dir should be rejected."""
        with pytest.raises(Exception) as exc_info:
            LocalWriter(outputs_dir="relative/path")
        assert "absolute" in str(exc_info.value).lower()

    def test_outputs_dir_requires_directory(self, tmp_path):
        """Non-directory outputs_dir should be rejected."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("data")

        with pytest.raises(Exception) as exc_info:
            LocalWriter(outputs_dir=file_path)
        assert "directory" in str(exc_info.value).lower()

    def test_outputs_dir_mkdir_error(self, tmp_path, monkeypatch):
        """Initialization should raise if outputs dir cannot be created."""

        def raise_os_error(*args, **kwargs):
            raise OSError("no permission")

        monkeypatch.setattr("goldfish.metrics.writer.Path.mkdir", raise_os_error)

        with pytest.raises(Exception) as exc_info:
            LocalWriter(outputs_dir=tmp_path)

        assert "outputs" in str(exc_info.value).lower()
