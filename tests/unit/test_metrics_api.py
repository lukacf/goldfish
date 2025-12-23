"""Unit tests for the public metrics API."""

import json
from unittest.mock import patch

import pytest

from goldfish.metrics import finish, log_artifact, log_metric, log_metrics


@pytest.fixture(autouse=True)
def reset_logger():
    """Reset global logger before each test."""
    from goldfish import metrics

    metrics._reset_global_logger()
    yield
    metrics._reset_global_logger()


class TestPublicMetricsAPI:
    """Tests for the public metrics API functions."""

    def test_log_metric_basic(self, tmp_path, monkeypatch):
        """Test basic metric logging."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5, step=1)
        finish()

        # Check that metrics file was created
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["name"] == "loss"
            assert data["value"] == 0.5
            assert data["step"] == 1

    def test_log_metrics_batch(self, tmp_path, monkeypatch):
        """Test batch metric logging."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)
        finish()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 2

    def test_log_artifact(self, tmp_path, monkeypatch):
        """Test artifact logging."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_artifact("model", "model.pt")
        finish()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

        with open(metrics_file) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["type"] == "artifact"
            assert data["name"] == "model"
            assert data["path"] == "model.pt"

    def test_multiple_calls_same_logger(self, tmp_path, monkeypatch):
        """Test that multiple calls use the same logger instance."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5, step=1)
        log_metric("loss", 0.4, step=2)
        log_metric("loss", 0.3, step=3)
        finish()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            lines = f.readlines()
            assert len(lines) == 3

    def test_finish_returns_backend_url(self, tmp_path, monkeypatch):
        """Test that finish() returns backend URL if available."""
        # This test will be more meaningful when we add backend configuration
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5)
        url = finish()

        # Without backend, should return None
        assert url is None

    def test_no_outputs_dir_uses_default(self, tmp_path, monkeypatch):
        """Test that missing GOLDFISH_OUTPUTS_DIR uses default /mnt/outputs."""
        # Remove the env var
        monkeypatch.delenv("GOLDFISH_OUTPUTS_DIR", raising=False)

        # Mock Path.mkdir to avoid actually creating /mnt/outputs
        with patch("goldfish.metrics.writer.Path.mkdir"):
            log_metric("loss", 0.5)

            # Just verify it doesn't crash - actual directory creation is mocked

    def test_idempotent_finish(self, tmp_path, monkeypatch):
        """Test that finish() can be called multiple times safely."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5)
        url1 = finish()
        url2 = finish()

        # Both should succeed (second is a no-op)
        assert url1 is None
        assert url2 is None

    def test_logger_reset_between_tests(self, tmp_path, monkeypatch):
        """Test that logger can be reset (for testing purposes)."""
        from goldfish import metrics

        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5)
        finish()

        # Reset the logger
        metrics._reset_global_logger()

        # Use a new directory
        tmp_path2 = tmp_path / "run2"
        tmp_path2.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path2))

        log_metric("accuracy", 0.9)
        finish()

        # First directory should have loss
        metrics_file1 = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file1) as f:
            data = json.loads(f.readline())
            assert data["name"] == "loss"

        # Second directory should have accuracy
        metrics_file2 = tmp_path2 / ".goldfish" / "metrics.jsonl"
        with open(metrics_file2) as f:
            data = json.loads(f.readline())
            assert data["name"] == "accuracy"

    def test_auto_finalize_flushes_metrics(self, tmp_path, monkeypatch):
        """Metrics should be flushed on auto-finalize (atexit hook)."""
        from goldfish import metrics

        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5, step=1)

        # Simulate process exit hook
        metrics._auto_finalize()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["name"] == "loss"

    def test_numpy_scalar_metric_values(self, tmp_path, monkeypatch):
        """NumPy scalar metric values should serialize correctly."""
        np = pytest.importorskip("numpy")

        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", np.float32(0.5), step=1)
        finish()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["value"] == 0.5

    def test_log_metric_accepts_iso_timestamp(self, tmp_path, monkeypatch):
        """ISO 8601 timestamps should be accepted in public API."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        log_metric("loss", 0.5, timestamp="2024-01-01T00:00:00Z")
        finish()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        with open(metrics_file) as f:
            data = json.loads(f.readline())
            assert data["timestamp"] == "2024-01-01T00:00:00+00:00"

    def test_log_artifact_returns_backend_url(self, tmp_path, monkeypatch):
        """log_artifact should return backend URL when available."""
        from goldfish import metrics as metrics_module
        from goldfish.metrics.backends import MetricsBackend
        from goldfish.metrics.logger import MetricsLogger

        class UrlBackend(MetricsBackend):
            def init_run(self, run_id: str, config: dict, workspace: str, stage: str) -> None:
                pass

            def log_metric(
                self, name: str, value: float, step: int | None = None, timestamp: float | None = None
            ) -> None:
                pass

            def log_metrics(
                self, metrics: dict[str, float], step: int | None = None, timestamp: float | None = None
            ) -> None:
                pass

            def log_artifact(self, name: str, path):  # type: ignore[override]
                return "https://example.com/run/abc"

            def finish(self) -> str | None:
                return None

            @classmethod
            def is_available(cls) -> bool:
                return True

            @classmethod
            def name(cls) -> str:
                return "url"

        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        backend = UrlBackend()
        metrics_module._global_logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc",
            config={},
            workspace="ws",
            stage="train",
        )

        url = log_artifact("model", "model.pt")
        assert url == "https://example.com/run/abc"

    def test_log_artifacts_batch_returns_urls(self, tmp_path, monkeypatch):
        """log_artifacts should return a mapping of names to URLs."""
        from goldfish import metrics as metrics_module
        from goldfish.metrics.backends import MetricsBackend
        from goldfish.metrics.logger import MetricsLogger

        class UrlBackend(MetricsBackend):
            def init_run(self, run_id: str, config: dict, workspace: str, stage: str) -> None:
                pass

            def log_metric(
                self, name: str, value: float, step: int | None = None, timestamp: float | None = None
            ) -> None:
                pass

            def log_metrics(
                self, metrics: dict[str, float], step: int | None = None, timestamp: float | None = None
            ) -> None:
                pass

            def log_artifact(self, name: str, path):  # type: ignore[override]
                return f"https://example.com/{name}"

            def finish(self) -> str | None:
                return None

            @classmethod
            def is_available(cls) -> bool:
                return True

            @classmethod
            def name(cls) -> str:
                return "url"

        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        backend = UrlBackend()
        metrics_module._global_logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc",
            config={},
            workspace="ws",
            stage="train",
        )

        from goldfish.metrics import log_artifacts

        urls = log_artifacts({"a": "a.txt", "b": "b.txt"})
        assert urls == {"a": "https://example.com/a", "b": "https://example.com/b"}
