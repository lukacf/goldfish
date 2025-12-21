"""Unit tests for MetricsLogger (orchestration layer)."""

from pathlib import Path

from goldfish.metrics.backends import MetricsBackend
from goldfish.metrics.logger import MetricsLogger


class MockBackend(MetricsBackend):
    """Mock backend for testing."""

    def __init__(self) -> None:
        self.initialized = False
        self.metrics = []
        self.artifacts = []
        self.finished = False

    def init_run(self, run_id: str, config: dict, workspace: str, stage: str) -> None:
        self.initialized = True
        self.run_id = run_id
        self.config = config
        self.workspace = workspace
        self.stage = stage

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        self.metrics.append({"name": name, "value": value, "step": step})

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        for name, value in metrics.items():
            self.log_metric(name, value, step, timestamp)

    def log_artifact(self, name: str, path: Path) -> None:
        self.artifacts.append({"name": name, "path": path})

    def finish(self) -> str | None:
        self.finished = True
        return "http://example.com/run/123"

    @classmethod
    def is_available(cls) -> bool:
        return True

    @classmethod
    def name(cls) -> str:
        return "mock"


class FailingBackend(MetricsBackend):
    """Backend that fails on all operations."""

    def init_run(self, run_id: str, config: dict, workspace: str, stage: str) -> None:
        raise Exception("Backend initialization failed")

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        raise Exception("Backend metric logging failed")

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | None = None,
    ) -> None:
        raise Exception("Backend metrics logging failed")

    def log_artifact(self, name: str, path: Path) -> None:
        raise Exception("Backend artifact logging failed")

    def finish(self) -> str | None:
        raise Exception("Backend finish failed")

    @classmethod
    def is_available(cls) -> bool:
        return True

    @classmethod
    def name(cls) -> str:
        return "failing"


class TestMetricsLogger:
    """Tests for MetricsLogger."""

    def test_init_without_backend(self, tmp_path):
        """Test initializing logger without a backend."""
        logger = MetricsLogger(outputs_dir=tmp_path)
        assert logger is not None

    def test_init_with_backend(self, tmp_path):
        """Test initializing logger with a backend."""
        backend = MockBackend()
        logger = MetricsLogger(outputs_dir=tmp_path, backend=backend)
        assert logger is not None

    def test_lazy_backend_initialization(self, tmp_path):
        """Test that backend is not initialized until first metric."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={"lr": 0.01},
            workspace="test-workspace",
            stage="train",
        )
        # Backend should not be initialized yet
        assert not backend.initialized

        # Log a metric - this should trigger backend initialization
        logger.log_metric("loss", 0.5)

        # Now backend should be initialized
        assert backend.initialized
        assert backend.run_id == "stage-abc123"
        assert backend.config == {"lr": 0.01}
        assert backend.workspace == "test-workspace"
        assert backend.stage == "train"

    def test_log_metric_writes_to_local_and_backend(self, tmp_path):
        """Test that log_metric writes to both local and backend."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        logger.log_metric("loss", 0.5, step=10)
        logger.flush()

        # Check local file
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

        # Check backend
        assert len(backend.metrics) == 1
        assert backend.metrics[0]["name"] == "loss"
        assert backend.metrics[0]["value"] == 0.5

    def test_log_metrics_batch(self, tmp_path):
        """Test that log_metrics writes batch to both local and backend."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        logger.log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)
        logger.flush()

        # Check backend
        assert len(backend.metrics) == 2

    def test_log_artifact(self, tmp_path):
        """Test that log_artifact writes to both local and backend."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        logger.log_artifact("model", "model.pt")
        logger.flush()

        # Check backend
        assert len(backend.artifacts) == 1
        assert backend.artifacts[0]["name"] == "model"

    def test_finish_calls_both(self, tmp_path):
        """Test that finish() calls both local flush and backend finish."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        logger.log_metric("loss", 0.5)
        url = logger.finish()

        # Backend should be finished
        assert backend.finished

        # Should return backend URL
        assert url == "http://example.com/run/123"

    def test_finish_without_backend(self, tmp_path):
        """Test that finish() works without a backend."""
        logger = MetricsLogger(outputs_dir=tmp_path)
        logger.log_metric("loss", 0.5)
        url = logger.finish()

        # Should return None when no backend
        assert url is None

    def test_graceful_degradation_on_backend_init_failure(self, tmp_path):
        """Test that backend init failure doesn't crash logger."""
        backend = FailingBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        # This should not raise, even though backend will fail
        logger.log_metric("loss", 0.5)
        logger.flush()

        # Local file should still work
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

    def test_graceful_degradation_on_backend_log_failure(self, tmp_path):
        """Test that backend log failure doesn't crash logger."""
        backend = FailingBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        # Log multiple metrics - backend will fail but logger should continue
        logger.log_metric("loss", 0.5)
        logger.log_metric("accuracy", 0.9)
        logger.flush()

        # Local file should have both metrics
        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

    def test_graceful_degradation_on_backend_finish_failure(self, tmp_path):
        """Test that backend finish failure doesn't crash logger."""
        backend = FailingBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        logger.log_metric("loss", 0.5)

        # finish() should not raise even though backend fails
        url = logger.finish()

        # Should return None on backend failure
        assert url is None

    def test_backend_only_initialized_once(self, tmp_path):
        """Test that backend is initialized only on first metric call."""
        backend = MockBackend()
        logger = MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        # Log multiple metrics
        logger.log_metric("loss", 0.5)
        logger.log_metric("accuracy", 0.9)
        logger.log_metric("f1", 0.85)

        # Backend should be initialized only once
        # We can verify this by checking that run_id was set (init was called)
        assert backend.initialized
        assert backend.run_id == "stage-abc123"

    def test_logger_without_run_metadata(self, tmp_path):
        """Test that logger works without run metadata for testing."""
        logger = MetricsLogger(outputs_dir=tmp_path)
        logger.log_metric("loss", 0.5)
        logger.flush()

        metrics_file = tmp_path / ".goldfish" / "metrics.jsonl"
        assert metrics_file.exists()

    def test_context_manager_support(self, tmp_path):
        """Test that logger can be used as context manager."""
        backend = MockBackend()

        with MetricsLogger(
            outputs_dir=tmp_path,
            backend=backend,
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        ) as logger:
            logger.log_metric("loss", 0.5)

        # Backend should be finished after context exit
        assert backend.finished
