"""Unit tests for metrics backend registry and base classes."""

from pathlib import Path

from goldfish.metrics.backends import MetricsBackend, MetricsBackendRegistry, get_registry


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

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        self.metrics.append({"name": name, "value": value, "step": step})

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        for name, value in metrics.items():
            self.log_metric(name, value, step, timestamp)

    def log_artifact(self, name: str, path: Path) -> str | None:
        self.artifacts.append({"name": name, "path": path})
        return "http://example.com/artifact/123"

    def finish(self) -> str | None:
        self.finished = True
        return "http://example.com/run/123"

    @classmethod
    def is_available(cls) -> bool:
        return True

    @classmethod
    def name(cls) -> str:
        return "mock"


class UnavailableBackend(MetricsBackend):
    """Mock backend that is not available."""

    def init_run(self, run_id: str, config: dict, workspace: str, stage: str) -> None:
        pass

    def log_metric(
        self,
        name: str,
        value: float,
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        pass

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        pass

    def log_artifact(self, name: str, path: Path) -> str | None:
        return None

    def finish(self) -> str | None:
        return None

    @classmethod
    def is_available(cls) -> bool:
        return False

    @classmethod
    def name(cls) -> str:
        return "unavailable"


class TestMetricsBackendRegistry:
    """Tests for MetricsBackendRegistry."""

    def test_register_backend(self):
        """Test registering a backend."""
        registry = MetricsBackendRegistry()
        registry.register(MockBackend)
        assert "mock" in registry.list_backends()

    def test_get_backend(self):
        """Test getting a registered backend."""
        registry = MetricsBackendRegistry()
        registry.register(MockBackend)
        backend_class = registry.get("mock")
        assert backend_class is MockBackend

    def test_get_nonexistent_backend(self):
        """Test getting a backend that doesn't exist."""
        registry = MetricsBackendRegistry()
        backend_class = registry.get("nonexistent")
        assert backend_class is None

    def test_list_backends(self):
        """Test listing all registered backends."""
        registry = MetricsBackendRegistry()
        registry.register(MockBackend)
        registry.register(UnavailableBackend)
        backends = registry.list_backends()
        assert "mock" in backends
        assert "unavailable" in backends

    def test_list_available(self):
        """Test listing only available backends."""
        registry = MetricsBackendRegistry()
        registry.register(MockBackend)
        registry.register(UnavailableBackend)
        available = registry.list_available()
        assert "mock" in available
        assert "unavailable" not in available

    def test_global_registry(self):
        """Test that get_registry returns a singleton."""
        registry1 = get_registry()
        registry2 = get_registry()
        assert registry1 is registry2


class TestMockBackend:
    """Tests for MockBackend to verify the interface works."""

    def test_init_run(self):
        """Test initializing a run."""
        backend = MockBackend()
        backend.init_run(
            run_id="stage-123",
            config={"lr": 0.01},
            workspace="test-workspace",
            stage="train",
        )
        assert backend.initialized
        assert backend.run_id == "stage-123"
        assert backend.config == {"lr": 0.01}

    def test_log_metric(self):
        """Test logging a single metric."""
        backend = MockBackend()
        backend.log_metric("loss", 0.5, step=10)
        assert len(backend.metrics) == 1
        assert backend.metrics[0]["name"] == "loss"
        assert backend.metrics[0]["value"] == 0.5
        assert backend.metrics[0]["step"] == 10

    def test_log_metrics(self):
        """Test logging multiple metrics."""
        backend = MockBackend()
        backend.log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)
        assert len(backend.metrics) == 2

    def test_log_artifact(self):
        """Test logging an artifact."""
        backend = MockBackend()
        url = backend.log_artifact("model", Path("/tmp/model.pt"))
        assert len(backend.artifacts) == 1
        assert backend.artifacts[0]["name"] == "model"
        assert url == "http://example.com/artifact/123"

    def test_finish(self):
        """Test finishing a run."""
        backend = MockBackend()
        url = backend.finish()
        assert backend.finished
        assert url == "http://example.com/run/123"

    def test_is_available(self):
        """Test backend availability check."""
        assert MockBackend.is_available()
        assert not UnavailableBackend.is_available()

    def test_name(self):
        """Test backend name."""
        assert MockBackend.name() == "mock"
        assert UnavailableBackend.name() == "unavailable"
