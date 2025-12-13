"""Unit tests for provider registry system."""

import pytest

from goldfish.errors import GoldfishError
from goldfish.providers.base import ExecutionProvider, StorageProvider
from goldfish.providers.registry import (
    ExecutionProviderRegistry,
    StorageProviderRegistry,
    get_execution_registry,
    get_storage_registry,
)


class MockExecutionProvider(ExecutionProvider):
    """Mock execution provider for testing."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.initialized_with = config

    def build_image(self, image_tag, dockerfile_path, context_path, base_image=None):
        return f"mock-{image_tag}"

    def launch_stage(self, **kwargs):
        from goldfish.providers.base import ExecutionResult

        return ExecutionResult(
            instance_id="mock-instance",
            metadata={"backend": "mock"},
        )

    def get_status(self, instance_id):
        from goldfish.providers.base import ExecutionStatus

        return ExecutionStatus(state="succeeded", exit_code=0)

    def get_logs(self, instance_id, tail=None):
        return "Mock logs"

    def cancel(self, instance_id):
        return True


class MockStorageProvider(StorageProvider):
    """Mock storage provider for testing."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.initialized_with = config

    def upload(self, local_path, remote_path, metadata=None):
        from goldfish.providers.base import StorageLocation

        return StorageLocation(
            uri=f"mock://{remote_path}",
            size_bytes=1024,
        )

    def download(self, remote_path, local_path):
        return local_path

    def exists(self, remote_path):
        return True

    def get_size(self, remote_path):
        return 1024


class TestExecutionProviderRegistry:
    """Test ExecutionProviderRegistry."""

    def test_register_provider(self):
        """Test registering a provider."""
        registry = ExecutionProviderRegistry()
        registry.register("mock", MockExecutionProvider)

        assert registry.has_provider("mock")
        assert "mock" in registry.list_providers()

    def test_register_duplicate_provider_raises_error(self):
        """Test that registering duplicate provider raises error."""
        registry = ExecutionProviderRegistry()
        registry.register("mock", MockExecutionProvider)

        with pytest.raises(GoldfishError, match="already registered"):
            registry.register("mock", MockExecutionProvider)

    def test_register_invalid_provider_raises_error(self):
        """Test that registering non-provider class raises error."""
        registry = ExecutionProviderRegistry()

        class NotAProvider:
            pass

        with pytest.raises(GoldfishError, match="must inherit from ExecutionProvider"):
            registry.register("invalid", NotAProvider)

    def test_get_provider(self):
        """Test getting and instantiating a provider."""
        registry = ExecutionProviderRegistry()
        registry.register("mock", MockExecutionProvider)

        config = {"key": "value"}
        provider = registry.get("mock", config)

        assert isinstance(provider, MockExecutionProvider)
        assert provider.initialized_with == config

    def test_get_nonexistent_provider_raises_error(self):
        """Test that getting nonexistent provider raises error."""
        registry = ExecutionProviderRegistry()

        with pytest.raises(GoldfishError, match="not found"):
            registry.get("nonexistent")

    def test_get_provider_with_empty_config(self):
        """Test getting provider with no config."""
        registry = ExecutionProviderRegistry()
        registry.register("mock", MockExecutionProvider)

        provider = registry.get("mock")

        assert isinstance(provider, MockExecutionProvider)
        assert provider.initialized_with == {}

    def test_list_providers(self):
        """Test listing registered providers."""
        registry = ExecutionProviderRegistry()
        registry.register("provider_a", MockExecutionProvider)
        registry.register("provider_b", MockExecutionProvider)

        providers = registry.list_providers()

        assert sorted(providers) == ["provider_a", "provider_b"]

    def test_has_provider(self):
        """Test checking if provider exists."""
        registry = ExecutionProviderRegistry()
        registry.register("mock", MockExecutionProvider)

        assert registry.has_provider("mock")
        assert not registry.has_provider("nonexistent")

    def test_provider_initialization_failure(self):
        """Test that provider initialization errors are wrapped."""

        class FailingProvider(ExecutionProvider):
            def __init__(self, config):
                raise ValueError("Initialization failed")

            def build_image(self, *args, **kwargs):
                pass

            def launch_stage(self, *args, **kwargs):
                pass

            def get_status(self, *args, **kwargs):
                pass

            def get_logs(self, *args, **kwargs):
                pass

            def cancel(self, *args, **kwargs):
                pass

        registry = ExecutionProviderRegistry()
        registry.register("failing", FailingProvider)

        with pytest.raises(GoldfishError, match="Failed to initialize"):
            registry.get("failing")


class TestStorageProviderRegistry:
    """Test StorageProviderRegistry."""

    def test_register_provider(self):
        """Test registering a provider."""
        registry = StorageProviderRegistry()
        registry.register("mock", MockStorageProvider)

        assert registry.has_provider("mock")
        assert "mock" in registry.list_providers()

    def test_register_duplicate_provider_raises_error(self):
        """Test that registering duplicate provider raises error."""
        registry = StorageProviderRegistry()
        registry.register("mock", MockStorageProvider)

        with pytest.raises(GoldfishError, match="already registered"):
            registry.register("mock", MockStorageProvider)

    def test_register_invalid_provider_raises_error(self):
        """Test that registering non-provider class raises error."""
        registry = StorageProviderRegistry()

        class NotAProvider:
            pass

        with pytest.raises(GoldfishError, match="must inherit from StorageProvider"):
            registry.register("invalid", NotAProvider)

    def test_get_provider(self):
        """Test getting and instantiating a provider."""
        registry = StorageProviderRegistry()
        registry.register("mock", MockStorageProvider)

        config = {"bucket": "test-bucket"}
        provider = registry.get("mock", config)

        assert isinstance(provider, MockStorageProvider)
        assert provider.initialized_with == config

    def test_get_nonexistent_provider_raises_error(self):
        """Test that getting nonexistent provider raises error."""
        registry = StorageProviderRegistry()

        with pytest.raises(GoldfishError, match="not found"):
            registry.get("nonexistent")


class TestGlobalRegistries:
    """Test global registry instances."""

    def test_get_execution_registry_returns_same_instance(self):
        """Test that get_execution_registry returns singleton."""
        registry1 = get_execution_registry()
        registry2 = get_execution_registry()

        assert registry1 is registry2

    def test_get_storage_registry_returns_same_instance(self):
        """Test that get_storage_registry returns singleton."""
        registry1 = get_storage_registry()
        registry2 = get_storage_registry()

        assert registry1 is registry2

    def test_builtin_providers_registered(self):
        """Test that built-in providers are auto-registered."""
        exec_registry = get_execution_registry()
        storage_registry = get_storage_registry()

        # Check built-in execution providers
        assert exec_registry.has_provider("gce")
        assert exec_registry.has_provider("local")

        # Check built-in storage providers
        assert storage_registry.has_provider("gcs")
        assert storage_registry.has_provider("local")
