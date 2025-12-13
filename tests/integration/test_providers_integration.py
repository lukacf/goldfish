"""Integration tests for provider system with core components."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from goldfish.config import GCSConfig, GCEConfig, GoldfishConfig, JobsConfig
from goldfish.datasets.registry import DatasetRegistry
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.providers.base import (
    ExecutionProvider,
    ExecutionResult,
    ExecutionStatus,
    StorageLocation,
    StorageProvider,
)


class MockExecutionProvider(ExecutionProvider):
    """Mock execution provider for integration tests."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.built_images = []
        self.launched_stages = []
        self.config_used = config

    def build_image(self, image_tag, dockerfile_path, context_path, base_image=None):
        self.built_images.append(
            {
                "image_tag": image_tag,
                "context_path": str(context_path),
                "base_image": base_image,
            }
        )
        return f"mock-{image_tag}"

    def launch_stage(
        self,
        image_tag,
        stage_run_id,
        entrypoint_script,
        stage_config,
        work_dir,
        inputs_dir=None,
        outputs_dir=None,
        machine_type=None,
        gpu_type=None,
        gpu_count=0,
        profile_hints=None,
    ):
        self.launched_stages.append(
            {
                "image_tag": image_tag,
                "stage_run_id": stage_run_id,
                "machine_type": machine_type,
                "gpu_type": gpu_type,
                "gpu_count": gpu_count,
                "profile_hints": profile_hints,
            }
        )
        return ExecutionResult(
            instance_id=stage_run_id,
            metadata={"backend": "mock", "config": self.config_used},
        )

    def get_status(self, instance_id):
        return ExecutionStatus(state="succeeded", exit_code=0)

    def get_logs(self, instance_id, tail=None):
        return f"Mock logs for {instance_id}"

    def cancel(self, instance_id):
        return True


class MockStorageProvider(StorageProvider):
    """Mock storage provider for integration tests."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.uploaded_files = []
        self.downloaded_files = []
        self.config_used = config

    def upload(self, local_path, remote_path, metadata=None):
        self.uploaded_files.append(
            {
                "local_path": str(local_path),
                "remote_path": remote_path,
                "metadata": metadata,
            }
        )
        return StorageLocation(
            uri=f"mock://{remote_path}",
            size_bytes=local_path.stat().st_size if local_path.exists() and local_path.is_file() else None,
            metadata=metadata,
        )

    def download(self, remote_path, local_path):
        self.downloaded_files.append(
            {
                "remote_path": remote_path,
                "local_path": str(local_path),
            }
        )
        return local_path

    def exists(self, remote_path):
        return True

    def get_size(self, remote_path):
        return 1024


class TestStageExecutorWithProviders:
    """Test StageExecutor integration with execution providers."""

    @pytest.fixture
    def mock_config(self, tmp_path):
        """Create test configuration."""
        return GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(execution_provider="mock"),
        )

    @pytest.fixture
    def stage_executor(self, test_db, mock_config, tmp_path):
        """Create StageExecutor with mock provider."""
        from goldfish.pipeline.manager import PipelineManager
        from goldfish.workspace.manager import WorkspaceManager

        # Create minimal dependencies
        workspace_manager = MagicMock(spec=WorkspaceManager)
        workspace_manager.get_workspace_path.return_value = tmp_path / "workspace"

        pipeline_manager = MagicMock(spec=PipelineManager)

        # Create mock execution provider
        mock_provider = MockExecutionProvider({"test": "config"})

        executor = StageExecutor(
            db=test_db,
            config=mock_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=tmp_path,
            execution_provider=mock_provider,
        )

        # Store mock provider for assertions
        executor.mock_provider = mock_provider

        return executor

    def test_executor_uses_injected_provider(self, stage_executor):
        """Test that StageExecutor uses injected execution provider."""
        assert isinstance(stage_executor.execution_provider, MockExecutionProvider)
        assert stage_executor.execution_provider.config_used == {"test": "config"}

    def test_executor_builds_image_via_provider(self, stage_executor, tmp_path):
        """Test that image building goes through provider."""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir(parents=True)

        # Mock workspace manager to return our workspace
        stage_executor.workspace_manager.get_workspace_path.return_value = workspace_dir

        image_tag = stage_executor._build_docker_image("test_ws", "v1")

        # Verify provider was called
        assert len(stage_executor.mock_provider.built_images) == 1
        built = stage_executor.mock_provider.built_images[0]
        assert built["image_tag"] == "goldfish-test_ws-v1"
        assert built["context_path"] == str(workspace_dir)

    def test_executor_auto_instantiates_provider_from_config(self, test_db, tmp_path):
        """Test that StageExecutor auto-creates provider from config."""
        from goldfish.pipeline.manager import PipelineManager
        from goldfish.providers import get_execution_registry
        from goldfish.workspace.manager import WorkspaceManager

        # Register mock provider
        registry = get_execution_registry()
        if not registry.has_provider("test_auto"):
            registry.register("test_auto", MockExecutionProvider)

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(execution_provider="test_auto"),
            providers={"test_auto": {"auto_created": True}},
        )

        workspace_manager = MagicMock(spec=WorkspaceManager)
        pipeline_manager = MagicMock(spec=PipelineManager)

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=tmp_path,
            # No execution_provider parameter - should auto-create
        )

        assert isinstance(executor.execution_provider, MockExecutionProvider)
        assert executor.execution_provider.config == {"auto_created": True}


class TestDatasetRegistryWithProviders:
    """Test DatasetRegistry integration with storage providers."""

    @pytest.fixture
    def mock_config(self):
        """Create test configuration."""
        return GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(storage_provider="mock"),
        )

    @pytest.fixture
    def dataset_registry(self, test_db, mock_config):
        """Create DatasetRegistry with mock storage provider."""
        mock_provider = MockStorageProvider({"bucket": "test-bucket"})

        registry = DatasetRegistry(
            db=test_db,
            config=mock_config,
            storage_provider=mock_provider,
        )

        # Store mock provider for assertions
        registry.mock_provider = mock_provider

        return registry

    def test_registry_uses_injected_provider(self, dataset_registry):
        """Test that DatasetRegistry uses injected storage provider."""
        assert isinstance(dataset_registry.storage_provider, MockStorageProvider)
        assert dataset_registry.storage_provider.config_used == {"bucket": "test-bucket"}

    def test_registry_uploads_dataset_via_provider(self, dataset_registry, tmp_path):
        """Test that dataset upload goes through storage provider."""
        # Create a test file
        test_file = tmp_path / "test_data.csv"
        test_file.write_text("col1,col2\n1,2\n3,4\n")

        # Register dataset
        dataset_registry.register_dataset(
            name="test_dataset",
            source=f"local:{test_file}",
            description="Test dataset",
            format="csv",
        )

        # Verify provider was called
        assert len(dataset_registry.mock_provider.uploaded_files) == 1
        uploaded = dataset_registry.mock_provider.uploaded_files[0]
        assert uploaded["remote_path"] == "test_dataset"
        assert uploaded["local_path"] == str(test_file)

    def test_registry_auto_instantiates_provider_from_config(self, test_db):
        """Test that DatasetRegistry auto-creates provider from config."""
        from goldfish.providers import get_storage_registry

        # Register mock provider
        registry = get_storage_registry()
        if not registry.has_provider("test_storage"):
            registry.register("test_storage", MockStorageProvider)

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(storage_provider="test_storage"),
            providers={"test_storage": {"auto_created": True}},
        )

        dataset_registry = DatasetRegistry(
            db=test_db,
            config=config,
            # No storage_provider parameter - should auto-create
        )

        assert isinstance(dataset_registry.storage_provider, MockStorageProvider)
        assert dataset_registry.storage_provider.config == {"auto_created": True}


class TestMixedProviders:
    """Test scenarios with mixed execution and storage providers."""

    def test_local_execution_with_gcs_storage_config(self, tmp_path):
        """Test configuration with local execution but GCS storage."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(
                execution_provider="local",
                storage_provider="gcs",
            ),
            providers={
                "gcs": {"bucket": "my-gcs-bucket"},
                "local": {"work_dir": "/tmp/goldfish-test"},
            },
        )

        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["work_dir"] == "/tmp/goldfish-test"
        assert storage_config["bucket"] == "my-gcs-bucket"

    def test_gce_execution_with_local_storage_config(self, tmp_path):
        """Test configuration with GCE execution but local storage."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(
                execution_provider="gce",
                storage_provider="local",
            ),
            providers={
                "gce": {
                    "project_id": "my-project",
                    "zone": "us-central1-a",
                    "bucket": "fallback-bucket",
                },
                "local": {"base_path": str(tmp_path / "storage")},
            },
        )

        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["project_id"] == "my-project"
        assert storage_config["base_path"] == str(tmp_path / "storage")


class TestLegacyConfigMigration:
    """Test that legacy configurations still work."""

    def test_legacy_gce_gcs_config_works(self, tmp_path):
        """Test that old-style GCE/GCS config still works."""
        config = GoldfishConfig(
            project_name="legacy-test",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(
                project_id="legacy-project",
                zones=["us-west1-a", "us-west1-b"],
            ),
            gcs=GCSConfig(bucket="legacy-bucket"),
        )

        # Verify auto-mapping
        assert config.jobs.effective_execution_provider == "gce"
        assert config.jobs.effective_storage_provider == "gcs"

        # Verify provider configs are derived correctly
        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["project_id"] == "legacy-project"
        assert exec_config["zones"] == ["us-west1-a", "us-west1-b"]
        assert exec_config["bucket"] == "legacy-bucket"

        assert storage_config["bucket"] == "legacy-bucket"

    def test_legacy_local_backend_config_works(self, tmp_path):
        """Test that old-style local backend config still works."""
        config = GoldfishConfig(
            project_name="legacy-test",
            dev_repo_path=str(tmp_path / "test-dev"),
            jobs=JobsConfig(backend="local"),
        )

        # Verify auto-mapping
        assert config.jobs.effective_execution_provider == "local"
        assert config.jobs.effective_storage_provider == "local"

        # Verify defaults are used
        exec_config = config.get_execution_provider_config()
        storage_config = config.get_storage_provider_config()

        assert exec_config["work_dir"] == "/tmp/goldfish"
        assert storage_config["base_path"] == ".goldfish/storage"
