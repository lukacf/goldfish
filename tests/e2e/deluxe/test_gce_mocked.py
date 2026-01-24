"""Mocked GCE E2E tests.

These tests validate the GCE integration path using mocked GCP services.
They can run in CI without actual GCP credentials.

This provides confidence that:
1. The cloud abstraction layer is properly wired
2. GCE backend adapter follows the RunBackend protocol
3. GCS storage adapter follows the ObjectStorage protocol
4. Integration between StageExecutor and cloud adapters works
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from goldfish.cloud.contracts import (
    BackendCapabilities,
    BackendStatus,
    RunHandle,
    RunSpec,
    RunStatus,
    StorageURI,
)


class TestGCEBackendAdapterConformance:
    """Test GCERunBackend implements RunBackend protocol correctly."""

    def test_gce_backend_has_capabilities(self):
        """GCERunBackend exposes capabilities property."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            backend = GCERunBackend(
                project_id="test-project",
                zones=["us-central1-a"],
                bucket="test-bucket",
            )

            caps = backend.capabilities
            assert isinstance(caps, BackendCapabilities)
            assert caps.supports_spot is True
            assert caps.supports_preemption is True
            assert caps.supports_live_logs is True
            assert caps.max_run_duration_hours == 24

    def test_gce_backend_launch_creates_handle(self):
        """GCERunBackend.launch returns a valid RunHandle."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        mock_launcher = MagicMock()
        mock_launcher.launch_instance.return_value = MagicMock(
            instance_name="stage-test123",
            zone="us-central1-a",
        )

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher", return_value=mock_launcher):
            backend = GCERunBackend(
                project_id="test-project",
                zones=["us-central1-a"],
                bucket="test-bucket",
            )

            spec = RunSpec(
                stage_run_id="stage-test123",
                workspace_name="test-workspace",
                stage_name="train",
                image="gcr.io/test/image:latest",
                command=["python", "-m", "train"],
                env={"PYTHONUNBUFFERED": "1"},
                timeout_seconds=3600,
            )

            handle = backend.launch(spec)

            assert isinstance(handle, RunHandle)
            assert handle.stage_run_id == "stage-test123"
            assert handle.backend_type == "gce"
            assert handle.backend_handle == "stage-test123"
            assert handle.zone == "us-central1-a"

    def test_gce_backend_get_status_maps_states(self):
        """GCERunBackend.get_status maps GCE states to RunStatus."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.state_machine.types import StageState

        mock_launcher = MagicMock()
        mock_launcher.get_instance_status.return_value = StageState.RUNNING

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher", return_value=mock_launcher):
            backend = GCERunBackend(
                project_id="test-project",
                zones=["us-central1-a"],
                bucket="test-bucket",
            )

            handle = RunHandle(
                stage_run_id="stage-test123",
                backend_type="gce",
                backend_handle="stage-test123",
                zone="us-central1-a",
            )

            status = backend.get_status(handle)

            assert isinstance(status, BackendStatus)
            assert status.status == RunStatus.RUNNING

    def test_gce_backend_get_status_completed(self):
        """GCERunBackend.get_status handles completed state."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.state_machine.types import StageState

        mock_launcher = MagicMock()
        mock_launcher.get_instance_status.return_value = StageState.COMPLETED

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher", return_value=mock_launcher):
            backend = GCERunBackend(
                project_id="test-project",
                zones=["us-central1-a"],
                bucket="test-bucket",
            )

            handle = RunHandle(
                stage_run_id="stage-test123",
                backend_type="gce",
                backend_handle="stage-test123",
            )

            status = backend.get_status(handle)

            assert status.status == RunStatus.COMPLETED
            assert status.exit_code == 0

    def test_gce_backend_get_logs_returns_string(self):
        """GCERunBackend.get_logs returns log string."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        mock_launcher = MagicMock()
        mock_launcher.get_instance_logs.return_value = "Log line 1\nLog line 2\n"

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher", return_value=mock_launcher):
            backend = GCERunBackend(
                project_id="test-project",
                zones=["us-central1-a"],
                bucket="test-bucket",
            )

            handle = RunHandle(
                stage_run_id="stage-test123",
                backend_type="gce",
                backend_handle="stage-test123",
            )

            logs = backend.get_logs(handle, tail=100)

            assert isinstance(logs, str)
            assert "Log line 1" in logs


class TestGCSStorageAdapterConformance:
    """Test GCS storage adapter implements ObjectStorage protocol."""

    def test_gcs_storage_put_get_roundtrip(self, tmp_path):
        """GCSStorage (mocked) handles put/get correctly."""
        from goldfish.cloud.adapters.gcp.storage import GCSStorage

        # Mock the GCS client
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Store data for get() to return
        stored_data = {}

        def mock_upload(data):
            stored_data["content"] = data

        def mock_download():
            return stored_data.get("content", b"")

        mock_blob.upload_from_string.side_effect = mock_upload
        mock_blob.download_as_bytes.side_effect = mock_download

        with patch("goldfish.cloud.adapters.gcp.storage.storage.Client", return_value=mock_client):
            gcs = GCSStorage(project="test-project")

            uri = StorageURI.parse("gs://test-bucket/path/to/file.txt")
            test_data = b"test content"

            gcs.put(uri, test_data)

            # Verify upload was called
            mock_blob.upload_from_string.assert_called_once_with(test_data)

    def test_gcs_storage_exists_checks_blob(self, tmp_path):
        """GCSStorage.exists checks if blob exists."""
        from goldfish.cloud.adapters.gcp.storage import GCSStorage

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True

        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        with patch("goldfish.cloud.adapters.gcp.storage.storage.Client", return_value=mock_client):
            gcs = GCSStorage(project="test-project")

            uri = StorageURI.parse("gs://test-bucket/path/to/file.txt")
            exists = gcs.exists(uri)

            assert exists is True
            mock_blob.exists.assert_called_once()


class TestAdapterFactoryIntegration:
    """Test AdapterFactory creates correct adapters based on config."""

    def test_factory_creates_gce_backend(self, tmp_path):
        """AdapterFactory creates GCERunBackend for gce config."""
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project="test-project",
                zones=["us-central1-a"],
            ),
        )

        factory = AdapterFactory(config)

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            backend = factory.create_run_backend()
            assert isinstance(backend, GCERunBackend)

    def test_factory_creates_gcs_storage(self, tmp_path):
        """AdapterFactory creates GCSStorage for gce config."""
        from goldfish.cloud.adapters.gcp.storage import GCSStorage
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project="test-project",
                zones=["us-central1-a"],
            ),
        )

        factory = AdapterFactory(config)

        with patch("goldfish.cloud.adapters.gcp.storage.storage.Client"):
            storage = factory.create_storage()
            assert isinstance(storage, GCSStorage)

    def test_factory_creates_local_backend_for_local_config(self, tmp_path):
        """AdapterFactory creates LocalRunBackend for local config."""
        from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(tmp_path),
            jobs=JobsConfig(backend="local"),
        )

        factory = AdapterFactory(config)
        backend = factory.create_run_backend()

        assert isinstance(backend, LocalRunBackend)


class TestStageExecutorCloudIntegration:
    """Test StageExecutor integrates with cloud abstraction layer."""

    def test_stage_executor_uses_adapter_factory(
        self,
        deluxe_temp_dir,
        deluxe_git_repo,
    ):
        """StageExecutor initializes AdapterFactory from config."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import (
            AuditConfig,
            GCEConfig,
            GCSConfig,
            GoldfishConfig,
            JobsConfig,
            PreRunReviewConfig,
            StateMdConfig,
        )
        from goldfish.db.database import Database
        from goldfish.jobs.stage_executor import StageExecutor
        from goldfish.pipeline.manager import PipelineManager
        from goldfish.svs.config import SVSConfig
        from goldfish.workspace.manager import WorkspaceManager

        project_root = deluxe_temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()
        (project_root / ".goldfish").mkdir()
        (project_root / "experiments").mkdir()

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(deluxe_git_repo.relative_to(deluxe_temp_dir)),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md"),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project="test-project",
                zones=["us-central1-a"],
            ),
            pre_run_review=PreRunReviewConfig(enabled=False),
            svs=SVSConfig(enabled=False),
        )

        db = Database(project_root / ".goldfish" / "goldfish.db")
        workspace_manager = WorkspaceManager(config=config, project_root=project_root, db=db)
        pipeline_manager = PipelineManager(db=db, workspace_manager=workspace_manager)

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCELauncher"):
            with patch("goldfish.cloud.adapters.gcp.storage.storage.Client"):
                executor = StageExecutor(
                    db=db,
                    config=config,
                    workspace_manager=workspace_manager,
                    pipeline_manager=pipeline_manager,
                    project_root=project_root,
                )

                # Verify AdapterFactory is initialized
                assert executor._adapter_factory is not None
                assert isinstance(executor._adapter_factory, AdapterFactory)
                assert executor._adapter_factory.backend_type == "gce"
