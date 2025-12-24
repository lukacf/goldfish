"""Integration tests for source metadata enforcement and updates."""

from unittest.mock import MagicMock

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.state.state_md import StateManager
from goldfish.validation import InvalidSourceMetadataError


def _valid_file_metadata() -> dict:
    return {
        "schema_version": 1,
        "description": "Model artifact file for metadata enforcement tests.",
        "source": {
            "format": "file",
            "size_bytes": 123,
            "created_at": "2025-12-24T12:00:00Z",
        },
        "schema": {"kind": "file", "content_type": "application/json"},
    }


def _valid_csv_metadata() -> dict:
    return {
        "schema_version": 1,
        "description": "CSV source for metadata enforcement tests.",
        "source": {
            "format": "csv",
            "size_bytes": 456,
            "created_at": "2025-12-24T12:00:00Z",
            "format_params": {"delimiter": ","},
        },
        "schema": {
            "kind": "tabular",
            "row_count": 10,
            "columns": ["col1"],
            "dtypes": {"col1": "int64"},
        },
    }


class TestSourceMetadataTools:
    """Test metadata enforcement in MCP tools."""

    def test_register_source_requires_metadata(self, temp_dir):
        """register_source should reject missing metadata."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            register_fn = server.register_source.fn if hasattr(server.register_source, "fn") else server.register_source
            with pytest.raises(InvalidSourceMetadataError, match="metadata"):
                register_fn(
                    name="tokens_v1",
                    gcs_path="gs://bucket/tokens_v1.npy",
                    description="Token dataset for unit tests",
                    reason="Registering without metadata should fail",
                    metadata=None,
                )
        finally:
            server.reset_server()

    def test_register_source_rejects_format_mismatch(self, temp_dir):
        """register_source should enforce format matching."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            register_fn = server.register_source.fn if hasattr(server.register_source, "fn") else server.register_source
            with pytest.raises(InvalidSourceMetadataError, match="format"):
                register_fn(
                    name="tokens_v2",
                    gcs_path="gs://bucket/tokens_v2.npy",
                    description=_valid_file_metadata()["description"],
                    reason="Format mismatch should be rejected by tool",
                    metadata=_valid_file_metadata(),
                    format="npy",
                )
        finally:
            server.reset_server()

    def test_register_dataset_rejects_size_bytes_mismatch(self, temp_dir):
        """register_dataset should enforce size_bytes matching."""
        from goldfish import server
        from goldfish.datasets.registry import DatasetRegistry

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
            gcs=None,
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)
        dataset_registry = DatasetRegistry(db=db, config=config)

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=dataset_registry,
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            register_fn = (
                server.register_dataset.fn if hasattr(server.register_dataset, "fn") else server.register_dataset
            )
            with pytest.raises(InvalidSourceMetadataError, match="size_bytes"):
                register_fn(
                    name="dataset_v1",
                    source="gs://bucket/data.csv",
                    description=_valid_file_metadata()["description"],
                    format="file",
                    metadata=_valid_file_metadata(),
                    size_bytes=999,
                )
        finally:
            server.reset_server()

    def test_update_source_metadata_updates_metadata(self, temp_dir):
        """update_source_metadata should update metadata and return SourceInfo."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Seed a source without metadata (legacy)
        db.create_source(
            source_id="legacy",
            name="legacy",
            gcs_location="gs://bucket/legacy",
            created_by="external",
            description="Legacy source without metadata",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            update_fn = (
                server.update_source_metadata.fn
                if hasattr(server.update_source_metadata, "fn")
                else server.update_source_metadata
            )
            result = update_fn(
                source_name="legacy",
                metadata=_valid_file_metadata(),
                reason="Backfilling metadata for legacy source.",
            )

            assert result.success is True
            assert result.source.metadata_status == "ok"
            assert result.source.metadata is not None

            stored = db.get_source("legacy")
            assert stored is not None
            assert stored["metadata"] is not None
        finally:
            server.reset_server()

    def test_update_source_metadata_rejects_format_change(self, temp_dir):
        """update_source_metadata should not allow format changes."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        db.create_source(
            source_id="csv_source",
            name="csv_source",
            gcs_location="gs://bucket/data.csv",
            created_by="external",
            description=_valid_csv_metadata()["description"],
            size_bytes=_valid_csv_metadata()["source"]["size_bytes"],
            metadata=_valid_csv_metadata(),
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            update_fn = (
                server.update_source_metadata.fn
                if hasattr(server.update_source_metadata, "fn")
                else server.update_source_metadata
            )
            with pytest.raises(InvalidSourceMetadataError, match="format"):
                update_fn(
                    source_name="csv_source",
                    metadata=_valid_file_metadata(),
                    reason="Attempting to change format should fail.",
                )
        finally:
            server.reset_server()

    def test_promote_artifact_requires_metadata(self, temp_dir):
        """promote_artifact should reject missing metadata."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        db.create_job(
            job_id="job-a1b2c3d4",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-a1b2c3d4",
            status="completed",
            artifact_uri="gs://bucket/artifacts/job-a1b2c3d4/",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = (
                server.promote_artifact.fn if hasattr(server.promote_artifact, "fn") else server.promote_artifact
            )
            with pytest.raises(InvalidSourceMetadataError, match="metadata"):
                promote_fn(
                    job_id="job-a1b2c3d4",
                    output_name="model",
                    source_name="promoted_model",
                    reason="Missing metadata should be rejected",
                    metadata=None,
                )
        finally:
            server.reset_server()

    def test_promote_artifact_rejects_description_mismatch(self, temp_dir):
        """promote_artifact should enforce description matching."""
        from goldfish import server

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        db.create_job(
            job_id="job-b2c3d4e5",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
        )
        db.update_job_status(
            job_id="job-b2c3d4e5",
            status="completed",
            artifact_uri="gs://bucket/artifacts/job-b2c3d4e5/",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            promote_fn = (
                server.promote_artifact.fn if hasattr(server.promote_artifact, "fn") else server.promote_artifact
            )
            with pytest.raises(InvalidSourceMetadataError, match="description"):
                promote_fn(
                    job_id="job-b2c3d4e5",
                    output_name="model",
                    source_name="promoted_model",
                    reason="Description mismatch should be rejected",
                    metadata=_valid_file_metadata(),
                    description="Different description",
                )
        finally:
            server.reset_server()
