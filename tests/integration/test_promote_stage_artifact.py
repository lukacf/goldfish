from unittest.mock import MagicMock

from goldfish import server
from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.models import StageRunStatus
from goldfish.state.state_md import StateManager


class TestPromoteStageArtifact:
    def test_promote_artifact_with_stage_id(self, temp_dir):
        """Test that promote_artifact accepts a stage run ID (stage-...)."""

        # 1. Setup Environment
        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # 2. Configure Server
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
            # 3. Insert a stage run (and dependencies)
            stage_run_id = "stage-1234567890abcdef"
            with db._conn() as conn:
                # Create dependencies to satisfy constraints/logic
                conn.execute(
                    """
                    INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at)
                    VALUES (?, datetime('now'))
                    """,
                    ("w1",),
                )

                # Need a version
                conn.execute(
                    """
                    INSERT OR IGNORE INTO workspace_versions (
                        workspace_name, version, git_tag, git_sha, created_at, created_by
                    ) VALUES (?, ?, ?, ?, datetime('now'), ?)
                    """,
                    ("w1", "v1", "w1-v1", "sha123", "manual"),
                )

                # Need a stage version
                cursor = conn.execute(
                    """
                    INSERT INTO stage_versions (
                        workspace_name, stage_name, version_num, git_sha, config_hash
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    ("w1", "preprocess", 1, "sha123", "hash123"),
                )
                stage_version_id = cursor.lastrowid

                conn.execute(
                    """
                    INSERT INTO stage_runs (
                        id, workspace_name, stage_name, stage_version_id, version,
                        status, started_at, completed_at, artifact_uri
                    ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?)
                    """,
                    (
                        stage_run_id,
                        "w1",
                        "preprocess",
                        stage_version_id,
                        "v1",
                        StageRunStatus.COMPLETED,
                        f"gs://bucket/{stage_run_id}",
                    ),
                )

            # 4. Prepare metadata
            description = "Test promotion artifact description must be long enough."
            metadata = {
                "schema_version": 1,
                "description": description,
                "schema": {"kind": "file", "content_type": "text/plain"},
                "source": {"format": "file", "size_bytes": 100, "created_at": "2025-01-01T12:00:00Z"},
            }

            # 5. Call promote_artifact
            promote_fn = (
                server.promote_artifact.fn if hasattr(server.promote_artifact, "fn") else server.promote_artifact
            )

            result = promote_fn(
                job_id=stage_run_id,
                output_name="output",
                source_name="new_source_v1",
                reason="Validated reasonable reason",
                metadata=metadata,
                description=description,
                format="file",
                size_bytes=100,
            )

            assert result.source.name == "new_source_v1"
            assert result.source.gcs_location == f"gs://bucket/{stage_run_id}/output/"

        finally:
            server.reset_server()
