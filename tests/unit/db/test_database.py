import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from goldfish.db.database import Database


def _table_columns(db_path: Path, table: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    finally:
        conn.close()
    return {row["name"] for row in rows}


def test_schema_migrates_build_context_columns_for_existing_db(tmp_path: Path) -> None:
    """Existing databases get new build context columns via migrations."""
    db_path = tmp_path / "existing.db"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            """
            CREATE TABLE schema_version (version INTEGER NOT NULL);
            INSERT INTO schema_version (version) VALUES (999);
            CREATE TABLE stage_runs (
                id TEXT PRIMARY KEY,
                workspace_name TEXT NOT NULL,
                version TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT
            );
            CREATE TABLE docker_builds (
                id TEXT PRIMARY KEY,
                image_type TEXT NOT NULL,
                target TEXT NOT NULL,
                backend TEXT NOT NULL,
                cloud_build_id TEXT,
                status TEXT NOT NULL,
                image_tag TEXT,
                registry_tag TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                error TEXT,
                logs_uri TEXT,
                workspace_name TEXT,
                version TEXT,
                content_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    Database(db_path)

    docker_builds = _table_columns(db_path, "docker_builds")
    assert "dockerfile_hash" in docker_builds
    assert "git_sha" in docker_builds
    assert "goldfish_runtime_hash" in docker_builds
    assert "base_image" in docker_builds
    assert "base_image_digest" in docker_builds
    assert "requirements_hash" in docker_builds
    assert "build_args_json" in docker_builds
    assert "build_context_json" in docker_builds

    stage_runs = _table_columns(db_path, "stage_runs")
    assert "build_context_hash" in stage_runs
    assert "image_tag" in stage_runs


def test_insert_docker_build_includes_build_context_fields(test_db: Database) -> None:
    """insert_docker_build stores build context columns for later retrieval."""
    now = datetime.now(UTC).isoformat()
    test_db.insert_docker_build(
        build_id="build-abc12345",
        image_type="cpu",
        target="workspace",
        backend="local",
        started_at=now,
        content_hash="content-hash",
        dockerfile_hash="dockerfile-hash",
        git_sha="git-sha",
        goldfish_runtime_hash="runtime-hash",
        base_image="python:3.11-slim",
        base_image_digest="sha256:deadbeef",
        requirements_hash="req-hash",
        build_args_json='{"A":"1"}',
        build_context_json='{"dockerfile_hash":"dockerfile-hash"}',
    )

    row = test_db.get_docker_build("build-abc12345")
    assert row is not None
    assert row["content_hash"] == "content-hash"
    assert row["dockerfile_hash"] == "dockerfile-hash"
    assert row["git_sha"] == "git-sha"
    assert row["goldfish_runtime_hash"] == "runtime-hash"
    assert row["base_image"] == "python:3.11-slim"
    assert row["base_image_digest"] == "sha256:deadbeef"
    assert row["requirements_hash"] == "req-hash"
    assert row["build_args_json"] == '{"A":"1"}'
    assert row["build_context_json"] == '{"dockerfile_hash":"dockerfile-hash"}'


def test_stage_run_build_context_columns_persist(test_db: Database) -> None:
    """Stage run records can store build_context_hash and image_tag."""
    test_db.create_workspace_lineage("ws", description="test workspace")
    test_db.create_version(
        workspace_name="ws",
        version="v1",
        git_tag="ws-v1",
        git_sha="abc123",
        created_by="manual",
    )

    test_db.create_stage_run(
        stage_run_id="stage-abc123",
        workspace_name="ws",
        version="v1",
        stage_name="train",
        build_context_hash="hash-1",
        image_tag="img:hash-1",
    )

    row = test_db.get_stage_run("stage-abc123")
    assert row is not None
    assert row["build_context_hash"] == "hash-1"
    assert row["image_tag"] == "img:hash-1"

    test_db.update_stage_run_status(
        stage_run_id="stage-abc123",
        build_context_hash="hash-2",
        image_tag="img:hash-2",
    )

    row2 = test_db.get_stage_run("stage-abc123")
    assert row2 is not None
    assert row2["build_context_hash"] == "hash-2"
    assert row2["image_tag"] == "img:hash-2"
