"""Unit tests for build context hash integration in stage image builds."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import UTC, datetime
from unittest.mock import MagicMock

from goldfish.config import GCEConfig, GoldfishConfig
from goldfish.infra import docker_builder as docker_builder_module
from goldfish.infra.docker_builder import DockerBuilder, compute_build_context_hash, resolve_base_image_digest
from goldfish.jobs.stage_executor import StageExecutor


def test_build_context_cache_hit(test_db, test_config, tmp_path, monkeypatch) -> None:
    """Same build context reuses an existing cached image (no rebuild)."""
    monkeypatch.setattr(docker_builder_module, "compute_goldfish_runtime_hash", lambda *args, **kwargs: "0" * 64)

    config = test_config.model_copy(
        deep=True,
        update={
            "gce": GCEConfig(
                project_id="test-proj",
                artifact_registry="us-docker.pkg.dev/test-proj/goldfish",
                zones=["us-central1-a"],
            )
        },
    )

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    (workspace_path / "modules").mkdir(parents=True)
    (workspace_path / "modules" / "train.py").write_text("# test module\n")
    (workspace_path / "configs").mkdir()

    # Ensure the version record exists so BuildContext.git_sha is populated.
    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version(
        workspace_name="test_ws",
        version="v1",
        git_tag="test_ws-v1",
        git_sha="deadbeef",
        created_by="run",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

    mock_caps = MagicMock()
    mock_caps.has_launch_delay = True
    mock_caps.timeout_becomes_pending = True
    mock_backend = MagicMock()
    mock_backend.capabilities = mock_caps

    mock_image_builder = MagicMock()

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=mock_workspace_manager,
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
        run_backend=mock_backend,
        image_builder=mock_image_builder,
    )

    # Compute the expected build_context_hash for this workspace/version.
    with executor.docker_builder.prepare_build_context(workspace_path, "test_ws", "v1") as (
        build_ctx,
        _context_path,
        _dockerfile_path,
        _local_tag,
    ):
        build_context_hash = compute_build_context_hash(build_ctx)

    cached_registry_tag = "us-docker.pkg.dev/test-proj/goldfish/goldfish-test_ws-v1"
    build_id = "build-1234abcd"
    now = datetime.now(UTC).isoformat()
    test_db.insert_docker_build(
        build_id=build_id,
        image_type="cpu",
        target="workspace",
        backend="cloud",
        started_at=now,
        registry_tag=cached_registry_tag,
        cloud_build_id=None,
        workspace_name="test_ws",
        version="v1",
        content_hash=build_context_hash,
    )
    test_db.update_docker_build_status(build_id, status="completed", completed_at=now, registry_tag=cached_registry_tag)

    image_tag, returned_hash = executor._build_docker_image("test_ws", "v1", profile_name=None)
    assert image_tag == cached_registry_tag
    assert returned_hash == build_context_hash
    mock_image_builder.build.assert_not_called()


def test_build_context_cache_miss(test_db, test_config, tmp_path, monkeypatch) -> None:
    """Different build context triggers a rebuild."""
    monkeypatch.setattr(docker_builder_module, "compute_goldfish_runtime_hash", lambda *args, **kwargs: "0" * 64)

    config = test_config.model_copy(
        deep=True,
        update={
            "gce": GCEConfig(
                project_id="test-proj",
                artifact_registry="us-docker.pkg.dev/test-proj/goldfish",
                zones=["us-central1-a"],
            )
        },
    )

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    (workspace_path / "modules").mkdir(parents=True)
    (workspace_path / "modules" / "train.py").write_text("# test module\n")
    (workspace_path / "configs").mkdir()

    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version(
        workspace_name="test_ws",
        version="v1",
        git_tag="test_ws-v1",
        git_sha="deadbeef",
        created_by="run",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

    mock_caps = MagicMock()
    mock_caps.has_launch_delay = True
    mock_caps.timeout_becomes_pending = True
    mock_backend = MagicMock()
    mock_backend.capabilities = mock_caps

    mock_image_builder = MagicMock()
    mock_image_builder.build.return_value = "us-docker.pkg.dev/test-proj/goldfish/goldfish-test_ws-v1"

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=mock_workspace_manager,
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
        run_backend=mock_backend,
        image_builder=mock_image_builder,
    )

    # Mock capture_pip_freeze_from_image to avoid real Docker calls
    monkeypatch.setattr(executor.docker_builder, "capture_pip_freeze_from_image", lambda _tag: None)

    # Compute the expected build_context_hash for this workspace/version.
    with executor.docker_builder.prepare_build_context(workspace_path, "test_ws", "v1") as (
        build_ctx,
        _context_path,
        _dockerfile_path,
        _local_tag,
    ):
        build_context_hash = compute_build_context_hash(build_ctx)

    # Insert a completed build with a *different* hash; should NOT be reused.
    cached_registry_tag = "us-docker.pkg.dev/test-proj/goldfish/goldfish-test_ws-v999"
    build_id = "build-deadbeef"
    now = datetime.now(UTC).isoformat()
    test_db.insert_docker_build(
        build_id=build_id,
        image_type="cpu",
        target="workspace",
        backend="cloud",
        started_at=now,
        registry_tag=cached_registry_tag,
        cloud_build_id=None,
        workspace_name="test_ws",
        version="v999",
        content_hash="f" * 64,
    )
    test_db.update_docker_build_status(build_id, status="completed", completed_at=now, registry_tag=cached_registry_tag)

    image_tag, returned_hash = executor._build_docker_image("test_ws", "v1", profile_name=None)
    assert image_tag == "us-docker.pkg.dev/test-proj/goldfish/goldfish-test_ws-v1"
    assert returned_hash == build_context_hash
    mock_image_builder.build.assert_called_once()

    build_row = test_db.get_docker_build_by_workspace("test_ws", "v1")
    assert build_row is not None
    assert build_row["dockerfile_hash"]
    assert build_row["git_sha"] == "deadbeef"
    assert build_row["goldfish_runtime_hash"] == "0" * 64
    assert build_row["base_image"]
    assert build_row["requirements_hash"]

    assert build_row["build_args_json"] is not None
    build_args = json.loads(build_row["build_args_json"])
    assert build_args["VERSION"] == "deadbeef"
    assert "SVS_AGENT_CLI_PACKAGES" in build_args

    assert build_row["build_context_json"] is not None
    build_ctx = json.loads(build_row["build_context_json"])
    assert build_ctx["git_sha"] == "deadbeef"
    assert build_ctx["goldfish_runtime_hash"] == "0" * 64


def test_digest_resolution_fallback(monkeypatch, caplog) -> None:
    """Registry digest resolution should fail open (log + continue)."""

    def fake_run(*_args, **_kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["gcloud"], 1, stdout="", stderr="registry down")

    monkeypatch.setattr(docker_builder_module.subprocess, "run", fake_run)

    with caplog.at_level(logging.WARNING):
        assert resolve_base_image_digest("us-docker.pkg.dev/test-proj/repo/image:tag") is None

    assert any("digest resolution failed" in rec.message for rec in caplog.records)


def test_resolve_base_image_digest_success(monkeypatch, caplog) -> None:
    """Digest resolution should return a sha256:... digest on success."""
    digest = f"sha256:{'a' * 64}"

    def fake_run(*_args, **_kwargs) -> subprocess.CompletedProcess[str]:
        payload = json.dumps({"image_summary": {"digest": digest}})
        return subprocess.CompletedProcess(["gcloud"], 0, stdout=payload, stderr="")

    monkeypatch.setattr(docker_builder_module.subprocess, "run", fake_run)

    with caplog.at_level(logging.WARNING):
        assert resolve_base_image_digest("us-docker.pkg.dev/test-proj/repo/image:tag") == digest

    assert not caplog.records


def test_generate_dockerfile_writes_pip_freeze(tmp_path) -> None:
    """The generated Dockerfile must capture pip freeze into /app/pip-freeze.txt."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module\n")
    (workspace_path / "configs").mkdir()

    config = GoldfishConfig(project_name="test", dev_repo_path=".")
    builder = DockerBuilder(config)
    dockerfile = builder.generate_dockerfile(workspace_dir=workspace_path, base_image="python:3.11-slim")

    assert "python -m pip freeze > /app/pip-freeze.txt" in dockerfile


def test_pip_freeze_stored_in_docker_build_context_json(test_db, test_config, tmp_path, monkeypatch) -> None:
    """A successful image build stores pip freeze output in docker_builds.build_context_json."""
    monkeypatch.setattr(docker_builder_module, "compute_goldfish_runtime_hash", lambda *args, **kwargs: "0" * 64)

    config = test_config.model_copy(
        deep=True,
        update={
            "gce": GCEConfig(
                project_id="test-proj",
                artifact_registry="us-docker.pkg.dev/test-proj/goldfish",
                zones=["us-central1-a"],
            )
        },
    )

    project_root = tmp_path / "project"
    project_root.mkdir()
    dev_repo = config.get_dev_repo_path(project_root)
    dev_repo.mkdir(parents=True, exist_ok=True)

    workspace_path = project_root / "workspaces" / "test_ws"
    (workspace_path / "modules").mkdir(parents=True)
    (workspace_path / "modules" / "train.py").write_text("# test module\n")
    (workspace_path / "configs").mkdir()

    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version(
        workspace_name="test_ws",
        version="v1",
        git_tag="test_ws-v1",
        git_sha="deadbeef",
        created_by="run",
    )

    mock_workspace_manager = MagicMock()
    mock_workspace_manager.get_workspace_path.return_value = workspace_path

    mock_caps = MagicMock()
    mock_caps.has_launch_delay = True
    mock_caps.timeout_becomes_pending = True
    mock_backend = MagicMock()
    mock_backend.capabilities = mock_caps

    registry_tag = "us-docker.pkg.dev/test-proj/goldfish/goldfish-test_ws-v1"
    mock_image_builder = MagicMock()
    mock_image_builder.build.return_value = registry_tag

    def fake_run(cmd, capture_output=True, text=True, check=False, timeout=None):
        assert cmd[:3] == ["docker", "run", "--rm"]
        return subprocess.CompletedProcess(cmd, 0, stdout="numpy==1.0.0\n", stderr="")

    monkeypatch.setattr(docker_builder_module.subprocess, "run", fake_run)

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=mock_workspace_manager,
        pipeline_manager=MagicMock(),
        project_root=project_root,
        dataset_registry=None,
        run_backend=mock_backend,
        image_builder=mock_image_builder,
    )

    executor._build_docker_image("test_ws", "v1", profile_name=None)

    build_row = test_db.get_docker_build_by_workspace("test_ws", "v1")
    assert build_row is not None
    assert build_row["build_context_json"] is not None
    build_ctx = json.loads(build_row["build_context_json"])
    assert build_ctx["pip_freeze"] == "numpy==1.0.0\n"


def test_prepare_build_context_warns_on_unpinned_requirements(tmp_path, caplog) -> None:
    """requirements.txt with >= or missing pins should log a warning."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module\n")
    (workspace_path / "configs").mkdir()

    (workspace_path / "requirements.txt").write_text(
        "\n".join(
            [
                "numpy>=1.20.0",
                "pandas",
                "scipy==1.0.0",
                "",
            ]
        )
        + "\n"
    )

    config = GoldfishConfig(project_name="test", dev_repo_path=".")
    builder = DockerBuilder(config)

    with caplog.at_level(logging.WARNING):
        with builder.prepare_build_context(workspace_path, "ws", "v1"):
            pass

    assert any("unpinned" in rec.message.lower() for rec in caplog.records)
