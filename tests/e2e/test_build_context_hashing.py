"""E2E tests for build_context_hash image caching and rebuild behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from goldfish.config import GCEConfig
from goldfish.infra import docker_builder as docker_builder_module
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.svs.config import SVSConfig


@pytest.fixture
def build_hash_executor(test_db, test_config, tmp_path, monkeypatch):
    """Create a StageExecutor configured for remote (cloud build) image builds."""
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
    monkeypatch.setattr(executor.docker_builder, "capture_pip_freeze_from_image", lambda *_args, **_kwargs: None)

    return executor, workspace_path, mock_image_builder


def test_dockerfile_change_triggers_rebuild(build_hash_executor, monkeypatch) -> None:
    """Changing generated Dockerfile content must trigger an image rebuild."""
    executor, _workspace_path, mock_image_builder = build_hash_executor

    dockerfiles = iter(
        [
            "FROM python:3.11-slim\nRUN echo first\n",
            "FROM python:3.11-slim\nRUN echo second\n",
        ]
    )
    monkeypatch.setattr(
        executor.docker_builder,
        "generate_dockerfile",
        lambda *_args, **_kwargs: next(dockerfiles),
    )

    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)
    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert hash_1 != hash_2
    assert mock_image_builder.build.call_count == 2


def test_identical_context_reuses_image(build_hash_executor) -> None:
    """Re-running with an identical build context must reuse the cached image."""
    executor, _workspace_path, mock_image_builder = build_hash_executor

    image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)
    image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert image_tag_1 == image_tag_2
    assert hash_1 == hash_2
    assert mock_image_builder.build.call_count == 1


def test_base_image_change_triggers_rebuild(build_hash_executor, monkeypatch) -> None:
    """Changing the resolved base image must trigger an image rebuild."""
    import goldfish.jobs._stage_executor_impl as stage_executor_impl

    executor, _workspace_path, mock_image_builder = build_hash_executor

    monkeypatch.setattr(
        stage_executor_impl,
        "resolve_compute_profile",
        lambda *_args, **_kwargs: {"base_image": "custom-base"},
    )

    base_images = iter(["example-base:one", "example-base:two"])
    monkeypatch.setattr(
        stage_executor_impl,
        "resolve_profile_base_image",
        lambda *_args, **_kwargs: next(base_images),
    )

    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name="cpu-small")
    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name="cpu-small")

    assert hash_1 != hash_2
    assert mock_image_builder.build.call_count == 2


def test_requirements_change_triggers_rebuild(build_hash_executor) -> None:
    """Changing requirements.txt must trigger an image rebuild."""
    executor, workspace_path, mock_image_builder = build_hash_executor

    (workspace_path / "requirements.txt").write_text("numpy==1.26.0\n")
    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    (workspace_path / "requirements.txt").write_text("numpy==1.26.0\npandas==2.0.0\n")
    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert hash_1 != hash_2
    assert mock_image_builder.build.call_count == 2


def test_svs_config_change_triggers_rebuild(build_hash_executor) -> None:
    """Enabling SVS (agent CLI install) must trigger an image rebuild."""
    executor, _workspace_path, mock_image_builder = build_hash_executor

    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    config_with_svs = executor.config.model_copy(
        update={
            "svs": SVSConfig(
                enabled=True,
                ai_post_run_enabled=True,
                agent_provider="anthropic_api",
            )
        }
    )
    executor.config = config_with_svs
    executor.docker_builder.config = config_with_svs

    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert hash_1 != hash_2
    assert mock_image_builder.build.call_count == 2


def test_build_context_hash_deterministic_e2e(build_hash_executor) -> None:
    """Invariant: identical build context yields the same build_context_hash."""
    executor, _workspace_path, mock_image_builder = build_hash_executor

    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)
    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert hash_1 == hash_2
    assert mock_image_builder.build.call_count == 1


def test_different_hash_triggers_rebuild_e2e(build_hash_executor) -> None:
    """Invariant: changing any build input triggers a rebuild (cache miss)."""
    executor, workspace_path, mock_image_builder = build_hash_executor

    (workspace_path / "requirements.txt").write_text("numpy==1.26.0\n")
    _image_tag_1, hash_1 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    (workspace_path / "requirements.txt").write_text("numpy==1.26.0\npandas==2.0.0\n")
    _image_tag_2, hash_2 = executor._build_docker_image("test_ws", "v1", profile_name=None)

    assert hash_1 != hash_2
    assert mock_image_builder.build.call_count == 2
