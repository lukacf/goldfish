"""Unit tests for W&B backend artifact handling."""

from __future__ import annotations

import sys

import pytest


class DummyArtifact:
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.type = type
        self.files: list[str] = []
        self.dirs: list[str] = []
        self.url = f"http://example.com/artifact/{name}"

    def add_file(self, path: str) -> None:
        self.files.append(path)

    def add_dir(self, path: str) -> None:
        self.dirs.append(path)


class DummyRun:
    def __init__(self) -> None:
        self.url = "http://example.com/run/123"
        self.logged: list[DummyArtifact] = []

    def log_artifact(self, artifact: DummyArtifact) -> DummyArtifact:
        self.logged.append(artifact)
        return artifact


class DummyWandb:
    def __init__(self) -> None:
        self.saved: list[str] = []
        self._run = DummyRun()

    class Settings:
        def __init__(self, git_commit: str | None = None) -> None:
            self.git_commit = git_commit

    Artifact = DummyArtifact

    def init(self, **kwargs) -> DummyRun:
        return self._run

    def save(self, path: str, base_path: str | None = None) -> None:
        self.saved.append(path)

    def finish(self) -> None:
        return None


def test_wandb_artifact_mode_uses_artifact(tmp_path, monkeypatch):
    """Artifact mode should use wandb.Artifact instead of wandb.save."""
    dummy = DummyWandb()
    monkeypatch.setitem(sys.modules, "wandb", dummy)
    monkeypatch.setenv("GOLDFISH_WANDB_ARTIFACT_MODE", "artifact")
    monkeypatch.setenv("GOLDFISH_WANDB_ARTIFACT_TYPE", "model")

    from goldfish.metrics.backends.wandb import WandBBackend

    backend = WandBBackend()
    backend.init_run(run_id="stage-123", config={}, workspace="ws", stage="train")

    file_path = tmp_path / "model.pt"
    file_path.write_text("data")

    url = backend.log_artifact("model", file_path)

    assert dummy.saved == []
    assert dummy._run.logged
    logged = dummy._run.logged[0]
    assert logged.type == "model"
    assert str(file_path) in logged.files
    assert url == logged.url


def test_wandb_artifact_mode_rejects_symlink_dir(tmp_path, monkeypatch):
    """Artifact logging should reject directories containing symlinks."""
    dummy = DummyWandb()
    monkeypatch.setitem(sys.modules, "wandb", dummy)
    monkeypatch.setenv("GOLDFISH_WANDB_ARTIFACT_MODE", "artifact")

    from goldfish.metrics.backends.wandb import WandBBackend
    from goldfish.validation import InvalidArtifactPathError

    backend = WandBBackend()
    backend.init_run(run_id="stage-123", config={}, workspace="ws", stage="train")

    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "model.pt").write_text("data")

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    (root / "link").symlink_to(outside, target_is_directory=True)

    with pytest.raises(InvalidArtifactPathError):
        backend.log_artifact("model", root)


def test_wandb_invalid_artifact_mode_defaults_to_file(tmp_path, monkeypatch, caplog):
    """Invalid artifact mode should warn and default to file."""
    dummy = DummyWandb()
    monkeypatch.setitem(sys.modules, "wandb", dummy)
    monkeypatch.setenv("GOLDFISH_WANDB_ARTIFACT_MODE", "nope")

    from goldfish.metrics.backends.wandb import WandBBackend

    backend = WandBBackend()
    backend.init_run(run_id="stage-123", config={}, workspace="ws", stage="train")

    file_path = tmp_path / "model.pt"
    file_path.write_text("data")

    with caplog.at_level("WARNING"):
        backend.log_artifact("model", file_path)

    assert any("GOLDFISH_WANDB_ARTIFACT_MODE" in record.message for record in caplog.records)
    assert str(file_path) in dummy.saved
