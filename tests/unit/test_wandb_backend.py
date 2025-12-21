"""Unit tests for W&B metrics backend."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from goldfish.metrics.backends.wandb import WandBBackend


# Mock wandb module for all tests
@pytest.fixture(autouse=True)
def mock_wandb():
    """Mock wandb module for testing."""
    mock = MagicMock()
    sys.modules["wandb"] = mock
    yield mock
    if "wandb" in sys.modules:
        del sys.modules["wandb"]


class TestWandBBackend:
    """Tests for WandBBackend."""

    def test_name(self):
        """Test backend name is 'wandb'."""
        assert WandBBackend.name() == "wandb"

    def test_is_available_when_wandb_installed(self):
        """Test is_available returns True when wandb is importable."""
        with patch("goldfish.metrics.backends.wandb.importlib.util.find_spec") as mock_find:
            mock_find.return_value = MagicMock()  # wandb is installed
            assert WandBBackend.is_available() is True

    def test_is_available_when_wandb_not_installed(self):
        """Test is_available returns False when wandb is not importable."""
        with patch("goldfish.metrics.backends.wandb.importlib.util.find_spec") as mock_find:
            mock_find.return_value = None  # wandb is not installed
            assert WandBBackend.is_available() is False

    def test_init_run_creates_wandb_run(self, mock_wandb):
        """Test init_run creates a W&B run with correct metadata."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run

        backend = WandBBackend()
        backend.init_run(
            run_id="stage-abc123",
            config={"lr": 0.01, "batch_size": 32},
            workspace="baseline_lstm",
            stage="train",
        )

        # Should call wandb.init with proper metadata
        mock_wandb.init.assert_called_once()
        call_kwargs = mock_wandb.init.call_args[1]

        assert call_kwargs["name"] == "train-stage-abc123"
        assert call_kwargs["config"] == {"lr": 0.01, "batch_size": 32}
        assert "baseline_lstm" in call_kwargs["tags"]
        assert "train" in call_kwargs["tags"]
        assert call_kwargs["notes"] == "Goldfish run stage-abc123"

    def test_init_run_uses_git_sha_from_env(self, mock_wandb, monkeypatch):
        """Test init_run passes git SHA to W&B from environment."""
        monkeypatch.setenv("GOLDFISH_GIT_SHA", "abc123def456")
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run(
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        # Should create Settings with git_commit
        mock_wandb.Settings.assert_called_once_with(git_commit="abc123def456")

    def test_init_run_uses_project_from_env(self, mock_wandb, monkeypatch):
        """Test init_run uses project from GOLDFISH_WANDB_PROJECT env var."""
        monkeypatch.setenv("GOLDFISH_WANDB_PROJECT", "my-custom-project")
        monkeypatch.setenv("GOLDFISH_GIT_SHA", "abc123")
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run(
            run_id="stage-abc123",
            config={},
            workspace="test",
            stage="train",
        )

        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["project"] == "my-custom-project"

    def test_init_run_defaults_to_workspace_as_project(self, mock_wandb):
        """Test init_run uses workspace name as project when not specified."""
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run(
            run_id="stage-abc123",
            config={},
            workspace="baseline_lstm",
            stage="train",
        )

        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["project"] == "goldfish-baseline_lstm"

    def test_log_metric_calls_wandb_log(self, mock_wandb):
        """Test log_metric calls wandb.log with correct data."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")
        backend.log_metric("loss", 0.5, step=10)

        # Should call wandb.log
        mock_wandb.log.assert_called_once_with({"loss": 0.5}, step=10)

    def test_log_metric_without_step(self, mock_wandb):
        """Test log_metric works without step parameter."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")
        backend.log_metric("accuracy", 0.92)

        # Should call wandb.log without step
        mock_wandb.log.assert_called_once_with({"accuracy": 0.92}, step=None)

    def test_log_metrics_batch(self, mock_wandb):
        """Test log_metrics logs multiple metrics at once."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")
        backend.log_metrics({"accuracy": 0.92, "f1": 0.88}, step=10)

        # Should call wandb.log with all metrics
        mock_wandb.log.assert_called_once_with({"accuracy": 0.92, "f1": 0.88}, step=10)

    def test_log_artifact_saves_file(self, mock_wandb, tmp_path):
        """Test log_artifact saves artifact to W&B."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        artifact_path = tmp_path / "model.pt"
        artifact_path.touch()  # Create file
        backend.log_artifact("model", artifact_path)

        # Should call wandb.save
        mock_wandb.save.assert_called_once_with(str(artifact_path), base_path=str(tmp_path))

    def test_log_artifact_with_directory(self, mock_wandb, tmp_path):
        """Test log_artifact handles directories."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        artifact_path = tmp_path / "checkpoints"
        artifact_path.mkdir()  # Create directory
        backend.log_artifact("checkpoints", artifact_path)

        # Should call wandb.save with glob pattern for directory
        mock_wandb.save.assert_called_once_with(str(artifact_path / "*"), base_path=str(tmp_path))

    def test_finish_returns_run_url(self, mock_wandb):
        """Test finish returns W&B run URL."""
        mock_run = MagicMock()
        mock_run.url = "https://wandb.ai/team/project/runs/abc123"
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        url = backend.finish()

        # Should call wandb.finish and return URL
        mock_wandb.finish.assert_called_once()
        assert url == "https://wandb.ai/team/project/runs/abc123"

    def test_finish_without_init(self, mock_wandb):
        """Test finish works even if init_run wasn't called."""
        backend = WandBBackend()
        url = backend.finish()

        # Should call wandb.finish anyway
        mock_wandb.finish.assert_called_once()
        assert url is None

    def test_multiple_metrics_logged_in_sequence(self, mock_wandb):
        """Test that multiple metrics can be logged sequentially."""
        mock_run = MagicMock()
        mock_wandb.init.return_value = mock_run
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        backend.log_metric("loss", 0.5, step=1)
        backend.log_metric("loss", 0.4, step=2)
        backend.log_metric("loss", 0.3, step=3)

        # Should have 3 wandb.log calls
        assert mock_wandb.log.call_count == 3

    def test_entity_from_env(self, mock_wandb, monkeypatch):
        """Test init_run uses entity from GOLDFISH_WANDB_ENTITY env var."""
        monkeypatch.setenv("GOLDFISH_WANDB_ENTITY", "my-team")
        monkeypatch.setenv("GOLDFISH_GIT_SHA", "abc123")
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["entity"] == "my-team"

    def test_group_defaults_to_workspace(self, mock_wandb):
        """Test init_run defaults group to workspace name."""
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "baseline_lstm", "train")

        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["group"] == "baseline_lstm"

    def test_group_from_env(self, mock_wandb, monkeypatch):
        """Test init_run uses group from GOLDFISH_WANDB_GROUP env var."""
        monkeypatch.setenv("GOLDFISH_WANDB_GROUP", "experiment-batch-1")
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "test", "train")

        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["group"] == "experiment-batch-1"

    def test_project_uses_goldfish_project_name(self, mock_wandb, monkeypatch):
        """Test init_run defaults to GOLDFISH_PROJECT_NAME when available."""
        monkeypatch.setenv("GOLDFISH_PROJECT_NAME", "sales-forecasting")
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "baseline_lstm", "train")

        call_kwargs = mock_wandb.init.call_args[1]
        # Should use project name, not workspace
        assert call_kwargs["project"] == "sales-forecasting"
        # Should still group by workspace
        assert call_kwargs["group"] == "baseline_lstm"

    def test_project_priority_override_wins(self, mock_wandb, monkeypatch):
        """Test GOLDFISH_WANDB_PROJECT overrides GOLDFISH_PROJECT_NAME."""
        monkeypatch.setenv("GOLDFISH_PROJECT_NAME", "sales-forecasting")
        monkeypatch.setenv("GOLDFISH_WANDB_PROJECT", "custom-override")
        mock_wandb.Settings = MagicMock()

        backend = WandBBackend()
        backend.init_run("stage-abc123", {}, "baseline_lstm", "train")

        call_kwargs = mock_wandb.init.call_args[1]
        # Override should win
        assert call_kwargs["project"] == "custom-override"
