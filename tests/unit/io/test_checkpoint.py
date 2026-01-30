"""Tests for goldfish.io checkpoint functionality.

Checkpoints provide immediate storage upload for resume functionality,
critical for preemptible/spot instances that can be terminated with ~30s notice.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.contracts import StorageURI


def _create_mock_storage():
    """Create a mock storage adapter."""
    mock_storage = MagicMock()
    mock_storage.put = MagicMock()
    mock_storage.get = MagicMock(return_value=b"test data")
    mock_storage.exists = MagicMock(return_value=True)
    mock_storage.delete = MagicMock()
    mock_storage.list_prefix = MagicMock(return_value=[])
    mock_storage.download_to_file = MagicMock(return_value=True)
    mock_storage.get_size = MagicMock(return_value=100)
    return mock_storage


class TestSaveCheckpoint:
    """Test save_checkpoint immediate upload."""

    def test_save_checkpoint_uploads_directory_to_gcs(self, tmp_path, monkeypatch):
        """save_checkpoint should immediately upload directory to storage."""
        from goldfish.io import save_checkpoint

        # Setup environment
        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        # Create checkpoint data
        ckpt_dir = tmp_path / "model_checkpoint"
        ckpt_dir.mkdir()
        (ckpt_dir / "model.pt").write_bytes(b"fake model weights")
        (ckpt_dir / "optimizer.pt").write_bytes(b"fake optimizer state")

        # Mock the storage adapter
        mock_storage = _create_mock_storage()

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            save_checkpoint("model", ckpt_dir)

            # Should have called put for each file
            assert mock_storage.put.call_count == 2

    def test_save_checkpoint_uploads_file_to_gcs(self, tmp_path, monkeypatch):
        """save_checkpoint should handle single file upload."""
        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        # Create single file checkpoint
        ckpt_file = tmp_path / "checkpoint.pt"
        ckpt_file.write_bytes(b"checkpoint data")

        mock_storage = _create_mock_storage()

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            save_checkpoint("latest", ckpt_file)

            mock_storage.put.assert_called_once()
            # Verify the URI contains the expected path
            call_args = mock_storage.put.call_args[0]
            uri = call_args[0]
            assert isinstance(uri, StorageURI)
            assert "checkpoints/stage-abc123/latest" in uri.path

    def test_save_checkpoint_with_step_creates_versioned_path(self, tmp_path, monkeypatch):
        """save_checkpoint with step should create versioned checkpoint path."""
        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        ckpt_dir = tmp_path / "ckpt"
        ckpt_dir.mkdir()
        (ckpt_dir / "model.pt").write_bytes(b"weights")

        mock_storage = _create_mock_storage()

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            save_checkpoint("model", ckpt_dir, step=1000)

            # Should include step in path
            call_args = mock_storage.put.call_args[0]
            uri = call_args[0]
            assert "step_1000" in uri.path

    def test_save_checkpoint_raises_without_gcs_bucket(self, tmp_path, monkeypatch):
        """save_checkpoint should raise if GCS bucket not configured."""
        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        # No GOLDFISH_GCS_BUCKET set

        ckpt_file = tmp_path / "checkpoint.pt"
        ckpt_file.write_bytes(b"data")

        with pytest.raises(RuntimeError, match="GCS bucket not configured"):
            save_checkpoint("model", ckpt_file)

    def test_save_checkpoint_raises_on_upload_failure(self, tmp_path, monkeypatch):
        """save_checkpoint should raise on storage upload failure."""
        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        ckpt_file = tmp_path / "checkpoint.pt"
        ckpt_file.write_bytes(b"data")

        mock_storage = _create_mock_storage()
        mock_storage.put.side_effect = Exception("Access denied")

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            with pytest.raises(RuntimeError, match="Checkpoint upload failed"):
                save_checkpoint("model", ckpt_file)

    def test_save_checkpoint_accepts_numpy_array(self, tmp_path, monkeypatch):
        """save_checkpoint should accept numpy array and save as .npy."""
        pytest.importorskip("numpy")
        import numpy as np

        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        weights = np.random.randn(100, 100)

        mock_storage = _create_mock_storage()

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            save_checkpoint("weights", weights)

            # Should have uploaded as .npy file
            call_args = mock_storage.put.call_args[0]
            uri = call_args[0]
            assert "weights.npy" in uri.path
            mock_storage.put.assert_called_once()


class TestLoadCheckpoint:
    """Test load_checkpoint for resume functionality."""

    def test_load_checkpoint_downloads_from_gcs(self, tmp_path, monkeypatch):
        """load_checkpoint should download checkpoint from storage."""
        from goldfish.io import load_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        mock_storage = _create_mock_storage()
        mock_storage.exists.return_value = True
        mock_storage.get.return_value = b"checkpoint data"

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            result = load_checkpoint("model")

            assert result is not None
            assert isinstance(result, Path)
            # exists is called to check if file exists
            assert mock_storage.exists.call_count >= 1
            # get is called to download the data
            mock_storage.get.assert_called_once()

    def test_load_checkpoint_with_step(self, tmp_path, monkeypatch):
        """load_checkpoint with step should load specific version."""
        from goldfish.io import load_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        def mock_download(uri, local_path):
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).write_bytes(b"checkpoint data")
            return True

        mock_storage = _create_mock_storage()
        mock_storage.exists.return_value = True
        mock_storage.download_to_file.side_effect = mock_download

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            load_checkpoint("model", step=1000)

            # Verify exists was called with step path
            call_args = mock_storage.exists.call_args_list[0][0]
            uri = call_args[0]
            assert "step_1000" in uri.path

    def test_load_checkpoint_returns_none_if_not_found(self, tmp_path, monkeypatch):
        """load_checkpoint should return None if checkpoint doesn't exist."""
        from goldfish.io import load_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        mock_storage = _create_mock_storage()
        mock_storage.exists.return_value = False
        mock_storage.list_prefix.return_value = []

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            result = load_checkpoint("nonexistent")

            assert result is None

    def test_load_checkpoint_from_previous_run(self, tmp_path, monkeypatch):
        """load_checkpoint should support loading from a different run_id."""
        from goldfish.io import load_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-new123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        def mock_download(uri, local_path):
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).write_bytes(b"checkpoint data")
            return True

        mock_storage = _create_mock_storage()
        mock_storage.exists.return_value = True
        mock_storage.download_to_file.side_effect = mock_download

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            load_checkpoint("model", run_id="stage-old456")

            # Should use the specified run_id, not current
            call_args = mock_storage.exists.call_args_list[0][0]
            uri = call_args[0]
            assert "stage-old456" in uri.path
            assert "stage-new123" not in uri.path


class TestListCheckpoints:
    """Test list_checkpoints functionality."""

    def test_list_checkpoints_returns_available_checkpoints(self, tmp_path, monkeypatch):
        """list_checkpoints should return available checkpoint names and steps."""
        from goldfish.io import list_checkpoints

        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        # Create mock URIs
        mock_uris = [
            StorageURI("gs", "my-bucket", "checkpoints/stage-abc123/model/model.pt"),
            StorageURI("gs", "my-bucket", "checkpoints/stage-abc123/model/step_1000/model.pt"),
            StorageURI("gs", "my-bucket", "checkpoints/stage-abc123/model/step_2000/model.pt"),
            StorageURI("gs", "my-bucket", "checkpoints/stage-abc123/optimizer/optimizer.pt"),
        ]

        mock_storage = _create_mock_storage()
        mock_storage.list_prefix.return_value = mock_uris

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            result = list_checkpoints()

            assert "model" in result
            assert "optimizer" in result
            assert 1000 in result.get("model", {}).get("steps", [])
            assert 2000 in result.get("model", {}).get("steps", [])

    def test_list_checkpoints_empty_when_none_exist(self, tmp_path, monkeypatch):
        """list_checkpoints should return empty dict when no checkpoints."""
        from goldfish.io import list_checkpoints

        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        monkeypatch.setenv("GOLDFISH_GCS_BUCKET", "gs://my-bucket")

        mock_storage = _create_mock_storage()
        mock_storage.list_prefix.return_value = []

        with patch("goldfish.io._get_storage_adapter", return_value=mock_storage):
            result = list_checkpoints()

            assert result == {}


class TestCheckpointLocalFallback:
    """Test checkpoint behavior when GCS is not available (local dev)."""

    def test_save_checkpoint_local_fallback(self, tmp_path, monkeypatch):
        """save_checkpoint should save locally when GCS not configured and local_ok=True."""
        from goldfish.io import save_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        # No GOLDFISH_GCS_BUCKET - local mode

        ckpt_file = tmp_path / "checkpoint.pt"
        ckpt_file.write_bytes(b"local checkpoint")

        # Should not raise with local_ok=True
        save_checkpoint("model", ckpt_file, local_ok=True)

        # Should save to local checkpoints directory
        local_ckpt = outputs_dir / ".goldfish" / "checkpoints" / "model"
        assert local_ckpt.exists()

    def test_load_checkpoint_local_fallback(self, tmp_path, monkeypatch):
        """load_checkpoint should check local first, then storage."""
        from goldfish.io import load_checkpoint

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))
        monkeypatch.setenv("GOLDFISH_RUN_ID", "stage-abc123")
        # No GOLDFISH_GCS_BUCKET

        # Create local checkpoint
        local_ckpt_dir = outputs_dir / ".goldfish" / "checkpoints" / "model"
        local_ckpt_dir.mkdir(parents=True)
        (local_ckpt_dir / "weights.pt").write_bytes(b"local weights")

        result = load_checkpoint("model")

        assert result is not None
        assert result == local_ckpt_dir
