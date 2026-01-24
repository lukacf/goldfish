"""Unit tests for GCSStorage adapter.

Tests the GCS implementation of the ObjectStorage protocol.
All google-cloud-storage SDK calls are mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.storage import GCSStorage
from goldfish.cloud.contracts import StorageURI
from goldfish.errors import NotFoundError, StorageError

# --- Fixtures ---


@pytest.fixture
def mock_storage_client():
    """Create a mock google.cloud.storage.Client."""
    with patch("goldfish.cloud.adapters.gcp.storage.storage.Client") as mock_class:
        mock_client = MagicMock()
        mock_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def storage(mock_storage_client):
    """Create a GCSStorage with mocked client."""
    return GCSStorage(project="test-project")


@pytest.fixture
def sample_uri():
    """Create a sample GCS URI."""
    return StorageURI(scheme="gs", bucket="test-bucket", path="path/to/file.txt")


@pytest.fixture
def sample_prefix_uri():
    """Create a sample GCS prefix URI."""
    return StorageURI(scheme="gs", bucket="test-bucket", path="data/")


# --- Initialization Tests ---


class TestGCSStorageInit:
    """Tests for GCSStorage initialization."""

    def test_init_creates_client_with_project(self, mock_storage_client):
        """Init creates storage client with specified project."""
        with patch("goldfish.cloud.adapters.gcp.storage.storage.Client") as mock_class:
            GCSStorage(project="my-project")
            mock_class.assert_called_once_with(project="my-project")

    def test_init_creates_client_with_none_project(self, mock_storage_client):
        """Init creates storage client with None project (uses default)."""
        with patch("goldfish.cloud.adapters.gcp.storage.storage.Client") as mock_class:
            GCSStorage()
            mock_class.assert_called_once_with(project=None)


# --- Put Tests ---


class TestGCSStoragePut:
    """Tests for put method."""

    def test_put_uploads_bytes(self, storage, mock_storage_client, sample_uri):
        """Put uploads bytes to GCS."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        storage.put(sample_uri, b"test content")

        mock_storage_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.txt")
        mock_blob.upload_from_string.assert_called_once_with(b"test content")

    def test_put_rejects_non_gs_scheme(self, storage):
        """Put rejects non-gs:// URIs."""
        uri = StorageURI(scheme="s3", bucket="bucket", path="path")

        with pytest.raises(StorageError) as exc_info:
            storage.put(uri, b"data")

        assert "only supports gs://" in str(exc_info.value)

    def test_put_raises_storage_error_on_failure(self, storage, mock_storage_client, sample_uri):
        """Put raises StorageError on upload failure."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = Exception("Permission denied")

        with pytest.raises(StorageError) as exc_info:
            storage.put(sample_uri, b"data")

        assert "Failed to upload" in str(exc_info.value)


# --- Get Tests ---


class TestGCSStorageGet:
    """Tests for get method."""

    def test_get_downloads_bytes(self, storage, mock_storage_client, sample_uri):
        """Get downloads bytes from GCS."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_bytes.return_value = b"file content"

        data = storage.get(sample_uri)

        assert data == b"file content"
        mock_storage_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.txt")

    def test_get_rejects_non_gs_scheme(self, storage):
        """Get rejects non-gs:// URIs."""
        uri = StorageURI(scheme="file", bucket="", path="/local/path")

        with pytest.raises(StorageError):
            storage.get(uri)

    def test_get_raises_not_found_error_when_missing(self, storage, mock_storage_client, sample_uri):
        """Get raises NotFoundError when object doesn't exist."""
        from google.cloud.exceptions import NotFound

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_bytes.side_effect = NotFound("Blob not found")

        with pytest.raises(NotFoundError):
            storage.get(sample_uri)

    def test_get_raises_storage_error_on_failure(self, storage, mock_storage_client, sample_uri):
        """Get raises StorageError on download failure."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_bytes.side_effect = Exception("Network error")

        with pytest.raises(StorageError) as exc_info:
            storage.get(sample_uri)

        assert "Failed to download" in str(exc_info.value)


# --- Exists Tests ---


class TestGCSStorageExists:
    """Tests for exists method."""

    def test_exists_returns_true_when_object_exists(self, storage, mock_storage_client, sample_uri):
        """Exists returns True when object exists."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        result = storage.exists(sample_uri)

        assert result is True

    def test_exists_returns_false_when_object_missing(self, storage, mock_storage_client, sample_uri):
        """Exists returns False when object doesn't exist."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        result = storage.exists(sample_uri)

        assert result is False

    def test_exists_rejects_non_gs_scheme(self, storage):
        """Exists rejects non-gs:// URIs."""
        uri = StorageURI(scheme="s3", bucket="bucket", path="path")

        with pytest.raises(StorageError):
            storage.exists(uri)

    def test_exists_returns_false_on_error(self, storage, mock_storage_client, sample_uri):
        """Exists returns False on error (fail-safe)."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.side_effect = Exception("Network timeout")

        result = storage.exists(sample_uri)

        assert result is False


# --- List Prefix Tests ---


class TestGCSStorageListPrefix:
    """Tests for list_prefix method."""

    def test_list_prefix_returns_matching_uris(self, storage, mock_storage_client, sample_prefix_uri):
        """List prefix returns URIs for matching objects."""
        mock_bucket = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket

        mock_blobs = [MagicMock(name="data/file1.txt"), MagicMock(name="data/file2.txt")]
        mock_blobs[0].name = "data/file1.txt"
        mock_blobs[1].name = "data/file2.txt"
        mock_bucket.list_blobs.return_value = mock_blobs

        result = storage.list_prefix(sample_prefix_uri)

        assert len(result) == 2
        assert all(isinstance(uri, StorageURI) for uri in result)
        mock_bucket.list_blobs.assert_called_once_with(prefix="data/")

    def test_list_prefix_returns_sorted_uris(self, storage, mock_storage_client, sample_prefix_uri):
        """List prefix returns URIs in lexicographic order."""
        mock_bucket = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket

        # Return in non-sorted order
        mock_blobs = [MagicMock(), MagicMock(), MagicMock()]
        mock_blobs[0].name = "data/z_file.txt"
        mock_blobs[1].name = "data/a_file.txt"
        mock_blobs[2].name = "data/m_file.txt"
        mock_bucket.list_blobs.return_value = mock_blobs

        result = storage.list_prefix(sample_prefix_uri)

        paths = [uri.path for uri in result]
        assert paths == ["data/a_file.txt", "data/m_file.txt", "data/z_file.txt"]

    def test_list_prefix_returns_empty_list_when_no_matches(self, storage, mock_storage_client, sample_prefix_uri):
        """List prefix returns empty list when no objects match."""
        mock_bucket = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        result = storage.list_prefix(sample_prefix_uri)

        assert result == []

    def test_list_prefix_rejects_non_gs_scheme(self, storage):
        """List prefix rejects non-gs:// URIs."""
        uri = StorageURI(scheme="s3", bucket="bucket", path="prefix/")

        with pytest.raises(StorageError):
            storage.list_prefix(uri)

    def test_list_prefix_returns_empty_on_error(self, storage, mock_storage_client, sample_prefix_uri):
        """List prefix returns empty list on error (fail-safe)."""
        mock_bucket = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.side_effect = Exception("Permission denied")

        result = storage.list_prefix(sample_prefix_uri)

        assert result == []


# --- Delete Tests ---


class TestGCSStorageDelete:
    """Tests for delete method."""

    def test_delete_removes_object(self, storage, mock_storage_client, sample_uri):
        """Delete removes object from GCS."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        storage.delete(sample_uri)

        mock_blob.delete.assert_called_once()

    def test_delete_is_idempotent_when_not_found(self, storage, mock_storage_client, sample_uri):
        """Delete is idempotent - no error when object doesn't exist."""
        from google.cloud.exceptions import NotFound

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.delete.side_effect = NotFound("Not found")

        # Should not raise
        storage.delete(sample_uri)

    def test_delete_rejects_non_gs_scheme(self, storage):
        """Delete rejects non-gs:// URIs."""
        uri = StorageURI(scheme="file", bucket="", path="/local")

        with pytest.raises(StorageError):
            storage.delete(uri)

    def test_delete_raises_storage_error_on_failure(self, storage, mock_storage_client, sample_uri):
        """Delete raises StorageError on non-NotFound failure."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.delete.side_effect = Exception("Permission denied")

        with pytest.raises(StorageError):
            storage.delete(sample_uri)


# --- Get Local Path Tests ---


class TestGCSStorageGetLocalPath:
    """Tests for get_local_path method."""

    def test_get_local_path_returns_none(self, storage, sample_uri):
        """Get local path returns None (GCS has no local paths)."""
        result = storage.get_local_path(sample_uri)
        assert result is None


# --- Download to File Tests ---


class TestGCSStorageDownloadToFile:
    """Tests for download_to_file method."""

    def test_download_to_file_writes_to_destination(self, storage, mock_storage_client, sample_uri, tmp_path):
        """Download to file writes object content to destination."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        dest = tmp_path / "downloaded.txt"
        result = storage.download_to_file(sample_uri, dest)

        assert result is True
        mock_blob.download_to_filename.assert_called_once_with(str(dest))

    def test_download_to_file_creates_parent_dirs(self, storage, mock_storage_client, sample_uri, tmp_path):
        """Download to file creates parent directories."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        dest = tmp_path / "nested" / "path" / "file.txt"
        storage.download_to_file(sample_uri, dest)

        assert dest.parent.exists()

    def test_download_to_file_returns_false_when_not_exists(self, storage, mock_storage_client, sample_uri, tmp_path):
        """Download to file returns False when object doesn't exist."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        dest = tmp_path / "missing.txt"
        result = storage.download_to_file(sample_uri, dest)

        assert result is False
        mock_blob.download_to_filename.assert_not_called()

    def test_download_to_file_returns_false_on_not_found_exception(
        self, storage, mock_storage_client, sample_uri, tmp_path
    ):
        """Download to file returns False on NotFound exception."""
        from google.cloud.exceptions import NotFound

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = NotFound("Not found")

        dest = tmp_path / "file.txt"
        result = storage.download_to_file(sample_uri, dest)

        assert result is False

    def test_download_to_file_rejects_non_gs_scheme(self, storage, tmp_path):
        """Download to file rejects non-gs:// URIs."""
        uri = StorageURI(scheme="s3", bucket="bucket", path="path")
        dest = tmp_path / "file.txt"

        with pytest.raises(StorageError):
            storage.download_to_file(uri, dest)

    def test_download_to_file_returns_false_on_error(self, storage, mock_storage_client, sample_uri, tmp_path):
        """Download to file returns False on other errors."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        mock_blob.download_to_filename.side_effect = Exception("Network error")

        dest = tmp_path / "file.txt"
        result = storage.download_to_file(sample_uri, dest)

        assert result is False


# --- Get Size Tests ---


class TestGCSStorageGetSize:
    """Tests for get_size method."""

    def test_get_size_returns_object_size(self, storage, mock_storage_client, sample_uri):
        """Get size returns object size in bytes."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.size = 1024

        result = storage.get_size(sample_uri)

        assert result == 1024
        mock_blob.reload.assert_called_once()

    def test_get_size_returns_none_when_not_found(self, storage, mock_storage_client, sample_uri):
        """Get size returns None when object doesn't exist."""
        from google.cloud.exceptions import NotFound

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.reload.side_effect = NotFound("Not found")

        result = storage.get_size(sample_uri)

        assert result is None

    def test_get_size_rejects_non_gs_scheme(self, storage):
        """Get size rejects non-gs:// URIs."""
        uri = StorageURI(scheme="file", bucket="", path="/local")

        with pytest.raises(StorageError):
            storage.get_size(uri)

    def test_get_size_returns_none_on_error(self, storage, mock_storage_client, sample_uri):
        """Get size returns None on error."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.reload.side_effect = Exception("Permission denied")

        result = storage.get_size(sample_uri)

        assert result is None
