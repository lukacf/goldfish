"""RCT Tests for Google Cloud Storage.

These tests validate our assumptions about GCS behavior against reality.
They run against real GCS and document the actual representations.

RCT-GCS-1: Upload/download round-trip preserves data exactly
RCT-GCS-2: List prefix returns expected structure
RCT-GCS-3: Metadata operations behave as expected
RCT-GCS-4: Error codes for missing objects
"""

import pytest
from google.cloud import storage  # type: ignore[attr-defined]
from google.cloud.exceptions import NotFound  # type: ignore[import-untyped]

# Mark all tests in this module as RCT tests
pytestmark = pytest.mark.rct


class TestGCSRoundTrip:
    """RCT-GCS-1: Upload/download round-trip tests."""

    def test_bytes_roundtrip_preserves_data(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate that bytes uploaded to GCS are returned exactly."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/bytes_roundtrip.bin"
        blob = bucket.blob(blob_path)

        # Test with various byte patterns including null bytes
        test_data = b"\x00\xff\x01\xfe binary data with nulls"

        # Upload
        blob.upload_from_string(test_data)

        # Download
        downloaded = blob.download_as_bytes()

        # Verify exact match
        assert downloaded == test_data, "GCS must preserve bytes exactly"

    def test_utf8_roundtrip_preserves_encoding(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate that UTF-8 text is preserved exactly."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/utf8_roundtrip.txt"
        blob = bucket.blob(blob_path)

        # Include various unicode
        test_data = "Hello 世界 🎉 émojis"

        blob.upload_from_string(test_data.encode("utf-8"), content_type="text/plain; charset=utf-8")
        downloaded = blob.download_as_text(encoding="utf-8")

        assert downloaded == test_data, "GCS must preserve UTF-8 exactly"

    def test_empty_file_roundtrip(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate that empty files are handled correctly."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/empty_file.txt"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(b"")
        downloaded = blob.download_as_bytes()

        assert downloaded == b"", "GCS must handle empty files"
        assert blob.exists(), "Empty blob must exist"

    def test_large_file_roundtrip(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate large file handling (10MB)."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/large_file.bin"
        blob = bucket.blob(blob_path)

        # 10MB of deterministic data
        import hashlib

        test_data = b"x" * (10 * 1024 * 1024)
        expected_hash = hashlib.md5(test_data).hexdigest()

        blob.upload_from_string(test_data)
        downloaded = blob.download_as_bytes()

        actual_hash = hashlib.md5(downloaded).hexdigest()
        assert actual_hash == expected_hash, "Large file must be preserved exactly"


class TestGCSListPrefix:
    """RCT-GCS-2: List prefix behavior tests."""

    def test_list_returns_all_objects_with_prefix(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate list_blobs returns all matching objects."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Create test hierarchy
        paths = [
            f"{cleanup_gcs_prefix}/dir1/file1.txt",
            f"{cleanup_gcs_prefix}/dir1/file2.txt",
            f"{cleanup_gcs_prefix}/dir2/file3.txt",
            f"{cleanup_gcs_prefix}/file4.txt",
        ]

        for path in paths:
            bucket.blob(path).upload_from_string(b"test")

        # List all with prefix
        all_blobs = list(bucket.list_blobs(prefix=cleanup_gcs_prefix))
        all_names = [b.name for b in all_blobs]

        assert len(all_blobs) == 4, f"Expected 4 blobs, got {len(all_blobs)}"
        for path in paths:
            assert path in all_names, f"Missing expected path: {path}"

        # RCT-GCS-2: GCS returns results in lexicographic order
        assert all_names == sorted(all_names), "GCS list_blobs must return results in lexicographic order"

    def test_list_prefix_trailing_slash_behavior(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Document trailing slash behavior in prefix listing."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Create a blob
        path = f"{cleanup_gcs_prefix}/testdir/file.txt"
        bucket.blob(path).upload_from_string(b"test")

        # List with and without trailing slash
        with_slash = list(bucket.list_blobs(prefix=f"{cleanup_gcs_prefix}/testdir/"))
        without_slash = list(bucket.list_blobs(prefix=f"{cleanup_gcs_prefix}/testdir"))

        # Both should return the same result for a file inside
        assert len(with_slash) == len(without_slash) == 1
        assert with_slash[0].name == path

    def test_list_empty_prefix_returns_empty(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate listing non-existent prefix returns empty, not error."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # List with prefix that doesn't exist
        blobs = list(bucket.list_blobs(prefix=f"{cleanup_gcs_prefix}/nonexistent_prefix_xyz/"))

        assert blobs == [], "Listing non-existent prefix must return empty list"

    def test_list_with_delimiter_returns_prefixes(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate delimiter returns directory-like prefixes."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Create hierarchy
        paths = [
            f"{cleanup_gcs_prefix}/dir1/file1.txt",
            f"{cleanup_gcs_prefix}/dir2/file2.txt",
            f"{cleanup_gcs_prefix}/file3.txt",
        ]
        for path in paths:
            bucket.blob(path).upload_from_string(b"test")

        # List with delimiter
        blobs = bucket.list_blobs(prefix=f"{cleanup_gcs_prefix}/", delimiter="/")
        items = list(blobs)
        prefixes = list(blobs.prefixes)

        # Should return 1 blob (file3.txt) and 2 prefixes (dir1/, dir2/)
        assert len(items) == 1, f"Expected 1 blob at root, got {len(items)}"
        assert f"{cleanup_gcs_prefix}/file3.txt" == items[0].name
        assert len(prefixes) == 2, f"Expected 2 prefixes, got {len(prefixes)}"


class TestGCSMetadata:
    """RCT-GCS-3: Metadata operations tests."""

    def test_custom_metadata_roundtrip(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate custom metadata is preserved."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/metadata_test.txt"
        blob = bucket.blob(blob_path)

        # Upload with custom metadata
        blob.metadata = {
            "goldfish_stage_run_id": "stage-abc123",
            "goldfish_signal_type": "output",
        }
        blob.upload_from_string(b"test data")

        # Reload and check
        blob.reload()

        assert blob.metadata is not None
        assert blob.metadata.get("goldfish_stage_run_id") == "stage-abc123"
        assert blob.metadata.get("goldfish_signal_type") == "output"

    def test_content_type_preserved(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate content_type is preserved."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/content_type_test.json"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(b'{"key": "value"}', content_type="application/json")
        blob.reload()

        assert blob.content_type == "application/json"


class TestGCSErrorCodes:
    """RCT-GCS-4: Error code tests."""

    def test_download_missing_raises_not_found(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate downloading non-existent blob raises NotFound."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/nonexistent_blob_xyz.txt"
        blob = bucket.blob(blob_path)

        with pytest.raises(NotFound):
            blob.download_as_bytes()

    def test_exists_returns_false_for_missing(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate exists() returns False for missing blob."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/nonexistent_blob_xyz.txt"
        blob = bucket.blob(blob_path)

        assert blob.exists() is False

    def test_exists_returns_true_for_existing(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate exists() returns True for existing blob."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/existing_blob.txt"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(b"exists")

        assert blob.exists() is True


class TestGCSEventualConsistency:
    """Document GCS consistency behavior."""

    def test_upload_immediately_visible(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Document: GCS uploads are immediately visible (strong consistency)."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/consistency_test.txt"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(b"test data")

        # Immediate read should work (GCS has strong consistency for objects)
        downloaded = blob.download_as_bytes()
        assert downloaded == b"test data"

    def test_list_immediately_reflects_upload(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Document: GCS list reflects uploads immediately (strong consistency)."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob_path = f"{cleanup_gcs_prefix}/list_consistency.txt"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(b"test")

        # Immediate list should include the blob
        blobs = list(bucket.list_blobs(prefix=cleanup_gcs_prefix))
        blob_names = [b.name for b in blobs]

        assert blob_path in blob_names, "GCS list should immediately reflect uploads"
