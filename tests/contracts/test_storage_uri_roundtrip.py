"""StorageURI round-trip contract tests."""

from __future__ import annotations

from goldfish.cloud.contracts import StorageURI


def test_storage_uri_roundtrip_when_stringified_then_parse_returns_equal() -> None:
    """parse(str(uri)) preserves scheme/bucket/path."""

    cases = [
        StorageURI("gs", "my-bucket", "path/to/file.txt"),
        StorageURI("s3", "my-bucket", "path/to/file.txt"),
        StorageURI("file", "", "/tmp/file.txt"),
    ]
    for uri in cases:
        assert StorageURI.parse(str(uri)) == uri
