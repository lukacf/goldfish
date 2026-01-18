"""Unit tests for Docker content hash computation.

These are REGRESSION TESTS for the content-based caching feature that
prevents unnecessary workspace image rebuilds.
"""

from pathlib import Path

import pytest


@pytest.fixture
def docker_builder():
    """Create a DockerBuilder instance for testing."""
    from goldfish.infra.docker_builder import DockerBuilder

    return DockerBuilder()


class TestComputeContentHash:
    """Tests for _compute_content_hash method.

    This method computes a SHA256 hash of the build context to detect
    when workspace content has changed. If the hash matches a previous
    build, we can skip rebuilding.
    """

    def test_same_content_produces_same_hash(self, docker_builder, tmp_path: Path):
        """Identical content should produce identical hash."""
        # Create build context
        context = tmp_path / "context"
        context.mkdir()
        (context / "file.txt").write_text("hello world")

        dockerfile = "FROM python:3.11\nRUN pip install numpy"
        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context, dockerfile, base_image)
        hash2 = docker_builder._compute_content_hash(context, dockerfile, base_image)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex digest length

    def test_different_files_produce_different_hash(self, docker_builder, tmp_path: Path):
        """Different file content should produce different hash."""
        context1 = tmp_path / "context1"
        context1.mkdir()
        (context1 / "file.txt").write_text("version 1")

        context2 = tmp_path / "context2"
        context2.mkdir()
        (context2 / "file.txt").write_text("version 2")  # Different content

        dockerfile = "FROM python:3.11"
        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context1, dockerfile, base_image)
        hash2 = docker_builder._compute_content_hash(context2, dockerfile, base_image)

        assert hash1 != hash2

    def test_different_dockerfile_produces_different_hash(self, docker_builder, tmp_path: Path):
        """Different Dockerfile content should produce different hash."""
        context = tmp_path / "context"
        context.mkdir()
        (context / "file.txt").write_text("same content")

        dockerfile1 = "FROM python:3.11\nRUN pip install numpy"
        dockerfile2 = "FROM python:3.11\nRUN pip install pandas"  # Different pip install

        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context, dockerfile1, base_image)
        hash2 = docker_builder._compute_content_hash(context, dockerfile2, base_image)

        assert hash1 != hash2

    def test_different_base_image_produces_different_hash(self, docker_builder, tmp_path: Path):
        """Different base image should produce different hash.

        This is important because switching from CPU to GPU base image
        should force a rebuild.
        """
        context = tmp_path / "context"
        context.mkdir()
        (context / "file.txt").write_text("same content")

        dockerfile = "FROM base\nRUN pip install numpy"

        hash_cpu = docker_builder._compute_content_hash(context, dockerfile, "goldfish-base-cpu:v1")
        hash_gpu = docker_builder._compute_content_hash(context, dockerfile, "goldfish-base-gpu:v1")

        assert hash_cpu != hash_gpu

    def test_hash_is_deterministic_with_multiple_files(self, docker_builder, tmp_path: Path):
        """Hash should be deterministic regardless of file system order."""
        context = tmp_path / "context"
        context.mkdir()

        # Create multiple files
        (context / "a_file.txt").write_text("aaa")
        (context / "b_file.txt").write_text("bbb")
        (context / "z_file.txt").write_text("zzz")
        subdir = context / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested content")

        dockerfile = "FROM python:3.11"
        base_image = "python:3.11"

        # Compute hash multiple times
        hashes = [docker_builder._compute_content_hash(context, dockerfile, base_image) for _ in range(5)]

        # All hashes should be identical
        assert len(set(hashes)) == 1

    def test_hash_includes_file_paths(self, docker_builder, tmp_path: Path):
        """Renaming a file should change the hash."""
        # Context 1: file named "alpha.txt"
        context1 = tmp_path / "context1"
        context1.mkdir()
        (context1 / "alpha.txt").write_text("content")

        # Context 2: same content but named "beta.txt"
        context2 = tmp_path / "context2"
        context2.mkdir()
        (context2 / "beta.txt").write_text("content")

        dockerfile = "FROM python:3.11"
        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context1, dockerfile, base_image)
        hash2 = docker_builder._compute_content_hash(context2, dockerfile, base_image)

        assert hash1 != hash2  # Different file names = different hash

    def test_empty_context_has_consistent_hash(self, docker_builder, tmp_path: Path):
        """Empty build context should produce consistent hash."""
        context = tmp_path / "empty_context"
        context.mkdir()

        dockerfile = "FROM python:3.11"
        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context, dockerfile, base_image)
        hash2 = docker_builder._compute_content_hash(context, dockerfile, base_image)

        assert hash1 == hash2

    def test_none_base_image_produces_consistent_hash(self, docker_builder, tmp_path: Path):
        """None base_image should be handled consistently."""
        context = tmp_path / "context"
        context.mkdir()
        (context / "file.txt").write_text("content")

        dockerfile = "FROM scratch"

        hash1 = docker_builder._compute_content_hash(context, dockerfile, None)
        hash2 = docker_builder._compute_content_hash(context, dockerfile, None)

        assert hash1 == hash2

    def test_binary_files_are_hashed(self, docker_builder, tmp_path: Path):
        """Binary files should be included in hash."""
        context = tmp_path / "context"
        context.mkdir()

        # Create a binary file
        binary_content = bytes(range(256))
        (context / "binary.bin").write_bytes(binary_content)

        dockerfile = "FROM python:3.11"
        base_image = "python:3.11"

        hash1 = docker_builder._compute_content_hash(context, dockerfile, base_image)

        # Modify binary content
        (context / "binary.bin").write_bytes(bytes(range(255, -1, -1)))  # Reversed

        hash2 = docker_builder._compute_content_hash(context, dockerfile, base_image)

        assert hash1 != hash2
