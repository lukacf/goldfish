import hashlib
import re
from pathlib import Path

import pytest

from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import (
    BuildContext,
    compute_build_context_hash,
    compute_goldfish_runtime_hash,
    compute_requirements_hash,
)


def test_build_context_hash_deterministic() -> None:
    """Same BuildContext yields the same full SHA256 hash."""
    ctx = BuildContext(
        dockerfile_hash="df",
        git_sha="abc123",
        goldfish_runtime_hash="rt",
        base_image="python:3.11-slim",
        base_image_digest=None,
        requirements_hash="req",
        build_args={"A": "1", "B": "2"},
    )

    h1 = compute_build_context_hash(ctx)
    h2 = compute_build_context_hash(ctx)

    assert h1 == h2
    assert re.fullmatch(r"[0-9a-f]{64}", h1)


def test_build_context_hash_sensitive() -> None:
    """Different BuildContext yields a different hash."""
    base = BuildContext(
        dockerfile_hash="df",
        git_sha="abc123",
        goldfish_runtime_hash="rt",
        base_image="python:3.11-slim",
        base_image_digest=None,
        requirements_hash="req",
        build_args={"A": "1", "B": "2"},
    )
    changed = BuildContext(
        dockerfile_hash=base.dockerfile_hash,
        git_sha=base.git_sha,
        goldfish_runtime_hash=base.goldfish_runtime_hash,
        base_image=base.base_image,
        base_image_digest=base.base_image_digest,
        requirements_hash=base.requirements_hash,
        build_args={"A": "1", "B": "3"},
    )

    assert compute_build_context_hash(base) != compute_build_context_hash(changed)


def test_goldfish_runtime_hash_deterministic(tmp_path: Path) -> None:
    """Hash is deterministic for the same runtime file tree."""
    root = tmp_path / "goldfish"
    (root / "io").mkdir(parents=True)
    (root / "svs").mkdir()
    (root / "metrics").mkdir()
    (root / "utils").mkdir()
    (root / "cloud").mkdir()
    (root / "config").mkdir()

    (root / "io" / "__init__.py").write_text("io\n")
    (root / "svs" / "a.py").write_text("svs\n")
    (root / "metrics" / "m.py").write_text("metrics\n")
    (root / "utils" / "u.py").write_text("utils\n")
    (root / "cloud" / "c.py").write_text("cloud\n")
    (root / "validation.py").write_text("validation\n")
    (root / "errors.py").write_text("errors\n")
    (root / "config" / "__init__.py").write_text("config\n")

    h1 = compute_goldfish_runtime_hash(root)
    h2 = compute_goldfish_runtime_hash(root)

    assert h1 == h2
    assert re.fullmatch(r"[0-9a-f]{64}", h1)


def test_goldfish_runtime_hash_sensitive(tmp_path: Path) -> None:
    """Any included file change changes the runtime hash."""
    root = tmp_path / "goldfish"
    (root / "io").mkdir(parents=True)
    (root / "svs").mkdir()
    (root / "metrics").mkdir()
    (root / "utils").mkdir()
    (root / "cloud").mkdir()
    (root / "config").mkdir()

    (root / "io" / "__init__.py").write_text("io\n")
    target = root / "svs" / "a.py"
    target.write_text("svs\n")
    (root / "metrics" / "m.py").write_text("metrics\n")
    (root / "utils" / "u.py").write_text("utils\n")
    (root / "cloud" / "c.py").write_text("cloud\n")
    (root / "validation.py").write_text("validation\n")
    (root / "errors.py").write_text("errors\n")
    (root / "config" / "__init__.py").write_text("config\n")

    h1 = compute_goldfish_runtime_hash(root)
    target.write_text("svs changed\n")
    h2 = compute_goldfish_runtime_hash(root)

    assert h1 != h2


def test_build_context_hash_sensitive_to_goldfish_rust_change(tmp_path: Path) -> None:
    """A goldfish-rust file change changes build_context_hash via goldfish_runtime_hash."""
    repo_root = tmp_path / "repo"
    goldfish_root = repo_root / "src" / "goldfish"
    (goldfish_root / "io").mkdir(parents=True)
    (goldfish_root / "svs").mkdir()
    (goldfish_root / "metrics").mkdir()
    (goldfish_root / "utils").mkdir()
    (goldfish_root / "cloud").mkdir()
    (goldfish_root / "config").mkdir()

    (goldfish_root / "io" / "__init__.py").write_text("io\n")
    (goldfish_root / "svs" / "a.py").write_text("svs\n")
    (goldfish_root / "metrics" / "m.py").write_text("metrics\n")
    (goldfish_root / "utils" / "u.py").write_text("utils\n")
    (goldfish_root / "cloud" / "c.py").write_text("cloud\n")
    (goldfish_root / "validation.py").write_text("validation\n")
    (goldfish_root / "errors.py").write_text("errors\n")
    (goldfish_root / "config" / "__init__.py").write_text("config\n")

    rust_root = repo_root / "goldfish-rust"
    (rust_root / "src").mkdir(parents=True)
    rust_file = rust_root / "src" / "lib.rs"
    rust_file.write_text("pub fn meaning_of_life() -> u32 { 42 }\n")

    runtime_hash_1 = compute_goldfish_runtime_hash(goldfish_root)
    hash_1 = compute_build_context_hash(
        BuildContext(
            dockerfile_hash="df",
            git_sha="abc123",
            goldfish_runtime_hash=runtime_hash_1,
            base_image="python:3.11-slim",
            base_image_digest=None,
            requirements_hash="req",
            build_args={"A": "1", "B": "2"},
        )
    )

    rust_file.write_text("pub fn meaning_of_life() -> u32 { 43 }\n")
    runtime_hash_2 = compute_goldfish_runtime_hash(goldfish_root)
    hash_2 = compute_build_context_hash(
        BuildContext(
            dockerfile_hash="df",
            git_sha="abc123",
            goldfish_runtime_hash=runtime_hash_2,
            base_image="python:3.11-slim",
            base_image_digest=None,
            requirements_hash="req",
            build_args={"A": "1", "B": "2"},
        )
    )

    assert hash_1 != hash_2


def test_goldfish_runtime_hash_succeeds_in_repo_tree() -> None:
    """Runtime hash computation succeeds against the real repo layout."""
    runtime_hash = compute_goldfish_runtime_hash()
    assert re.fullmatch(r"[0-9a-f]{64}", runtime_hash)


def test_requirements_hash_empty_string(tmp_path: Path) -> None:
    """Missing requirements.txt uses the hash of empty string."""
    assert compute_requirements_hash(tmp_path) == hashlib.sha256(b"").hexdigest()


def test_build_context_rejects_secret_build_args() -> None:
    """BuildContext rejects build args that look like secrets."""
    with pytest.raises(GoldfishError, match="build_args.*secret"):
        BuildContext(
            dockerfile_hash="df",
            git_sha="abc123",
            goldfish_runtime_hash="rt",
            base_image="python:3.11-slim",
            base_image_digest=None,
            requirements_hash="req",
            build_args={"PASSWORD": "x"},
        )


def test_build_context_rejects_build_context_hash_build_arg() -> None:
    """BuildContext rejects build_context_hash within build args (circular dependency)."""
    with pytest.raises(GoldfishError, match="build_context_hash"):
        BuildContext(
            dockerfile_hash="df",
            git_sha="abc123",
            goldfish_runtime_hash="rt",
            base_image="python:3.11-slim",
            base_image_digest=None,
            requirements_hash="req",
            build_args={"BUILD_CONTEXT_HASH": "deadbeef"},
        )
