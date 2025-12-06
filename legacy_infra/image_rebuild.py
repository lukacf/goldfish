"""Image rebuild detection helpers."""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Iterable, List, Optional

WATCH_DEFAULT = [
    "marketlm/",
    "scripts/",
    "entrypoints/",
    "requirements.txt",
    "Dockerfile",
]

# Key algorithm files to snapshot for reproducibility
SNAPSHOT_FILES = [
    "marketlm/tokenizer/temporal_bpe.py",
    "marketlm/tokenizer/tbpe_sharding.py",
    "scripts/mine_phrases_multiscale.py",
    "scripts/generate_quant_tokens.py",
    "scripts/prepare_eurusd_data_multiscale.py",
    "scripts/train_transformer_lm.py",
]


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=True)


def get_image_sha(image_uri: str) -> Optional[str]:
    """Return the git_sha label from a Docker image if present."""
    try:
        result = _run([
            "docker",
            "inspect",
            "--format={{ index .Config.Labels \"git_sha\" }}",
            image_uri,
        ])
    except Exception:
        return None
    value = result.stdout.strip()
    return value or None


def get_image_digest(image_uri: str) -> Optional[str]:
    """Return the immutable digest (sha256:...) of a Docker image."""
    try:
        result = _run([
            "docker",
            "inspect",
            "--format={{index .RepoDigests 0}}",
            image_uri,
        ])
    except Exception:
        return None
    value = result.stdout.strip()
    # RepoDigests format: repo@sha256:... - extract just the digest
    if "@" in value:
        return value.split("@")[-1]
    return value or None


def get_current_sha() -> Optional[str]:
    try:
        result = _run(["git", "rev-parse", "HEAD"])
        return result.stdout.strip()
    except Exception:
        return None


def get_changed_paths(old_sha: str, new_sha: str, watch_paths: Iterable[str]) -> List[str]:
    try:
        result = _run(["git", "diff", "--name-only", old_sha, new_sha, "--", *watch_paths])
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except Exception:
        return []


def needs_rebuild(image_uri: str, watch_paths: Iterable[str] = WATCH_DEFAULT) -> bool:
    image_sha = get_image_sha(image_uri)
    current_sha = get_current_sha()
    if not current_sha:
        return False
    if not image_sha:
        return True
    changed = get_changed_paths(image_sha, current_sha, watch_paths)
    return bool(changed)


def snapshot_code_files(dest_dir: Path, files: Iterable[str] = SNAPSHOT_FILES) -> List[str]:
    """Copy key algorithm files to dest_dir/code_snapshot/ for reproducibility.

    Returns list of files that were successfully copied.
    """
    repo_root = Path(__file__).resolve().parents[1]
    snapshot_dir = dest_dir / "code_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    copied = []
    for rel_path in files:
        src = repo_root / rel_path
        if src.exists():
            # Preserve directory structure
            dst = snapshot_dir / rel_path
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(src.read_text())
            copied.append(rel_path)

    return copied


def get_code_diff_summary(old_sha: str, new_sha: str = "HEAD") -> str:
    """Get a summary of code changes between two commits."""
    try:
        # Get list of changed files
        result = _run(["git", "diff", "--stat", old_sha, new_sha, "--", *WATCH_DEFAULT])
        return result.stdout.strip()
    except Exception:
        return ""


def rebuild_image(image_config: dict[str, Any], variant_name: str | None = None) -> tuple[str, str]:
    """Rebuild the Docker image using the repo's build script.

    Returns tuple of (git_sha, sha_tagged_uri).

    Expects `image_config` to contain at least `uri`. Optionally honors `context`
    and `dockerfile` fields if provided. This is a thin wrapper around
    `scripts/build_and_push_docker.sh` and assumes Docker + gcloud are available.

    The image is always tagged with both :latest and :<git_sha_short> to enable
    parallel runs with different code versions.
    """

    uri = image_config.get("uri")
    if not uri:
        raise ValueError("image_config.uri is required for rebuild")

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "build_and_push_docker.sh"
    context = image_config.get("context", ".")

    cmd = [str(script), uri, context]
    _run(cmd)

    sha = get_current_sha() or ""
    short_sha = sha[:8] if sha else "unknown"

    # Extract base URI (without :tag)
    if ":" in uri and not uri.count(":") > 1:  # Don't split on port
        base_uri = uri.rsplit(":", 1)[0]
    else:
        base_uri = uri

    # Tag with short git SHA for parallel execution
    sha_uri = f"{base_uri}:{short_sha}"
    try:
        _run(["docker", "tag", uri, sha_uri])
        _run(["docker", "push", sha_uri])
    except Exception:
        sha_uri = uri  # Fall back to original if tagging fails

    # If variant name provided, also tag with variant
    if variant_name:
        variant_uri = f"{base_uri}:{variant_name}"
        try:
            _run(["docker", "tag", uri, variant_uri])
            _run(["docker", "push", variant_uri])
        except Exception:
            pass  # Non-fatal if variant tag fails

    return sha, sha_uri

