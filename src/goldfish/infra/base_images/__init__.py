"""Base image building for Goldfish.

Pre-built images with common ML libraries to avoid per-workspace dependency installation.
"""

import subprocess
from pathlib import Path

from goldfish.errors import GoldfishError
from goldfish.infra.profiles import BASE_IMAGE_CPU, BASE_IMAGE_GPU, BASE_IMAGE_VERSION

# Directory containing Dockerfiles
BASE_IMAGES_DIR = Path(__file__).parent


def build_base_image(image_type: str, no_cache: bool = False) -> str:
    """Build a base image locally.

    Args:
        image_type: "cpu" or "gpu"
        no_cache: Force rebuild without Docker cache

    Returns:
        Local image tag (e.g., "goldfish-base-cpu:v1")

    Raises:
        GoldfishError: If build fails
    """
    if image_type not in ("cpu", "gpu"):
        raise GoldfishError(f"Invalid image type: {image_type}. Must be 'cpu' or 'gpu'")

    image_name = BASE_IMAGE_CPU if image_type == "cpu" else BASE_IMAGE_GPU
    image_tag = f"{image_name}:{BASE_IMAGE_VERSION}"
    dockerfile = BASE_IMAGES_DIR / f"Dockerfile.{image_type}"

    if not dockerfile.exists():
        raise GoldfishError(f"Dockerfile not found: {dockerfile}")

    build_cmd = [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "-f",
        str(dockerfile),
        "-t",
        image_tag,
    ]

    if no_cache:
        build_cmd.append("--no-cache")

    build_cmd.append(str(BASE_IMAGES_DIR))

    try:
        result = subprocess.run(build_cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise GoldfishError(f"Base image build failed: {result.stderr}")
        return image_tag
    except FileNotFoundError as err:
        raise GoldfishError("Docker not found. Please install Docker.") from err


def push_base_image(image_type: str, registry_url: str) -> str:
    """Push a base image to Artifact Registry.

    Args:
        image_type: "cpu" or "gpu"
        registry_url: Registry URL (e.g., "us-docker.pkg.dev/project/goldfish")

    Returns:
        Full registry image tag

    Raises:
        GoldfishError: If push fails
    """
    import shutil

    if image_type not in ("cpu", "gpu"):
        raise GoldfishError(f"Invalid image type: {image_type}. Must be 'cpu' or 'gpu'")

    # Validate registry URL format
    if not registry_url or "://" in registry_url or "/" not in registry_url:
        raise GoldfishError(
            f"Invalid artifact_registry URL: {registry_url}. " "Expected format: us-docker.pkg.dev/<project>/<repo>"
        )

    image_name = BASE_IMAGE_CPU if image_type == "cpu" else BASE_IMAGE_GPU
    local_tag = f"{image_name}:{BASE_IMAGE_VERSION}"
    registry_tag = f"{registry_url}/{image_name}:{BASE_IMAGE_VERSION}"

    try:
        # Configure Docker authentication
        registry_domain = registry_url.split("/")[0]
        if not shutil.which("gcloud"):
            raise GoldfishError("gcloud not found; configure gcloud before pushing images.")

        auth_result = subprocess.run(
            ["gcloud", "auth", "configure-docker", registry_domain, "--quiet"],
            capture_output=True,
            text=True,
            check=False,
        )
        if auth_result.returncode != 0:
            raise GoldfishError(f"Failed to configure Docker authentication: {auth_result.stderr}")

        # Tag for registry
        tag_result = subprocess.run(
            ["docker", "tag", local_tag, registry_tag], capture_output=True, text=True, check=False
        )
        if tag_result.returncode != 0:
            raise GoldfishError(f"Docker tag failed: {tag_result.stderr}")

        # Push to registry
        push_result = subprocess.run(["docker", "push", registry_tag], capture_output=True, text=True, check=False)
        if push_result.returncode != 0:
            raise GoldfishError(f"Docker push failed: {push_result.stderr}")

        return registry_tag

    except FileNotFoundError as err:
        raise GoldfishError("Docker not found.") from err


def build_and_push_all(registry_url: str, no_cache: bool = False) -> dict[str, str]:
    """Build and push all base images.

    Args:
        registry_url: Registry URL
        no_cache: Force rebuild without Docker cache

    Returns:
        Dict mapping image type to registry tag
    """
    results = {}
    for image_type in ("cpu", "gpu"):
        build_base_image(image_type, no_cache=no_cache)
        registry_tag = push_base_image(image_type, registry_url)
        results[image_type] = registry_tag
    return results
