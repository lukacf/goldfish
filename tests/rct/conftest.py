"""RCT Test Configuration.

RCT (Representation Contract Tests) validate that our assumptions about
GCP services match reality. These tests run against REAL GCP infrastructure
and are skipped if credentials or bucket are not configured.

Run with:
    pytest tests/rct/ -v --rct

Skip in CI (unless explicitly testing GCP):
    pytest tests/rct/ -v -m "not rct"
"""

import os
import subprocess

import pytest


def pytest_configure(config):
    """Register RCT marker."""
    config.addinivalue_line(
        "markers",
        "rct: RCT (Representation Contract Tests) that run against real GCP",
    )


def pytest_addoption(parser):
    """Add --rct option to enable RCT tests."""
    parser.addoption(
        "--rct",
        action="store_true",
        default=False,
        help="Run RCT tests against real GCP infrastructure",
    )


def pytest_collection_modifyitems(config, items):
    """Skip RCT tests unless --rct flag is provided.

    Only skips tests with explicit @pytest.mark.rct marker, not tests
    that just happen to be in the rct/ directory (like local parity tests).
    """
    if config.getoption("--rct"):
        return

    skip_rct = pytest.mark.skip(reason="RCT tests require --rct flag")
    for item in items:
        # Check for explicit @pytest.mark.rct marker, not directory name in keywords
        if item.get_closest_marker("rct") is not None:
            item.add_marker(skip_rct)


@pytest.fixture(scope="session")
def gcp_project_id() -> str | None:
    """Get GCP project ID from environment or gcloud."""
    project = os.environ.get("GOLDFISH_GCP_PROJECT")
    if project:
        return project

    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


@pytest.fixture(scope="session")
def gcs_bucket(gcp_project_id) -> str | None:
    """Get GCS bucket for RCT tests.

    Uses GOLDFISH_GCS_BUCKET env var, or constructs default from project ID.
    """
    bucket = os.environ.get("GOLDFISH_GCS_BUCKET")
    if bucket:
        return bucket.replace("gs://", "").rstrip("/")

    if gcp_project_id:
        return f"{gcp_project_id}-goldfish-dev"
    return None


@pytest.fixture(scope="session")
def gce_zone() -> str:
    """Get GCE zone for RCT tests."""
    return os.environ.get("GOLDFISH_GCE_ZONE", "us-central1-a")


@pytest.fixture(scope="session")
def gcp_available(gcp_project_id, gcs_bucket) -> bool:
    """Check if GCP is available for testing."""
    if not gcp_project_id or not gcs_bucket:
        return False

    # Verify we can actually talk to GCS
    try:
        result = subprocess.run(
            ["gcloud", "storage", "ls", f"gs://{gcs_bucket}/", "--project", gcp_project_id],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="function")
def rct_test_prefix(request):
    """Generate unique prefix for test artifacts to avoid collisions."""
    import uuid

    test_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    unique_id = uuid.uuid4().hex[:8]
    return f"rct_tests/{test_name}_{unique_id}"


@pytest.fixture(scope="function")
def cleanup_gcs_prefix(gcs_bucket, rct_test_prefix):
    """Cleanup GCS objects after test."""
    yield rct_test_prefix

    # Cleanup after test
    if gcs_bucket:
        try:
            subprocess.run(
                ["gcloud", "storage", "rm", "-r", f"gs://{gcs_bucket}/{rct_test_prefix}/"],
                capture_output=True,
                check=False,
            )
        except Exception:
            pass
