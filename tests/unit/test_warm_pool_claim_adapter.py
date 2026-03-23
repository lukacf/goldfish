"""Adapter-level tests for WarmPoolManager.try_claim().

Exercises the real claim path through the manager→controller→DB chain
with mocked gcloud/gsutil subprocess calls. Covers:
- Success: find_claimable → CLAIM_SENT → upload → signal → ACK → CLAIM_ACKED
- ACK timeout: find_claimable → CLAIM_SENT → timeout → CLAIM_TIMEOUT → deleting
- No idle instance: returns None immediately
- GCS upload failure: claim rolled back via CLAIM_TIMEOUT
- Metadata uses --metadata-from-file (JSON-safe signaling)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.warm_pool import WarmPoolManager
from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database


@pytest.fixture
def test_db(tmp_path) -> Database:
    return Database(tmp_path / "test_claim_adapter.db")


@pytest.fixture
def enabled_config() -> WarmPoolConfig:
    return WarmPoolConfig(enabled=True, max_instances=5, idle_timeout_minutes=30)


@pytest.fixture
def manager(test_db, enabled_config) -> WarmPoolManager:
    return WarmPoolManager(
        db=test_db,
        config=enabled_config,
        bucket="test-bucket",
        project_id="test-project",
    )


def _insert_idle_instance(db: Database, name: str = "goldfish-warm-001") -> None:
    """Insert an idle_ready instance matching a3-highgpu-1g."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """
            INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at, created_at)
            VALUES (?, 'us-central1-a', 'test-project', 'a3-highgpu-1g', 1,
                    'debian-12', 'debian-cloud', 0, 'idle_ready', ?, ?)
            """,
            (name, now, now),
        )


def _mock_gcloud_with_ack(ack_run_id: str | None):
    """Create subprocess.run side_effect that simulates gcloud + gsutil calls."""

    def side_effect(*args, **kwargs):
        cmd = args[0] if args else kwargs.get("args", [])
        result = MagicMock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""

        if "gsutil" in cmd:
            # gsutil cp — succeed
            return result

        if "add-metadata" in cmd:
            # gcloud add-metadata — succeed
            return result

        if "describe" in cmd:
            # gcloud describe — return metadata with/without ACK
            if ack_run_id:
                result.stdout = json.dumps({"metadata": {"items": [{"key": "goldfish_ack", "value": ack_run_id}]}})
            else:
                result.stdout = json.dumps({"metadata": {"items": []}})
            return result

        return result

    return side_effect


class TestTryClaimSuccess:
    """Full claim success path through the adapter."""

    @patch("goldfish.cloud.adapters.gcp.warm_pool.time")
    @patch("subprocess.run")
    def test_claim_success_returns_handle(self, mock_run, mock_time, manager, test_db):
        _insert_idle_instance(test_db)

        mock_time.monotonic.side_effect = [0, 0]
        mock_time.sleep = MagicMock()
        mock_run.side_effect = _mock_gcloud_with_ack("stage-run-001")

        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
            job_spec={"image": "test:latest"},
        )

        assert handle is not None
        assert handle.stage_run_id == "stage-run-001"
        assert handle.backend_handle == "goldfish-warm-001"
        assert handle.zone == "us-central1-a"

        # Instance should be in busy state (claimed → ACK → busy)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "busy"

        # Lease should be active
        lease = test_db.get_active_lease_for_instance("goldfish-warm-001")
        assert lease is not None
        assert lease["stage_run_id"] == "stage-run-001"

    @patch("goldfish.cloud.adapters.gcp.warm_pool.time")
    @patch("subprocess.run")
    def test_claim_uses_metadata_from_file(self, mock_run, mock_time, manager, test_db):
        """Verify --metadata-from-file is used instead of --metadata."""
        _insert_idle_instance(test_db)

        mock_time.monotonic.side_effect = [0, 0]
        mock_time.sleep = MagicMock()
        mock_run.side_effect = _mock_gcloud_with_ack("stage-run-001")

        manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
            job_spec={"image": "test:latest"},
        )

        # Find the add-metadata call
        metadata_calls = [
            c
            for c in mock_run.call_args_list
            if any("add-metadata" in str(arg) for arg in (c.args[0] if c.args else []))
        ]
        assert len(metadata_calls) >= 1
        cmd = metadata_calls[0].args[0] if metadata_calls[0].args else metadata_calls[0][0]

        # Should use --metadata-from-file, NOT --metadata
        assert "--metadata-from-file" in cmd
        assert (
            "--metadata" not in cmd or cmd.index("--metadata-from-file") < cmd.index("--metadata")
            if "--metadata" in cmd and "--metadata-from-file" in cmd
            else True
        )


class TestTryClaimAckTimeout:
    """ACK timeout path: instance should be deleted, not recycled."""

    @patch("goldfish.cloud.adapters.gcp.warm_pool.time")
    @patch("subprocess.run")
    def test_ack_timeout_transitions_to_deleting(self, mock_run, mock_time, manager, test_db):
        _insert_idle_instance(test_db)

        # Time advances past 30s deadline
        mock_time.monotonic.side_effect = [0, 0, 31]
        mock_time.sleep = MagicMock()
        mock_run.side_effect = _mock_gcloud_with_ack(None)  # No ACK

        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
        )

        assert handle is None

        # Instance should be in deleting state (not recycled to idle)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "deleting"

        # Lease should be released
        assert test_db.get_active_lease_for_instance("goldfish-warm-001") is None


class TestTryClaimNoIdle:
    def test_returns_none_when_no_idle(self, manager):
        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
        )
        assert handle is None


class TestTryClaimUploadFailure:
    """GCS upload failure: claim should be rolled back."""

    @patch("subprocess.run")
    def test_upload_failure_rolls_back_claim(self, mock_run, manager, test_db):
        _insert_idle_instance(test_db)

        def side_effect(*args, **kwargs):
            cmd = args[0] if args else kwargs.get("args", [])
            if "gsutil" in cmd:
                raise OSError("gsutil not found")
            result = MagicMock()
            result.returncode = 0
            return result

        mock_run.side_effect = side_effect

        handle = manager.try_claim(
            machine_type="a3-highgpu-1g",
            gpu_count=1,
            image_family="debian-12",
            image_project="debian-cloud",
            stage_run_id="stage-run-001",
        )

        assert handle is None

        # Instance should be in deleting state (claim rolled back)
        inst = test_db.get_warm_instance("goldfish-warm-001")
        assert inst is not None
        assert inst["state"] == "deleting"

        # Lease should be released
        assert test_db.get_active_lease_for_instance("goldfish-warm-001") is None
