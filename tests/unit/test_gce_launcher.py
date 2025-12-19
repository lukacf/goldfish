"""Unit tests for GCE launcher functionality.

Includes regression tests for critical bugs:
- Bug #1: GCS paths missing gs:// prefix caused 100% failure rate for exit code
  and log retrieval. The bucket was stored without prefix but used directly in
  gsutil commands, causing all reads to fail silently.
"""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from goldfish.infra.gce_launcher import GCELauncher


class TestBucketUri:
    """Test bucket_uri property normalization."""

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked init."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.project_id = "test-project"
            return launcher

    def test_bucket_uri_with_prefix(self, launcher):
        """Should return as-is when bucket already has gs:// prefix."""
        launcher.bucket = "gs://test-bucket"
        assert launcher.bucket_uri == "gs://test-bucket"

    def test_bucket_uri_without_prefix(self, launcher):
        """Should add gs:// prefix when bucket doesn't have it."""
        launcher.bucket = "test-bucket"
        assert launcher.bucket_uri == "gs://test-bucket"

    def test_bucket_uri_none(self, launcher):
        """Should return None when bucket is None."""
        launcher.bucket = None
        assert launcher.bucket_uri is None

    def test_bucket_uri_empty(self, launcher):
        """Should return None when bucket is empty string."""
        launcher.bucket = ""
        assert launcher.bucket_uri is None


class TestGetExitCode:
    """Test _get_exit_code retry logic."""

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.bucket = "test-bucket"  # Without gs:// prefix
            launcher.project_id = "test-project"
            launcher.config = MagicMock()
            return launcher

    def test_get_exit_code_success_first_try(self, launcher):
        """Should return exit code on first successful attempt."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="0\n",
                stderr="",
            )
            result = launcher._get_exit_code("stage-abc123")
            assert result == 0
            assert mock_run.call_count == 1

    def test_get_exit_code_non_zero_exit(self, launcher):
        """Should return non-zero exit code correctly."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="42\n",
                stderr="",
            )
            result = launcher._get_exit_code("stage-abc123")
            assert result == 42

    def test_get_exit_code_retry_on_failure(self, launcher):
        """Should retry on gsutil failure and succeed eventually."""
        with patch("subprocess.run") as mock_run, patch("time.sleep"):
            # First 2 attempts fail, third succeeds
            mock_run.side_effect = [
                subprocess.CalledProcessError(1, "gsutil", stderr="CommandException: No URLs matched"),
                subprocess.CalledProcessError(1, "gsutil", stderr="CommandException: No URLs matched"),
                MagicMock(returncode=0, stdout="0\n", stderr=""),
            ]
            result = launcher._get_exit_code("stage-abc123", max_attempts=5, retry_delay=0.1)
            assert result == 0
            assert mock_run.call_count == 3

    def test_get_exit_code_all_retries_exhausted(self, launcher):
        """Should return 1 after all retries exhausted."""
        with patch("subprocess.run") as mock_run, patch("time.sleep"):
            # All attempts fail
            mock_run.side_effect = subprocess.CalledProcessError(
                1, "gsutil", stderr="CommandException: No URLs matched"
            )
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)
            assert result == 1
            assert mock_run.call_count == 3

    def test_get_exit_code_timeout_retry(self, launcher):
        """Should retry on timeout and succeed eventually."""
        with patch("subprocess.run") as mock_run, patch("time.sleep"):
            # First attempt times out, second succeeds
            mock_run.side_effect = [
                subprocess.TimeoutExpired("gsutil", 30),
                MagicMock(returncode=0, stdout="0\n", stderr=""),
            ]
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)
            assert result == 0
            assert mock_run.call_count == 2

    def test_get_exit_code_invalid_content_no_retry(self, launcher):
        """Should not retry on invalid file content (ValueError)."""
        with patch("subprocess.run") as mock_run, patch("time.sleep") as mock_sleep:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="not_a_number\n",
                stderr="",
            )
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)
            assert result == 1
            # Should only try once - invalid content means file exists but is corrupt
            assert mock_run.call_count == 1
            # No retries means no sleep calls
            assert mock_sleep.call_count == 0

    def test_get_exit_code_no_bucket(self, launcher):
        """Should return 0 if no bucket configured."""
        launcher.bucket = None
        result = launcher._get_exit_code("stage-abc123")
        assert result == 0

    def test_get_exit_code_correct_gcs_path(self, launcher):
        """Should construct correct GCS path with gs:// prefix."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="0\n",
                stderr="",
            )
            # launcher.bucket is "test-bucket" (without gs://)
            # but _get_exit_code should use bucket_uri which adds gs://
            launcher._get_exit_code("stage-abc123")
            args = mock_run.call_args[0][0]
            assert args == ["gsutil", "cat", "gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"]

    def test_get_exit_code_handles_bucket_with_prefix(self, launcher):
        """Should work correctly when bucket already has gs:// prefix."""
        launcher.bucket = "gs://already-prefixed-bucket"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="0\n",
                stderr="",
            )
            launcher._get_exit_code("stage-abc123")
            args = mock_run.call_args[0][0]
            # Should NOT double the gs:// prefix
            assert args == ["gsutil", "cat", "gs://already-prefixed-bucket/runs/stage-abc123/logs/exit_code.txt"]


class TestGetInstanceLogs:
    """Test get_instance_logs GCS path construction.

    Regression tests for Bug #1: Bucket without gs:// prefix caused log retrieval
    to fail silently, returning empty logs even when logs existed in GCS.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.bucket = "mlm-artifacts-bucket"  # Without gs:// prefix (realistic)
            launcher.project_id = "test-project"
            launcher.default_zone = "us-central1-a"
            launcher.zones = ["us-central1-a"]
            return launcher

    def test_get_instance_logs_uses_bucket_uri(self, launcher):
        """REGRESSION: get_instance_logs must use bucket_uri not bucket directly.

        This test documents the bug where bucket="mlm-artifacts-bucket" was used
        directly in gsutil commands, resulting in invalid paths like:
            gsutil cat mlm-artifacts-bucket/runs/stage-xxx/logs/stdout.log
        instead of:
            gsutil cat gs://mlm-artifacts-bucket/runs/stage-xxx/logs/stdout.log
        """
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc123"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
        ):
            # Mock successful stdout fetch
            mock_proc = MagicMock()
            mock_proc.stdout = MagicMock()
            mock_proc.stdout.__enter__ = MagicMock(return_value=mock_proc.stdout)
            mock_proc.stdout.__exit__ = MagicMock(return_value=False)
            mock_proc.stdout.__iter__ = MagicMock(return_value=iter(["log line 1\n", "log line 2\n"]))
            mock_proc.wait = MagicMock(return_value=0)
            mock_proc.returncode = 0
            mock_popen.return_value = mock_proc

            launcher.get_instance_logs("stage-abc123")

            # Verify the path includes gs:// prefix
            call_args = mock_popen.call_args_list[0][0][0]
            assert call_args[0] == "gsutil"
            assert call_args[1] == "cat"
            # CRITICAL: Must have gs:// prefix
            assert call_args[2].startswith("gs://"), f"GCS path missing gs:// prefix: {call_args[2]}"
            assert call_args[2] == "gs://mlm-artifacts-bucket/runs/stage-abc123/logs/stdout.log"

    def test_get_instance_logs_bucket_with_prefix_no_double(self, launcher):
        """Should not double gs:// prefix when bucket already has it."""
        launcher.bucket = "gs://already-prefixed"
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc123"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
        ):
            mock_proc = MagicMock()
            mock_proc.stdout = MagicMock()
            mock_proc.stdout.__enter__ = MagicMock(return_value=mock_proc.stdout)
            mock_proc.stdout.__exit__ = MagicMock(return_value=False)
            mock_proc.stdout.__iter__ = MagicMock(return_value=iter(["log\n"]))
            mock_proc.wait = MagicMock(return_value=0)
            mock_proc.returncode = 0
            mock_popen.return_value = mock_proc

            launcher.get_instance_logs("stage-abc123")

            call_args = mock_popen.call_args_list[0][0][0]
            # Should NOT have gs://gs://
            assert "gs://gs://" not in call_args[2], f"Double gs:// prefix detected: {call_args[2]}"
            assert call_args[2] == "gs://already-prefixed/runs/stage-abc123/logs/stdout.log"


class TestMapGceStatusIntegration:
    """Integration tests for _map_gce_status with exit code retrieval.

    Regression tests for Bug #1: TERMINATED instances with exit_code=0 were
    incorrectly marked as FAILED because _get_exit_code couldn't read the
    exit code file due to missing gs:// prefix.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with realistic bucket config."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            # Realistic bucket name without gs:// prefix (as stored in config)
            launcher.bucket = "mlm-artifacts-king-dev"
            launcher.project_id = "test-project"
            return launcher

    def test_terminated_with_exit_code_zero_returns_completed(self, launcher):
        """REGRESSION: TERMINATED instance with exit_code=0 must return COMPLETED.

        This test documents the critical bug where successful stages were marked
        as FAILED because the bucket path was constructed without gs:// prefix,
        causing gsutil to fail and _get_exit_code to return 1 (failure).
        """
        from goldfish.models import StageRunStatus

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="0\n",  # Exit code 0 = success
                stderr="",
            )

            status = launcher._map_gce_status("TERMINATED", "stage-68043eed")

            # Must return COMPLETED, not FAILED
            assert status == StageRunStatus.COMPLETED, f"TERMINATED with exit_code=0 should be COMPLETED, got {status}"

            # Verify correct GCS path was used
            call_args = mock_run.call_args[0][0]
            assert call_args == ["gsutil", "cat", "gs://your-bucket/runs/stage-68043eed/logs/exit_code.txt"]

    def test_terminated_with_exit_code_nonzero_returns_failed(self, launcher):
        """TERMINATED instance with non-zero exit code should return FAILED."""
        from goldfish.models import StageRunStatus

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="1\n",  # Exit code 1 = failure
                stderr="",
            )

            status = launcher._map_gce_status("TERMINATED", "stage-xyz")
            assert status == StageRunStatus.FAILED
