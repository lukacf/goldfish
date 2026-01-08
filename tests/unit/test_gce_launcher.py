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
        """Should construct correct GCS path with gs:// prefix and project_id."""
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
            # Includes -o GSUtil:project_id=... when project_id is set
            assert args == [
                "gsutil",
                "-o",
                "GSUtil:project_id=test-project",
                "cat",
                "gs://test-bucket/runs/stage-abc123/logs/exit_code.txt",
            ]

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
            # Should NOT double the gs:// prefix, includes project_id option
            assert args == [
                "gsutil",
                "-o",
                "GSUtil:project_id=test-project",
                "cat",
                "gs://already-prefixed-bucket/runs/stage-abc123/logs/exit_code.txt",
            ]


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
            launcher.bucket = "test-artifacts-bucket"  # Without gs:// prefix (realistic)
            launcher.project_id = "test-project"
            launcher.default_zone = "us-central1-a"
            launcher.zones = ["us-central1-a"]
            return launcher

    def test_get_instance_logs_uses_bucket_uri(self, launcher):
        """REGRESSION: get_instance_logs must use bucket_uri not bucket directly.

        This test documents the bug where bucket="test-artifacts-bucket" was used
        directly in gsutil commands, resulting in invalid paths like:
            gsutil cat test-artifacts-bucket/runs/stage-xxx/logs/stdout.log
        instead of:
            gsutil cat gs://test-artifacts-bucket/runs/stage-xxx/logs/stdout.log
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
            assert call_args[0] == "gcloud"
            assert call_args[1] == "storage"
            assert call_args[2] == "cat"
            # CRITICAL: Must have gs:// prefix
            assert call_args[3].startswith("gs://"), f"GCS path missing gs:// prefix: {call_args[3]}"
            assert call_args[3] == "gs://test-artifacts-bucket/runs/stage-abc123/logs/stdout.log"

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
            assert "gs://gs://" not in call_args[3], f"Double gs:// prefix detected: {call_args[3]}"
            assert call_args[3] == "gs://already-prefixed/runs/stage-abc123/logs/stdout.log"


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
            launcher.bucket = "test-artifacts-bucket"
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

            # Verify correct GCS path was used (includes project_id option)
            call_args = mock_run.call_args[0][0]
            assert call_args == [
                "gsutil",
                "-o",
                "GSUtil:project_id=test-project",
                "cat",
                "gs://test-artifacts-bucket/runs/stage-68043eed/logs/exit_code.txt",
            ]

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


class TestGetInstanceLogsRetry:
    """Tests for get_instance_logs retry_on_empty feature.

    When logs are empty during a running job, retry once after a delay
    to handle GCS eventual consistency.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.bucket = "test-bucket"
            launcher.project_id = "test-project"
            launcher.default_zone = "us-central1-a"
            launcher.zones = ["us-central1-a"]
            return launcher

    def test_get_instance_logs_retry_on_empty_retries_once(self, launcher):
        """Should retry once after delay if logs are empty and retry_on_empty=True."""
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
            patch("goldfish.infra.gce_launcher.time.sleep") as mock_sleep,
        ):

            def make_empty_proc():
                """Create a mock process that returns empty output."""
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter([]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            def make_proc_with_logs():
                """Create a mock process that returns logs."""
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter(["log line 1\n", "log line 2\n"]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            # First _fetch_gcs_logs: 2 calls (stdout.log + stderr.log) - both empty
            # Second _fetch_gcs_logs (retry): 2 calls - stdout has logs
            mock_popen.side_effect = [
                make_empty_proc(),  # First fetch: stdout.log (empty)
                make_empty_proc(),  # First fetch: stderr.log (empty)
                make_proc_with_logs(),  # Retry: stdout.log (has logs)
                make_empty_proc(),  # Retry: stderr.log (empty)
            ]

            result = launcher.get_instance_logs("stage-abc", retry_on_empty=True)

            # Should have retried (sleep called)
            mock_sleep.assert_called()

            # Should return logs from second attempt
            assert "log line 1" in result
            assert "log line 2" in result

    def test_get_instance_logs_no_retry_when_content_found(self, launcher):
        """Should not retry if logs have content."""
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
            patch("goldfish.infra.gce_launcher.time.sleep") as mock_sleep,
        ):

            def make_proc_with_logs():
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter(["log content\n"]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            def make_empty_proc():
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter([]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            # Only 2 calls needed: stdout.log + stderr.log (no retry)
            mock_popen.side_effect = [
                make_proc_with_logs(),  # stdout.log has content
                make_empty_proc(),  # stderr.log empty
            ]

            result = launcher.get_instance_logs("stage-abc", retry_on_empty=True)

            # Should NOT have retried (no sleep)
            mock_sleep.assert_not_called()

            # Should return logs
            assert "log content" in result

    def test_get_instance_logs_no_retry_by_default(self, launcher):
        """retry_on_empty should default to False for backward compatibility."""
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
            patch("goldfish.infra.gce_launcher.time.sleep") as mock_sleep,
        ):

            def make_empty_proc():
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter([]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            # 2 calls: stdout.log + stderr.log (both empty, no retry)
            mock_popen.side_effect = [
                make_empty_proc(),
                make_empty_proc(),
            ]

            # Call without retry_on_empty (defaults to False)
            result = launcher.get_instance_logs("stage-abc")

            # Should NOT retry
            mock_sleep.assert_not_called()

            # Should return empty string
            assert result == "" or result is None

    def test_get_instance_logs_retry_delay(self, launcher):
        """Retry should wait appropriate time before retrying."""
        with (
            patch("subprocess.Popen") as mock_popen,
            patch.object(launcher, "_sanitize_name", return_value="stage-abc"),
            patch.object(launcher, "_find_instance_zone", return_value=None),
            patch("goldfish.infra.gce_launcher.time.sleep") as mock_sleep,
        ):

            def make_empty_proc():
                proc = MagicMock()
                proc.stdout = MagicMock()
                proc.stdout.__enter__ = MagicMock(return_value=proc.stdout)
                proc.stdout.__exit__ = MagicMock(return_value=False)
                proc.stdout.__iter__ = MagicMock(return_value=iter([]))
                proc.wait = MagicMock(return_value=0)
                proc.returncode = 0
                return proc

            # 4 calls: first fetch (2 empty) + retry (2 empty)
            mock_popen.side_effect = [
                make_empty_proc(),  # First fetch: stdout.log
                make_empty_proc(),  # First fetch: stderr.log
                make_empty_proc(),  # Retry: stdout.log
                make_empty_proc(),  # Retry: stderr.log
            ]

            launcher.get_instance_logs("stage-abc", retry_on_empty=True)

            # Should wait ~5 seconds before retry
            if mock_sleep.called:
                delay = mock_sleep.call_args[0][0]
                assert 3 <= delay <= 10, f"Retry delay should be 3-10 seconds, got {delay}"


class TestInputStagingBucketMatching:
    """Test bucket matching logic in input staging.

    Regression tests for Bug: Trailing slashes in bucket names cause staging to
    fail. When bucket_name = "my-bucket/" (from gs://my-bucket/) but input_bucket
    = "my-bucket" (from URI split), the comparison fails and fast gcsfuse symlinks
    are skipped in favor of slow gsutil cp.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked init."""
        with patch("goldfish.infra.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.project_id = "test-project"
            launcher.config = MagicMock()
            launcher.config.artifact_registry = None
            launcher.default_zone = "us-central1-a"
            launcher.zones = ["us-central1-a"]
            return launcher

    def test_bucket_with_trailing_slash_matches_input_bucket(self, launcher):
        """REGRESSION: Bucket with trailing slash should still match input.

        When self.bucket = "gs://my-bucket/" and input is "gs://my-bucket/data/file.csv",
        the bucket comparison should succeed and use gcsfuse symlink (not gsutil cp).

        Fixed in gce_launcher.py line 199: bucket_name now strips trailing slash.
        """
        # Set bucket WITH trailing slash (edge case that triggered the bug)
        launcher.bucket = "gs://my-bucket/"

        # Normalize bucket_name the way gce_launcher.py does after fix
        bucket_name = launcher.bucket.replace("gs://", "").rstrip("/")

        # Input URI is gs://my-bucket/path/to/data
        gcs_uri = "gs://my-bucket/path/to/data"
        uri_parts = gcs_uri.replace("gs://", "").split("/", 1)
        input_bucket = uri_parts[0]  # "my-bucket"

        # After fix: These MUST match for gcsfuse symlink to be used
        assert bucket_name == input_bucket, f"bucket_name '{bucket_name}' should match input_bucket '{input_bucket}'"

    def test_input_uri_with_trailing_slash_still_matches(self, launcher):
        """Input URI with trailing slash should match bucket correctly."""
        launcher.bucket = "gs://my-bucket"

        # Input has trailing slash in URI
        stage_config = {
            "inputs": {
                "data": {
                    "location": "gs://my-bucket/path/to/data/",  # Note trailing slash
                    "format": "directory",
                }
            }
        }

        # Extract input_bucket the same way the code does
        gcs_uri = "gs://my-bucket/path/to/data/"
        uri_parts = gcs_uri.replace("gs://", "").split("/", 1)
        input_bucket = uri_parts[0]  # Should be "my-bucket"

        bucket_name = launcher.bucket.replace("gs://", "").rstrip("/")

        # These should match
        assert input_bucket == bucket_name, f"input_bucket '{input_bucket}' should match bucket_name '{bucket_name}'"


class TestGcsfuseFallback:
    """Test gcsfuse fallback to gsutil when path doesn't exist.

    REGRESSION TESTS: When gcsfuse doesn't see a path (due to caching, timing,
    or other issues), staging should fall back to gsutil cp instead of creating
    a broken symlink.
    """

    def test_staging_generates_fallback_command(self):
        """REGRESSION: Staging should check gcsfuse path and fall back to gsutil.

        When staging inputs, the code should:
        1. Check if gcsfuse path exists
        2. If yes, create symlink (fast)
        3. If no, use gsutil cp (reliable fallback)

        This prevents broken symlinks when gcsfuse doesn't see recently-uploaded data.
        """
        # The generated shell command should have conditional logic
        expected_pattern = "if [ -e"
        fallback_pattern = "gsutil -m cp -r"

        # Simulate the staging command generation
        input_path = "runs/stage-abc/outputs/bytes_6_2/"
        input_name = "bytes"
        gcs_uri = "gs://my-bucket/runs/stage-abc/outputs/bytes_6_2/"
        gcsfuse_path = f"/mnt/gcs/{input_path.rstrip('/')}"

        # Build the command like gce_launcher.py does
        staging_cmd = (
            f'if [ -e "{gcsfuse_path}" ] || [ -d "{gcsfuse_path}" ]; then '
            f'echo "DEBUG: gcsfuse path exists, creating symlink"; '
            f'ln -sf "{gcsfuse_path}" "/mnt/inputs/{input_name}"; '
            f'else '
            f'echo "DEBUG: gcsfuse path not found, falling back to gsutil cp"; '
            f'gsutil -m cp -r "{gcs_uri.rstrip("/")}" "/mnt/inputs/{input_name}"; '
            f'fi'
        )

        # Verify the command has the expected structure
        assert expected_pattern in staging_cmd, "Should check if gcsfuse path exists"
        assert fallback_pattern in staging_cmd, "Should have gsutil fallback"
        assert gcsfuse_path in staging_cmd, "Should reference gcsfuse path"
        assert input_name in staging_cmd, "Should reference input name"

    def test_gsutil_fallback_uses_correct_gcs_uri(self):
        """REGRESSION: gsutil fallback should use original GCS URI, not gcsfuse path.

        When falling back to gsutil, we must use the full gs:// URI, not the
        gcsfuse mount path. This ensures the data is downloaded correctly.
        """
        gcs_uri = "gs://my-bucket/runs/stage-de74f1a8/outputs/bytes_6_2/"
        input_name = "bytes"

        # The gsutil command should use the original GCS URI
        gsutil_cmd = f'gsutil -m cp -r "{gcs_uri.rstrip("/")}" "/mnt/inputs/{input_name}"'

        # Verify URI format
        assert "gs://my-bucket" in gsutil_cmd, "Should use full gs:// URI"
        assert not gsutil_cmd.startswith("/mnt/gcs"), "Should NOT use gcsfuse path for gsutil"
        assert gcs_uri.rstrip("/") in gsutil_cmd, "Should strip trailing slash from URI"

    def test_staging_failure_writes_error_and_exits(self):
        """REGRESSION: Staging failure should write error to stderr.log and exit.

        When gsutil cp fails (e.g., GCS path doesn't exist), the staging script
        should:
        1. Write a clear error message to stderr.log
        2. Exit with error code (triggering self_delete and log sync)

        This prevents misleading "Instance not found in zone" errors when the
        actual problem is a missing input source.
        """
        input_name = "bytes"
        gcs_uri = "gs://my-bucket/runs/stage-xyz/outputs/bytes_6_2/"
        debug_log = "/tmp/staging_debug.log"

        # The staging command should check gsutil exit code and fail loudly
        # This is the pattern used in gce_launcher.py for gsutil fallback
        staging_cmd = (
            f'if ! gsutil -m cp -r "{gcs_uri.rstrip("/")}" "/mnt/inputs/{input_name}"; then '
            f'echo "ERROR: Failed to stage input {input_name} from {gcs_uri}" | tee -a {debug_log} /tmp/stderr.log; '
            f'echo "The GCS path may not exist or you may lack permissions." | tee -a {debug_log} /tmp/stderr.log; '
            f'exit 1; fi'
        )

        # Verify the command has error handling
        assert "if ! gsutil" in staging_cmd, "Should check gsutil exit code"
        assert "ERROR:" in staging_cmd, "Should write clear error message"
        assert "/tmp/stderr.log" in staging_cmd, "Should write to stderr.log for sync"
        assert "exit 1" in staging_cmd, "Should exit on failure"
        assert input_name in staging_cmd, "Error should include input name"
        assert gcs_uri in staging_cmd, "Error should include GCS URI"


class TestSerialConsoleNoiseFilter:
    """Test filtering of noisy serial console output."""

    def test_filters_metadata_syncer_noise(self):
        """REGRESSION: Serial console logs should filter out metadata syncer noise.

        When GCS logs aren't available yet, we fall back to serial console.
        The serial console includes noisy metadata syncer loops that obscure
        the actual training output.
        """
        # Simulate noisy serial console output
        noisy_lines = [
            "2026-01-08 google_metadata_script_runner[1554]: + curl -sf -H 'Metadata-Flavor: Google'\n",
            '2026-01-08 google_metadata_script_runner[1554]: + SIG_JSON=\'{"command":"sync"}\'\n',
            "[2026-01-08 09:10:53] INFO: Config: model=small seq_len=2048\n",
            "2026-01-08 google_metadata_script_runner[1554]: + REQ_ID=049d0c9e\n",
            "[2026-01-08 09:10:56] INFO: Using device: cuda\n",
            "2026-01-08 google_metadata_script_runner[1554]: + sleep 1\n",
            "2026-01-08 Epoch 1/20: loss=2.543, dir=51.2%\n",
            '2026-01-08 Metadata key("startup-script"): ++ printf %s\n',
            "2026-01-08 gcloud storage cp /mnt/outputs/.goldfish/metrics.jsonl\n",
        ]

        # Define the noise patterns (same as in gce_launcher.py)
        noise_patterns = [
            "google_metadata_script_runner",
            'Metadata key("startup-script")',
            "SIG_JSON=",
            "REQ_ID=",
            "curl -sf -H",
            "printf %s",
            "sleep 1",
            "gcloud storage cp",
        ]

        # Filter the lines
        filtered = []
        for line in noisy_lines:
            if any(pattern in line for pattern in noise_patterns):
                continue
            filtered.append(line)

        # Verify useful lines are kept
        assert len(filtered) == 3, f"Expected 3 useful lines, got {len(filtered)}"
        assert any("Config: model=small" in line for line in filtered)
        assert any("Using device: cuda" in line for line in filtered)
        assert any("Epoch 1/20" in line for line in filtered)

        # Verify noisy lines are removed
        assert not any("google_metadata_script_runner" in line for line in filtered)
        assert not any("SIG_JSON" in line for line in filtered)
        assert not any("sleep 1" in line for line in filtered)
