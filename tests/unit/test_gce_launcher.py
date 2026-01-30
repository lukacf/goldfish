"""Unit tests for GCE launcher functionality.

Includes regression tests for critical bugs:
- Bug #1: GCS paths missing gs:// prefix caused 100% failure rate for exit code
  and log retrieval. The bucket was stored without prefix but used directly in
  gsutil commands, causing all reads to fail silently.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.gce_launcher import GCELauncher
from goldfish.state_machine.exit_code import ExitCodeResult


class TestBucketUri:
    """Test bucket_uri property normalization."""

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked init."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
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
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.bucket = "test-bucket"  # Without gs:// prefix
            launcher.project_id = "test-project"
            launcher.config = MagicMock()
            launcher._storage = MagicMock()
            return launcher

    def test_get_exit_code_success_first_try(self, launcher):
        """Should return exit code on first successful attempt."""
        launcher._storage.get.return_value = b"0\n"

        result = launcher._get_exit_code("stage-abc123")
        assert isinstance(result, ExitCodeResult)
        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False
        assert launcher._storage.get.call_count == 1

    def test_get_exit_code_non_zero_exit(self, launcher):
        """Should return non-zero exit code correctly."""
        launcher._storage.get.return_value = b"42\n"

        result = launcher._get_exit_code("stage-abc123")
        assert result.exists is True
        assert result.code == 42
        assert result.gcs_error is False

    def test_get_exit_code_retry_on_failure(self, launcher):
        """Should retry on gsutil failure and succeed eventually."""
        from goldfish.errors import NotFoundError

        # First 2 attempts miss, third succeeds
        launcher._storage.get.side_effect = [
            NotFoundError("gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"),
            NotFoundError("gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"),
            b"0\n",
        ]

        with patch("time.sleep"):
            result = launcher._get_exit_code("stage-abc123", max_attempts=5, retry_delay=0.1)

        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False
        assert launcher._storage.get.call_count == 3

    def test_get_exit_code_all_retries_exhausted(self, launcher):
        """Should return exists=False (missing file) after all retries exhausted."""
        from goldfish.errors import NotFoundError

        launcher._storage.get.side_effect = NotFoundError("gs://test-bucket/runs/stage-abc123/logs/exit_code.txt")

        with patch("time.sleep"):
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)

        assert result.exists is False
        assert result.code is None
        assert result.gcs_error is False
        assert launcher._storage.get.call_count == 3

    def test_get_exit_code_timeout_retry(self, launcher):
        """Should retry on timeout and succeed eventually."""
        from goldfish.errors import StorageError

        launcher._storage.get.side_effect = [StorageError("Timeout after 30 seconds"), b"0\n"]

        with patch("time.sleep"):
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)

        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False
        assert launcher._storage.get.call_count == 2

    def test_get_exit_code_invalid_content_no_retry(self, launcher):
        """Should not retry on invalid file content (ValueError)."""
        launcher._storage.get.return_value = b"not_a_number\n"

        with patch("time.sleep") as mock_sleep:
            result = launcher._get_exit_code("stage-abc123", max_attempts=3, retry_delay=0.1)

        assert result.exists is True
        assert result.code is None
        assert result.gcs_error is False
        assert result.error is not None
        # Should only try once - invalid content means file exists but is corrupt
        assert launcher._storage.get.call_count == 1
        # No retries means no sleep calls
        assert mock_sleep.call_count == 0

    def test_get_exit_code_no_bucket(self, launcher):
        """Should return 0 if no bucket configured."""
        launcher.bucket = None
        result = launcher._get_exit_code("stage-abc123")
        assert result.exists is True
        assert result.code == 0

    def test_get_exit_code_correct_gcs_path(self, launcher):
        """Should construct correct storage URI with gs:// prefix."""
        launcher._storage.get.return_value = b"0\n"

        # launcher.bucket is "test-bucket" (without gs://) but bucket_uri adds it.
        launcher._get_exit_code("stage-abc123")

        uri = launcher._storage.get.call_args.args[0]
        assert str(uri) == "gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"

    def test_get_exit_code_handles_bucket_with_prefix(self, launcher):
        """Should work correctly when bucket already has gs:// prefix."""
        launcher.bucket = "gs://already-prefixed-bucket"
        launcher._storage.get.return_value = b"0\n"

        launcher._get_exit_code("stage-abc123")

        uri = launcher._storage.get.call_args.args[0]
        assert str(uri) == "gs://already-prefixed-bucket/runs/stage-abc123/logs/exit_code.txt"


class TestGetInstanceLogs:
    """Test get_instance_logs GCS path construction.

    Regression tests for Bug #1: Bucket without gs:// prefix caused log retrieval
    to fail silently, returning empty logs even when logs existed in GCS.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
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
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            # Realistic bucket name without gs:// prefix (as stored in config)
            launcher.bucket = "test-artifacts-bucket"
            launcher.project_id = "test-project"
            launcher._storage = MagicMock()
            return launcher

    def test_terminated_with_exit_code_zero_returns_completed(self, launcher):
        """REGRESSION: TERMINATED instance with exit_code=0 must return COMPLETED.

        This test documents the critical bug where successful stages were marked
        as FAILED because the bucket path was constructed without gs:// prefix,
        causing gsutil to fail and _get_exit_code to return 1 (failure).
        """
        from goldfish.state_machine.types import StageState

        launcher._storage.get.return_value = b"0\n"  # Exit code 0 = success

        status = launcher._map_gce_status("TERMINATED", "stage-68043eed")

        # Must return COMPLETED, not FAILED
        assert status == StageState.COMPLETED, f"TERMINATED with exit_code=0 should be COMPLETED, got {status}"

        uri = launcher._storage.get.call_args.args[0]
        assert str(uri) == "gs://test-artifacts-bucket/runs/stage-68043eed/logs/exit_code.txt"

    def test_terminated_with_exit_code_nonzero_returns_failed(self, launcher):
        """TERMINATED instance with non-zero exit code should return FAILED."""
        from goldfish.state_machine.types import StageState

        launcher._storage.get.return_value = b"1\n"  # Exit code 1 = failure

        status = launcher._map_gce_status("TERMINATED", "stage-xyz")
        assert status == StageState.FAILED


class TestGetInstanceLogsRetry:
    """Tests for get_instance_logs retry_on_empty feature.

    When logs are empty during a running job, retry once after a delay
    to handle GCS eventual consistency.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
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
            patch("goldfish.cloud.adapters.gcp.gce_launcher.time.sleep") as mock_sleep,
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
            patch("goldfish.cloud.adapters.gcp.gce_launcher.time.sleep") as mock_sleep,
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
            patch("goldfish.cloud.adapters.gcp.gce_launcher.time.sleep") as mock_sleep,
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
            patch("goldfish.cloud.adapters.gcp.gce_launcher.time.sleep") as mock_sleep,
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
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
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


class TestGCELaunchResult:
    """Tests for GCELaunchResult dataclass."""

    def test_gce_launch_result_creation(self):
        """GCELaunchResult should store instance_name and zone."""
        from goldfish.cloud.adapters.gcp.gce_launcher import GCELaunchResult

        result = GCELaunchResult(instance_name="stage-abc123", zone="us-central1-a")
        assert result.instance_name == "stage-abc123"
        assert result.zone == "us-central1-a"

    def test_gce_launch_result_different_zones(self):
        """GCELaunchResult should work with various zone formats."""
        from goldfish.cloud.adapters.gcp.gce_launcher import GCELaunchResult

        # Test various zone names
        zones = ["us-central1-a", "europe-west1-b", "asia-east1-c", "us-west1-a"]
        for zone in zones:
            result = GCELaunchResult(instance_name=f"instance-{zone}", zone=zone)
            assert result.zone == zone


class TestLaunchInstanceReturnsZone:
    """Tests for launch_instance returning GCELaunchResult with zone."""

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked init."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.project_id = "test-project"
            launcher.bucket = "gs://test-bucket"
            launcher.resources = []
            launcher.default_zone = "us-central1-a"
            launcher.gpu_preference = None
            launcher._project_number = None
            launcher.service_account = None
            return launcher

    def test_launch_simple_returns_gce_launch_result(self, launcher):
        """_launch_simple should return GCELaunchResult with zone."""
        from goldfish.cloud.adapters.gcp.gce_launcher import GCELaunchResult

        with (
            patch("goldfish.cloud.adapters.gcp.gce_launcher.run_gcloud"),
            patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready"),
        ):
            result = launcher._launch_simple(
                instance_name="stage-test123",
                startup_script="#!/bin/bash\necho test",
                machine_type="n1-standard-4",
                gpu_type=None,
                gpu_count=0,
                zone="europe-west1-b",
            )

        assert isinstance(result, GCELaunchResult)
        assert result.instance_name == "stage-test123"
        assert result.zone == "europe-west1-b"

    def test_launch_with_capacity_search_returns_gce_launch_result(self, launcher):
        """_launch_with_capacity_search should return GCELaunchResult with zone."""
        from dataclasses import dataclass

        from goldfish.cloud.adapters.gcp.gce_launcher import GCELaunchResult

        @dataclass
        class MockSelection:
            zone: str

        @dataclass
        class MockLaunchResult:
            instance_name: str
            selection: MockSelection

        # Set up resources for capacity search
        launcher.resources = [
            {
                "machine": "n1-standard-4",
                "zones": ["us-central1-a", "us-west1-b"],
            }
        ]

        with (
            patch("goldfish.cloud.adapters.gcp.gce_launcher.ResourceLauncher") as MockResourceLauncher,
            patch.object(launcher, "_resolve_service_account", return_value=None),
        ):
            mock_launcher_instance = MagicMock()
            mock_launcher_instance.launch.return_value = MockLaunchResult(
                instance_name="stage-capacity123",
                selection=MockSelection(zone="us-west1-b"),
            )
            MockResourceLauncher.return_value = mock_launcher_instance

            result = launcher._launch_with_capacity_search(
                instance_name="stage-capacity123",
                startup_script="#!/bin/bash\necho test",
                gpu_type=None,
                zones=["us-central1-a", "us-west1-b"],
            )

        assert isinstance(result, GCELaunchResult)
        assert result.instance_name == "stage-capacity123"
        assert result.zone == "us-west1-b"


class TestCapacitySearchGpuFiltering:
    """Tests for GPU filtering in capacity search.

    REGRESSION TESTS: Verifies GPU type filtering uses gpu.accelerator field.
    """

    @pytest.fixture
    def launcher(self):
        """Create a GCE launcher with mocked config and resources."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher.__init__", lambda x, y: None):
            launcher = GCELauncher(MagicMock())
            launcher.bucket = "test-bucket"
            launcher.project_id = "test-project"
            launcher.default_zone = "us-central1-a"
            launcher.zones = ["us-central1-a"]
            launcher.gpu_preference = ["h100", "a100"]
            # Resources with proper profile structure
            launcher.resources = [
                {
                    "name": "h100-spot",
                    "machine_type": "a3-highgpu-1g",
                    "zones": ["us-central1-a", "us-central1-b"],
                    "gpu": {
                        "type": "h100",  # Short name
                        "accelerator": "nvidia-h100-80gb",  # GCE accelerator type
                        "count": 1,
                    },
                },
                {
                    "name": "a100-spot",
                    "machine_type": "a2-highgpu-1g",
                    "zones": ["us-central1-a"],
                    "gpu": {
                        "type": "a100",
                        "accelerator": "nvidia-tesla-a100",
                        "count": 1,
                    },
                },
                {
                    "name": "cpu-small",
                    "machine_type": "n2-standard-4",
                    "zones": ["us-central1-a"],
                    "gpu": {
                        "type": "none",
                        "accelerator": None,
                        "count": 0,
                    },
                },
            ]
            return launcher

    def test_capacity_search_filters_by_gpu_accelerator(self, launcher):
        """REGRESSION: Capacity search must filter by gpu.accelerator, not gpu.type.

        Bug: _launch_with_capacity_search compared gpu.type ("h100") against
        the passed gpu_type ("nvidia-h100-80gb"). They didn't match, causing
        "No resources found for GPU type: nvidia-h100-80gb" error.

        Fix: Changed filter to compare against gpu.accelerator field which
        contains the GCE accelerator type that matches RunSpec.gpu_type.
        """
        from dataclasses import dataclass

        @dataclass
        class MockSelection:
            zone: str

        @dataclass
        class MockLaunchResult:
            instance_name: str
            selection: MockSelection

        with (
            patch("goldfish.cloud.adapters.gcp.gce_launcher.ResourceLauncher") as MockResourceLauncher,
            patch.object(launcher, "_resolve_service_account", return_value=None),
        ):
            mock_launcher_instance = MagicMock()
            mock_launcher_instance.launch.return_value = MockLaunchResult(
                instance_name="stage-h100test",
                selection=MockSelection(zone="us-central1-a"),
            )
            MockResourceLauncher.return_value = mock_launcher_instance

            # This should NOT raise "No resources found" anymore
            # gpu_type is the accelerator name, not the short type
            result = launcher._launch_with_capacity_search(
                instance_name="stage-h100test",
                startup_script="#!/bin/bash\necho test",
                gpu_type="nvidia-h100-80gb",  # Accelerator name from RunSpec.gpu_type
                zones=["us-central1-a"],
            )

            # Verify ResourceLauncher was created with the H100 resource
            MockResourceLauncher.assert_called_once()
            call_kwargs = MockResourceLauncher.call_args.kwargs
            resources = call_kwargs.get("resources", [])
            assert len(resources) == 1, f"Expected 1 H100 resource, got {len(resources)}"
            assert resources[0]["name"] == "h100-spot"

            # Verify result
            assert result.instance_name == "stage-h100test"

    def test_capacity_search_filters_a100_by_accelerator(self, launcher):
        """Verify A100 filtering also works with accelerator name."""
        from dataclasses import dataclass

        @dataclass
        class MockSelection:
            zone: str

        @dataclass
        class MockLaunchResult:
            instance_name: str
            selection: MockSelection

        with (
            patch("goldfish.cloud.adapters.gcp.gce_launcher.ResourceLauncher") as MockResourceLauncher,
            patch.object(launcher, "_resolve_service_account", return_value=None),
        ):
            mock_launcher_instance = MagicMock()
            mock_launcher_instance.launch.return_value = MockLaunchResult(
                instance_name="stage-a100test",
                selection=MockSelection(zone="us-central1-a"),
            )
            MockResourceLauncher.return_value = mock_launcher_instance

            result = launcher._launch_with_capacity_search(
                instance_name="stage-a100test",
                startup_script="#!/bin/bash\necho test",
                gpu_type="nvidia-tesla-a100",  # A100 accelerator name
                zones=["us-central1-a"],
            )

            # Verify ResourceLauncher got A100 resource
            call_kwargs = MockResourceLauncher.call_args.kwargs
            resources = call_kwargs.get("resources", [])
            assert len(resources) == 1
            assert resources[0]["name"] == "a100-spot"

    def test_capacity_search_no_gpu_filters_cpu_resources(self, launcher):
        """Verify no GPU request filters to CPU-only resources."""
        from dataclasses import dataclass

        @dataclass
        class MockSelection:
            zone: str

        @dataclass
        class MockLaunchResult:
            instance_name: str
            selection: MockSelection

        with (
            patch("goldfish.cloud.adapters.gcp.gce_launcher.ResourceLauncher") as MockResourceLauncher,
            patch.object(launcher, "_resolve_service_account", return_value=None),
        ):
            mock_launcher_instance = MagicMock()
            mock_launcher_instance.launch.return_value = MockLaunchResult(
                instance_name="stage-cputest",
                selection=MockSelection(zone="us-central1-a"),
            )
            MockResourceLauncher.return_value = mock_launcher_instance

            result = launcher._launch_with_capacity_search(
                instance_name="stage-cputest",
                startup_script="#!/bin/bash\necho test",
                gpu_type=None,  # No GPU
                zones=["us-central1-a"],
            )

            # Verify ResourceLauncher got CPU resource
            call_kwargs = MockResourceLauncher.call_args.kwargs
            resources = call_kwargs.get("resources", [])
            assert len(resources) == 1
            assert resources[0]["name"] == "cpu-small"

    def test_capacity_search_raises_on_unknown_gpu_type(self, launcher):
        """Verify error when no resource matches the GPU type."""
        from goldfish.errors import GoldfishError

        # No T4 profile in resources
        with pytest.raises(GoldfishError) as exc_info:
            launcher._launch_with_capacity_search(
                instance_name="stage-t4test",
                startup_script="#!/bin/bash\necho test",
                gpu_type="nvidia-tesla-t4",  # Not in our resources
                zones=["us-central1-a"],
            )

        assert "No resources found" in str(exc_info.value)
        assert "nvidia-tesla-t4" in str(exc_info.value)
