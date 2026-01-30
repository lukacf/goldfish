"""Tests for ExitCodeResult and exit code retrieval fixes.

This module tests the fix for the critical exit code bug where GCS failures
were incorrectly reported as exit code 1, making it impossible to distinguish:
- GCS unavailable (network/auth issue)
- Exit code file missing (crash/preemption)
- Actual exit code 1 (process failure)

TDD: These tests are written BEFORE the implementation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    pass


class TestExitCodeResult:
    """Tests for ExitCodeResult dataclass."""

    def test_exit_code_result_distinguishes_missing_from_exit_1(self) -> None:
        """ExitCodeResult must distinguish 'file missing' from 'exit code 1'."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        # File missing (crash/preemption)
        missing = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)
        assert missing.exists is False
        assert missing.code is None
        assert missing.gcs_error is False

        # Actual exit code 1 (process failure)
        exit_1 = ExitCodeResult(exists=True, code=1, gcs_error=False, error=None)
        assert exit_1.exists is True
        assert exit_1.code == 1
        assert exit_1.gcs_error is False

        # They are different!
        assert missing.exists != exit_1.exists

    def test_gcs_unavailable_returns_gcs_error_true(self) -> None:
        """GCS unavailable must return gcs_error=True, not exit code 1."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult(
            exists=False,
            code=None,
            gcs_error=True,
            error="ServiceUnavailable: 503",
        )
        assert result.gcs_error is True
        assert result.exists is False
        assert result.code is None
        assert result.error is not None

    def test_exit_code_0_returns_success(self) -> None:
        """Exit code 0 file must return success."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)
        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False
        assert result.is_success() is True

    def test_exit_code_nonzero_returns_failure(self) -> None:
        """Exit code non-zero must return failure with code."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        for code in [1, 2, 137, 255]:
            result = ExitCodeResult(exists=True, code=code, gcs_error=False, error=None)
            assert result.exists is True
            assert result.code == code
            assert result.gcs_error is False
            assert result.is_success() is False

    def test_is_success_false_when_missing(self) -> None:
        """is_success() must be False when exit code file is missing."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)
        assert result.is_success() is False

    def test_is_success_false_when_gcs_error(self) -> None:
        """is_success() must be False when GCS error occurred."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult(exists=False, code=None, gcs_error=True, error="Timeout")
        assert result.is_success() is False

    def test_is_definite_failure_distinguishes_real_failures(self) -> None:
        """is_definite_failure() must only be True for confirmed exit failures."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        # Exit code 1 - definite failure
        exit_1 = ExitCodeResult(exists=True, code=1, gcs_error=False, error=None)
        assert exit_1.is_definite_failure() is True

        # File missing - NOT a definite failure (could be crash/preemption)
        missing = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)
        assert missing.is_definite_failure() is False

        # GCS error - NOT a definite failure (need to retry)
        gcs_err = ExitCodeResult(exists=False, code=None, gcs_error=True, error="503")
        assert gcs_err.is_definite_failure() is False

        # Exit code 0 - not a failure at all
        success = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)
        assert success.is_definite_failure() is False


class TestGetExitCodeGCE:
    """Tests for GCE exit code retrieval."""

    def test_gcs_unavailable_returns_gcs_error_not_exit_1(self) -> None:
        """GCS unavailable must return gcs_error=True, not exit code 1."""
        from goldfish.errors import StorageError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = StorageError("ServiceUnavailable: 503")

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
            max_attempts=1,
        )

        assert result.gcs_error is True
        assert result.exists is False
        assert result.code is None  # NOT 1!

    def test_file_not_found_returns_exists_false(self) -> None:
        """Missing exit code file must return exists=False."""
        from goldfish.errors import NotFoundError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = NotFoundError("gs://test-bucket/runs/stage-123/logs/exit_code.txt")

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
            max_attempts=1,
        )

        assert result.exists is False
        assert result.code is None
        assert result.gcs_error is False  # File not found is not a GCS error

    def test_exit_code_0_file_returns_success(self) -> None:
        """Exit code 0 file must return success."""
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.return_value = b"0\n"

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
        )

        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False

    def test_exit_code_nonzero_returns_failure(self) -> None:
        """Exit code non-zero must return failure with code."""
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.return_value = b"137\n"

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
        )

        assert result.exists is True
        assert result.code == 137
        assert result.gcs_error is False

    def test_timeout_returns_gcs_error(self) -> None:
        """Subprocess timeout must return gcs_error=True."""
        from goldfish.errors import StorageError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = StorageError("Timeout after 30s")

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
            max_attempts=1,
        )

        assert result.gcs_error is True
        assert result.exists is False
        assert "timeout" in (result.error or "").lower()

    def test_invalid_content_returns_error(self) -> None:
        """Invalid exit code content must return error, not 1."""
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.return_value = b"not_a_number\n"

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
        )

        # Invalid content should be treated as exists=True but with error
        assert result.exists is True
        assert result.code is None
        assert result.error is not None

    def test_auth_error_returns_gcs_error(self) -> None:
        """Authentication error must return gcs_error=True."""
        from goldfish.errors import StorageError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = StorageError("AccessDeniedException: 403")

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-123",
            storage=storage,
            project_id="test-project",
            max_attempts=1,
        )

        # Auth error is a GCS error, not "file not found"
        assert result.gcs_error is True
        assert result.exists is False

    def test_retries_on_transient_errors(self) -> None:
        """Function must retry on transient GCS errors."""
        from goldfish.errors import StorageError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = [StorageError("ServiceUnavailable: 503"), b"0\n"]

        with patch("time.sleep"):
            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-123",
                storage=storage,
                project_id="test-project",
                max_attempts=2,
            )

        assert result.exists is True
        assert result.code == 0
        assert storage.get.call_count == 2


class TestGetExitCodeDocker:
    """Tests for Docker (local) exit code retrieval."""

    def test_container_not_found_returns_exists_false(self) -> None:
        """Missing container must return exists=False."""
        from goldfish.state_machine.exit_code import get_exit_code_docker

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "docker")
            error.stderr = "No such container"
            mock_run.side_effect = error

            result = get_exit_code_docker(container_id="test-container")

            assert result.exists is False
            assert result.code is None

    def test_exit_code_0_returns_success(self) -> None:
        """Exit code 0 from docker inspect must return success."""
        from goldfish.state_machine.exit_code import get_exit_code_docker

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="0\n", returncode=0)

            result = get_exit_code_docker(container_id="test-container")

            assert result.exists is True
            assert result.code == 0

    def test_exit_code_nonzero_returns_failure(self) -> None:
        """Exit code non-zero from docker inspect must return failure."""
        from goldfish.state_machine.exit_code import get_exit_code_docker

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="1\n", returncode=0)

            result = get_exit_code_docker(container_id="test-container")

            assert result.exists is True
            assert result.code == 1

    def test_running_container_returns_exists_false(self) -> None:
        """Running container (no exit code yet) must return exists=False."""
        from goldfish.state_machine.exit_code import get_exit_code_docker

        with patch("subprocess.run") as mock_run:
            # Docker returns empty or special value for running container
            mock_run.return_value = MagicMock(stdout="\n", returncode=0)

            result = get_exit_code_docker(container_id="test-container")

            # Running container has no exit code yet
            assert result.exists is False
            assert result.code is None


class TestExitCodeResultEquality:
    """Tests for ExitCodeResult equality and comparison."""

    def test_equality_based_on_all_fields(self) -> None:
        """Two ExitCodeResult with same fields must be equal."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        r1 = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)
        r2 = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)
        assert r1 == r2

    def test_inequality_on_different_exists(self) -> None:
        """Different exists values must not be equal."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        r1 = ExitCodeResult(exists=True, code=1, gcs_error=False, error=None)
        r2 = ExitCodeResult(exists=False, code=None, gcs_error=False, error=None)
        assert r1 != r2

    def test_inequality_on_different_code(self) -> None:
        """Different code values must not be equal."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        r1 = ExitCodeResult(exists=True, code=0, gcs_error=False, error=None)
        r2 = ExitCodeResult(exists=True, code=1, gcs_error=False, error=None)
        assert r1 != r2


class TestExitCodeResultFromGCSError:
    """Tests for creating ExitCodeResult from various GCS errors."""

    def test_from_not_found_error(self) -> None:
        """Factory for 'not found' errors."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult.from_not_found()
        assert result.exists is False
        assert result.code is None
        assert result.gcs_error is False
        assert result.error is None

    def test_from_gcs_error(self) -> None:
        """Factory for GCS errors."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult.from_gcs_error("ServiceUnavailable: 503")
        assert result.exists is False
        assert result.code is None
        assert result.gcs_error is True
        assert result.error == "ServiceUnavailable: 503"

    def test_from_success(self) -> None:
        """Factory for successful retrieval."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult.from_code(0)
        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False
        assert result.error is None

    def test_from_failure(self) -> None:
        """Factory for exit code failure."""
        from goldfish.state_machine.exit_code import ExitCodeResult

        result = ExitCodeResult.from_code(137)
        assert result.exists is True
        assert result.code == 137
        assert result.gcs_error is False
        assert result.error is None
