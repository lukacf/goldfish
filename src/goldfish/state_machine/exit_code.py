"""Exit code retrieval with proper error handling.

This module fixes the critical exit code bug where GCS failures were incorrectly
reported as exit code 1, making it impossible to distinguish:
- GCS unavailable (network/auth issue) → gcs_error=True
- Exit code file missing (crash/preemption) → exists=False
- Actual exit code 1 (process failure) → exists=True, code=1
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass

from goldfish.validation import (
    validate_container_id,
    validate_project_id,
    validate_stage_run_id,
)

logger = logging.getLogger(__name__)


@dataclass
class ExitCodeResult:
    """Result of attempting to retrieve an exit code.

    This dataclass distinguishes between:
    - File exists with code → exists=True, code=N
    - File missing → exists=False, code=None
    - GCS/Docker error → gcs_error=True (or error set for Docker)

    Attributes:
        exists: Whether the exit code file/container state exists.
        code: The exit code value (None if file doesn't exist or error).
        gcs_error: Whether a GCS error occurred (transient, should retry).
        error: Error message if any.
    """

    exists: bool
    code: int | None
    gcs_error: bool
    error: str | None

    def is_success(self) -> bool:
        """Check if this represents a successful execution (exit code 0)."""
        return self.exists and self.code == 0 and not self.gcs_error

    def is_definite_failure(self) -> bool:
        """Check if this is a confirmed process failure (exit code != 0).

        Returns False for:
        - Missing exit code (could be crash/preemption)
        - GCS errors (need to retry)
        - Success (exit code 0)

        Only returns True when we definitively know the process
        ran to completion and returned a non-zero exit code.
        """
        return self.exists and self.code is not None and self.code != 0

    @classmethod
    def from_code(cls, code: int) -> ExitCodeResult:
        """Create result from a successfully retrieved exit code."""
        return cls(exists=True, code=code, gcs_error=False, error=None)

    @classmethod
    def from_not_found(cls) -> ExitCodeResult:
        """Create result when exit code file is missing."""
        return cls(exists=False, code=None, gcs_error=False, error=None)

    @classmethod
    def from_gcs_error(cls, error: str) -> ExitCodeResult:
        """Create result when GCS error occurred."""
        return cls(exists=False, code=None, gcs_error=True, error=error)

    @classmethod
    def from_parse_error(cls, error: str) -> ExitCodeResult:
        """Create result when exit code content couldn't be parsed."""
        return cls(exists=True, code=None, gcs_error=False, error=error)


def get_exit_code_gce(
    bucket_uri: str,
    stage_run_id: str,
    project_id: str | None = None,
    max_attempts: int = 5,
    retry_delay: float = 2.0,
) -> ExitCodeResult:
    """Get exit code from GCS with proper error handling.

    Uses retries to handle GCS eventual consistency and temporary failures.
    The instance uploads exit_code.txt before self-deleting, but there can be
    a race condition where the daemon checks before GCS is synced.

    Args:
        bucket_uri: GCS bucket URI (e.g., "gs://my-bucket").
        stage_run_id: Stage run identifier.
        project_id: Optional GCP project ID.
        max_attempts: Number of retry attempts (default 5).
        retry_delay: Seconds between retries (default 2.0).

    Returns:
        ExitCodeResult with proper categorization of the outcome.

    Raises:
        InvalidStageRunIdError: If stage_run_id is invalid.
        InvalidProjectIdError: If project_id is invalid.
    """
    # Validate inputs before subprocess calls (security)
    validate_stage_run_id(stage_run_id)
    if project_id:
        validate_project_id(project_id)

    gcs_path = f"{bucket_uri.rstrip('/')}/runs/{stage_run_id}/logs/exit_code.txt"
    last_error: str | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            cmd = ["gsutil"]
            if project_id:
                cmd.extend(["-o", f"GSUtil:project_id={project_id}"])
            cmd.extend(["cat", gcs_path])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )

            content = result.stdout.strip()
            if not content:
                # Empty file - treat as exists but parse error
                return ExitCodeResult.from_parse_error("Empty exit code file")

            try:
                exit_code = int(content)
                if attempt > 1:
                    logger.info(
                        "exit_code.txt retrieved for %s on attempt %d (exit_code=%d)",
                        stage_run_id,
                        attempt,
                        exit_code,
                    )
                return ExitCodeResult.from_code(exit_code)
            except ValueError as e:
                return ExitCodeResult.from_parse_error(f"Invalid exit code content: {e}")

        except subprocess.TimeoutExpired:
            last_error = f"Timeout after 30s (attempt {attempt}/{max_attempts})"
            logger.warning(
                "gsutil timeout for %s (attempt %d/%d)",
                stage_run_id,
                attempt,
                max_attempts,
            )

        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").lower()

            # Check for "not found" errors vs GCS errors
            if "no urls matched" in stderr or "commandexception" in stderr:
                # File not found - not a GCS error, file genuinely doesn't exist
                logger.debug(
                    "exit_code.txt not found for %s (attempt %d/%d)",
                    stage_run_id,
                    attempt,
                    max_attempts,
                )
                # Retry in case of eventual consistency
                if attempt == max_attempts:
                    return ExitCodeResult.from_not_found()
                last_error = "File not found"

            elif "accessdeniedexception" in stderr or "403" in stderr:
                # Auth error - this is a GCS error
                last_error = f"Access denied: {e.stderr}"
                logger.warning(
                    "gsutil auth error for %s (attempt %d/%d): %s",
                    stage_run_id,
                    attempt,
                    max_attempts,
                    e.stderr,
                )
                # Return immediately for auth errors - retries won't help
                return ExitCodeResult.from_gcs_error(last_error)

            elif "serviceunavailable" in stderr or "503" in stderr:
                # Transient GCS error - retry
                last_error = f"Service unavailable: {e.stderr}"
                logger.warning(
                    "gsutil service error for %s (attempt %d/%d): %s",
                    stage_run_id,
                    attempt,
                    max_attempts,
                    e.stderr,
                )

            else:
                # Other error
                last_error = f"gsutil error: {e.stderr}"
                logger.warning(
                    "gsutil error for %s (attempt %d/%d): %s",
                    stage_run_id,
                    attempt,
                    max_attempts,
                    e.stderr,
                )

        except Exception as e:
            last_error = str(e)
            logger.warning(
                "Unexpected error reading exit_code.txt for %s (attempt %d/%d): %s",
                stage_run_id,
                attempt,
                max_attempts,
                e,
            )

        # Wait before retry (except on last attempt)
        if attempt < max_attempts:
            time.sleep(retry_delay)

    # All retries exhausted - return GCS error
    return ExitCodeResult.from_gcs_error(last_error or "Unknown error after retries")


def get_exit_code_docker(
    container_id: str,
    max_attempts: int = 3,
    retry_delay: float = 1.0,
) -> ExitCodeResult:
    """Get exit code from Docker container.

    Args:
        container_id: Docker container ID or name.
        max_attempts: Number of retry attempts (default 3).
        retry_delay: Seconds between retries (default 1.0).

    Returns:
        ExitCodeResult with proper categorization of the outcome.

    Raises:
        InvalidContainerIdError: If container_id is invalid.
    """
    # Validate inputs before subprocess calls (security)
    validate_container_id(container_id)

    last_error: str | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            # Use docker inspect to get exit code
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.ExitCode}}", container_id],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )

            content = result.stdout.strip()

            # Check if container is still running (empty or no exit code yet)
            if not content:
                return ExitCodeResult(
                    exists=False,
                    code=None,
                    gcs_error=False,
                    error=None,
                )

            try:
                exit_code = int(content)
                return ExitCodeResult.from_code(exit_code)
            except ValueError:
                # Running container may return empty or invalid
                return ExitCodeResult(
                    exists=False,
                    code=None,
                    gcs_error=False,
                    error=None,
                )

        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").lower()

            if "no such container" in stderr or "not found" in stderr:
                # Container doesn't exist
                return ExitCodeResult.from_not_found()

            last_error = f"docker error: {e.stderr}"
            logger.warning(
                "docker inspect error for %s (attempt %d/%d): %s",
                container_id,
                attempt,
                max_attempts,
                e.stderr,
            )

        except subprocess.TimeoutExpired:
            last_error = f"Timeout after 10s (attempt {attempt}/{max_attempts})"
            logger.warning(
                "docker inspect timeout for %s (attempt %d/%d)",
                container_id,
                attempt,
                max_attempts,
            )

        except Exception as e:
            last_error = str(e)
            logger.warning(
                "Unexpected error inspecting container %s (attempt %d/%d): %s",
                container_id,
                attempt,
                max_attempts,
                e,
            )

        # Wait before retry (except on last attempt)
        if attempt < max_attempts:
            time.sleep(retry_delay)

    # All retries exhausted
    return ExitCodeResult(
        exists=False,
        code=None,
        gcs_error=False,
        error=last_error,
    )
