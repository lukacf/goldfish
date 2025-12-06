"""Job status tracking and monitoring.

This module handles:
- Polling job status from the infrastructure
- Updating job records in the database
- Retrieving job logs and artifacts
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from goldfish.db.database import Database
from goldfish.errors import GoldfishError, JobNotFoundError
from goldfish.models import CancelJobResponse, JobInfo, JobStatus
from goldfish.validation import validate_log_path


def _safe_opener(path, flags):
    """Open files without following symlinks (O_NOFOLLOW).

    This prevents TOCTOU attacks where a validated file path is replaced
    with a symlink between validation and reading.

    Args:
        path: File path to open
        flags: File open flags

    Returns:
        File descriptor

    Raises:
        OSError: If path is a symlink (ELOOP on macOS/BSD, EMLINK on Linux)
    """
    return os.open(path, flags | os.O_NOFOLLOW)


class JobTracker:
    """Tracks job status and retrieves logs/artifacts."""

    def __init__(self, db: Database, project_root: Path):
        """Initialize job tracker.

        Args:
            db: Database instance
            project_root: Project root path
        """
        self.db = db
        self.project_root = project_root

    def get_job(self, job_id: str) -> JobInfo:
        """Get job information.

        Args:
            job_id: The job ID

        Returns:
            JobInfo with current status

        Raises:
            JobNotFoundError: If job doesn't exist
        """
        job = self.db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        return self._job_dict_to_info(job)

    def get_active_jobs(self) -> list[JobInfo]:
        """Get all active (pending/running) jobs.

        Returns:
            List of active JobInfo objects
        """
        jobs = self.db.get_active_jobs()
        return [self._job_dict_to_info(j) for j in jobs]

    def list_jobs(
        self,
        workspace: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[JobInfo]:
        """List jobs with optional filters.

        Args:
            workspace: Filter by workspace name
            status: Filter by status (pending, running, completed, failed)
            limit: Maximum number of jobs to return

        Returns:
            List of JobInfo objects
        """
        jobs = self.db.list_jobs(status=status, workspace=workspace, limit=limit)
        return [self._job_dict_to_info(j) for j in jobs]

    def update_job_status(
        self,
        job_id: str,
        status: str,
        log_uri: Optional[str] = None,
        artifact_uri: Optional[str] = None,
        error: Optional[str] = None,
    ) -> JobInfo:
        """Update job status.

        Used by infrastructure callbacks or polling to update job state.

        Args:
            job_id: The job ID
            status: New status (pending, running, completed, failed)
            log_uri: Optional URI to job logs
            artifact_uri: Optional URI to job artifacts
            error: Optional error message (for failed jobs)

        Returns:
            Updated JobInfo
        """
        # Verify job exists
        job = self.db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        # Set completed_at for terminal states
        completed_at = None
        if status in (JobStatus.COMPLETED, JobStatus.FAILED):
            completed_at = datetime.now(timezone.utc).isoformat()

        self.db.update_job_status(
            job_id=job_id,
            status=status,
            completed_at=completed_at,
            log_uri=log_uri,
            artifact_uri=artifact_uri,
            error=error,
        )

        return self.get_job(job_id)

    def get_job_logs(self, job_id: str) -> Optional[str]:
        """Get job logs if available.

        Args:
            job_id: The job ID

        Returns:
            Log content if available, None otherwise

        Raises:
            JobNotFoundError: If job doesn't exist
            InvalidLogPathError: If log_uri contains path traversal or is outside project
        """
        job = self.db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        log_uri = job.get("log_uri")
        if not log_uri:
            return None

        # For local files - validate path before reading
        if log_uri.startswith("/") or log_uri.startswith("file://"):
            # Parse URI to get original path (before resolution)
            if log_uri.startswith("file://"):
                path_str = log_uri[7:]
            else:
                path_str = log_uri

            # Get the unresolved path for symlink checking
            unresolved_path = Path(path_str)

            # Security: validate path is within project_root
            # Note: validate_log_path() returns resolved path, but we still call it
            # for validation
            validate_log_path(log_uri, self.project_root)

            # Security: Check if path is a symlink BEFORE reading
            # This prevents TOCTOU attacks where file is replaced with symlink
            if unresolved_path.is_symlink():
                raise GoldfishError(
                    f"Security error: log path is a symlink, refusing to read: {log_uri}"
                )

            if unresolved_path.exists():
                # Security: Check file size before reading to prevent memory exhaustion
                try:
                    file_size = unresolved_path.stat().st_size
                    max_size = 100 * 1024 * 1024  # 100MB
                    if file_size > max_size:
                        raise GoldfishError(
                            f"Log file too large ({file_size / (1024*1024):.1f}MB). "
                            f"Maximum size is {max_size / (1024*1024):.0f}MB. "
                            f"Download the log file directly from the experiment directory."
                        )
                except (OSError, IOError) as e:
                    raise GoldfishError(f"Failed to check log file size: {e}") from e

                # Security: Use O_NOFOLLOW as defense-in-depth
                # Even though we checked is_symlink() above, use O_NOFOLLOW
                # in case of race condition
                try:
                    with open(unresolved_path, 'r', opener=_safe_opener) as f:
                        return f.read()
                except (OSError, IOError) as e:
                    raise GoldfishError(f"Failed to read log file: {e}") from e

        # For GCS URIs, would need gsutil or cloud client
        # For now, return None for remote logs
        return None

    def poll_status(self, job_id: str) -> JobInfo:
        """Poll infrastructure for current job status.

        Checks for completion/failure markers in the experiment directory
        and updates the database accordingly.

        Args:
            job_id: The job ID

        Returns:
            Updated JobInfo
        """
        # Get current job record
        job = self.db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        # Only poll if job is active
        if job["status"] not in (JobStatus.PENDING, JobStatus.RUNNING):
            return self._job_dict_to_info(job)

        # Check for completion/failure markers in experiment directory
        exp_dir_str = job.get("experiment_dir")
        if exp_dir_str:
            exp_dir = Path(exp_dir_str)
            if exp_dir.exists():
                # Check for completion marker
                completed_marker = exp_dir / "COMPLETED"
                if completed_marker.exists():
                    self.db.update_job_status(
                        job_id=job_id,
                        status=JobStatus.COMPLETED,
                        completed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    return self.get_job(job_id)

                # Check for failure marker
                failed_marker = exp_dir / "FAILED"
                if failed_marker.exists():
                    error_msg = failed_marker.read_text().strip() or "Job failed"
                    self.db.update_job_status(
                        job_id=job_id,
                        status=JobStatus.FAILED,
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        error=error_msg,
                    )
                    return self.get_job(job_id)

        # No status change detected
        return self._job_dict_to_info(job)

    def _job_dict_to_info(self, job: dict) -> JobInfo:
        """Convert database job dict to JobInfo model."""
        from goldfish.jobs.conversion import job_dict_to_info
        return job_dict_to_info(job, self.db)

    def cancel_job(self, job_id: str, reason: str) -> CancelJobResponse:
        """Cancel a running or pending job.

        Args:
            job_id: The job ID to cancel
            reason: Why the job is being cancelled

        Returns:
            CancelJobResponse with result

        Raises:
            JobNotFoundError: If job doesn't exist
            GoldfishError: If job is already in terminal state
        """
        job = self.db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        previous_status = job["status"]

        # Check if job is in a cancellable state
        if previous_status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            raise GoldfishError(
                f"Job {job_id} is already {previous_status} and cannot be cancelled"
            )

        # Update job status to cancelled
        self.db.update_job_status(
            job_id=job_id,
            status=JobStatus.CANCELLED,
            completed_at=datetime.now(timezone.utc).isoformat(),
            error=f"Cancelled: {reason}",
        )

        return CancelJobResponse(
            success=True,
            job_id=job_id,
            previous_status=previous_status,
        )
