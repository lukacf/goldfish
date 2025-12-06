"""Job launcher - creates experiments and launches jobs.

This module coordinates:
1. Creating a checkpoint (snapshot) of the workspace
2. Exporting the snapshot to experiment directory format
3. Launching the job via the infrastructure layer
4. Recording the job in the database
"""

import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, SlotEmptyError
from goldfish.jobs.exporter import SnapshotExporter
from goldfish.models import RunJobResponse, SlotState, JobStatus
from goldfish.workspace.manager import WorkspaceManager


class JobLauncher:
    """Launches jobs from workspace snapshots."""

    def __init__(
        self,
        config: GoldfishConfig,
        project_root: Path,
        db: Database,
        workspace_manager: WorkspaceManager,
        state_manager=None,
    ):
        """Initialize job launcher.

        Args:
            config: Goldfish configuration
            project_root: Project root path
            db: Database instance
            workspace_manager: Workspace manager instance
            state_manager: Optional state manager for STATE.md updates
        """
        self.config = config
        self.project_root = project_root
        self.db = db
        self.workspace_manager = workspace_manager
        self.state_manager = state_manager

        # Initialize exporter
        experiments_dir = project_root / config.jobs.experiments_dir
        self.exporter = SnapshotExporter(experiments_dir)

    def _validate_and_get_workspace(self, slot: str) -> tuple[str, Path]:
        """Validate slot has a mounted workspace and return workspace info.

        Args:
            slot: Slot to validate

        Returns:
            Tuple of (workspace_name, slot_path)

        Raises:
            SlotEmptyError: If slot is empty
        """
        slot_info = self.workspace_manager.get_slot_info(slot)
        if slot_info.state == SlotState.EMPTY:
            raise SlotEmptyError(f"Slot {slot} is empty - mount a workspace first")

        workspace_name = slot_info.workspace
        slot_path = self.workspace_manager.get_slot_path(slot)
        return workspace_name, slot_path

    def _create_checkpoint(self, slot: str, reason: str) -> str:
        """Create a checkpoint of the workspace.

        Args:
            slot: Slot to checkpoint
            reason: Reason for the checkpoint

        Returns:
            Snapshot ID of the created checkpoint
        """
        checkpoint_response = self.workspace_manager.checkpoint(
            slot, f"Pre-job checkpoint: {reason}"
        )
        return checkpoint_response.snapshot_id

    def _export_snapshot(
        self,
        slot_path: Path,
        workspace_name: str,
        snapshot_id: str,
        script: str,
        reason: str,
        config_overrides: Optional[dict],
    ) -> Path:
        """Export snapshot to experiment directory.

        Args:
            slot_path: Path to the workspace slot
            workspace_name: Name of the workspace
            snapshot_id: Snapshot to export
            script: Script to run
            reason: Reason for the job
            config_overrides: Optional config overrides

        Returns:
            Path to the experiment directory

        Raises:
            GoldfishError: If export fails
        """
        try:
            return self.exporter.export(
                workspace_path=slot_path,
                workspace_name=workspace_name,
                snapshot_id=snapshot_id,
                script=script,
                reason=reason,
                config_overrides=config_overrides,
            )
        except GoldfishError:
            raise
        except (OSError, IOError) as e:
            raise GoldfishError(f"Failed to export snapshot: {e}") from e
        except Exception as e:
            raise GoldfishError(f"Unexpected error during export: {e}") from e

    def _create_job_record(
        self,
        job_id: str,
        workspace_name: str,
        snapshot_id: str,
        script: str,
        exp_dir: Path,
        reason: str,
        config_overrides: Optional[dict],
        source_inputs: Optional[dict[str, str]],
    ) -> None:
        """Create job record in database with inputs atomically.

        Args:
            job_id: Unique job identifier
            workspace_name: Workspace name
            snapshot_id: Snapshot ID
            script: Script to execute
            exp_dir: Experiment directory path
            reason: Reason for the job
            config_overrides: Optional config overrides
            source_inputs: Optional input sources

        Raises:
            GoldfishError: If database operation fails (cleans up exp_dir)
        """
        try:
            self.db.create_job_with_inputs(
                job_id=job_id,
                workspace=workspace_name,
                snapshot_id=snapshot_id,
                script=script,
                experiment_dir=str(exp_dir),
                inputs=source_inputs,
                metadata={
                    "reason": reason,
                    "config_overrides": config_overrides,
                },
            )
        except GoldfishError:
            raise
        except Exception as e:
            # Database error - clean up experiment directory
            self._cleanup_exp_dir(exp_dir)
            raise GoldfishError(f"Database error creating job record: {e}") from e

    def _launch_and_update_status(self, job_id: str, exp_dir: Path, script: str) -> None:
        """Launch job and update status, with cleanup on failure.

        Args:
            job_id: Job identifier
            exp_dir: Experiment directory
            script: Script to execute

        Raises:
            GoldfishError: If launch fails (status updated, exp_dir cleaned)
        """
        try:
            self._launch_job(job_id, exp_dir, script)
            self.db.update_job_status(job_id, JobStatus.RUNNING)
        except GoldfishError as e:
            self._cleanup_exp_dir(exp_dir)
            self.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
            )
            raise
        except subprocess.SubprocessError as e:
            self._cleanup_exp_dir(exp_dir)
            self.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
            )
            raise GoldfishError(f"Job launch subprocess failed: {e}") from e
        except (OSError, IOError) as e:
            self._cleanup_exp_dir(exp_dir)
            self.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
            )
            raise GoldfishError(f"Job launch failed (file system error): {e}") from e
        except Exception as e:
            self._cleanup_exp_dir(exp_dir)
            self.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
            )
            raise GoldfishError(f"Unexpected error launching job: {e}") from e

    def _log_job_launch(
        self,
        slot: str,
        workspace_name: str,
        reason: str,
        job_id: str,
        snapshot_id: str,
        script: str,
        exp_dir: Path,
    ) -> None:
        """Log job launch to audit trail and STATE.md.

        Args:
            slot: Slot used
            workspace_name: Workspace name
            reason: Reason for the job
            job_id: Job identifier
            snapshot_id: Snapshot ID
            script: Script executed
            exp_dir: Experiment directory
        """
        self.db.log_audit(
            operation="run_job",
            slot=slot,
            workspace=workspace_name,
            reason=reason,
            details={
                "job_id": job_id,
                "snapshot_id": snapshot_id,
                "script": script,
                "experiment_dir": str(exp_dir),
            },
        )

        if self.state_manager:
            self.state_manager.add_action(f"Launched job {job_id}: {script}")

    def run_job(
        self,
        slot: str,
        script: str,
        reason: str,
        config_overrides: Optional[dict] = None,
        source_inputs: Optional[dict[str, str]] = None,
    ) -> RunJobResponse:
        """Launch a job on the current workspace snapshot.

        Flow:
        1. Create checkpoint (snapshot) of the workspace
        2. Export snapshot to experiment directory
        3. Create job record with inputs (atomic transaction)
        4. Launch the job (async)
        5. Return job info

        Args:
            slot: Slot containing the code to run (w1, w2, w3)
            script: Script to execute (e.g., "scripts/train.py")
            reason: What this job is testing (min 15 chars)
            config_overrides: Optional config overrides for this run
            source_inputs: Optional map of input_name -> source_id

        Returns:
            RunJobResponse with job_id and metadata
        """
        # 1. Validate and get workspace info
        workspace_name, slot_path = self._validate_and_get_workspace(slot)

        # 2. Create checkpoint
        snapshot_id = self._create_checkpoint(slot, reason)

        # 3. Export to experiment directory
        exp_dir = self._export_snapshot(
            slot_path, workspace_name, snapshot_id, script, reason, config_overrides
        )

        # 4. Generate job ID and record in database
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        self._create_job_record(
            job_id, workspace_name, snapshot_id, script, exp_dir,
            reason, config_overrides, source_inputs
        )

        # 5. Launch the job
        self._launch_and_update_status(job_id, exp_dir, script)

        # 6. Log audit
        self._log_job_launch(
            slot, workspace_name, reason, job_id, snapshot_id, script, exp_dir
        )

        # 7. Return response
        artifact_uri = self._get_artifact_uri(job_id)
        return RunJobResponse(
            success=True,
            job_id=job_id,
            snapshot_id=snapshot_id,
            experiment_dir=str(exp_dir),
            artifact_uri=artifact_uri,
        )

    def _cleanup_exp_dir(self, exp_dir: Optional[Path]) -> None:
        """Best-effort cleanup of experiment directory.

        Args:
            exp_dir: Path to experiment directory to clean up
        """
        if exp_dir and exp_dir.exists():
            import shutil
            try:
                shutil.rmtree(exp_dir)
            except OSError:
                # Best effort - ignore cleanup failures
                pass

    def _launch_job(self, job_id: str, exp_dir: Path, script: str) -> None:
        """Launch the actual job.

        This is where we'd integrate with the infrastructure layer.
        For now, we just mark it as pending.

        In production, this would:
        1. Call infra/create_run.py to create a run
        2. The run handles Docker build, GCE launch, etc.
        """
        # Check if infra path exists and has create_run.py
        infra_path = self.config.jobs.infra_path
        if infra_path:
            infra_dir = self.project_root / infra_path
            create_run_script = infra_dir / "create_run.py"

            if create_run_script.exists():
                # Launch via infra
                self._launch_via_infra(job_id, exp_dir, script, create_run_script)
                return

        # No infra configured or found - just log
        # In development mode, we might want to run locally
        # For now, we'll update status to indicate it's "launched"
        # but not actually executing

    def _launch_via_infra(
        self, job_id: str, exp_dir: Path, script: str, create_run_script: Path
    ) -> None:
        """Launch job via infrastructure layer.

        Calls the create_run.py script to handle:
        - Docker image build
        - GCE instance launch
        - Job monitoring setup

        Args:
            job_id: The job ID
            exp_dir: Path to exported experiment directory
            script: Script to run
            create_run_script: Path to the create_run.py script

        Raises:
            GoldfishError: If launch fails or times out
        """
        import sys

        cmd = [
            sys.executable,
            str(create_run_script),
            "--experiment", str(exp_dir),
            "--script", script,
            "--job-id", job_id,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout for job launch
                cwd=self.project_root,
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                raise GoldfishError(
                    f"Job launch failed (exit code {result.returncode}): {error_msg}"
                )

        except subprocess.TimeoutExpired:
            raise GoldfishError(
                f"Job launch timed out after 300 seconds. "
                f"The infrastructure may be slow or unresponsive."
            )

    def _get_artifact_uri(self, job_id: str) -> Optional[str]:
        """Get the artifact URI for a job.

        Constructs the GCS location where job outputs will be stored.
        Format: gs://{bucket}/{artifacts_prefix}/{job_id}/

        Args:
            job_id: The job ID

        Returns:
            GCS URI string, or None if GCS not configured
        """
        if self.config.gcs is None:
            return None

        bucket = self.config.gcs.bucket
        prefix = self.config.gcs.artifacts_prefix.rstrip("/")

        return f"gs://{bucket}/{prefix}/{job_id}/"
