"""Warm Pool Manager for GCE instances.

Manages warm instances: claiming idle ones for reuse, releasing after runs,
uploading job specs, and reaping expired instances.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.cloud.contracts import RunHandle

if TYPE_CHECKING:
    from goldfish.config import WarmPoolConfig
    from goldfish.db.database import Database
    from goldfish.infra.metadata.base import MetadataBus

logger = logging.getLogger(__name__)


class WarmPoolManager:
    """Manages warm GCE instances for reuse between runs."""

    def __init__(
        self,
        db: Database,
        config: WarmPoolConfig,
        signal_bus: MetadataBus | None = None,
        bucket: str | None = None,
        project_id: str | None = None,
    ) -> None:
        self._db = db
        self._config = config
        self._signal_bus = signal_bus
        self._bucket = bucket
        self._project_id = project_id

    def is_enabled_for(self, profile_name: str) -> bool:
        """Check if warm pool is enabled for a given profile."""
        if not self._config.enabled:
            return False
        if not self._config.profiles:
            return True  # Empty = all profiles
        return profile_name in self._config.profiles

    def try_claim(
        self,
        machine_type: str,
        gpu_count: int,
        stage_run_id: str,
        image: str,
        env_map: dict[str, str],
        pre_run_script: str,
        post_run_script: str,
        docker_cmd_script: str,
        run_path: str,
    ) -> RunHandle | None:
        """Try to claim a warm instance and dispatch a new job to it.

        Returns RunHandle if successful, None if no warm instance available
        (caller should fall through to fresh launch).
        """
        if not self._config.enabled:
            return None

        # Check pool isn't over capacity
        if self._db.count_warm_instances(statuses=("idle", "claimed", "running")) >= self._config.max_instances:
            # Pool is full but we might have an idle one to claim
            pass

        # Atomically claim an idle instance
        claimed = self._db.claim_warm_instance(machine_type=machine_type, gpu_count=gpu_count)
        if not claimed:
            return None

        instance_name = claimed["instance_name"]
        zone = claimed["zone"]
        logger.info(
            "Claimed warm instance %s (zone=%s) for %s",
            instance_name,
            zone,
            stage_run_id,
        )

        try:
            # Upload job spec to GCS
            spec_gcs_path = self._upload_job_spec(
                instance_name=instance_name,
                stage_run_id=stage_run_id,
                image=image,
                env_map=env_map,
                pre_run_script=pre_run_script,
                post_run_script=post_run_script,
                docker_cmd_script=docker_cmd_script,
                run_path=run_path,
            )

            # Send new_job signal via metadata bus
            if self._signal_bus:
                from goldfish.infra.metadata.base import MetadataSignal

                signal = MetadataSignal(
                    command="new_job",
                    request_id=stage_run_id,
                    payload={"spec_gcs_path": spec_gcs_path},
                )
                self._signal_bus.set_signal("goldfish", signal, target=instance_name)

                # Wait for ACK (30s timeout)
                for _ in range(30):
                    ack = self._signal_bus.get_ack("goldfish", target=instance_name)
                    if ack == stage_run_id:
                        logger.info("Warm instance %s ACKed job %s", instance_name, stage_run_id)
                        # Update DB: claimed -> running
                        with self._db._conn() as conn:
                            conn.execute(
                                "UPDATE warm_instances SET status = 'running', current_stage_run_id = ?, last_job_at = ? WHERE instance_name = ?",
                                (stage_run_id, datetime.now(UTC).isoformat(), instance_name),
                            )
                        return RunHandle(
                            stage_run_id=stage_run_id,
                            backend_type="gce",
                            backend_handle=instance_name,
                            zone=zone,
                            warm_instance=True,
                        )
                    time.sleep(1)

                # ACK timeout — release the instance and fall through
                logger.warning("Warm instance %s failed to ACK within 30s, releasing", instance_name)
                self._db.release_warm_instance(instance_name)
                return None
            else:
                # No signal bus — can't communicate with instance
                logger.warning("No signal bus available for warm pool dispatch")
                self._db.release_warm_instance(instance_name)
                return None

        except Exception as e:
            logger.exception("Failed to dispatch to warm instance %s: %s", instance_name, e)
            self._db.release_warm_instance(instance_name)
            return None

    def register_instance(
        self,
        instance_name: str,
        zone: str,
        machine_type: str,
        gpu_count: int,
        image_tag: str | None = None,
    ) -> bool:
        """Register a newly launched instance as warm-pool-eligible.

        Returns True if registered, False if pool is full.
        """
        current = self._db.count_warm_instances()
        if current >= self._config.max_instances:
            return False

        self._db.register_warm_instance(
            instance_name=instance_name,
            zone=zone,
            project_id=self._project_id or "",
            machine_type=machine_type,
            gpu_count=gpu_count,
            image_tag=image_tag,
        )
        return True

    def release_instance(self, instance_name: str) -> None:
        """Release an instance back to idle after a run completes."""
        self._db.release_warm_instance(instance_name)

    def reap_idle(self) -> int:
        """Delete instances past idle_timeout. Called by daemon.

        Returns number of instances reaped.
        """
        expired = self._db.list_expired_warm_instances(self._config.idle_timeout_minutes)
        reaped = 0
        for inst in expired:
            name = inst["instance_name"]
            zone = inst.get("zone", "")
            logger.info("Reaping expired warm instance %s (zone=%s)", name, zone)
            try:
                self._delete_gce_instance(name, zone)
            except Exception as e:
                logger.warning("Failed to delete warm instance %s: %s", name, e)
            self._db.delete_warm_instance(name)
            reaped += 1
        return reaped

    def reap_all(self) -> int:
        """Emergency: delete all warm instances."""
        instances = self._db.list_warm_instances()
        reaped = 0
        for inst in instances:
            name = inst["instance_name"]
            zone = inst.get("zone", "")
            logger.info("Emergency reap: deleting warm instance %s", name)
            try:
                self._delete_gce_instance(name, zone)
            except Exception:
                pass
            self._db.delete_warm_instance(name)
            reaped += 1
        return reaped

    def _upload_job_spec(
        self,
        instance_name: str,
        stage_run_id: str,
        image: str,
        env_map: dict[str, str],
        pre_run_script: str,
        post_run_script: str,
        docker_cmd_script: str,
        run_path: str,
    ) -> str:
        """Upload job spec files to GCS for the warm instance to download."""
        if not self._bucket:
            raise ValueError("Bucket required for warm pool job spec upload")

        gcs_base = f"gs://{self._bucket}/warm_pool/{instance_name}/jobs/{stage_run_id}"

        # Write files to temp dir, then upload
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            # Image tag
            (tmp / "image_tag").write_text(image)

            # Run path (for log sync paths)
            (tmp / "run_path").write_text(run_path)

            # Environment script
            env_lines = [f"export {k}={_shell_quote(v)}" for k, v in env_map.items()]
            (tmp / "env.sh").write_text("\n".join(env_lines) + "\n")

            # Pre-run script (input staging)
            (tmp / "pre_run.sh").write_text(pre_run_script)

            # Post-run script (output upload)
            (tmp / "post_run.sh").write_text(post_run_script)

            # Docker command script
            (tmp / "docker_cmd.sh").write_text(docker_cmd_script)

            # Upload all files
            cmd = ["gsutil", "-m", "cp", "-r", f"{tmpdir}/*", f"{gcs_base}/"]
            if self._project_id:
                cmd.insert(1, "-o")
                cmd.insert(2, f"GSUtil:project_id={self._project_id}")
            subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)

        return gcs_base

    def _delete_gce_instance(self, instance_name: str, zone: str) -> None:
        """Delete a GCE instance."""
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "delete",
            instance_name,
            f"--zone={zone}",
            "--quiet",
        ]
        if self._project_id:
            cmd.append(f"--project={self._project_id}")
        subprocess.run(cmd, capture_output=True, text=True, timeout=60)


def _shell_quote(s: str) -> str:
    """Quote a string for safe shell usage."""
    import shlex

    return shlex.quote(str(s))
