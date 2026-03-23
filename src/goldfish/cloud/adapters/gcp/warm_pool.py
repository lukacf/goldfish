"""Warm Pool Manager for GCE instances (v2: state-machine-driven).

Manages warm GCE instances for reuse between runs. All lifecycle
decisions go through InstanceController — this module handles only
GCE adapter operations (gcloud calls, metadata signaling, ACK polling).

Key design principles:
- Instance state machine is sole authority for instance lifecycle
- InstanceController is the single entry point for state transitions
- This module handles GCE operations only (claim signaling, deletion, liveness)
- No direct warm_instances.state updates
"""

from __future__ import annotations

import json
import logging
import subprocess
import time

from goldfish.cloud.contracts import RunHandle
from goldfish.config import WarmPoolConfig
from goldfish.db.database import Database
from goldfish.db.types import WarmInstanceRow
from goldfish.state_machine.instance_controller import InstanceController

logger = logging.getLogger("goldfish.warm_pool")


class WarmPoolManager:
    """Manages warm GCE instances for reuse between runs."""

    def __init__(
        self,
        db: Database,
        config: WarmPoolConfig,
        bucket: str | None = None,
        project_id: str | None = None,
    ):
        self._db = db
        self._config = config
        self._bucket = bucket
        self._project_id = project_id
        self._controller = InstanceController(db)

    @property
    def controller(self) -> InstanceController:
        """Access the instance controller for external callers."""
        return self._controller

    def is_enabled_for(self, profile_name: str) -> bool:
        """Check if warm pool is enabled for this profile.

        Empty profiles list means all profiles are enabled.
        """
        if not self._config.enabled:
            return False
        if not self._config.profiles:
            return True
        return profile_name in self._config.profiles

    def pre_register(
        self,
        instance_name: str,
        zone: str,
        machine_type: str,
        gpu_count: int,
        image_family: str,
        image_project: str,
        image_tag: str | None = None,
        preemptible: bool = False,
    ) -> bool:
        """Atomically pre-register instance if pool has capacity.

        Returns True if registered (capacity available), False if pool full.
        """
        return self._db.pre_register_warm_instance(
            instance_name=instance_name,
            zone=zone,
            project_id=self._project_id or "",
            machine_type=machine_type,
            gpu_count=gpu_count,
            image_family=image_family,
            image_project=image_project,
            max_instances=self._config.max_instances,
            image_tag=image_tag,
            preemptible=preemptible,
        )

    def try_claim(
        self,
        machine_type: str,
        gpu_count: int,
        image_family: str,
        image_project: str,
        preemptible: bool = False,
        stage_run_id: str = "",
        job_spec: dict | None = None,
    ) -> RunHandle | None:
        """Try to claim an idle_ready warm instance matching the hardware spec.

        Returns RunHandle if claimed successfully, None to fall through to fresh launch.
        """
        if not self._config.enabled:
            return None

        # 1. Find + claim with retry. find_claimable_instance is a SELECT (no lock),
        # so a concurrent claimer can win the CAS in on_claim_start. Retry with
        # a different idle instance if available.
        instance_name: str | None = None
        zone: str | None = None
        for _attempt in range(3):
            found = self._db.find_claimable_instance(
                machine_type,
                gpu_count,
                image_family,
                image_project,
                preemptible=preemptible,
            )
            if not found:
                return None

            result = self._controller.on_claim_start(found["instance_name"], stage_run_id)
            if result.success:
                instance_name = found["instance_name"]
                zone = found["zone"]
                break
            # CAS race — another claimer won. Try again.
            logger.debug("Claim race for %s, retrying", found["instance_name"])

        if instance_name is None or zone is None:
            return None

        try:
            # 3. Upload job spec to GCS
            spec_gcs_path = f"gs://{self._bucket}/warm_pool/{instance_name}/jobs/{stage_run_id}/spec.json"
            self._upload_job_spec(spec_gcs_path, job_spec or {})

            # 4. Signal via metadata (flat structure — VM parser uses grep, not jq)
            signal = json.dumps(
                {
                    "command": "new_job",
                    "request_id": stage_run_id,
                    "spec_gcs_path": spec_gcs_path,
                }
            )
            self._set_instance_metadata(instance_name, zone, "goldfish", signal)

            # 5. Poll for ACK (30s timeout)
            ack = self._wait_for_ack(instance_name, zone, stage_run_id, timeout_seconds=30)

            if ack:
                # 6. ACK received → claimed → busy
                ack_result = self._controller.on_claim_acked(instance_name, stage_run_id)
                if not ack_result.success:
                    logger.warning(
                        "on_claim_acked failed for %s: %s — deleting instance",
                        instance_name,
                        ack_result.details,
                    )
                    self._controller.on_claim_timeout(instance_name, stage_run_id)
                    return None
                return RunHandle(
                    stage_run_id=stage_run_id,
                    backend_type="gce",
                    backend_handle=instance_name,
                    zone=zone,
                )
            else:
                # 7. ACK timeout → claimed → deleting
                logger.warning(
                    "ACK timeout for warm instance %s — deleting to prevent double-dispatch",
                    instance_name,
                )
                self._controller.on_claim_timeout(instance_name, stage_run_id)
                return None

        except Exception as e:
            logger.warning("Warm pool claim failed for %s: %s — deleting instance", instance_name, e)
            self._controller.on_claim_timeout(instance_name, stage_run_id)
            return None

    def get_instance(self, instance_name: str) -> WarmInstanceRow | None:
        """Get a warm instance by name."""
        return self._db.get_warm_instance(instance_name)

    def delete_instance(self, instance_name: str) -> None:
        """Delete VM via gcloud, routing through the instance controller.

        Transitions through the state machine (DELETE_REQUESTED → deleting,
        then DELETE_CONFIRMED → gone) to maintain audit trail. Falls back
        to direct deletion if the instance is already in a terminal state.
        """
        instance = self._db.get_warm_instance(instance_name)
        if not instance:
            return

        state = instance["state"]

        # If not already deleting/gone, request deletion through controller
        if state not in ("deleting", "gone"):
            self._controller.on_delete_requested(instance_name, reason="delete_instance called")

        gce_ok = self._delete_gce_instance(instance_name, instance["zone"])
        if gce_ok:
            self._controller.on_delete_confirmed(instance_name)
            self._db.delete_warm_instance(instance_name)
        else:
            logger.warning(
                "GCE delete failed for %s — keeping DB row for recovery retry",
                instance_name,
            )

    def pool_status(self) -> dict:
        """Return pool status dict for MCP tool."""
        instances = self._db.list_warm_instances()
        by_state: dict[str, int] = {}
        instance_details = []
        for inst in instances:
            state = inst["state"]
            by_state[state] = by_state.get(state, 0) + 1
            instance_details.append(
                {
                    "instance_name": inst["instance_name"],
                    "zone": inst["zone"],
                    "machine_type": inst["machine_type"],
                    "gpu_count": inst["gpu_count"],
                    "state": state,
                    "state_entered_at": inst.get("state_entered_at"),
                    "active_lease_run": inst.get("current_lease_run_id"),
                }
            )
        return {
            "enabled": self._config.enabled,
            "max_instances": self._config.max_instances,
            "idle_timeout_minutes": self._config.idle_timeout_minutes,
            "total": len(instances),
            "by_state": by_state,
            "instances": instance_details,
        }

    # =========================================================================
    # GCE Operations (adapter layer)
    # =========================================================================

    def check_instance_alive(self, instance_name: str, zone: str) -> bool:
        """Check if GCE instance is alive via gcloud.

        Returns True if alive, False if dead/not-found/error.
        For callers that need to distinguish "not found" from "transient error",
        use check_instance_status() instead.
        """
        status = self.check_instance_status(instance_name, zone)
        return status == "alive"

    def check_instance_status(self, instance_name: str, zone: str) -> str:
        """Check GCE instance status with tri-state return.

        Returns:
            "alive" — VM exists and is RUNNING/STAGING/PROVISIONING
            "not_found" — VM confirmed not to exist (404 / "not found" in stderr)
            "error" — transient/unknown failure (auth, network, timeout, etc.)
        """
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "describe",
                    instance_name,
                    "--zone",
                    zone,
                    *self._gcloud_project_args(),
                    "--format",
                    "value(status)",
                ],
                capture_output=True,
                timeout=30,
                text=True,
            )
            if result.returncode == 0:
                status = result.stdout.strip().upper()
                if status in ("RUNNING", "STAGING", "PROVISIONING"):
                    return "alive"
                # TERMINATED, STOPPED, SUSPENDED — VM exists but not usable.
                # Return "dead" (distinct from "not_found" which means the VM
                # doesn't exist at all). Both mean "not alive" but callers may
                # care about the difference for deletion confirmation.
                return "dead"
            # Non-zero exit: check if it's "not found" vs transient error
            stderr = result.stderr.lower()
            if "not found" in stderr or "was not found" in stderr:
                return "not_found"
            logger.warning("GCE describe failed for %s (rc=%d): %s", instance_name, result.returncode, stderr[:200])
            return "error"
        except Exception as e:
            logger.warning("GCE liveness check failed for %s: %s", instance_name, e)
            return "error"

    def get_instance_metadata(self, instance_name: str, zone: str) -> dict:
        """Get instance metadata as a dict of key→value."""
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "describe",
                    instance_name,
                    "--zone",
                    zone,
                    *self._gcloud_project_args(),
                    "--format",
                    "json(metadata)",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                items = data.get("metadata", {}).get("items", [])
                return {item["key"]: item["value"] for item in items if "key" in item}
        except Exception as e:
            logger.debug("Metadata fetch error for %s: %s", instance_name, e)
        return {}

    def delete_gce_instance(self, instance_name: str, zone: str) -> bool:
        """Delete via gcloud. Returns True if succeeded or already gone."""
        return self._delete_gce_instance(instance_name, zone)

    def _gcloud_project_args(self) -> list[str]:
        if self._project_id:
            return ["--project", self._project_id]
        return []

    def _upload_job_spec(self, gcs_path: str, spec_dict: dict) -> None:
        spec_json = json.dumps(spec_dict)
        subprocess.run(
            ["gsutil", "cp", "-", gcs_path],
            input=spec_json.encode(),
            capture_output=True,
            timeout=30,
            check=True,
        )

    def _set_instance_metadata(self, instance_name: str, zone: str, key: str, value: str) -> None:
        """Set instance metadata via gcloud.

        Uses --metadata-from-file for JSON-safe signaling. GCE parses
        --metadata as comma-delimited key=value pairs, so commas/quotes
        in JSON payloads get mangled. --metadata-from-file reads the raw
        file content as-is, avoiding this problem.
        """
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(value)
            tmp_path = f.name

        try:
            subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "add-metadata",
                    instance_name,
                    "--zone",
                    zone,
                    *self._gcloud_project_args(),
                    "--metadata-from-file",
                    f"{key}={tmp_path}",
                ],
                capture_output=True,
                timeout=30,
                check=True,
            )
        finally:
            import os

            os.unlink(tmp_path)

    def _wait_for_ack(
        self,
        instance_name: str,
        zone: str,
        stage_run_id: str,
        timeout_seconds: int = 30,
    ) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                metadata = self.get_instance_metadata(instance_name, zone)
                if metadata.get("goldfish_ack") == stage_run_id:
                    return True
            except Exception as e:
                logger.debug("ACK poll error for %s: %s", instance_name, e)
            time.sleep(2)
        return False

    def _delete_gce_instance(self, instance_name: str, zone: str) -> bool:
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "delete",
                    instance_name,
                    "--zone",
                    zone,
                    *self._gcloud_project_args(),
                    "--quiet",
                ],
                capture_output=True,
                timeout=15,  # Short timeout — daemon retries on next poll if this times out
                text=True,
            )
            if result.returncode == 0:
                return True
            if "not found" in result.stderr.lower():
                return True
            logger.warning(
                "gcloud delete failed for %s (rc=%d): %s", instance_name, result.returncode, result.stderr[:200]
            )
            return False
        except Exception as e:
            logger.warning("gcloud delete failed for %s: %s", instance_name, e)
            return False
