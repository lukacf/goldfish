"""Pipeline execution engine for Goldfish."""

import atexit
import copy
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import RunReason, StageRunInfo
from goldfish.pipeline.manager import PipelineManager

# Lease timeout for claimed stages (seconds) - if a stage is claimed but not launched
# within this time, it can be reclaimed by another worker
_CLAIM_LEASE_TIMEOUT_SECONDS = 300


class PipelineExecutor:
    """Execute full or partial pipelines."""

    _pool_size = int(os.getenv("GOLDFISH_PIPELINE_WORKERS", "8"))
    MAX_WORKER_ERRORS = int(os.getenv("GOLDFISH_PIPELINE_MAX_ERRORS", "10"))
    _pool = ThreadPoolExecutor(max_workers=_pool_size, thread_name_prefix="pipeline-worker")
    atexit.register(_pool.shutdown, wait=True, cancel_futures=True)
    _logger = logging.getLogger(__name__)

    def __init__(
        self,
        stage_executor: StageExecutor,
        pipeline_manager: PipelineManager,
        db: Database,
    ):
        self.stage_executor = stage_executor
        self.pipeline_manager = pipeline_manager
        self.db = db
        self._shutdown = False  # Flag to stop workers when executor is destroyed
        self._recover_inflight_pipelines()
        self._race_loss_counter = 0
        self._race_loss_lock = threading.Lock()

    def shutdown(self) -> None:
        """Signal workers to stop. Call this before discarding the executor."""
        self._shutdown = True

    def _recover_inflight_pipelines(self) -> None:
        """On startup, reschedule any pipelines that were mid-flight."""
        with self.db._conn() as conn:
            rows = conn.execute(
                "SELECT id, workspace_name, pipeline_name, config_override, inputs_override FROM pipeline_runs WHERE status IN ('pending','running')"
            ).fetchall()
            for row in rows:
                prun = row["id"]
                # If there are still pending/running stages, restart a worker
                counts = conn.execute(
                    "SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running FROM pipeline_stage_queue WHERE pipeline_run_id=?",
                    (prun,),
                ).fetchone()
                if (counts["pending"] or 0) > 0 or (counts["running"] or 0) > 0:
                    workspace = row["workspace_name"]
                    pipeline_name = row["pipeline_name"]
                    # Load persisted overrides
                    config_override = json.loads(row["config_override"]) if row["config_override"] else None
                    inputs_override = json.loads(row["inputs_override"]) if row["inputs_override"] else None
                    # submit worker to continue processing
                    self._pool.submit(
                        self._worker_loop,
                        prun,
                        workspace,
                        pipeline_name,
                        config_override,
                        inputs_override,
                        None,  # reason
                    )

    def run_stages(
        self,
        workspace: str,
        stages: list[str] | None = None,
        pipeline_name: str | None = None,
        config_override: dict | None = None,
        inputs_override: dict | None = None,
        reason: str | None = None,
        reason_structured: RunReason | None = None,
        async_mode: bool = True,
    ) -> dict:
        """
        Run pipeline stages - unified entry point for all stage execution.

        Args:
            workspace: Workspace name
            stages: Which stages to run (None = all stages in pipeline order)
            pipeline_name: Pipeline file (None = pipeline.yaml)
            config_override: Per-stage config overrides {"stage_name": {"VAR": "val"}}
            inputs_override: Per-stage input overrides {"stage_name": {"input": "path"}}
            reason: Why running (string summary)
            reason_structured: Structured RunReason object with hypothesis/approach/etc
            async_mode: True = queue-based async, False = sequential blocking

        Returns:
            Dict with pipeline_run_id and stage_runs list
        """
        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        all_stage_names = [s.name for s in pipeline.stages]

        # Determine which stages to run
        if stages:
            # Reject duplicate stage names in input
            if len(stages) != len(set(stages)):
                seen = set()
                duplicates = [s for s in stages if s in seen or seen.add(s)]  # type: ignore[func-returns-value]
                raise ValueError(f"Duplicate stage names in request: {duplicates}")

            # Validate requested stages exist
            for s in stages:
                if s not in all_stage_names:
                    raise ValueError(f"Stage '{s}' not found in pipeline")
            stages_to_run = stages
        else:
            # Run all stages
            stages_to_run = all_stage_names

        # Build stage objects for the stages we're running (renamed from run_stages to avoid shadowing)
        stage_map = {s.name: s for s in pipeline.stages}
        stages_to_execute = [stage_map[name] for name in stages_to_run]

        # Normalize override dicts: if running a single stage and the dict is flat
        # (e.g., {"LR": 0.001} instead of {"train": {"LR": 0.001}}), wrap it
        def normalize_override(override: dict | None, stage_names: list[str]) -> dict | None:
            if not override:
                return None
            # Check if any top-level key is a stage name - if so, it's already nested
            if any(k in stage_names for k in override):
                return override
            # If running exactly one stage and dict is flat, wrap it
            if len(stage_names) == 1:
                return {stage_names[0]: override}
            # Multiple stages with flat dict - ambiguous, return as-is (will be ignored)
            return override

        normalized_config = normalize_override(config_override, stages_to_run)
        normalized_inputs = normalize_override(inputs_override, stages_to_run)

        # Deep-copy override dicts for thread safety (workers may run on different threads)
        safe_config_override = copy.deepcopy(normalized_config) if normalized_config else None
        safe_inputs_override = copy.deepcopy(normalized_inputs) if normalized_inputs else None

        # Validate all inputs can be resolved BEFORE queuing
        self._validate_inputs_resolvable(workspace, stages_to_execute, safe_inputs_override)

        # Convert reason_structured to dict if it's a RunReason object
        reason_dict = None
        if reason_structured:
            if hasattr(reason_structured, "model_dump"):
                reason_dict = reason_structured.model_dump()
            else:
                reason_dict = reason_structured

        if not async_mode:
            # Sequential blocking mode - run each stage and wait
            runs: list[StageRunInfo] = []
            for stage in stages_to_execute:
                cfg = safe_config_override.get(stage.name) if safe_config_override else None
                inp = safe_inputs_override.get(stage.name) if safe_inputs_override else None
                sr = self.stage_executor.run_stage(
                    workspace=workspace,
                    stage_name=stage.name,
                    pipeline_name=pipeline_name,
                    pipeline_run_id=None,
                    config_override=cfg,
                    inputs_override=inp,
                    reason=reason,
                    reason_structured=reason_dict,
                    wait=True,
                )
                runs.append(sr)
            return {"stage_runs": [r.model_dump(mode="json") for r in runs], "pipeline_run_id": None}

        # Async queue-based execution
        pipeline_run_id = f"prun-{uuid4().hex[:8]}"
        now = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at, config_override, inputs_override)
                VALUES (?, ?, ?, 'running', ?, ?, ?)
                """,
                (
                    pipeline_run_id,
                    workspace,
                    pipeline_name,
                    now,
                    json.dumps(safe_config_override) if safe_config_override else None,
                    json.dumps(safe_inputs_override) if safe_inputs_override else None,
                ),
            )

            # Build dependency chain - each stage depends on the previous one in the run list
            prev = None
            for stage in stages_to_execute:
                deps: list[str] = []
                if prev:
                    deps.append(prev)

                # Create queue entry (stage_run created when worker picks it up)
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, deps, status)
                    VALUES (?, ?, ?, 'pending')
                    """,
                    (pipeline_run_id, stage.name, json.dumps(deps)),
                )

                prev = stage.name

        # Submit to thread pool immediately - don't block on docker build
        # The worker loop will handle all stage execution asynchronously
        self._pool.submit(
            self._worker_loop,
            pipeline_run_id,
            workspace,
            pipeline_name,
            safe_config_override,
            safe_inputs_override,
            reason,
        )

        # Build queued stage info for immediate feedback
        queued_stages = [
            {
                "stage": stage.name,
                "status": "queued",
                "pipeline_run_id": pipeline_run_id,
            }
            for stage in stages_to_execute
        ]

        return {
            "pipeline_run_id": pipeline_run_id,
            "stages_queued": [s.name for s in stages_to_execute],
            "status": "Pipeline queued. Use list_runs(pipeline_run_id=...) to check progress.",
            "queued": queued_stages,
        }

    def _validate_inputs_resolvable(
        self,
        workspace: str,
        stages: list,
        inputs_override: dict | None,
    ) -> None:
        """Validate all stage inputs can be resolved before queuing.

        Raises:
            GoldfishError: If any input cannot be resolved and has no override
        """
        from goldfish.errors import GoldfishError

        for stage in stages:
            stage_override = inputs_override.get(stage.name, {}) if inputs_override else {}

            for input_name, input_def in stage.inputs.items():
                # If there's an override for this input, it's valid
                # (source names are resolved at runtime, paths used as-is)
                if input_name in stage_override:
                    continue

                # Check if input can be resolved
                if input_def.from_stage:
                    # Need a completed run of the upstream stage
                    stage_runs = self.db.list_stage_runs(workspace_name=workspace, stage_name=input_def.from_stage)
                    has_completed = any(r["status"] == "completed" for r in stage_runs)
                    if not has_completed:
                        raise GoldfishError(
                            f"Stage '{stage.name}' requires input '{input_name}' from stage "
                            f"'{input_def.from_stage}', but no successful run exists. "
                            f"Either run '{input_def.from_stage}' first, or provide inputs_override."
                        )

                elif input_def.dataset:
                    # Need the dataset to exist
                    dataset = self.db.get_source(input_def.dataset)
                    if not dataset:
                        raise GoldfishError(
                            f"Stage '{stage.name}' requires dataset '{input_def.dataset}' "
                            f"for input '{input_name}', but dataset not found. "
                            f"Register it first, or provide inputs_override."
                        )

    def _worker_loop(self, pipeline_run_id, workspace, pipeline_name, config_override, inputs_override, reason):
        self._logger.info("Worker started for pipeline %s workspace=%s", pipeline_run_id, workspace)
        start_time = time.time()
        error_count = 0
        while not self._shutdown:
            try:
                pending, running = self._pipeline_queue_counts(pipeline_run_id)
                self._logger.debug("Pipeline %s: pending=%d running=%d", pipeline_run_id, pending, running)
                if pending == 0 and running == 0:
                    self._finalize_pipeline_run(pipeline_run_id)
                    self._logger.info("Pipeline %s completed", pipeline_run_id)
                    break
                self._process_pipeline_queue_once(
                    pipeline_run_id, workspace, pipeline_name, config_override, inputs_override, reason
                )
                error_count = 0
            except Exception as e:
                error_count += 1
                self._logger.exception("Pipeline worker error (run=%s err#=%s): %s", pipeline_run_id, error_count, e)
                if error_count >= self.MAX_WORKER_ERRORS:
                    if self._shutdown:
                        break  # Don't try to update DB if shutting down
                    try:
                        with self.db._conn() as conn:
                            conn.execute(
                                "UPDATE pipeline_stage_queue SET status='failed' WHERE pipeline_run_id=? AND status IN ('pending','running')",
                                (pipeline_run_id,),
                            )
                            conn.execute(
                                "UPDATE pipeline_runs SET status='failed', error=? WHERE id=?",
                                (f"Worker loop crashed: {e}", pipeline_run_id),
                            )
                    except Exception:
                        pass  # DB may be gone if executor was shutdown
                    break
                time.sleep(min(60, 5 * error_count))
                continue
            elapsed = time.time() - start_time
            if elapsed >= int(os.getenv("GOLDFISH_PIPELINE_MAX_ELAPSED_SECONDS", "86400")):
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE pipeline_stage_queue SET status='failed' WHERE pipeline_run_id=? AND status IN ('pending','running')",
                        (pipeline_run_id,),
                    )
                    conn.execute(
                        "UPDATE pipeline_runs SET status='failed', error=? WHERE id=?",
                        ("Pipeline exceeded max elapsed time", pipeline_run_id),
                    )
                break
            interval = self._poll_interval(int(elapsed))
            time.sleep(interval)

    def _pipeline_status(self, pipeline_run_id: str) -> dict:
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt, SUM(CASE WHEN status IN ('pending','running') THEN 1 ELSE 0 END) AS remaining FROM pipeline_stage_queue WHERE pipeline_run_id = ?",
                (pipeline_run_id,),
            ).fetchone()
            return {"total": row["cnt"], "remaining": row["remaining"]}

    def _pipeline_queue_counts(self, pipeline_run_id: str) -> tuple[int, int]:
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running FROM pipeline_stage_queue WHERE pipeline_run_id = ?",
                (pipeline_run_id,),
            ).fetchone()
            return (row["pending"] or 0, row["running"] or 0)

    def _finalize_pipeline_run(self, pipeline_run_id: str) -> None:
        # Set pipeline_runs status based on queued stages
        # Pipeline is failed if any stage failed, canceled, or skipped
        with self.db._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status='canceled' THEN 1 ELSE 0 END) AS canceled,
                    SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped,
                    SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(1) AS total
                FROM pipeline_stage_queue WHERE pipeline_run_id = ?
                """,
                (pipeline_run_id,),
            ).fetchone()
            failed_count = (row["failed"] or 0) + (row["canceled"] or 0) + (row["skipped"] or 0)
            status = "completed" if failed_count == 0 else "failed"
            conn.execute(
                "UPDATE pipeline_runs SET status=?, completed_at=? WHERE id=?",
                (status, datetime.now(UTC).isoformat(), pipeline_run_id),
            )

    def _list_pipeline_stage_runs(self, pipeline_run_id: str) -> list[StageRunInfo]:
        rows = self.db.list_stage_runs(pipeline_run_id=pipeline_run_id)
        result = []
        for r in rows:
            result.append(
                StageRunInfo(
                    stage_run_id=r["id"],
                    pipeline_run_id=r.get("pipeline_run_id"),
                    workspace=r["workspace_name"],
                    pipeline=r.get("pipeline_name"),
                    version=r["version"],
                    stage=r["stage_name"],
                    status=r["status"],
                    started_at=datetime.fromisoformat(r["started_at"]) if r.get("started_at") else None,
                    completed_at=datetime.fromisoformat(r["completed_at"]) if r.get("completed_at") else None,
                    progress=r.get("progress"),
                    log_uri=r.get("log_uri"),
                    artifact_uri=r.get("artifact_uri"),
                )
            )
        return result

    def _process_pipeline_queue_once(
        self,
        pipeline_run_id: str,
        workspace: str,
        pipeline_name: str | None,
        config_override: dict | None,
        inputs_override: dict | None,
        reason: str | None,
    ) -> list[StageRunInfo]:
        launched: list[StageRunInfo] = []
        now = datetime.now(UTC)
        now_iso = now.isoformat()

        # First pass: update running items, handle failures, and claim new rows
        to_launch: list[tuple[str, str, dict | None, dict | None]] = []
        with self.db._conn() as conn:
            # Update running items that have completed/failed/canceled in stage_runs
            running = conn.execute(
                "SELECT id, stage_run_id FROM pipeline_stage_queue WHERE pipeline_run_id=? AND status='running' AND stage_run_id IS NOT NULL",
                (pipeline_run_id,),
            ).fetchall()
            if running:
                ids = [r["stage_run_id"] for r in running]
                placeholders = ",".join(["?"] * len(ids))
                # Safe: ids come from previously stored queue rows (not user input)
                stage_rows = conn.execute(
                    f"SELECT id,status FROM stage_runs WHERE id IN ({placeholders})",
                    tuple(ids),
                ).fetchall()
                status_map = {r["id"]: r["status"] for r in stage_rows}
                for row in running:
                    sr_status = status_map.get(row["stage_run_id"])
                    if sr_status in ("completed", "failed", "canceled"):
                        conn.execute(
                            "UPDATE pipeline_stage_queue SET status=? WHERE id=?",
                            (sr_status, row["id"]),
                        )

            # Handle stuck claimed rows (claimed but no stage_run_id after timeout)
            # These can occur if a worker crashes between claiming and launching
            lease_cutoff = datetime.fromtimestamp(now.timestamp() - _CLAIM_LEASE_TIMEOUT_SECONDS, tz=UTC).isoformat()
            stuck_rows = conn.execute(
                """
                SELECT id, stage_name FROM pipeline_stage_queue
                WHERE pipeline_run_id = ? AND status = 'running'
                AND stage_run_id IS NULL AND claimed_at < ?
                """,
                (pipeline_run_id, lease_cutoff),
            ).fetchall()
            for stuck in stuck_rows:
                # Reset to pending so it can be reclaimed
                conn.execute(
                    "UPDATE pipeline_stage_queue SET status='pending', claimed_at=NULL WHERE id=?",
                    (stuck["id"],),
                )
                self._logger.warning(
                    "Reset stuck stage %s (claimed but never launched) in pipeline %s",
                    stuck["stage_name"],
                    pipeline_run_id,
                )

            # Build a map of all stages and their current statuses for dependency checking
            all_queue_rows = conn.execute(
                "SELECT stage_name, status FROM pipeline_stage_queue WHERE pipeline_run_id = ?",
                (pipeline_run_id,),
            ).fetchall()
            stage_status_map = {r["stage_name"]: r["status"] for r in all_queue_rows}

            # Get pending rows that can potentially be launched
            rows = conn.execute(
                """
                SELECT * FROM pipeline_stage_queue
                WHERE pipeline_run_id = ? AND status = 'pending' AND (claimed_at IS NULL)
                """,
                (pipeline_run_id,),
            ).fetchall()

            for row in rows:
                deps = json.loads(row["deps"]) if row["deps"] else []

                if deps:
                    # Check that all deps exist in the queue (guard against missing deps)
                    missing_deps = [d for d in deps if d not in stage_status_map]
                    if missing_deps:
                        # Mark as skipped - deps don't exist
                        conn.execute(
                            "UPDATE pipeline_stage_queue SET status='skipped', error=? WHERE id=?",
                            (f"Missing dependencies: {missing_deps}", row["id"]),
                        )
                        self._logger.error(
                            "Stage %s skipped - missing deps %s in pipeline %s",
                            row["stage_name"],
                            missing_deps,
                            pipeline_run_id,
                        )
                        continue

                    # Check dependency statuses
                    dep_statuses = [(d, stage_status_map.get(d)) for d in deps]

                    # If any dep failed/canceled/skipped, mark this stage as skipped (deadlock prevention)
                    failed_deps = [d for d, s in dep_statuses if s in ("failed", "canceled", "skipped")]
                    if failed_deps:
                        conn.execute(
                            "UPDATE pipeline_stage_queue SET status='skipped', error=? WHERE id=?",
                            (f"Upstream dependencies failed: {failed_deps}", row["id"]),
                        )
                        self._logger.info(
                            "Stage %s skipped due to failed deps %s in pipeline %s",
                            row["stage_name"],
                            failed_deps,
                            pipeline_run_id,
                        )
                        continue

                    # If any dep not completed, wait
                    if any(s != "completed" for _, s in dep_statuses):
                        continue

                # All deps completed (or no deps) - try to claim this row
                updated = conn.execute(
                    "UPDATE pipeline_stage_queue SET status='running', claimed_at=? WHERE id=? AND status='pending' AND (claimed_at IS NULL)",
                    (now_iso, row["id"]),
                ).rowcount
                if updated == 0:
                    # Lost the race - another worker claimed it
                    with self._race_loss_lock:
                        self._race_loss_counter += 1
                        counter = self._race_loss_counter
                        if counter > 10000:
                            self._race_loss_counter = 0
                    if counter % 50 == 0:
                        self._logger.debug(
                            "Lost CAS claim %s times (latest stage %s, run %s)",
                            counter,
                            row["stage_name"],
                            pipeline_run_id,
                        )
                    continue

                stage_config = None
                if config_override and row["stage_name"] in config_override:
                    stage_config = config_override[row["stage_name"]]
                stage_inputs = None
                if inputs_override and row["stage_name"] in inputs_override:
                    stage_inputs = inputs_override[row["stage_name"]]
                to_launch.append((row["id"], row["stage_name"], stage_config, stage_inputs))

        # Second pass: launch outside the transaction to avoid DB locks
        for queue_id, stage_name, stage_config, stage_inputs in to_launch:
            try:
                stage_run = self.stage_executor.run_stage(
                    workspace=workspace,
                    stage_name=stage_name,
                    pipeline_name=pipeline_name,  # Pass original, not effective (for file resolution)
                    pipeline_run_id=pipeline_run_id,
                    config_override=stage_config,
                    inputs_override=stage_inputs,
                    reason=reason,
                )
                launched.append(stage_run)
                # Update queue with the stage_run_id for status tracking
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE pipeline_stage_queue SET stage_run_id = ? WHERE id = ?",
                        (stage_run.stage_run_id, queue_id),
                    )
            except Exception as e:
                # Mark queue entry as failed with error message so it's visible to the user
                error_msg = str(e)
                self._logger.error("Stage %s failed to launch: %s", stage_name, error_msg)
                with self.db._conn() as conn:
                    conn.execute(
                        "UPDATE pipeline_stage_queue SET status = 'failed', error = ? WHERE id = ?",
                        (error_msg, queue_id),
                    )

        return launched

    @staticmethod
    def _poll_interval(elapsed: int) -> int:
        if elapsed < 60:
            return 5
        if elapsed < 600:
            return 10
        if elapsed < 3600:
            return 30
        return 60
