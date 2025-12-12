"""Pipeline execution engine for Goldfish."""

import atexit
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunInfo
from goldfish.pipeline.manager import PipelineManager


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
        self._recover_inflight_pipelines()
        self._race_loss_counter = 0

    def _recover_inflight_pipelines(self) -> None:
        """On startup, reschedule any pipelines that were mid-flight."""
        with self.db._conn() as conn:
            rows = conn.execute(
                "SELECT id, workspace_name, pipeline_name FROM pipeline_runs WHERE status IN ('pending','running')"
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
                    # submit worker to continue processing
                    self._pool.submit(
                        self._worker_loop,
                        prun,
                        workspace,
                        pipeline_name,
                        None,
                        None,
                    )

    def run_pipeline(
        self,
        workspace: str,
        pipeline_name: str | None = None,
        config_override: dict | None = None,
        reason: str | None = None,
        async_mode: bool = True,
    ) -> dict:
        """
        Run full pipeline.
        - async_mode=True: enqueue stages, return immediately with pipeline_run_id + launched stage_runs (pending/running).
        - async_mode=False: sequential, blocking (legacy behavior) using wait=True; no queue required.
        """

        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        effective_pipeline_name = pipeline_name or pipeline.name

        if not async_mode:
            runs: list[StageRunInfo] = []
            for stage in pipeline.stages:
                stage_config = config_override.get(stage.name) if config_override else None
                sr = self.stage_executor.run_stage(
                    workspace=workspace,
                    stage_name=stage.name,
                    pipeline_name=effective_pipeline_name,
                    pipeline_run_id=None,
                    config_override=stage_config,
                    reason=reason,
                    wait=True,
                )
                runs.append(sr)
            return {"stage_runs": [r.model_dump(mode="json") for r in runs], "pipeline_run_id": None}

        # async path with queue
        pipeline_run_id = f"prun-{uuid4().hex[:8]}"
        now = datetime.now(UTC).isoformat()

        with self.db._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at)
                VALUES (?, ?, ?, 'pending', ?)
                """,
                (pipeline_run_id, workspace, effective_pipeline_name, now),
            )

            prev = None
            for stage in pipeline.stages:
                deps: list[str] = []
                if prev:
                    deps.append(prev)
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, deps, status)
                    VALUES (?, ?, ?, 'pending')
                    """,
                    (pipeline_run_id, stage.name, json.dumps(deps)),
                )
                prev = stage.name

        launched = self._process_pipeline_queue_once(
            pipeline_run_id, workspace, effective_pipeline_name, config_override, reason
        )

        self._pool.submit(
            self._worker_loop,
            pipeline_run_id,
            workspace,
            effective_pipeline_name,
            config_override,
            reason,
        )

        return {
            "pipeline_run_id": pipeline_run_id,
            "stage_runs": [r.model_dump(mode="json") for r in launched],
        }

    def _worker_loop(self, pipeline_run_id, workspace, pipeline_name, config_override, reason):
        start_time = time.time()
        error_count = 0
        while True:
            try:
                pending, running = self._pipeline_queue_counts(pipeline_run_id)
                if pending == 0 and running == 0:
                    self._finalize_pipeline_run(pipeline_run_id)
                    break
                self._process_pipeline_queue_once(pipeline_run_id, workspace, pipeline_name, config_override, reason)
                error_count = 0
            except Exception as e:
                error_count += 1
                self._logger.exception("Pipeline worker error (run=%s err#=%s)", pipeline_run_id, error_count)
                if error_count >= self.MAX_WORKER_ERRORS:
                    with self.db._conn() as conn:
                        conn.execute(
                            "UPDATE pipeline_stage_queue SET status='failed' WHERE pipeline_run_id=? AND status IN ('pending','running')",
                            (pipeline_run_id,),
                        )
                        conn.execute(
                            "UPDATE pipeline_runs SET status='failed', error=? WHERE id=?",
                            (f"Worker loop crashed: {e}", pipeline_run_id),
                        )
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
        # set pipeline_runs status based on queued stages
        with self.db._conn() as conn:
            row = conn.execute(
                "SELECT SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed, SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed, SUM(1) AS total FROM pipeline_stage_queue WHERE pipeline_run_id = ?",
                (pipeline_run_id,),
            ).fetchone()
            status = "completed" if (row["failed"] or 0) == 0 else "failed"
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
        reason: str | None,
    ) -> list[StageRunInfo]:
        effective_pipeline_name = pipeline_name
        launched: list[StageRunInfo] = []

        # First pass: update running items and claim new rows to launch
        to_launch: list[tuple[str, str, dict | None]] = []
        with self.db._conn() as conn:
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
                    placeholders = ",".join(["?"] * len(deps))
                    dep_states = conn.execute(
                        f"SELECT stage_name, status FROM pipeline_stage_queue WHERE pipeline_run_id = ? AND stage_name IN ({placeholders})",
                        (pipeline_run_id, *deps),
                    ).fetchall()
                    if any(d["status"] != "completed" for d in dep_states):
                        continue

                # Try to claim this row atomically using UPDATE ... RETURNING
                claimed = conn.execute(
                    """
                    UPDATE pipeline_stage_queue
                    SET status='running', claimed_at=?
                    WHERE id=? AND status='pending' AND (claimed_at IS NULL)
                    RETURNING *
                    """,
                    (datetime.now(UTC).isoformat(), row["id"]),
                ).fetchone()
                if not claimed:
                    # Lost the race - another worker claimed it
                    self._race_loss_counter += 1
                    if self._race_loss_counter > 10000:
                        self._race_loss_counter = 0
                    if self._race_loss_counter % 50 == 0:
                        self._logger.debug(
                            "Lost CAS claim %s times (latest stage %s, run %s)",
                            self._race_loss_counter,
                            row["stage_name"],
                            pipeline_run_id,
                        )
                    continue

                stage_config = None
                if config_override and row["stage_name"] in config_override:
                    stage_config = config_override[row["stage_name"]]
                to_launch.append((row["id"], row["stage_name"], stage_config))

        # Second pass: launch outside the transaction to avoid DB locks
        for queue_id, stage_name, stage_config in to_launch:
            stage_run = self.stage_executor.run_stage(
                workspace=workspace,
                stage_name=stage_name,
                pipeline_name=effective_pipeline_name,
                pipeline_run_id=pipeline_run_id,
                config_override=stage_config,
                reason=reason,
            )
            launched.append(stage_run)
            with self.db._conn() as conn:
                conn.execute(
                    "UPDATE pipeline_stage_queue SET stage_run_id = ? WHERE id = ?",
                    (stage_run.stage_run_id, queue_id),
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

    def run_partial_pipeline(
        self,
        workspace: str,
        from_stage: str,
        to_stage: str,
        pipeline_name: str | None = None,
        config_override: dict | None = None,
        reason: str | None = None,
        async_mode: bool = True,
    ) -> dict:
        """Run a contiguous subset by constructing a temporary linear pipeline."""
        pipeline = self.pipeline_manager.get_pipeline(workspace, pipeline_name)
        effective_pipeline_name = pipeline_name or pipeline.name
        names = [s.name for s in pipeline.stages]
        try:
            start_idx = names.index(from_stage)
            end_idx = names.index(to_stage)
        except ValueError as err:
            raise ValueError("Stage not found in pipeline") from err
        if start_idx > end_idx:
            raise ValueError("from_stage must come before to_stage")
        sub_stages = pipeline.stages[start_idx : end_idx + 1]
        # Build a temporary PipelineDef-like object
        temp = type(pipeline)(name=pipeline.name, description=pipeline.description, stages=sub_stages)
        # Bypass queue: launch sequentially but non-blocking per stage
        stage_runs = []
        for stage in temp.stages:
            override = config_override.get(stage.name) if config_override else None
            sr = self.stage_executor.run_stage(
                workspace,
                stage.name,
                pipeline_name=effective_pipeline_name,
                config_override=override,
                reason=reason,
                wait=not async_mode,
            )
            stage_runs.append(sr)
            if not async_mode:
                self.stage_executor.refresh_status_once(sr.stage_run_id)
        return {"stage_runs": [s.model_dump(mode="json") for s in stage_runs]}
