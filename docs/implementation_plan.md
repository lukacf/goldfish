# Goldfish Implementation Plan — Stage-First System

Purpose: turn the current codebase into the stage-first design in docs/system_design.md, reusing existing components. Each phase has clear tasks, TDD-first tests, and exit criteria.

## Current-State Inventory (what we already have)
- Execution
  - MCP: `run_stage`, `run_pipeline` (sequential wrapper, returns `{ "runs": ... }`).
  - Engine: `jobs/stage_executor.py`, `jobs/pipeline_executor.py`, auto-versioning, signal lineage recording.
- Data/DB
  - Tables: `stage_runs`, `signal_lineage`, `workspace_versions`, legacy `jobs` (unused by run_stage/pipeline).
  - DB helpers for stage_runs in `db/database.py`; no MCP listing/monitoring tools.
- Pipelines
  - Single `pipeline.yaml` per workspace via `pipeline/manager.py` and parser; no named pipelines.
- Observability
  - No MCP tools for list_runs/stage_status/stage_logs/get_outputs/get_run/cancel_run; tests sometimes read SQLite.
- Outputs
  - StageExecutor writes outputs + lineage; auto-registration to sources is partial/manual (`promote_artifact` exists).
- Infra
  - Profiles abstraction exists; infra hidden behind profile names.
- Docs/tests
  - README/CLAUDE cover concepts; pipeline-heavy prompts. E2E pipeline tests exist; no tests for monitoring tools or named pipelines.

## Phase 0 — Schema & Contract Alignment (blocker for all later work)
Goal: fix current contract violations and add columns needed for observability/progress.
Tasks (TDD):
- Migrate DB: add columns to `stage_runs` → `pipeline_run_id`, `log_uri`, `artifact_uri`, `outputs_json`, `config_json`, `hints_json`, `progress`, `profile`; keep existing `job_id` for legacy run_job, but clarify semantics (job_id = legacy, pipeline_run_id = pipeline grouping). Add optional `pipeline_runs` table skeleton (id, workspace, pipeline, status, timestamps) for summaries.
- Add `count_stage_runs()` in `db/database.py` (for pagination symmetry with jobs).
- Models: update RunPipelineResponse to use key `stage_runs` (fix current bug that returns "runs"), extend StageRunInfo with new fields above.
Tests to write first:
- schema migration test: new columns exist; inserts/reads include pipeline_run_id and progress.
- response contract test: run_pipeline returns {"stage_runs": [...]}; Pydantic validation passes.
Exit criteria: migrations/tests green; run_pipeline returns correct key; StageRunInfo covers required fields.

## Phase 1 — Observability MCP Surface (critical path)
Goal: give Claude first-class visibility into stage runs (today: zero stage monitoring tools exist).
Tasks (TDD):
- MCP tools: `list_runs` (pagination uses count_stage_runs), `stage_status`, `stage_logs(tail,since)`, `get_outputs`, `get_run` (inputs+config+outputs), `cancel_run`.
- DB wiring: use existing list_stage_runs/get_stage_run/update_stage_run_status; add pagination via count_stage_runs.
- Executor support: add `tail_lines` and `since` to `local_executor.get_container_logs` and `gce_launcher.get_instance_logs` (currently full logs only). Persist container_id/instance_name in stage_run row to enable cancellation.
- Cancellation: extend StageExecutor to store handles; implement cancel_run to kill container/instance.
Tests:
- unit: each MCP tool validates args, pagination correct, cancel transitions status.
- integration: run_stage(local) → stage_status/logs (with since) → get_outputs; cancel_run stops a running container.
Exit: Claude can run a stage and see status/logs/outputs via MCP only (no DB/SSH). This phase unblocks usability.

## Phase 2 — Outputs & Auto-Registration
Goal: downstream wiring without manual source YAML; honor artifact flags.
Critical corrections incorporated:
- Add `artifact: Optional[bool]=False` to SignalDef (currently missing; artifact silently dropped).
- In `_record_output_signals`, pass `is_artifact=output_def.artifact` to DB.
- Implement `_auto_register_artifacts` to create/update sources when `artifact=true` (idempotent, ties produced_by_stage_run_id).
Tasks (TDD):
- Model update (SignalDef); parser accepts artifact flag.
- StageExecutor writes outputs_json including `from_stage_ref`, `storage_location`, `is_artifact`.
- Auto-registration path implemented and covered.
Tests:
- unit: artifact flag survives parsing; add_signal receives is_artifact true.
- integration: run_stage with artifact output → source created once; get_outputs returns from_stage_ref usable by from_stage.
Exit: artifacts auto-registered; no manual source files needed for typical flow.

## Phase 3 — Named Pipelines & Pipeline Argument
Goal: train/inference/ablation pipelines side-by-side.
Critical corrections: pipeline.yaml is hardcoded everywhere today; pipeline param missing on all calls.
Tasks (TDD):
- PipelineManager/parser: accept optional pipeline_name → `pipelines/<name>.yaml`, default to pipeline.yaml for BC.
- Propagate pipeline param through run_stage, run_pipeline, validate_pipeline, StageExecutor, PipelineExecutor.
- Add MCP tool `list_pipelines` (scan pipelines/ dir).
Tests:
- unit: validate_pipeline resolves named file; errors on missing.
- integration: run_stage with pipeline="inference" uses that DAG; list_pipelines shows available files.
Exit: multiple named pipelines functional with backward-compatible default.

## Phase 4 — Run Semantics, Grouping, and Non-Blocking (Stateless MCP)
Goal: satisfy Claude’s non-blocking requirement without relying on in-memory state (MCP servers are stateless and may restart).
Constraints: no volatile orchestrator state; restarts must not lose pipeline/run scheduling.
Plan:
- run_stage: expose `wait: bool = False` (default). Launch container immediately, persist all runtime handles (container_id or instance_name) + backend type in stage_runs so a restarted server can poll/cancel.
- pipeline_run_id: add column (Phase 0) and optional `pipeline_runs` table. Store pipeline DAG expansion into a durable queue table (e.g., pipeline_stage_queue: pipeline_run_id, stage, status, deps_resolved bool).
- Stateless async pipeline: run_pipeline returns immediately after writing pipeline_runs row + queue entries (all pending). A restart-safe worker loop reads pending rows, checks dependencies via stage_run statuses, launches next stage, and records stage_run_ids. Worker can be started in every server process; use DB row-level locking/"claimed_at" to avoid double-launch.
- No in-memory orchestrator assumptions; all scheduling decisions derive from DB state and stage_run statuses.
- If full DAG worker is too heavy, interim: run_pipeline expands DAG, launches first stage, marks remaining stages with dependency metadata; same restartable worker promotes stages as deps complete.
Tests (TDD):
- unit: run_stage wait flag; stage_runs store backend handle.
- integration: run_pipeline async → immediately returns pipeline_run_id + pending stage_run placeholders; worker loop (simulated restart) continues launching remaining stages honoring deps; list_runs(pipeline_run_id) shows progression.
Exit: run_stage non-blocking; run_pipeline returns immediately; pipeline execution survives server restarts with no in-memory state reliance.

## Phase 5 — Progress & Logs Quality
Goal: actionable progress + durable logs with tail/since.
Corrections:
- stage_runs schema lacks progress column (added in Phase 0).
- LocalExecutor currently uses `docker run --rm` with no persisted logs; GCE launcher assumes GCS logs but startup script doesn’t write them.
Tasks (TDD):
- Persist logs: LocalExecutor writes to `.goldfish/runs/<stage_run_id>/logs/output.log`; GCE startup script streams container stdout/stderr to GCS path recorded as log_uri.
- Add `since` to executor log fetchers; support tail_lines server-side.
- Progress: StageExecutor emits phase strings (build/launch/run or task-specific) stored in stage_runs.progress.
Tests:
- unit: progress stored; log retrieval respects tail/since; log_uri set.
- integration: long-running dummy stage → progressive logs, progress updates; GCE path mocked/written.
Exit: stage_logs is incremental and bounded; progress visible via stage_status.

## Phase 6 — Docs & Prompts (Stage-First UX)
Goal: reflect new tools/semantics and non-blocking behavior.
Tasks:
- Update CLAUDE.md/README quickstart to stage-first loop, non-blocking run_stage, named pipelines, pipeline_run_id usage.
- Add examples using stage_logs(since) and get_outputs/from_stage_ref.
Tests: doc lint/check; examples match tool signatures.
Exit: docs and prompts align with new API; legacy job tooling not promoted.

## Phase 7 — Cleanup & Deprecation
Goal: eliminate competing architectures.
Tasks:
- Deprecate or hide legacy job MCP tools (list_jobs/job_status/run_job) from prompts; keep code only if required by old tests, otherwise remove.
- Remove hardcoded pipeline.yaml assumptions left after Phase 3.
Tests: grep guard for legacy references; full test suite green without job tools in Claude prompts.
Exit: only stage_run_id/pipeline_run_id flow is exposed.

----------------------------------------------------------------------

Execution approach
- TDD: write or adapt tests per phase before coding tasks; gate merges on phase tests.
- Incremental PRs per phase; keep run_pipeline shape fix (Phase 0) first to unblock downstream work.
- Reuse existing StageExecutor, profile resolver, infra launchers; extend not rewrite.
