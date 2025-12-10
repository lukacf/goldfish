# Goldfish System Design (Stage-First, Claude-Oriented)

Status: draft proposal — greenfield (no backward compatibility required).

Audience: An engineer or Claude instance with no prior Goldfish context who needs to understand the problem, concepts, data/flow, and MCP surface well enough to build features or run experiments.

----------------------------------------------------------------------

## 1. Problem Statement & Objectives

### Problem
ML practitioners (and Claude acting on their behalf) currently juggle code, configs, Docker builds, cloud launches, and ad-hoc lineage tracking. The result is slow iteration, brittle reproducibility, and high cognitive load. Existing manual workflows (edit YAML, build/push, gcloud run, grep logs, hand-write source manifests) do not scale and hide context from Claude.

### Objectives
- **Stage-first** execution as the core primitive; pipelines optional.
- **Claude-friendly** API: a small, coherent MCP surface for run/monitor/wire.
- **Reproducible** by default: auto-version workspaces, capture effective config/inputs/outputs.
- **Infra-hidden**: only profile names are exposed; GCE/Docker details remain server-side.
- **Auto-wiring**: outputs become immediately referenceable via `from_stage_ref`; no manual source YAML.
- **Multi-pipeline**: support train/inference/ablation side-by-side in one workspace.
- **Observability**: first-class status/logs/output discovery; no SSH or DB spelunking.

Non-goal: supporting legacy jobs/list_jobs/run_job or any dual run abstraction.

----------------------------------------------------------------------

## 2. Glossary (short, canonical)
- **Workspace**: mutable code+config tree; versioned on run (vN). Maps to a git worktree/branch internally.
- **Module**: code + ML config for one logical step (file pair under modules/ & configs/).
- **Stage**: runnable recipe referencing one module, inputs, outputs, and a profile. One stage ⇔ one container run.
- **Pipeline**: optional DAG of stages defined in `pipelines/<name>.yaml` (default `pipeline.yaml`).
- **stage_run_id**: ID for a single stage execution.
- **pipeline_run_id**: ID grouping all stage runs from one pipeline invocation (may be null for ad-hoc run_stage).
- **Profile**: named infra bundle; hides machine/zone/disk/image.
- **Hint**: optional metadata guiding infra (spot_ok, priority, estimated_hours); not a hard constraint.
- **Signal / Output**: named data produced by a stage; stored with URI and lineage.

----------------------------------------------------------------------

## 3. Architecture Overview

### Control Plane vs Data Plane
- **Control Plane** (MCP tools): validate inputs, orchestrate StageExecutor, persist metadata, audit log, return structured results to Claude.
- **Data Plane** (Execution): build/push container, launch Local or GCE, stream logs, write outputs to storage, emit lineage.

### File Layout (per mounted workspace)
```
workspaces/<slot>/
  modules/<stage>.py          # Code
  configs/<stage>.yaml        # ML hyperparams + profile + hints
  pipelines/
    pipeline.yaml             # Default pipeline (optional)
    train.yaml                # Named pipeline example
    inference.yaml            # Named pipeline example
  STATE.md
```

### Logical Component Diagram (textual)
- **MCP Server**
  - server_tools/
    - execution_tools: run_stage, run_pipeline, stage_status, stage_logs, list_runs, get_run, get_outputs, cancel_run
    - pipeline_tools: validate_pipeline, list_pipelines
  - managers/
    - StageExecutor (jobs/stage_executor.py)
    - PipelineExecutor (jobs/pipeline_executor.py)
    - PipelineManager (pipeline/manager.py)
    - WorkspaceManager (workspace/manager.py)
    - Database (db/database.py)
  - infra/
    - docker_builder, local_executor, gce_launcher, resource_launcher

----------------------------------------------------------------------

## 4. Data Model (proposed, greenfield)

### stage_runs (canonical run table)
- stage_run_id (pk)
- pipeline_run_id (nullable)
- workspace, pipeline, stage, version
- profile, hints_json
- status: pending | running | completed | failed | canceled
- progress: optional string
- started_at, completed_at
- log_uri, artifact_uri
- inputs_json (resolved URIs and from_stage/dataset refs)
- outputs_json (names → URIs, types, is_artifact, from_stage_ref)
- config_json (effective ML config used)
- error_json (message, code, details)

### pipeline_runs (optional grouping)
- pipeline_run_id (pk)
- workspace, pipeline
- status, started_at, completed_at

### signal_lineage
- from_stage_run_id, output_name
- to_stage_run_id, input_name
- storage_location

### sources (for auto-registration)
- source_name, description
- storage_location
- produced_by_stage_run_id
- type, metadata_json

----------------------------------------------------------------------

## 5. Execution Flows (detailed sequences)

### A) Single-stage iteration (default path)
```
Claude
  | run_stage(workspace, stage="tbpe", pipeline="train", wait=false)
Goldfish MCP
  | validate args
  | assign pipeline_run_id (new if none passed)
  | auto-version workspace -> tag v12
  | insert stage_run (pending)
  | StageExecutor:
      - resolve inputs (datasets or from_stage refs) using signal_lineage
      - resolve profile + hints
      - build/push image
      - launch backend (Local/GCE)
      - set stage_run status=running, log_uri
  | return stage_run_id + pipeline_run_id
Claude monitors
  | poll stage_status(stage_run_id)
  | poll stage_logs(stage_run_id, tail=200, since=ISO) to avoid repeat logs
Stage completes (success or fail)
  StageExecutor:
    - capture outputs (URIs, types)
    - write signal_lineage rows
    - if output.artifact=true -> register source (idempotent)
    - set status=completed|failed, artifact_uri/log_uri
Claude fetches outputs
  | get_outputs(stage_run_id) -> from_stage_ref strings for downstream wiring
```

### B) Pipeline invocation (optional)
```
Claude
  | run_pipeline(workspace, pipeline="train", async_mode=true)
Goldfish MCP
  | create pipeline_run_id
  | topologically order stages from pipelines/train.yaml
  | for each stage: call run_stage(..., pipeline_run_id=that)
  | return pipeline_run_id + stage_run_ids (pending)
Monitoring
  | list_runs(pipeline_run_id=...) to see all stage runs
  | stage_status/stage_logs per stage_run_id
```

### C) Auto-registration of outputs (no manual sources)
```
On stage completion:
  outputs_json recorded: name, type, storage_location, from_stage_ref, is_artifact
  signal_lineage rows inserted
  if is_artifact: create/update source (sources table) with produced_by_stage_run_id
Downstream inputs can always specify from_stage: "<stage>/<output>"
```

----------------------------------------------------------------------

## 6. MCP Tool Specification (contract for Claude)

### run_stage
- Args: workspace:str; stage:str; pipeline:str|None; config_override:dict|None; inputs_override:dict|None; reason:str|None; wait:bool=false
- Returns wait=false: `{stage_run_id, pipeline_run_id, workspace, pipeline, stage, status:"running", started_at, message}`
- Returns wait=true: above + `completed_at, error?, outputs (see get_outputs shape)`

### stage_status
- Args: stage_run_id:str
- Returns: `StageRunInfo {stage_run_id, pipeline_run_id, workspace, pipeline, stage, status, progress?, started_at, completed_at?, log_uri?, artifact_uri?, profile, hints}`

### stage_logs
- Args: stage_run_id:str; tail_lines:int=200; since:str|None (ISO8601)
- Returns: `{stage_run_id, status, logs, log_uri}`

### list_runs
- Args: workspace:str|None; stage:str|None; status:str|None; pipeline_run_id:str|None; limit:int=50; offset:int=0
- Ordering: started_at DESC by default
- Returns: `{runs:[StageRunInfo], total_count:int, has_more:bool}`

### get_run
- Args: stage_run_id:str
- Returns: `{stage_run: StageRunInfo, inputs:{name:{from_stage|dataset, resolved_uri}}, outputs:[OutputInfo], config: dict}`

### get_outputs
- Args: stage_run_id:str
- Returns: `{stage_run_id, outputs:[{name, type, storage_location, from_stage_ref, is_artifact, size_bytes|null}]}`

### cancel_run
- Args: stage_run_id:str; reason:str
- Returns: `{success:bool, previous_status:str}`

### list_pipelines
- Args: workspace:str
- Returns: `{pipelines:[{name, path}]}` (from `pipelines/*.yaml`)

### validate_pipeline
- Args: workspace:str; pipeline:str|None
- Returns: `{workspace, pipeline, valid:bool, errors:[]}`

### run_pipeline (optional wrapper)
- Args: workspace:str; pipeline:str|None; config_override:dict|None; reason:str|None; async_mode:bool=true
- Returns async: `{pipeline_run_id, stage_runs:[{stage_run_id, stage, status:"pending"}]}`; blocking variant returns final statuses

Status enums: pending, running, completed, failed, canceled.
Errors: `{code, message, details}` persisted to stage_run.error_json.

----------------------------------------------------------------------

## 7. Config Model (what belongs where)

### Stage config (ML-only + profile + hints)
```yaml
# configs/tbpe.yaml
profile: gpu-h100-train
hints:
  spot_ok: true
  estimated_hours: 6
inputs:
  tokens:
    from_stage: preprocess/tokens
outputs:
  tbpe:
    type: dataset
    artifact: true
params:
  epochs: 12
  lr: 3e-4
  batch_size: 8
```

Rules:
- ML params live here (epochs, lr, batch_size, arch sizes).
- Infra is a profile name only; hints are advisory.
- Inputs can be `from_stage` or `dataset` (project-level source).
- Outputs declare type and optional `artifact: true` for auto-registration.

### Named pipelines
```yaml
# pipelines/train.yaml
stages:
  - name: preprocess
  - name: tbpe
    inputs: {tokens: {from_stage: preprocess/tokens}}
  - name: lm_train
    inputs: {tokens: {from_stage: tbpe/tokens}}

# pipelines/inference.yaml
stages:
  - name: inference
    inputs:
      model: {dataset: lm_checkpoint}
      data: {dataset: eval_corpus}
```

----------------------------------------------------------------------

## 8. Observability & Monitoring (user experience)

User (Claude) loop:
1) `run_stage(..., wait=false)`
2) `stage_status` to see status/progress/log_uri
3) `stage_logs(tail, since)` to stream incrementally
4) On terminal state: `get_outputs` to wire next stage, `get_run` for full debug (inputs/config/outputs)
5) `list_runs(workspace)` to see active/recent runs; newest-first

Design choices:
- `since` prevents refetching logs; cap tail_lines to avoid huge payloads.
- Progress is an optional string; executor may emit phase markers (e.g., "download 3/5", "epoch 2/20").
- log_uri/artifact_uri always recorded for offline access.

----------------------------------------------------------------------

## 9. Failure, Cancellation, and Timeouts

- StageExecutor always writes a terminal status and error_json.
- cancel_run(stage_run_id) triggers backend kill (GCE delete or local stop); status→canceled with reason.
- Per-profile defaults: max runtime, retry policy (none by default), preemption handling (if spot_ok).
- Outputs/logs recorded even on failure when available.

----------------------------------------------------------------------

## 10. Security & Guardrails

- Name validation: workspace/stage/pipeline `^[a-zA-Z0-9_-]+$`.
- Path validation: no traversal, no symlinks; all file ops relative to workspace root.
- Profiles allowlisted; raw machine/zone never exposed to MCP.
- Resource caps per profile; network access per executor policy.
- Audit logging for all MCP tool calls with reason when provided.

----------------------------------------------------------------------

## 11. Performance & Cost Considerations

- Image reuse: tag by workspace+version to leverage Docker cache; optional build arg to force no-cache.
- GCE placement: resource_launcher searches zones; hints.priority can bias placement.
- Spot vs on-demand: hinted via profile/hints; preemption backoff can be profile-defined.
- Log streaming: tail + since to reduce data volume.

----------------------------------------------------------------------

## 12. Extensibility

- New backends: implement Executor interface used by StageExecutor (launch, tail_logs, cancel, fetch_outputs).
- New signal types: extend models.py SignalDef, update parser, IO library, validation.
- New profiles: add to profiles catalog; expose only names to MCP clients.
- Additional observability: aggregate pipeline status, metrics export; built atop stage_runs/pipeline_run_id.

----------------------------------------------------------------------

## 13. Example End-to-End Flows

### Training iteration (TBPE tweak)
1) Edit `modules/tbpe.py` and `configs/tbpe.yaml`.
2) `run_stage(workspace=w1, stage="tbpe", pipeline="train", wait=false)`.
3) Poll `stage_status`, stream `stage_logs`.
4) `get_outputs` → returns from_stage_ref `tbpe/tokens`.
5) `run_stage(..., stage="lm_train", inputs_override={tokens:{from_stage:"tbpe/tokens"}})` or rely on pipeline wiring.

### Full pipeline smoke (train)
1) `run_pipeline(workspace=w1, pipeline="train", async_mode=true)` → pipeline_run_id + stage_run_ids.
2) `list_runs(pipeline_run_id=...)` to view all stages; `stage_status/logs` per stage.
3) On completion, `get_outputs` per stage for artifacts.

### Inference-only path
1) Keep `pipelines/inference.yaml` with a single `inference` stage.
2) `run_stage(stage="inference", pipeline="inference")`; monitor as usual.

----------------------------------------------------------------------

## 14. Testing Strategy (for this design)

- Unit: validate_tool_args, profile resolver, parser for named pipelines, signal lineage writing, output auto-registration.
- Integration: run_stage local backend; run_stage GCE backend (mock); run_pipeline with pipeline_run_id grouping.
- Observability: stage_logs since/tail, list_runs ordering, get_run completeness.
- Failure paths: cancel_run, executor failure writes error_json, outputs partially recorded.

----------------------------------------------------------------------

## 15. Open Decisions (explicit)
1) Keep `pipeline_run_id` naming vs `correlation_id`? (default: pipeline_run_id)
2) Should run_pipeline remain once stage tools ship, or be a thin wrapper only? (default: keep as async wrapper)
3) Artifact auto-registration: always on vs require `artifact: true` (default: honor flag)
4) Progress representation: free-form string vs structured phases (default: free-form string with optional phase tags)
5) Do we create a `pipeline_runs` table or store pipeline_run_id only on stage_runs? (default: optional table for summaries)

----------------------------------------------------------------------

## 16. Summary of Required Changes (implementation checklist)
- Schema: add pipeline_run_id to stage_runs; optional pipeline_runs table; ensure outputs_json/log_uri fields.
- MCP: implement/run_stage (wait flag), stage_status, stage_logs(since), list_runs DESC, get_run, get_outputs, cancel_run, list_pipelines, validate_pipeline, run_pipeline wrapper.
- Pipeline manager: support `pipelines/<name>.yaml` and pipeline arg across tools.
- Stage executor: emit progress/log_uri, persist outputs_json, auto-register artifacts, honor hints/profile.
- Docs: keep CLAUDE.md concise; point to this design; provide quickstart for stage-first workflow.

----------------------------------------------------------------------

### ASCII Sequence (run_stage → completion, expanded)
```
Claude client
  | run_stage(w, stage, pipeline="train", wait=false)
  v
MCP tool run_stage
  | validate -> assign pipeline_run_id -> tag vN -> stage_run row (pending)
  | call StageExecutor
  v
StageExecutor
  | resolve inputs (datasets / signal_lineage) -> mount inputs
  | resolve profile + hints -> pick backend
  | build/push image -> launch container
  | update stage_run (running, log_uri)
  v
Executor backend (GCE/local)
  | stream logs -> StageExecutor writes log tail, progress
  | on completion: collect outputs -> upload/store -> emit URIs
  v
StageExecutor
  | record outputs_json + signal_lineage
  | auto-register artifacts
  | update stage_run (completed/failed, artifact_uri, completed_at, error?)
  v
Claude client
  | stage_status -> stage_logs(since) -> get_outputs -> wire next stage
```
