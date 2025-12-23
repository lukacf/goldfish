---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill when the user asks to create workspaces, run ML pipelines, manage datasets, track experiment lineage, or conduct any ML experimentation workflow. Goldfish provides 39 MCP tools for workspace management, pipeline execution, data management, and provenance tracking.
---

# Goldfish ML

This skill enables effective use of Goldfish ML, an MCP server that transforms Claude into an ML experimentation agent with full provenance tracking, reproducibility, and infrastructure abstraction.

## Core Mental Model

Goldfish manages ML experiments through **six key abstractions**:

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLDFISH ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────┤
│  WORKSPACE          VERSION           PIPELINE                   │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ w1/     │──────▶│ v1, v2  │       │ stages:             │    │
│  │ (slot)  │       │ (tags)  │       │   - preprocess      │    │
│  └─────────┘       └─────────┘       │   - train           │    │
│       │                              │   - evaluate        │    │
│       ▼                              └─────────────────────┘    │
│  STAGE (Docker)    SIGNAL            PROFILE                    │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ train.py│──────▶│ features│       │ h100-spot           │    │
│  │ (module)│       │ (npy)   │       │ a100-on-demand      │    │
│  └─────────┘       └─────────┘       └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key invariants:**
- All infrastructure (Docker, GCS, GCE) is hidden from Claude
- User workspace is plain files (no `.git`) - all versioning handled internally
- Every `run()` creates a version BEFORE execution (100% provenance)
- Signals connect stages with typed data flow
- **Pre-run review**: Claude reviews your code before execution to catch bugs early

## Workflow Decision Tree

```
START: What task?
  │
  ├─▶ "First time / Need orientation"
  │     └─▶ status() → See slots, active jobs, STATE.md
  │
  ├─▶ "Start new experiment"
  │     └─▶ create_workspace() → mount() → Edit files → run()
  │
  ├─▶ "Continue existing work"
  │     └─▶ status() → mount(slot, workspace) → Edit → run()
  │
  ├─▶ "Run ML training"
  │     └─▶ run(workspace, stages=["train"]) or run(workspace) for all
  │
  ├─▶ "Check run status"
  │     └─▶ list_runs() → get_run(run_id) → logs(run_id)
  │
  ├─▶ "Manage data"
  │     └─▶ register_dataset() or list_sources() → get_source()
  │
  ├─▶ "Track lineage"
  │     └─▶ get_workspace_lineage() → get_stage_lineage() → get_run_provenance()
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)
```

## Essential Workflows

### 1. Starting a New Experiment

```
1. Create workspace with clear goal
   create_workspace(name="lstm_baseline", goal="Train LSTM for price prediction")

2. Mount to edit slot
   mount(slot="w1", workspace="lstm_baseline")

3. Create workspace structure in workspaces/w1/:

   workspaces/w1/
   ├── pipeline.yaml          # Define stages and signals
   ├── requirements.txt       # Optional: only for project-specific packages
   ├── configs/
   │   ├── preprocess.yaml
   │   └── train.yaml
   └── modules/
       ├── preprocess.py
       └── train.py

   **Note:** Stages automatically use pre-built images with common ML libraries
   (numpy, pandas, torch, scikit-learn, etc.). No setup required.

4. Run the pipeline (with structured reason for experiment tracking)
   run("w1", reason={
       "description": "Baseline LSTM training",
       "hypothesis": "LSTM should achieve 85%+ accuracy"
   })

   run("w1", stages=["train"], reason={
       "description": "Testing larger batch size",
       "hypothesis": "Batch size 64 will improve stability",
       "approach": "Increased from 32 to 64",
       "min_result": "Lower loss variance",
       "goal": "Faster convergence with stable loss"
   })
```

### Pre-Run Review (Automatic)

Before executing any stage, Goldfish automatically reviews your code using Claude:

```
run("w1", stages=["train"])
→ Pre-run review activates
→ Reviews: pipeline.yaml, modules/train.py, configs/train.yaml
→ Checks for: undefined variables, logic errors, missing imports
→ Blocks run if ERRORs found, allows with WARNINGs

Example review output:
  ✗ BLOCKED: modules/train.py:12 - `learning_rate` undefined
  ✗ BLOCKED: modules/train.py:15 - `metrics` never assigned
  ⚠ WARNING: No validation split - training on full data
```

**Benefits:**
- Catches bugs before wasting GPU time
- Reviews use experiment context (diff, hypothesis, config)
- Fails open (approves on timeout/error) to avoid blocking
- Can be disabled: `pre_run_review.enabled: false` in goldfish.yaml
```

### 2. Pipeline Structure

A pipeline.yaml defines stages and their data flow:

```yaml
stages:
  - name: preprocess
    inputs:
      raw_data:
        type: dataset
        dataset: sales_v1      # Registered dataset
    outputs:
      features:
        type: npy              # NumPy array output

  - name: train
    inputs:
      features:
        type: npy
        from_stage: preprocess  # Consume from upstream
        signal: features
    outputs:
      model:
        type: directory         # Model checkpoint dir

  - name: evaluate
    inputs:
      model:
        from_stage: train
        signal: model
      features:
        from_stage: preprocess
        signal: features
    outputs:
      metrics:
        type: csv
```

### 3. Stage Implementation Pattern

Stage modules follow a consistent pattern:

```python
# modules/train.py
from goldfish.io import load_input, save_output, heartbeat

def main():
    # Load inputs (from /mnt/inputs/)
    features = load_input("features")  # Returns numpy array

    # Training logic with heartbeat for long-running jobs
    for epoch in range(epochs):
        heartbeat(f"Training epoch {epoch}")  # Signal "I'm alive"
        train_epoch(model, features)

    # Save outputs (to /mnt/outputs/)
    save_output("model", model_dir)

if __name__ == "__main__":
    main()
```

**Heartbeat API**: Call `heartbeat()` periodically in long-running computations to prevent the job from being terminated due to inactivity. See `references/stage_authoring.md` for details.

### 4. Metrics API (Stage Code)

Use the Metrics API from inside stage modules to record scalars and artifacts.

```python
# modules/train.py
from goldfish.metrics import (
    log_metric,
    log_metrics,
    log_artifact,
    log_artifacts,
    finish,
)

def main():
    for step in range(epochs):
        loss = train_step(...)
        acc = evaluate(...)

        # Single metric
        log_metric("train/loss", loss, step=step)

        # Batch metrics (same step)
        log_metrics({"train/acc": acc, "train/lr": lr}, step=step)

    # Artifacts must be relative to outputs dir
    log_artifact("model", "model.pt")
    log_artifacts({"checkpoints": "checkpoints/epoch_10"})

    # Ensure flush at end (safe to call multiple times)
    finish()
```

**Key semantics (important):**
- **Step consistency:** A given metric name must be logged with either `step=None` or `step=int` consistently.
  Mixing `None` and integers for the same metric is **skipped with a warning** (no crash).
- **Timestamp formats:** `timestamp` accepts ISO 8601 strings (UTC) or Unix float seconds.
  Stored and returned (via MCP tools) as ISO 8601 UTC strings.
- **Values:** Must be numeric (bools are rejected; use 0/1). NumPy scalars are supported.
- **Metric names:** Start with a letter, up to 256 chars. Use slashes for grouping (e.g., `train/loss`).
- **Metric name cap:** Per run, unique metric names are capped (default 10,000) to prevent abuse.
- **Artifacts:** `path` is **relative** to outputs dir; absolute paths and symlinks are rejected.
  `log_artifact` returns a backend URL if available.
- **Live metrics:** `get_run_metrics` will attempt a best-effort live sync for running runs
  (throttled by `GOLDFISH_METRICS_LIVE_SYNC_INTERVAL`).
- **Auto-finalize:** `finish()` is optional but recommended in a `finally` block. Auto-finalize uses `atexit`
  and won’t run on SIGKILL/crash.
- **Backend errors:** Use `had_backend_errors()` / `get_backend_errors()` to detect backend failures.

**Tuning flush behavior:**
- `GOLDFISH_METRICS_FLUSH_THRESHOLD` controls auto-flush (default 100).
- `GOLDFISH_METRICS_MAX_NAMES` caps unique metric names per run (default 10000).
- `GOLDFISH_METRICS_MAX_FUTURE_DRIFT_SECONDS` controls allowed future timestamp drift (default 86400).
- `GOLDFISH_METRICS_LIVE_SYNC` enables live DB sync for running runs (default true).
- `GOLDFISH_METRICS_LIVE_SYNC_INTERVAL` controls live sync cadence in seconds (default 15).
- `GOLDFISH_METRICS_MAX_OFFSET` caps pagination offset for metrics/artifacts (default 1,000,000).
- `GOLDFISH_WANDB_ARTIFACT_MODE` set to `artifact` to use W&B Artifacts (default `file`).
- `GOLDFISH_WANDB_ARTIFACT_TYPE` sets artifact type when using W&B Artifacts (default `artifact`).

**Advanced (avoid global logger):**
- `use_logger(custom_logger)` context manager routes calls to a specific `MetricsLogger`.

**Querying metrics (server tools):**
- `list_metric_names(run_id, metric_prefix=None)` to discover metrics without loading all data.
- `get_run_metrics(run_id, limit=1000, offset=0, metric_name=None, metric_prefix=None,
  artifact_limit=1000, artifact_offset=0)` for pagination. `limit=None` or `artifact_limit=None`
  returns all and may include a warning for very large runs.
- Optional `workspace=` parameter on both tools enforces run ownership.

### 5. Monitoring Runs

```
1. List recent runs
   list_runs(workspace="lstm_baseline", status="running")

2. Get run details
   get_run(run_id="stage-abc123")

3. Stream logs
   logs(run_id="stage-abc123", tail=500)

4. Cancel if needed
   cancel(run_id="stage-abc123", reason="Wrong hyperparameters")
```

### 6. Lineage & Provenance

Track exactly what produced what:

```
# Full workspace history
get_workspace_lineage("lstm_baseline")
→ versions, parent workspace, branches

# Compare versions
get_version_diff("lstm_baseline", from_version="v1", to_version="v3")
→ git commits, file changes

# Trace run inputs recursively
get_stage_lineage(run_id="stage-abc123")
→ Which preprocessing version fed this training run

# Full run provenance
get_run_provenance(stage_run_id="stage-abc123")
→ workspace, version, git SHA, config, inputs, outputs
```

## Tool Reference

### Workspace Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `status()` | Orientation - slots, jobs, sources | None |
| `create_workspace()` | New experiment | name, goal |
| `mount()` | Activate workspace in slot | slot, workspace |
| `hibernate()` | Deactivate (auto-saves) | slot, reason |
| `save_version()` | Create version save point | slot, message |
| `get_workspace()` | Workspace details + pipeline | workspace |
| `diff()` | Show uncommitted changes | slot |
| `rollback()` | Revert to version | slot, version, reason |
| `delete_workspace()` | Remove workspace | workspace, reason |
| `list_workspaces()` | All workspaces | limit, offset |

### Execution

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `run()` | Execute stages (with pre-run review) | workspace, stages, reason, wait |
| `get_run()` | Run details | run_id |
| `logs()` | Container logs | run_id, tail, since |
| `cancel()` | Stop run | run_id, reason |
| `list_runs()` | Query runs | workspace, stage, status |
| `get_outputs()` | Run outputs | run_id |

### Data Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `register_dataset()` | Register data source | name, source, format |
| `list_sources()` | Available sources | status, created_by |
| `get_source()` | Source details | name |
| `delete_source()` | Remove source | source_name, reason |
| `get_source_lineage()` | Data provenance | source_name |
| `promote_artifact()` | Stage output → source | job_id, output_name |

### Lineage & Provenance

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `get_workspace_lineage()` | Full history | workspace |
| `get_version_diff()` | Compare versions | workspace, from/to |
| `get_run_provenance()` | Exact run inputs | stage_run_id |
| `get_stage_lineage()` | Upstream tree | run_id, max_depth |
| `list_stage_versions()` | Stage version history | workspace, stage |
| `find_runs_using_stage_version()` | Impact analysis | workspace, stage, version |

### Utility

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `initialize_project()` | New Goldfish project | project_name, project_root |
| `reload_config()` | Hot-reload goldfish.yaml | None |
| `get_audit_log()` | Operation history | limit, workspace |
| `log_thought()` | Record reasoning | thought |

**Important:** After editing `goldfish.yaml`, always call `reload_config()` to apply changes without restarting the MCP server.

## Signal Types

| Type | Format | Use Case |
|------|--------|----------|
| `dataset` | External | Registered project data |
| `npy` | NumPy | Arrays, embeddings, tensors |
| `csv` | Pandas | Tabular data |
| `directory` | Dir | Model checkpoints, multi-file outputs |
| `file` | Single | Configs, small outputs |

## Resource Profiles

For `compute.profile` in stage configs:

| Profile | Hardware | Use Case |
|---------|----------|----------|
| `cpu-small` | 2 vCPU, 4GB | Light preprocessing |
| `cpu-large` | 8 vCPU, 32GB | Heavy data processing |
| `h100-spot` | H100 GPU, spot | Training (cost-effective) |
| `h100-on-demand` | H100 GPU | Critical training |
| `a100-spot` | A100 GPU, spot | Training alternative |
| `a100-on-demand` | A100 GPU | Guaranteed availability |

## Common Patterns

### Pattern: Iterative Experimentation

```
1. create_workspace("exp_v1", "Baseline LSTM")
2. mount("w1", "exp_v1")
3. Edit code, run, analyze
4. save_version("w1", "Working baseline")  # Creates v1
5. Edit more, run
6. hibernate("w1", "Completed baseline")

# Later: branch for variation
7. create_workspace("exp_v2", "Add attention", fork_from="exp_v1")
```

### Pattern: Debug Failed Run

```
1. list_runs(status="failed")
2. get_run(run_id)           # See error, config
3. logs(run_id, tail=500)    # Full error trace
4. get_run_provenance(run_id) # What inputs were used
5. Fix code, run again
```

### Pattern: Reproduce Past Result

```
1. get_run_provenance(stage_run_id)
   → Returns: workspace, version, git_sha, config, inputs

2. rollback(slot, snapshot_id)  # Restore exact code state

3. run(workspace, stages, config_override)  # Re-run with same config
```

### Pattern: Impact Analysis

```
"If I change preprocessing, what's affected?"

1. list_stage_versions("my_experiment", stage="preprocess")
   → preprocessing-v1, v2, v3...

2. find_runs_using_stage_version("my_experiment", "preprocess", 3)
   → All training runs that consumed preprocessing-v3
```

## Audit & Context Recovery

Goldfish maintains full audit trails:

```
log_thought("Switching to attention mechanism because...")

get_audit_log(limit=20, workspace="lstm_baseline")
→ All operations with reasons and timestamps
```

After context compaction, always start with:
```
status()  # Recover orientation: slots, jobs, STATE.md
```

## Best Practices

1. **Always provide clear goals** when creating workspaces
2. **Checkpoint frequently** with descriptive messages
3. **Use descriptive stage names** that reflect the operation
4. **Register datasets** before referencing in pipelines
5. **Check status()** after context recovery
6. **Use log_thought()** to document decisions
7. **Monitor long runs** with logs() and list_runs()

## Resources

Detailed documentation is available in the references directory:

- `references/config_reference.md` - **goldfish.yaml schema and GCE/GCS setup**
- `references/tools_reference.md` - Complete tool API documentation
- `references/pipeline_guide.md` - Pipeline YAML specification
- `references/stage_authoring.md` - Stage module development guide
- `references/end_to_end_example.md` - Complete worked example from scratch
