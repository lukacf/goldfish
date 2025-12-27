---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill for workspace management, pipeline execution, data registry operations, and provenance tracking. Goldfish provides 24 master tools for efficient ML workflows.
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
- **Pre-run review**: The configured SVS agent reviews your code before execution to catch bugs early

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
  │     └─▶ list_runs() → inspect_run(run_id) → logs(run_id)
  │
  ├─▶ "Manage data"
  │     └─▶ register_source() or manage_sources(action="list")
  │
  ├─▶ "Track lineage"
  │     └─▶ inspect_workspace() → inspect_run(include=["provenance"])
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)
```

## Data Source Metadata (Required, Strict)

All new sources/datasets/artifacts must include **mandatory metadata**.
Goldfish does **not** infer metadata and treats local vs GCS identically.

### Required Top-Level Structure

```json
{
  "schema_version": 1,
  "description": "Human/LLM description (min 20 chars)",
  "source": { ... },
  "schema": { ... }
}
```

### Source Section (format/encoding only)

```json
{
  "format": "npy|npz|csv|file",
  "size_bytes": 123456,
  "created_at": "2025-12-24T12:00:00Z"
}
```

CSV requires `format_params`:

```json
{ "format_params": { "delimiter": "," } }
```

Rules:
- `delimiter` must be a single character from: `, ; | \t :`.

**Directory sources are rejected.**

### Schema Section (meaning/semantics)

Tensor (npy/npz):

```json
{
  "kind": "tensor",
  "arrays": {
    "features": {
      "role": "features",
      "shape": [1000, 768],
      "dtype": "float32",
      "feature_names": { "kind": "list", "values": ["f1","f2","..."] }
    }
  },
  "primary_array": "features"
}
```

Tabular (csv):

```json
{
  "kind": "tabular",
  "row_count": 100000,
  "columns": ["col1", "col2"],
  "dtypes": { "col1": "float32", "col2": "int64" }
}
```

File (blob):

```json
{
  "kind": "file",
  "content_type": "application/json"
}
```

### Feature names (required)

`feature_names` must be present and use one of:

```json
{ "kind": "list", "values": ["f1", "f2"] }
```

```json
{ "kind": "pattern", "template": "token_{i}", "start": 1, "count": 50000,
  "sample": ["token_1", "token_2"] }
```

```json
{ "kind": "none", "reason": "scalar value" }
```

### Tools that REQUIRE metadata

- `register_source(..., reason, metadata)`
- `promote_artifact(..., reason, metadata)`
- `manage_sources(action="update", ..., metadata)`

Optional tool arguments (must match metadata if provided):
- `format`
- `size_bytes`
  - Must be a positive integer and ≤ 1 PB when known.
  - May be `null` for stage outputs when the size is unknown at authoring time.
- `description` (for promote_artifact)

Missing or invalid metadata is rejected.

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

Before executing any stage, Goldfish automatically reviews your code using the configured SVS agent provider
(default: Claude Code CLI):

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

**Recovery from BLOCKED status:**
1. **Analyze findings**: Read the review output carefully. `BLOCKED` indicates high-confidence errors.
2. **Fix the code/config**: Edit the offending files in your slot (e.g., `workspaces/w1/modules/train.py`).
3. **Re-run**: Call `run()` again. Goldfish will perform a new review.
4. **Dispute (rare)**: If you are certain a finding is a false positive, you can briefly explain why in the `reason` field of your next `run()` call, though the mechanistic block remains until fixed.
5. **Disable (if permitted)**: If the validation system is being too restrictive, you can disable it in `goldfish.yaml` via `svs.enabled: false`.

**Benefits:**
- Catches bugs before wasting GPU time
- Reviews use experiment context (diff, hypothesis, config)
- Fails open (approves on timeout/error) to avoid blocking
- Can be disabled: `svs.ai_pre_run_enabled: false` (or `svs.enabled: false`) in goldfish.yaml

Requires the selected CLI to be installed and authenticated on the host.

### 2. Pipeline Structure

A pipeline.yaml defines stages and their data flow:

```yaml
stages:
  - name: preprocess
    inputs:
      raw_data:
        type: dataset
        dataset: sales_v1      # Registered dataset
        schema:
          kind: tabular
          columns: ["price", "volume"]
          dtypes: {price: "float32", volume: "int64"}
    outputs:
      features:
        type: npy              # NumPy array output
        schema:                # Required contract (use null only if unknown)
          kind: tensor
          shape: [null, 768]
          dtype: float32

  - name: train
    inputs:
      features:
        type: npy
        from_stage: preprocess  # Consume from upstream
        signal: features
        schema:
          kind: tensor
          shape: [null, 768]
          dtype: float32
    outputs:
      model:
        type: directory         # Model checkpoint dir
        schema: null            # Required tag; set null if unknown

  - name: evaluate
    inputs:
      model:
        from_stage: train
        signal: model
        schema: null
      features:
        from_stage: preprocess
        signal: features
        schema:
          kind: tensor
          shape: [null, 768]
          dtype: float32
    outputs:
      metrics:
        type: csv
        schema: null
```

**Signal aliasing (important):**
- `signal` is optional. If omitted, it defaults to the input name.
- Use `signal` to map an input name to a different upstream output name.
  Example: `X: { from_stage: preprocess, signal: features }`

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

**Heartbeat API**: Call `heartbeat()` periodically in long-running computations to prevent the job from being terminated due to inactivity. 
- **Default Timeout**: 600 seconds (10 minutes).
- **Configuration**: Increase this via `compute.heartbeat_timeout_seconds` in the stage-specific YAML config.
- **Auto-Termination**: If the supervisor detects a stale heartbeat, it will terminate the container to free up resources.

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
  - **Inconsistency rule**: If you first log "loss" with `step=1`, and then log "loss" with `step=None`, the latter is **skipped** with a warning.
- **Timestamp formats:** `timestamp` accepts ISO 8601 strings (UTC) or Unix float seconds.
  Stored and returned (via MCP tools) as ISO 8601 UTC strings.
- **Values:** Must be numeric (bools are rejected; use 0/1). NumPy scalars are supported.
- **Metric names:** Start with a letter, up to 256 chars. Use slashes for grouping (e.g., `train/loss`).
- **Metric name cap:** Per run, unique metric names are capped (default 10,000) to prevent abuse.
- **Artifacts:** `path` is **relative** to outputs dir; absolute paths and symlinks are rejected.
  `log_artifact` returns a backend URL if available.
- **Live metrics:** `inspect_run` will attempt a best-effort live sync (Overdrive) for running runs.
- **Auto-finalize:** `finish()` is optional but recommended in a `finally` block. Auto-finalize uses `atexit`
  and won’t run on SIGKILL/crash.

### 4.5 SVS (Schema Contracts + Output Stats)

SVS is enabled by default and can be opted out. It enforces **output contracts** (shape/dtype) and computes lightweight **output stats** (entropy, null ratio, variance, etc.) for silent-failure detection.

**Schemas are required (inputs + outputs).** Use `schema: null` only when you truly cannot define a contract.
Goldfish will emit a non‑blocking runtime warning whenever a schema is null.

**Output contract (pipeline.yaml) — required:**
```yaml
stages:
  - name: preprocess
    outputs:
      features:
        type: npy
        # Schema is required; set to null only if unknown.
        schema:
          kind: tensor
          shape: [null, 768]
          dtype: float32
```

**JSON-heavy outputs** (lists/dicts) use `kind: json`:
```yaml
outputs:
  records:
    type: file
    schema:
      kind: json  # accepts dict or list
```

**Enforcement mode** (goldfish.yaml):
```yaml
svs:
  default_enforcement: warning  # "blocking" or "warning"
```
When in `warning` mode (default), contract mismatches log a warning but allow the run to continue. In `blocking` mode, the stage fails if the contract is violated.

**Preflight warnings & errors:**
- When SVS is enabled, a preflight validation pass runs for every stage.
- Errors block the run; warnings are recorded and surfaced via `inspect_run(include=["svs"])`.

**Key Semantic Checks:**
- `entropy`: Shannon entropy of values (catches mode collapse or data corruption).
- `null_ratio`: Fraction of NaN/None values (catches loading errors).
- `vocab_utilization`: Fraction of vocab indices used (catches dead embeddings).
- `unique_count`: Distinct values in sample.

**Live SVS sync:** `inspect_run` performs a best‑effort live sync of SVS findings for running runs via the metadata bus.

**Experimental self‑learning:**
```yaml
svs:
  auto_learn_failures: false  # default: off
```

### 5. Monitoring Runs

```
1. List recent runs
   list_runs(workspace="lstm_baseline", status="running")

2. Get run details (includes dashboard + trends + health)
   inspect_run(run_id="stage-abc123")

3. Stream logs (if dashboard is not enough)
   logs(run_id="stage-abc123", tail=500)

4. Cancel if needed
   cancel(run_id="stage-abc123", reason="Wrong hyperparameters")
```

### 6. Lineage & Provenance

Track exactly what produced what:

```
# Full workspace history
inspect_workspace("lstm_baseline")
→ versions, parent workspace, branches

# Compare versions
diff(target="lstm_baseline", against="v1")
→ git commits, file changes

# Full run provenance
inspect_run(run_id="stage-abc123", include=["provenance"])
→ workspace, version, git SHA, config, inputs, outputs
```

### 7. Version Tags & Pruning

ML experiments generate many versions, most of which are failed attempts. Tags and pruning help manage this:

**Tags** mark significant versions with memorable names:
```
# Mark a milestone (can be applied retroactively to any version)
manage_versions(workspace="lstm_baseline", action="tag", version="v24", tag="baseline-working")
manage_versions(workspace="lstm_baseline", action="tag", version="v47", tag="best-model")

# List all tags for a workspace
manage_versions(workspace="lstm_baseline", action="list")
→ [{"version": "v24", "tag_name": "baseline-working"}, ...]

# Remove a tag
manage_versions(workspace="lstm_baseline", action="untag", tag="baseline-working")
```

**Pruning** hides noise versions while preserving audit trail:
```
# Prune a single version (fails if tagged - tags are protected)
manage_versions(workspace="lstm_baseline", action="prune", version="v5", reason="Failed experiment")

# Prune a range of failed experiments
manage_versions(workspace="lstm_baseline", action="prune", 
                from_version="v1", to_version="v23",
                reason="All early experiments before baseline")

# Restore pruned versions if needed
manage_versions(workspace="lstm_baseline", action="unprune", version="v5")
manage_versions(workspace="lstm_baseline", action="unprune", from_version="v1", to_version="v10")
```

**Key behaviors:**
- Tagged versions are **protected** and cannot be pruned
- Pruned versions don't appear in `status()` or STATE.md
- Version numbering continues unaffected (v1...v50 pruned, next is still v51)
- Pruning is **reversible** via unprune

## Tool Reference

### Workspace Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `status()` | Orientation - slots, jobs, sources | None |
| `create_workspace()` | New experiment | name, goal |
| `mount()` | Activate workspace in slot | slot, workspace |
| `hibernate()` | Deactivate (auto-saves) | slot, reason |
| `save_version()` | Create version save point | slot, message |
| `inspect_workspace()` | Workspace details + pipeline | name |
| `diff()` | Show uncommitted changes | target, against |
| `rollback()` | Revert to version | slot, version, reason |
| `delete_workspace()` | Remove workspace | workspace, reason |

### Execution

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `run()` | Execute stages (with pre-run review) | workspace, stages, reason |
| `inspect_run()` | Run dashboard & details | run_id, include |
| `logs()` | Container logs | run_id, tail, since |
| `cancel()` | Stop run | run_id, reason |
| `list_runs()` | Query runs | workspace, stage, status |
| `mark_outcome()` | Classify result | run_id, outcome |

### Version Management (Tags & Pruning)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `manage_versions()` | Unified tagging, pruning, listing | action, workspace, version, tag, from/to_version |

### Data Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `register_source()` | Register data source | name, gcs_path, metadata |
| `manage_sources()` | List, Get, Delete, Lineage | action, name, metadata |
| `promote_artifact()` | Stage output → source | job_id, output_name |

### Utility

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `initialize_project()` | New Goldfish project | project_name, project_root |
| `reload_config()` | Hot-reload goldfish.yaml | None |
| `validate_config()` | Dry-run validation | workspace |
| `log_thought()` | Record reasoning | thought |
| `manage_patterns()` | Manage SVS failure patterns | action, pattern_id |
| `search_goldfish_logs()` | LogsQL search | query |

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

### Pattern: Iterative Experimentation with Tags

```
1. create_workspace("exp_v1", "Baseline LSTM")
2. mount("w1", "exp_v1")
3. Edit code, run, analyze          # Creates v1, v2, v3... (many failures)
4. Finally working!
5. manage_versions(workspace="exp_v1", action="tag", version="v24", tag="baseline-working")
6. Continue experimenting           # Creates v25, v26... (more failures)
7. manage_versions(workspace="exp_v1", action="tag", version="v47", tag="best-model")
8. manage_versions(workspace="exp_v1", action="prune", 
                   from_version="v1", to_version="v23", 
                   reason="Cleanup early failures")
9. hibernate("w1", "Completed baseline")
```

### Pattern: Debug Failed Run

```
1. list_runs(status="failed")
2. inspect_run(run_id)             # See error, dashboard, trends
3. logs(run_id, tail=500)          # Full error trace
4. inspect_run(run_id, include=["provenance"]) # What inputs were used
5. Fix code, run again
```

### Pattern: Reproduce Past Result

```
1. inspect_run(stage_run_id, include=["provenance"])
   → Returns: workspace, version, git_sha, config, inputs

2. rollback(slot, version)  # Restore exact code state

3. run(workspace, stages, config_override)  # Re-run with same config
```

## Audit & Context Recovery

Goldfish maintains full audit trails:

```
log_thought("Switching to attention mechanism because...")

status()
→ Shows recent audit history
```

## Troubleshooting & Recovery

### SVS Pre-Run Blocks
If `run()` returns a `BLOCKED` status:
1. **Examine Findings**: Read the error messages in the tool output. They pin-point specific lines and logic flaws.
2. **Apply Fixes**: Edit the code or config in your workspace slot.
3. **Re-run**: Simply call `run()` again.
4. **Bypass (Caution)**: If a finding is truly a false positive, you can use `run(..., skip_review=True)` to bypass the AI review for that specific execution. Only use this if you are certain the code is safe.

### Long-Running Job Failures
- **Timeout**: If a job is terminated with `TIMEOUT`, increase the `timeout` in `goldfish.yaml` (global) or the `hints.timeout` in the stage config (per-stage).
- **Heartbeat Stale**: If the logs show "Supervisor: Heartbeat stale", your code is not calling `heartbeat()` frequently enough. Ensure it's called at least every 10 minutes (default) or increase `compute.heartbeat_timeout_seconds` in the stage config.

## Common Failure Patterns (Knowledge Base)

Goldfish auto-extracts failure patterns to prevent regression. Be aware of these common ML anti-patterns:

- **Silent Feature Degradation**: Picking up "junk" signals (like sine waves from a test generator) instead of real market features. Goldfish flags this via low entropy checks.
- **Label/Token Desynchronization**: In NLP/Time-series BPE, labels must be updated when tokens are merged. Failure to do so results in silent MI gating failure.
- **Horizon Mismatch**: Using 5-minute forward labels to mine patterns that only exist at 1Hz. Results in extremely low Mutual Information (MI).
- **Tool Assumption Mismatch**: Feeding dense labels to a tool expecting sequence-level labels. Many tools silently fall back to "dumb" modes when input shapes don't match.

## Best Practices

1. **Always provide clear goals** when creating workspaces
2. **Save versions frequently** with descriptive messages
3. **Tag significant milestones** (e.g., "first-working", "best-model", "submitted")
4. **Prune failed experiments** to reduce clutter after reaching milestones
5. **Use descriptive stage names** that reflect the operation
6. **Register datasets** before referencing in pipelines
7. **Check status()** after context recovery
8. **Use log_thought()** to document decisions
9. **Monitor long runs** with logs() and list_runs()
10. **Provide structured reasons** for runs with hypothesis and approach