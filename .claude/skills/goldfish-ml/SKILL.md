---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill for workspace management, pipeline execution, data registry operations, and provenance tracking. Goldfish provides a suite of master tools for efficient ML workflows.
---

# Goldfish ML

This skill enables effective use of Goldfish ML, an MCP server that transforms Claude into an ML experimentation agent with full provenance tracking, reproducibility, and infrastructure abstraction.

## Core Mental Model

Goldfish manages ML experiments through **seven key abstractions** (with a new Experiment Record layer):

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLDFISH ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────┤
│  WORKSPACE          RECORD            PIPELINE                   │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ w1/     │──────▶│ r123    │       │ stages:             │    │
│  │ (slot)  │       │ (run)   │       │   - preprocess      │    │
│  └─────────┘       └─────────┘       │   - train           │    │
│       │                              │   - evaluate        │    │
│       ▼                              └─────────────────────┘    │
│  VERSION (v1)      SIGNAL            PROFILE                    │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ train.py│──────▶│ features│       │ h100-spot           │    │
│  │ (module)│       │ (npy)   │       │ a100-on-demand      │    │
│  └─────────┘       └─────────┘       └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key invariants:**
- All infrastructure (Docker, GCS, GCE) is hidden from Claude
- User workspace is plain files (no `.git`) - all versioning handled internally
- Every `run()` creates a **record** and a version BEFORE execution (100% provenance)
- `run()` requires `results_spec` (structured + verbose context)
- Terminal runs must be finalized with `finalize_run()` before new runs start
- Signals connect stages with typed data flow
- **Pre-run review**: The configured SVS agent reviews your code before execution to catch bugs early
- **Experiment Records** hide the run/version split in normal UX (use record_id or tags)

**CRITICAL: Never use git directly.**
Goldfish manages all version control internally via a hidden dev repo. **Never run git commands** on the user's project directory or the dev repo (`*-dev/`). Direct git operations will corrupt Goldfish's state and break provenance tracking. Use Goldfish tools instead:
- `save_version()` instead of `git commit`
- `rollback()` instead of `git reset`
- `diff()` instead of `git diff`
- `manage_versions()` for tagging and history

## Workflow Decision Tree

```
START: What task?
  │
  ├─▶ "First time / Need orientation"
  │     └─▶ status() or dashboard() → See slots, active jobs, recent outcomes
  │
  ├─▶ "Start new experiment"
  │     └─▶ create_workspace() → mount() → Edit files → run(results_spec=...) → finalize_run()
  │
  ├─▶ "Continue existing work"
  │     └─▶ status() → mount(workspace, slot, reason) → Edit → run(results_spec=...) → finalize_run()
  │
  ├─▶ "Run ML training"
  │     └─▶ run(workspace, stages=["train"], results_spec=...) → finalize_run()
  │
  ├─▶ "Check run status"
  │     └─▶ list_history() → inspect_record(record_id) → inspect_run/run logs (infra)
  │
  ├─▶ "Manage data"
  │     └─▶ register_source() or manage_sources(action="list") or manage_sources(action="get", name="...")
  │
  ├─▶ "Track lineage"
  │     └─▶ get_lineage(run_id, direction) → inspect_run(include=["provenance"])
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)

## Experiment Memory (New)

Goldfish now treats **experiment records** as the primary UX layer.

- **records** = runs or checkpoints
- **results_spec** is required at `run()` time (structured + verbose)
- **results_auto** is extracted after completion
- **results_final** is set by `finalize_run()` and is authoritative
- **ML outcome** (success/partial/miss/unknown) is only set on finalization
- **Infra outcome** (completed/preempted/crashed/canceled) is tracked separately

Use `list_history()` and `inspect_record()` for experiment memory. Use `inspect_run()` for low-level infra details (logs, SVS, provenance).
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
   create_workspace(name="lstm_baseline", goal="Train LSTM for price prediction", reason="Starting new baseline experiment")

2. Mount to edit slot
   mount(workspace="lstm_baseline", slot="w1", reason="Begin preprocessing module development")

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

4. Run the pipeline (with results_spec for experiment tracking)
   run("w1", reason={
       "description": "Baseline LSTM training",
       "hypothesis": "LSTM should achieve 85%+ accuracy"
   }, results_spec={
       "primary_metric": "dir_acc_binary",
       "direction": "maximize",
       "min_value": 0.60,
       "goal_value": 0.63,
       "dataset_split": "val",
       "tolerance": 0.003,
       "context": "Baseline LSTM, 25M params. Focus on directional accuracy."
   })

5. Finalize results (required once run is terminal)
   finalize_run("stage-abc123", {
       "primary_metric": "dir_acc_binary",
       "direction": "maximize",
       "value": 0.631,
       "dataset_split": "val",
       "ml_outcome": "success",
       "notes": "Achieved target; preempted but results are valid."
   })
```

### SVS: Continuous Quality Oversight

SVS (Semantic Validation System) provides three layers of protection:

**Run Command Visibility:** All SVS phases see the full run command including `config_override` and `inputs_override`. This prevents false positives when runtime overrides address issues flagged in static config.

#### Layer 1: Pre-Run AI Review (Automatic)
Before execution, an AI reviews code/config for logic errors, undefined variables, and anti-patterns.
- **Blocked**: High-confidence errors found. Fix code and re-run.
- **Warned**: Potential issues noted (e.g., no validation split). Proceed with caution.
- **Bypass**: Use `run(..., skip_review=True)` if review is blocking safe code.

#### Layer 2: During-Run AI Monitoring
A background monitor reviews metrics (`.goldfish/metrics.jsonl`) and runtime logs (`.goldfish/logs.txt`) every 5 minutes.
- **Anomalies**: Detects diverging loss, exploding gradients, or stalled progress.
- **Early Stop**: Can terminate runs early if `ai_during_run_auto_stop: true` in config.
- **Usage**: Check progress with `inspect_run(run_id)` (infra), and `inspect_record(record_id)` for results.

#### Layer 3: Output Contract Enforcement
Enforces schema contracts (shape, dtype) and computes output stats (entropy, null ratio).
- **Silent Failures**: Catches mode collapse (low entropy) or data corruption (NaN spikes).
- **View**: Findings appear in `inspect_run(include=["svs"])`.

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

Stage modules follow a consistent pattern. **Using `goldfish.io` and the Metrics API is MANDATORY for observability and monitoring to function.**

```python
# modules/train.py
from goldfish.io import load_input, save_output, runtime_log, should_stop

def main():
    # MANDATORY: Use goldfish.io for ALL signal I/O
    features = load_input("features")

    for epoch in range(epochs):
        # 1. Logic
        # ...

        # 2. Monitoring support (MANDATORY for AI oversight)
        runtime_log(f"Epoch {epoch} loss: {l:.4f}")
        if should_stop():
             print("Early stop requested by SVS")
             break

    # MANDATORY: Use save_output for persistent artifacts
    save_output("model", model_dir)
```

### 3.1 Checkpoint API (Resume on Preemption)

For preemptible/spot instances that can be terminated with ~30 seconds notice, use the **Checkpoint API** for immediate GCS upload:

```python
# modules/train.py
from goldfish.io import (
    load_input, save_output, save_checkpoint, load_checkpoint, list_checkpoints
)

def main():
    features = load_input("features")

    # Try to resume from previous checkpoint
    ckpt = load_checkpoint("training_state")
    if ckpt:
        state = torch.load(ckpt)
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        start_step = state["step"]
        print(f"Resuming from step {start_step}")
    else:
        start_step = 0

    for step in range(start_step, total_steps):
        train_step(model, optimizer)

        # Save checkpoint every 1000 steps (IMMEDIATE GCS upload)
        if step % 1000 == 0:
            save_checkpoint("training_state", {
                "step": step,
                "model": model.state_dict(),
                "optimizer": optimizer.state_dict(),
            }, step=step)

            # Also checkpoint model directory
            save_checkpoint("model", model_dir, step=step)

    # Final output (batched at stage completion)
    save_output("model", model_dir)
```

**Key differences:**

| Function | Upload Timing | Use Case |
|----------|---------------|----------|
| `save_output()` | Batched at stage completion | Final artifacts |
| `save_checkpoint()` | **Immediate** GCS upload | Preemption recovery |

**Checkpoint API:**

```python
# Save checkpoint (immediate upload)
save_checkpoint(name, data, step=None, local_ok=False)
# - name: checkpoint name (e.g., "model", "optimizer")
# - data: Path, numpy array, or any picklable object
# - step: optional step for versioned checkpoints
# - local_ok: allow local-only save when GCS not configured

# Load checkpoint (for resume)
path = load_checkpoint(name, step=None, run_id=None)
# - Returns Path to checkpoint, or None if not found
# - run_id: load from a different run (for cross-run resume)

# List available checkpoints
checkpoints = list_checkpoints(run_id=None)
# Returns: {"model": {"steps": [1000, 2000, 3000]}, ...}
```

**Checkpoint storage:** `gs://{bucket}/checkpoints/{run_id}/{name}/step_{step}/`

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
  and won't run on SIGKILL/crash.

### 4.1 Rust Stage Implementation (Alternative)

For performance-critical stages, you can use the **Rust SDK** (`goldfish-rust`) instead of Python. The API mirrors the Python version and **Rust stages are first-class** (can be mixed with Python stages in the same pipeline).

**Add to Cargo.toml:**
```toml
[dependencies]
goldfish-rust = { path = "../goldfish-rust" }  # or from registry when published
```

**Pipeline.yaml (Rust stage):**
```yaml
stages:
  - name: encode
    runtime: rust   # per-stage runtime (can interleave with python)
```

**Basic stage pattern:**
```rust
// modules/train.rs
use goldfish_rust::{init, load_input, save_output, OutputData, GoldfishError};
use goldfish_rust::{runtime_log, heartbeat, log_metric, log_artifact, should_stop};

fn main() -> Result<(), GoldfishError> {
    let _guard = init();  // RAII: finalizes SVS stats on drop (if enabled)

    // Load inputs (returns OutputData enum)
    let features = load_input("features", None)?;
    let arr = features.into_tensor_f32().expect("expected f32 tensor");

    for epoch in 0..epochs {
        // Training logic...

        // Monitoring (MANDATORY for AI oversight)
        runtime_log(&format!("Epoch {} loss: {:.4}", epoch, loss), "INFO");
        log_metric("train/loss", loss, Some(epoch as i64));
        heartbeat(Some(&format!("Epoch {}/{}", epoch, epochs)), false);

        if should_stop() {
            println!("Early stop requested by SVS");
            break;
        }
    }

    // Save outputs
    save_output("model", OutputData::Path(model_dir), false)?;

    // Log artifact (path relative to outputs dir)
    log_artifact("checkpoint", "model/checkpoint.pt");
    Ok(())
}
```

**Rust source convention (symmetric to Python):**
- Python stage → `modules/<stage>.py`
- Rust stage → `modules/<stage>.rs` (**required**)
  - Goldfish compiles this at runtime and runs the resulting binary.
  - Optional: `modules/<stage>.Cargo.toml` to add dependencies or customize build.
  - Custom base images must include `cargo`; Goldfish base images already do.

**Key Rust API functions:**

| Function | Purpose |
|----------|---------|
| `init()` | Returns RAII guard that finalizes SVS stats on drop |
| `load_input(name, format)` | Load input signal → `OutputData` (does NOT auto-load .npz) |
| `save_output(name, data, artifact)` | Save output with schema validation |
| `runtime_log(msg, level)` | Structured log for AI monitoring |
| `heartbeat(msg, force)` | Prevent inactivity timeout |
| `log_metric(name, value, step)` | Record scalar metric |
| `log_metrics(map, step)` | Record multiple metrics |
| `log_artifact(name, path)` | Record artifact (path relative to outputs dir) |
| `should_stop()` | Check if SVS requested early termination |

**SVS stats:** Stats are computed only when `GOLDFISH_SVS_STATS_ENABLED=true`. The `init()` guard finalizes stats on drop when enabled.

**OutputData variants:**
```rust
OutputData::TensorF32(ArrayD<f32>)
OutputData::TensorF64(ArrayD<f64>)
OutputData::TensorI64(ArrayD<i64>)
OutputData::TensorI32(ArrayD<i32>)
OutputData::TensorU8(ArrayD<u8>)
OutputData::Json(serde_json::Value)
OutputData::Tabular(polars::DataFrame)
OutputData::Path(PathBuf)
OutputData::MultiTensor(HashMap<String, OutputData>)
```

**Multi-array autosave:** `OutputData::MultiTensor` is auto-saved only when the output format is `directory` (or default `file`). Each array is written as `outputs/<signal>/<array_name>.npy`. Stats are recorded as `signal.array` in `svs_stats.json`. Note: `save_output` does **not** write `.npz` files.

**save_output + Path:** `OutputData::Path` with schema validation only works for `directory` outputs with `arrays` schema—Goldfish loads `.npz`/`.npy` files inside the directory to validate. For other schema types, use in-memory `OutputData` variants.

**Entrypoints:** Rust stages do **not** need an explicit `entrypoints/<stage>` file. Goldfish compiles `modules/<stage>.rs` and runs the compiled binary under `entrypoints/` automatically. Use `entrypoint:` in pipeline.yaml only if you need a custom binary path.

**Type extraction:**
```rust
// Borrow (no move)
if let Some(arr) = data.as_tensor_f32() { ... }

// Take ownership (returns Err(self) if wrong type)
let arr = data.into_tensor_f32().expect("expected f32");
```

**NPZ loading:** `load_input` does **not** auto-load `.npz` files. Use explicit functions:
```rust
use goldfish_rust::{load_npz, load_npz_array};

let npz = load_npz("model.npz")?;
let weights = npz.get("weights");  // borrow
let bias = npz.take("bias");       // take ownership

// Or load single array directly
let weights = load_npz_array("model.npz", "weights")?;
```

**Security:** The Rust SDK includes path traversal protection, NPY header size limits (1MB), and NPZ decompression bomb protection (1GB per entry).

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
1. Get overview
   dashboard() → orientation on active/failed runs
   get_experiment_context("w1") → baseline + trends + pending finalizations
   list_unfinalized_runs("w1") → runs blocked on finalization

2. Experiment status (PRIMARY)
   list_history("w1", sort_by="created") → recent records
   inspect_record(record_id, include=["results", "comparison"]) → results + diffs

3. Infra details (SECONDARY)
   inspect_run(run_id) → infra status, logs, SVS findings
   logs(run_id, follow=True) → stream new logs

4. Cancel if needed
   cancel(run_id, reason="Wrong hyperparameters")
```

### 6. Lineage & Provenance

```
# Experiment-level context
list_history("baseline")  # Returns reason, primary_metric, age for each record
inspect_record(record_id, include=["results", "comparison"])

# Track data dependencies (essential for understanding experiment flow)
get_lineage("stage-abc123")  # Who consumed my outputs?
get_lineage("stage-abc123", direction="upstream")  # Where did my inputs come from?

# Infra-level provenance
inspect_run(run_id, include=["provenance"])  # Includes reason field

# Debugging: map record/tag to GCS/logs
get_debug_info("@best-v1", workspace="baseline")
```

### 7. Version Management (Tags & Pruning)

```
# Tag a run (also tags its version)
tag_record("stage-abc123", "best-v1")

# Tag a checkpoint (get record_id via list_history)
list_history("exp_v1", record_type="checkpoint")
tag_record("01HXYZ...", "checkpoint-1")

# Clean up failed experiments
manage_versions(
    workspace="exp_v1",
    action="prune",
    from_version="v1",
    to_version="v23",
    reason="Cleanup noise",
)

# List version history (includes tags + pruned info)
manage_versions(workspace="exp_v1", action="list")
```

**Key behaviors:**

- Tagging a run via `tag_record()` also creates a version tag with the same name
- Tagged versions are **protected** and cannot be pruned
- Pruned versions don't appear in `status()` or STATE.md
- Version numbering continues unaffected
- Pruning is **reversible** via unprune

### 8. Managing Docker Images

Goldfish manages **two layers** of Docker images:
1. **Base images** (`goldfish-base-gpu`, `goldfish-base-cpu`) - foundation with ML libraries + FlashAttention-3
2. **Project images** (`{project}-gpu`, `{project}-cpu`) - extend base with project-specific packages

**Image version resolution:**
- **Base images**: config → DB → default (v10). Always have a fallback since Goldfish ships them.
- **Project images**: config → DB → **None**. No default - they're user-built, not Goldfish-shipped.

**Database is required for project image builds** - version tracking ensures reproducibility.

**Check image status (both layers)**
```python
manage_base_images(action="list")
# Returns: base_images (goldfish-base-*) and project_images ({project}-*)
# Shows current version from database for each image type
```

**Build/push base images (one-time setup or updates)**

Two build backends are available:
- `backend="local"` (default): Uses local Docker daemon
- `backend="cloud"`: Uses Google Cloud Build (recommended for GPU images - faster, doesn't tie up local machine)

```python
# Build goldfish base GPU image on Cloud Build (recommended, ~15-20 min)
result = manage_base_images(action="build", image_type="gpu", target="base", backend="cloud")
# Returns immediately with build_id, poll with get_build_status(result["build_id"])

# Or build locally (ties up machine, but works without GCP)
manage_base_images(action="build", image_type="gpu", target="base", wait=True)

# Push to Artifact Registry
manage_base_images(action="push", image_type="gpu", target="base")

# IMPORTANT: After pushing a new base image version, register it in the database:
# This updates the project's database to use the new version
manage_base_images(action="set_version", image_type="gpu", version="v11")

# Build goldfish base CPU image (~5 min)
manage_base_images(action="build", image_type="cpu", target="base", backend="cloud")
# or wait=True for local
```

**Version management:**
```python
# List version history for an image type
manage_base_images(action="list_versions", image_type="gpu")
# Returns: [{version: "v11", is_current: true, created_at: ...}, ...]

# Set a specific version as current (e.g., rollback to older version)
manage_base_images(action="set_version", image_type="gpu", version="v10")

# Get next version number for a new build
manage_base_images(action="next_version", image_type="gpu")
# Returns: "v12" (auto-increments from current max)
```

**Customize project images (optional)**

Add packages via config:
```yaml
# goldfish.yaml
docker:
  extra_packages:
    gpu:
      - triton
    cpu:
      - lightgbm
```

Or create custom Dockerfile in project root:
```dockerfile
# Dockerfile.gpu
FROM goldfish-base-gpu:v11
RUN pip install my-custom-package
```

**Build/push project images**
```python
# Build project image (uses base + extra_packages/Dockerfile)
manage_base_images(action="build", image_type="gpu", target="project", wait=True)
manage_base_images(action="push", image_type="gpu", target="project")
```

**Key points:**
- Base GPU image: CUDA 12.8 + PyTorch 2.9.1 + FlashAttention-3 + numpy/pandas/scikit-learn
- Base CPU image: PyTorch (CPU) + numpy/pandas/scikit-learn
- **Base image versions**: tracked in DB, fallback to v10 if not set
- **Project image versions**: tracked in DB, **no default** - must build before running
- `target="base"` builds goldfish-base-*, `target="project"` (default) builds {project}-*
- `backend="cloud"` recommended for GPU builds (Cloud Build, doesn't tie up local machine)
- Base images must exist in registry before project images can be built
- Cloud Build requires `gce.project_id` in goldfish.yaml + Artifact Registry write permission for Cloud Build service account
- After building/pushing a new base image, use `action="set_version"` to register it in the database

## Master Tool Reference







### Workspace







| Tool | Purpose | Key Parameters |



|------|---------|----------------|



| `status()` | Orientation - slots, jobs, STATE.md | None |



| `dashboard()` | Alerts, blocks, active runs with reason/age | None |



| `create_workspace()` | New experiment from main | name, goal, reason |



| `mount()` | Activate workspace in slot | workspace, slot, reason |



| `hibernate()` | Deactivate (auto-saves) | slot, reason |



| `save_version()` | Create version save point | slot, message |



| `inspect_workspace()` | Master view of workspace history/DAG | name |



| `diff()` | Compare slot, workspace, or versions | target, against |



| `rollback()` | Revert slot to version | slot, version, reason |



| `delete_workspace()` | Remove workspace (irreversible) | workspace, reason |







### Execution

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `run()` | Execute stages (finalization gate enforced). **Note:** `results_spec` is REQUIRED (not optional) | workspace, stages, results_spec, reason |
| `finalize_run()` | Finalize ML results and outcome (authoritative) | record_or_run_id, results |
| `list_history()` | Experiment records with reason, metric, age | workspace, tagged, stage, metric, min_value, finalized_only: bool = False |
| `inspect_record()` | Record details (results, comparison, tags) | ref, include, workspace |
| `tag_record()` | Tag a run/checkpoint (runs also tag version) | ref, tag |
| `list_unfinalized_runs()` | Runs needing finalization | workspace |
| `get_experiment_context()` | Best run + pending finalizations + trends | workspace |
| `get_debug_info()` | Infra IDs + GCS paths for a record | ref, workspace |
| `inspect_run()` | Infra status, SVS, provenance, reason | run_id, include |
| `get_lineage()` | Track upstream/downstream run dependencies | run_id, direction |
| `logs()` | Container logs (supports follow mode) | run_id, tail, follow |
| `cancel()` | Stop a running stage | run_id, reason |
| `save_results_spec()` | Pre-save results_spec for a run | workspace, stage, results_spec |

### Version Management







| Tool | Purpose | Key Parameters |



|------|---------|----------------|



| `manage_versions()` | Unified tagging, pruning, listing | action, workspace, version, tag |







### Data Management







| Tool | Purpose | Key Parameters |



|------|---------|----------------|



| `register_source()` | Register external GCS data | name, gcs_path, metadata |



| `manage_sources()` | Registry management (list, get, lineage) | action, name |



| `promote_artifact()` | Stage output → reusable source | job_id, output_name, metadata |

### Infrastructure

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `manage_base_images()` | Docker base image management. **Note:** `image_type` is required for all actions except 'list' and 'check' | action, image_type, target, backend, version |
| `get_build_status()` | Poll image build progress | build_id |

**`manage_base_images` actions:**
- `action="list"` - show image status (both base and project layers)
- `action="build"` - build an image
- `action="push"` - push image to Artifact Registry
- `action="set_version"` - register/set current base image version in database
- `action="list_versions"` - list version history for an image type
- `action="next_version"` - get next auto-incremented version number

**`manage_base_images` parameters:**
- `target="base"` - goldfish-base-{cpu,gpu} images (foundation)
- `target="project"` (default) - {project}-{cpu,gpu} images (customized)
- `backend="local"` (default) - build using local Docker daemon
- `backend="cloud"` - build using Google Cloud Build (recommended for GPU images)
- `version` - version string for set_version action (e.g., "v11")

### Backup

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `list_backups()` | List database backups | tier, include_deleted |
| `create_backup()` | Create manual backup | trigger, details |
| `cleanup_backups()` | Remove old backups per retention policy | None |
| `get_backup_status()` | Current backup configuration and health | None |

### Utility







| Tool | Purpose | Key Parameters |



|------|---------|----------------|



| `validate_config()` | Check YAML files for errors/typos | workspace |



| `log_thought()` | Record reasoning in audit/STATE.md | thought, workspace, run_id |



| `get_workspace_thoughts()` | Retrieve thoughts for a workspace | workspace |



| `manage_patterns()` | AI failure pattern knowledge base | action, pattern_id |



| `search_goldfish_logs()` | LogsQL search via VictoriaLogs | query |



| `initialize_project()` | Setup new Goldfish project | project_name, project_root |



| `reload_config()` | Hot-reload goldfish.yaml | None |
| `get_audit_log()` | Get recent audit trail entries | limit, workspace |







## Troubleshooting & Recovery

### Finalization Gate

If `run()` is blocked due to unfinalized runs:
- Call `list_unfinalized_runs(workspace)` to see pending records.
- Use `inspect_record(record_id, include=["results", "comparison"])` to review auto-results.
- Call `finalize_run(record_or_run_id, results)` to unlock new runs.



### SVS Blocks

If `run()` returns a `BLOCKED` status: Fix the reported code/config errors in your workspace slot and call `run()` again. Use `skip_review=True` ONLY as a last resort for safe code misidentified as faulty.



### Long-Running Job Failures

- **Heartbeat Stale**: Ensure your code calls `heartbeat()` (default: 10 min timeout).

- **SVS Stop**: Check `inspect_run(include=["svs"])` to see why background monitor stopped the run.



## Common Failure Patterns (Knowledge Base)



Goldfish auto-extracts failure patterns to prevent regression. Be aware of these common ML anti-patterns:



- **Silent Feature Degradation**: Picking up "junk" signals (like sine waves from a test generator) instead of real market features. Goldfish flags this via low entropy checks.

- **Label/Token Desynchronization**: In NLP/Time-series BPE, labels must be updated when tokens are merged. Failure to do so results in silent MI gating failure.

- **Horizon Mismatch**: Using 5-minute forward labels to mine patterns that only exist at 1Hz. Results in extremely low Mutual Information (MI).

- **Tool Assumption Mismatch**: Feeding dense labels to a tool expecting sequence-level labels. Many tools silently fall back to "dumb" modes when input shapes don't match.



## Best Practices

1. **Always provide clear goals** when creating workspaces
2. **Always provide results_spec** for every `run()` (required)
3. **Finalize every terminal run** with `finalize_run()` before starting new runs
4. **Tag significant milestones** with `tag_record()` (also tags versions)
5. **Use list_history/inspect_record** as primary experiment memory; use `inspect_run`/`logs` for infra
6. **Save versions for checkpoints** and **prune noise** with `manage_versions(action="prune")`
7. **Register external data** using `register_source()` before referencing in pipelines
8. **Check status()/dashboard() + get_experiment_context()** after context recovery
9. **Use log_thought()** to document decisions and conclusions
10. **Provide structured reasons** for runs with hypothesis and approach
11. **MANDATORY: Always use `goldfish.io` and `goldfish.metrics`** for I/O and telemetry
