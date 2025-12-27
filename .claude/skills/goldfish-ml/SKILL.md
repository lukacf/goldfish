---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill for workspace management, pipeline execution, data registry operations, and provenance tracking. Goldfish provides 24 master tools and a real-time sync bus for high-frequency ML training.
---

# Goldfish ML

Goldfish is an AI-native ML experimentation platform. It treats Claude as a "Principal Researcher" who manages workspaces (isolated branches), executes pipeline stages (Docker containers), and tracks 100% of experiment provenance.

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

**Key Invariants:**
- **Zero-Infra:** Docker, GCS, and GCE details are hidden. Claude works only with files and tools.
- **Immutable Provenance:** Every `run()` creates a version *before* execution. You can `rollback()` to any run's exact code state.
- **Inspect-First:** Use `inspect_run()` for real-time dashboards and `inspect_workspace()` for global state.
- **Signal Signaling:** `inspect_run()` sends a low-latency metadata signal to containers to trigger an immediate "Overdrive" sync of logs and metrics.

---

## Workflow Decision Tree

```
START: What task?
  │
  ├─▶ "First time / Need orientation"
  │     └─▶ status() → See slots, active jobs, recent audit trail.
  │
  ├─▶ "Start new experiment"
  │     └─▶ create_workspace() → mount() → Edit files → run()
  │
  ├─▶ "Inspect existing workspace"
  │     └─▶ inspect_workspace(name) → Goal, Pipeline, Lineage, Tags.
  │
  ├─▶ "Check run progress/health"
  │     └─▶ inspect_run(run_id) → Dashboard (Trends ↑↓), GPU Health, Provenance.
  │
  ├─▶ "Deep debug failure"
  │     └─▶ inspect_run(run_id, include=["manifest", "svs"]) + logs(run_id).
  │
  ├─▶ "Manage data sources"
  │     └─▶ manage_sources(action="list|get|lineage") or register_source()
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)
```

---

## 1. Stage Implementation Pattern (Inside Container)

Stage modules use `goldfish.io` for data and `goldfish.metrics` for tracking.

```python
# modules/train.py
from goldfish.io import load_input, save_output, heartbeat
from goldfish.metrics import log_metric, log_metrics, finish

def main():
    # 1. Load inputs (NumPy arrays or file paths)
    features = load_input("features") 

    # 2. Training loop
    for epoch in range(10):
        # HEARTBEAT: Call this every ~5-10 mins in long jobs to prevent auto-kill.
        heartbeat(f"Training epoch {epoch}") 
        
        loss, acc = train_step(features)
        
        # LOGGING: Data here feeds the 'inspect_run' Real-time Dashboard.
        log_metrics({"loss": loss, "accuracy": acc}, step=epoch)

    # 3. Save outputs (to GCS/Storage via /mnt/outputs/)
    save_output("model", model_dir)
    
    # 4. Finalize metrics (flush to storage)
    finish()

if __name__ == "__main__":
    main()
```

---

## 2. Semantic Validation System (SVS)

SVS is the "immune system" of Goldfish. It operates in three phases:

### Phase 1: Pre-Run AI Review (Automatic)
Before `run()` launches, an AI agent reviews your code for syntax, logic, and hypothesis coherence.
- **ERROR**: Blocks execution.
- **WARNING**: Logged but allowed.

### Phase 2: During-Run Mechanistic Checks
- **NaN/Inf detection**: Blocks early if loss explodes.
- **Divergence**: Warns if loss spikes >10x from baseline.

### Phase 3: Post-Run Output Stats
Goldfish computes stats (via reservoir sampling) for every output signal:
- **Entropy**: Shannon entropy (bits) to catch mode collapse or constant-value "sine wave" noise.
- **Null Ratio**: Fraction of NaN/None values to catch loading bugs.
- **Vocab Utilization**: % of embedding vocab indices used.

---

## 3. Data Source Metadata (Strict)

All new sources/artifacts MUST include **mandatory metadata**. Goldfish does not infer schema.

### Required Structure
```json
{
  "schema_version": 1,
  "description": "Descriptive text (min 20 chars)",
  "source": { 
    "format": "npy|npz|csv|file", 
    "size_bytes": 1234, 
    "created_at": "ISO-TS" 
  },
  "schema": {
    "kind": "tensor|tabular|file",
    "arrays": { // only for tensor
       "features": { "role": "features", "shape": [1000, 768], "dtype": "float32" }
    }
  }
}
```

---

## 4. Version Management (Tags & Pruning)

Iterative experiments create noise. Use this workflow to keep the workspace clean:

1. **Tag Milestones**: Mark working points using `manage_versions(action="tag", version="v24", tag="baseline")`.
2. **Prune Noise**: Clear intermediate failures with `manage_versions(action="prune", from_version="v1", to_version="v23")`.
3. **Protected Status**: Tagged versions are **protected** and cannot be pruned.

---

## 5. Master Tool Reference (The Goldfish 24)

| Category | Tool | Description |
| :--- | :--- | :--- |
| **Observation** | `status()` | Orient via slots, active jobs, and recent audit trail. |
| | `inspect_run()` | **Master Tool.** Synthesizes Dashboard + Trends + Health + Provenance. |
| | `inspect_workspace()` | Unified view of Goal + Pipeline + Lineage + Versions. |
| | `list_runs()` | Compact history of recent history. |
| **Execution** | `run()` | Launch stages with automated pre-run AI review. |
| | `cancel()` | Immediate termination of a running job. |
| | `mark_outcome()` | Semantic classification: `success` or `bad_results`. |
| | `logs()` | Raw text stream for deep debugging. |
| **Versioning** | `manage_versions()` | Unified interface for `tag`, `prune`, and `list` actions. |
| | `save_version()` | Immutable save point for current slot state. |
| | `rollback()` | Destructive revert to a previous version. |
| | `diff()` | Compare any two slots, versions, or workspaces. |
| **Data** | `manage_sources()` | Unified interface for `list`, `get`, `lineage`, and `update`. |
| | `register_source()` | Register external GCS data or local datasets. |
| | `promote_artifact()` | Turn a run output into a registered registry source. |
| **System** | `manage_patterns()` | Manage SVS failure knowledge base and AI reviews. |
| | `search_goldfish_logs()` | LogsQL search across all system components (incl. guide). |
| | `log_thought()` | Record reasoning for the project audit trail. |
| | `initialize_project()` | Setup new directory with Goldfish structure. |
| | `reload_config()` | Hot-reload `goldfish.yaml` without restarting server. |
| | `validate_config()` | Dry-run config validation for typos/schema errors. |

---

## 6. Common Patterns

### Reproduce a Past Result
1. `inspect_run(run_id, include=["provenance"])` → Get exact `version` (e.g., v12).
2. `rollback(slot="w2", version="v12")` → Reconstitute code state in slot.
3. `run("w2")` with original config overrides.

### Debugging Failed Training
1. `inspect_run(run_id)` → Check trends ↑↓ and SVS findings.
2. If Loss NaN: Check `inspect_run(..., include=["manifest"])` for learning rate.
3. `logs(run_id, tail=500)` → Look for stack trace.
4. `log_thought("Reducing LR to 1e-5 because...")`.

---

## 7. Resource Profiles (`compute.profile`)

| Profile | Hardware | Use Case |
|---------|----------|----------|
| `cpu-small` | 2 vCPU, 4GB | Light data processing / tests. |
| `cpu-large` | 8 vCPU, 32GB | Heavy preprocessing / indexing. |
| `h100-spot` | H100 GPU, Spot | Cost-effective heavy training. |
| `h100-on-demand` | H100 GPU | Guaranteed availability for long runs. |
| `a100-spot` | A100 GPU, Spot | Training alternative (lower cost). |

## Best Practices
1. **Never use raw `logs`** if `inspect_run` dashboard provides the answer.
2. **Tag Early**: Tag working versions before attempting risky refactors.
3. **Trigger Sync**: Calling `inspect_run` triggers an automatic "Overdrive" sync from the cloud container.
4. **Context Recovery**: Always run `status()` first when resuming a session.
