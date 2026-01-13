# New Experiment Model (Optimal Scope) - Spec

## Status
Draft for implementation. Reviewed by ML Claude (feedback incorporated).

## Goals
- Make experiment memory first-class (results, comparisons, summaries) rather than relying on manual thoughts.
- Eliminate run/version cognitive overhead in the UX while preserving internal correctness.
- Split infra outcomes from ML outcomes (preemption != ML failure).
- Require structured + verbose results spec at run time (LLM-friendly, mechanically validated).
- Strict finalization gate with parallel runs allowed.

## Non-Goals
- Removing versions or run IDs from internal storage.
- Replacing all legacy tools in one step (backward compatibility required).

---

## Core Concepts

### Experiment Record (user-facing)
A single user-facing entity representing either:
- a **run** (execution + results), or
- a **checkpoint** (snapshot without execution).

User-facing tools must operate on **records** instead of raw runs/versions.

Record properties (canonical):
- `record_id` (stable, human-facing; ULID or similar sortable id)
- `type`: `run | checkpoint`
- `workspace`
- `snapshot` (version id, e.g., v11)
- `stage` (run only)
- `created_at`
- `tags` (merged run tags + version tags)
- `results` (run only; auto + final)
- `run_id` (debug only; hidden by default)

### Infra Outcome vs ML Outcome
- Infra outcome: `completed | preempted | crashed | canceled | unknown`
- ML outcome: `success | partial | miss | unknown`

Infra outcome is auto-derived from execution state. ML outcome is unknown until finalization.

### Results Lifecycle
- `results_auto`: auto-extracted by Goldfish (SVS assisted, deterministic).
- `results_final`: authoritative, set by ML Claude via finalize.
- `results_status`: `missing | auto | finalized`

---

## Database Schema (Exact DDL)

**All new tables must be added to `db/schema.sql` with indexes.**

```sql
-- =============================
-- Experiment Records (user-facing)
-- =============================
CREATE TABLE IF NOT EXISTS experiment_records (
    record_id TEXT PRIMARY KEY,
    workspace_name TEXT NOT NULL,
    type TEXT NOT NULL,               -- run | checkpoint
    stage_run_id TEXT,                -- FK stage_runs (NULL for checkpoints)
    version TEXT NOT NULL,            -- FK workspace_versions
    created_at TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version)
);

CREATE INDEX IF NOT EXISTS idx_experiment_records_workspace
    ON experiment_records(workspace_name);
CREATE INDEX IF NOT EXISTS idx_experiment_records_version
    ON experiment_records(workspace_name, version);
CREATE INDEX IF NOT EXISTS idx_experiment_records_run
    ON experiment_records(stage_run_id);

-- =============================
-- Run Results (auto + final)
-- =============================
CREATE TABLE IF NOT EXISTS run_results (
    stage_run_id TEXT PRIMARY KEY,    -- FK stage_runs
    record_id TEXT NOT NULL,          -- FK experiment_records
    results_status TEXT NOT NULL,     -- missing | auto | finalized
    infra_outcome TEXT NOT NULL,      -- completed | preempted | crashed | canceled | unknown
    ml_outcome TEXT NOT NULL,         -- success | partial | miss | unknown
    results_auto TEXT,                -- JSON (immutable)
    results_final TEXT,               -- JSON (authoritative)
    comparison TEXT,                  -- JSON (computed at finalize)
    finalized_by TEXT,
    finalized_at TEXT,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_results_record
    ON run_results(record_id);
CREATE INDEX IF NOT EXISTS idx_run_results_status
    ON run_results(results_status);
CREATE INDEX IF NOT EXISTS idx_run_results_ml_outcome
    ON run_results(ml_outcome);

-- =============================
-- Run Results Spec (required at run time)
-- =============================
CREATE TABLE IF NOT EXISTS run_results_spec (
    stage_run_id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_results_spec_record
    ON run_results_spec(record_id);

-- =============================
-- Run Tags
-- =============================
CREATE TABLE IF NOT EXISTS run_tags (
    workspace_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_name, tag_name),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_tags_record
    ON run_tags(record_id);
```

Notes:
- Tag uniqueness per workspace must be enforced across both `run_tags` and `workspace_version_tags` in code.
- `record_id` should be ULID for lexicographic ordering.

---

## Tool Interfaces (Exact)

### run()
**Changed: requires `results_spec`.**

```
run(
  workspace: str,
  stages: list[str] | None = None,
  pipeline: str | None = None,
  config_override: dict | None = None,
  inputs_override: dict | None = None,
  reason: str | dict | None = None,
  results_spec: dict,              # REQUIRED
  experiment_group: str | None = None,
  wait: bool = False,
  dry_run: bool = False,
  skip_review: bool = False,
) -> dict
```

`results_spec` schema (required):
```
{
  "primary_metric": "dir_acc_binary",
  "direction": "maximize" | "minimize",
  "min_value": 0.60,
  "goal_value": 0.63,
  "dataset_split": "val" | "test" | "train" | "other",
  "tolerance": 0.003,
  "secondary_metrics": ["val_loss", "test_loss"],
  "baseline_run": "stage-b33b632e" | "@best-25m-63pct" | null,
  "failure_threshold": 0.55,
  "known_caveats": ["Small dataset, high variance", "First 5 epochs unstable"],
  "context": "Verbose LLM description (required)."
}
```

Notes:
- `context` is required with a minimum length.
- `baseline_run` can be a run/record id or a tag reference (`@tag`). Resolution happens at comparison time.
- `experiment_group` is optional (for filtering and summary grouping).

#### JSON Schema: `results_spec`
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "primary_metric",
    "direction",
    "min_value",
    "goal_value",
    "dataset_split",
    "tolerance",
    "context"
  ],
  "properties": {
    "primary_metric": { "type": "string", "minLength": 1 },
    "direction": { "type": "string", "enum": ["maximize", "minimize"] },
    "min_value": { "type": "number" },
    "goal_value": { "type": "number" },
    "dataset_split": { "type": "string", "enum": ["train", "val", "test", "other"] },
    "tolerance": { "type": "number", "minimum": 0 },
    "secondary_metrics": {
      "type": "array",
      "items": { "type": "string", "minLength": 1 }
    },
    "baseline_run": {
      "type": ["string", "null"],
      "description": "Run/record id or tag reference (e.g., \"@best-25m\")."
    },
    "failure_threshold": { "type": "number" },
    "known_caveats": {
      "type": "array",
      "items": { "type": "string", "minLength": 1 }
    },
    "context": { "type": "string", "minLength": 15 }
  }
}
```

### finalize_run()
Authoritative ML result finalization.

```
finalize_run(
  record_or_run_id: str,
  results: dict
) -> dict
```

`results` schema:
```
{
  "primary_metric": "dir_acc_binary",
  "direction": "maximize" | "minimize",
  "value": 0.631,
  "unit": "fraction" | "percent" | "loss" | null,
  "dataset_split": "val",
  "step": 19,
  "epoch": 19,
  "secondary": {"val_loss": 2.279},
  "termination": {"infra_outcome": "preempted"},
  "ml_outcome": "success" | "partial" | "miss" | "unknown",
  "notes": "Verbose rationale and interpretation."
}
```

Behavior:
- Writes `results_final` and `results_status=finalized`.
- Sets `ml_outcome` (authoritative).
- Preserves `results_auto` unchanged.
- Computes and stores `comparison` (see below).

#### JSON Schema: `finalize_run.results`
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "primary_metric",
    "direction",
    "value",
    "dataset_split",
    "ml_outcome",
    "notes"
  ],
  "properties": {
    "primary_metric": { "type": "string", "minLength": 1 },
    "direction": { "type": "string", "enum": ["maximize", "minimize"] },
    "value": { "type": "number" },
    "unit": { "type": ["string", "null"] },
    "dataset_split": { "type": "string", "enum": ["train", "val", "test", "other"] },
    "step": { "type": "integer", "minimum": 0 },
    "epoch": { "type": "integer", "minimum": 0 },
    "secondary": { "type": "object" },
    "termination": {
      "type": "object",
      "additionalProperties": false,
      "required": ["infra_outcome"],
      "properties": {
        "infra_outcome": {
          "type": "string",
          "enum": ["completed", "preempted", "crashed", "canceled", "unknown"]
        }
      }
    },
    "ml_outcome": { "type": "string", "enum": ["success", "partial", "miss", "unknown"] },
    "notes": { "type": "string", "minLength": 15 }
  }
}
```

### list_history()
Returns experiment records (runs + checkpoints), newest first.

```
list_history(
  workspace: str,
  type: "run" | "checkpoint" | null = null,
  stage: str | null = null,
  tagged: bool | str | null = null,  # true or specific tag
  metric: str | null = null,
  min_value: float | null = null,
  sort_by: "created" | "metric" = "created",
  desc: bool = true,
  include_pruned: bool = false,
  include_internal_ids: bool = false,
  limit: int = 50,
  offset: int = 0
) -> dict
```

`metric` queries use `results_final` if present, else `results_auto` if allowed.

### inspect_record()
Accepts `record_id`, tag, snapshot, or run_id.

```
inspect_record(ref: str, include: list[str] | None = None) -> dict
```

Include options:
- `results` (auto + final)
- `comparison`
- `tags` (merged)
- `manifest` (config/inputs)
- `provenance`
- `svs`
- `debug` (run_id, GCS path, instance)

### tag_record()
```
tag_record(ref: str, tag: str) -> dict
```
Behavior:
- If ref is run record: create run tag AND version tag with same name.
- If ref is checkpoint: create version tag only.
- Enforce tag uniqueness per workspace across run_tags and workspace_version_tags.

### get_debug_info()
```
get_debug_info(ref: str) -> dict
```
Returns run_id, GCS paths, instance name, log URIs.

### list_unfinalized_runs()
```
list_unfinalized_runs(workspace: str) -> dict
```
Returns terminal infra runs missing `results_final`.

---

## Automatic Comparison Block
Computed at finalization time and stored in `run_results.comparison`.

```
comparison = {
  "vs_best": {"record": "r157", "tag": "best-25m", "delta": -0.096},
  "vs_previous": {"record": "r156", "delta": -0.021},
  "config_diff": {
    "stage": ["train", "train_25m"],
    "encoding": ["1b", "2b_factorized"],
    "model": ["small", "small_rope"]
  }
}
```

Rules:
- `vs_best`: resolved using `baseline_run` if present, else best tagged record.
- `vs_previous`: last finalized run for same stage.
- `config_diff`: only include changed keys; truncate large values.

---

## Strict Finalization Gate

### Decision
Block `run()` only if there exists a terminal infra run in the same workspace with `results_status != finalized`.

Terminal infra outcomes:
- completed
- preempted
- crashed
- canceled

Running/pending runs do not block. Parallel runs are allowed.

---

## Mount + Dashboard + STATE.md

### mount()
Returns experiment context in response:
- `current_best` (tag, record, metric, value)
- `awaiting_finalization` (record ids)
- `recent_trend` (last N finalized values)
- `regression_alerts`

### dashboard()
Add `pending_finalizations` list.

### STATE.md
Add Experiment Summary block:
- Current best (tag + metric + value)
- Pending finalizations
- Recent trend
- Regression alerts

---

## Pruning Behavior
- Versions remain the pruning unit.
- Runs associated with pruned versions are annotated with `pruned_summary`.
- `list_history()` hides pruned records by default (`include_pruned=true` to show).

---

## Attempt Numbers
- `attempt_num` is deprecated and no longer incremented or surfaced in tools.

---

## Migration Requirements

1) **Schema migration**
- Add new tables: `experiment_records`, `run_results`, `run_results_spec`, `run_tags`.
- Add indexes for record lookup by workspace, tags, record_id.

2) **Backfill records**
- For each stage_run, create a run record in `experiment_records` with new `record_id`.
- For each workspace_version without a stage_run, create a checkpoint record.
- For versions created by run, link records via version field in experiment_records.

3) **Backfill results**
- For existing runs, create `run_results` with:
  - `results_status = missing`
  - `infra_outcome` derived from run status
  - `ml_outcome = unknown`
- Optionally auto-populate `results_auto` from metrics summary if available.

4) **Tag migration**
- Keep existing version tags in `workspace_version_tags`.
- No backfill needed for run_tags.

5) **Tool compatibility**
- `list_runs()` should delegate to `list_history()` to preserve older clients.
- `inspect_run()` should map to `inspect_record()` and expose results/tags.

---

## Implementation Checklist (TDD First)

- [ ] Define JSON schema for `results_spec` and `finalize_run` payloads (tests first).
- [ ] Add schema migrations for new tables (tests validate schema).
- [ ] Implement `experiment_records` creation on run + save_version (tests).
- [ ] Implement `run_results_spec` persistence on run() (tests).
- [ ] Implement auto results extraction into `results_auto` (tests).
- [ ] Implement `finalize_run` tool and schema validation (tests).
- [ ] Implement comparison block computation (tests).
- [ ] Add run_tags and tag_record (tests).
- [ ] Implement list_history + inspect_record (tests).
- [ ] Add strict finalization gate (tests).
- [ ] Update mount() response with experiment context (tests).
- [ ] Update dashboard() and STATE.md summary block (tests).
- [ ] Deprecate attempts from list/inspect outputs (tests).

---

## Notes
- LLMs are producers and consumers; verbose context is required, not optional.
- Structured fields are for mechanical validation and safe automation.
- All auto results are drafts; finalized results are authoritative.
