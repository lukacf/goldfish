# Goldfish Semantic Validation System (SVS)
## A Hybrid Mechanistic + AI Approach to Catching Silent Failures

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DEV-SIDE (Pre-Execution)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐                   │
│  │ Pre-Run Review   │  │ Config Review    │                   │
│  │ (Code + Logic)   │  │ (Coherence)      │                   │
│  └────────┬─────────┘  └────────┬─────────┘                   │
│           │                     │                               │
│           └─────────────────────┴─────────────────────          │
│                                 │                                │
│                           Claude Agent                           │
│                    (Read-only workspace access)                  │
│                                 │                                │
│                          Context Sources:                        │
│              ┌──────────────────┴──────────────────┐            │
│              │ • Git history + lineage              │            │
│              │ • CLAUDE.md + domain docs            │            │
│              │ • Failure patterns (DB, generated)   │            │
│              │ • Past run history (DB)              │            │
│              │ • RunReason (user intent)            │            │
│              │ • STATE.md (workspace state)         │            │
│              └─────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 │ run() approved
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                 CONTAINER-SIDE (During + Post Execution)         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Stage Execution:                                                │
│  ┌────────────────────────────────────────────────────────────┐│
│  │  1. Load inputs                                             ││
│  │  2. Run computation ──► DURING-RUN checks (every N steps)  ││
│  │     │                    ├─ loss_nan, divergence, grad_norm ││
│  │     │                    ├─ CRITICAL → early-stop (if on)   ││
│  │     │                    └─ WARN → log, continue            ││
│  │  3. Save output ──► POST-RUN checks (schema, distribution) ││
│  │                     ├─ PASS ─► Post-Stage AI Review         ││
│  │                     └─ FAIL ─► WARN or BLOCK (by policy)    ││
│  └────────────────────────────────────────────────────────────┘│
│                                                                  │
│                          Claude Agent                            │
│                   (Full data access via goldfish.io)             │
│                                 │                                │
│                          Context Sources:                        │
│              ┌──────────────────┴──────────────────┐            │
│              │ • All dev-side context +             │            │
│              │ • Actual output data (raw + stats)   │            │
│              │ • Runtime metrics (real-time)        │            │
│              │ • Resource usage (mem, GPU)          │            │
│              │ • Intermediate checkpoints           │            │
│              └─────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Gap Resolutions (Architectural Decisions)

### 1. Stats Storage

**Decision:** Extend `signal_lineage` table with `stats_json TEXT` column.

**Schema addition:**
```sql
-- Add to signal_lineage table
stats_json TEXT  -- JSON: {entropy, null_ratio, unique_count, min, max, mean, std, samples_used, computed_at}
```

**Rationale:** Follows existing pattern (signal_lineage already tracks per-output metadata). Avoids new table.

**Stats schema (versioned):**
```json
{
  "stats_version": 1,
  "computed_at": "2025-12-25T10:32:07Z",
  "samples_used": 10000,
  "shape": [1000, 768],
  "dtype": "float32",
  "metrics": {
    "entropy": 9.06,
    "null_ratio": 0.0,
    "unique_count": 12709,
    "min": 0,
    "max": 15033,
    "mean": 7234.5,
    "std": 4102.3
  },
  "skipped_checks": {
    "compression_ratio": "unsupported for dtype float32"
  }
}
```

---

### 2. Schema as Contract, Metadata as Observation

**Decision:** `pipeline.yaml` schema is the **contract (law)** for signals.  
Datasource metadata is the **observed reality** of registered artifacts.

**Validation hierarchy:**
1. **Pipeline schema** defines the signal contract (structure) → authoritative.
2. **Registered input metadata** must be **compatible** with the input contract (if defined).  
   If metadata is missing (legacy), SVS warns and skips contract verification.
3. **Stage outputs** must satisfy the output contract at `save_output` time.
4. **Output metadata** (emitted post-run) is recorded as observation and must remain compatible with the contract.

**Compatibility rule:** For a given signal:
- `type` must align (`npy` ↔ `tensor`, `csv` ↔ `tabular`, `file` ↔ `json|file`)
- `schema.kind` must match expected kind (`tensor`, `tabular`, `json`)
- `shape`/`dtype` checks apply if defined in the contract

**Tensor array enforcement (inputs):**
- If the input contract declares `schema.arrays`, each named array must exist in the registered metadata.
- For each declared array, `shape` and `dtype` are checked (wildcards allowed).
- If the input contract uses a flat `shape`/`dtype` (no arrays), it is checked against the metadata `primary_array`.

**Contract resolution (config-aware):**
Contracts may reference config parameters for dynamic shapes, e.g. `"{embedding_dim}"`.
Resolution rules:
- Values are resolved from the stage config **for the current run** (including overrides).
- Missing or non‑numeric values → validation error.
- Preflight and runtime must resolve the **same** config to avoid drift.

**Preflight contract check:**
`validate_pipeline` performs a structural compatibility pass:
- Resolve all schemas with the run config.
- Ensure upstream output schema is compatible with downstream input schema.
- If dataset inputs declare `schema`, compare against registered metadata (when available).
- Fail fast on mismatches (no container build).

---

### 3. Hierarchy of Truth (Law vs Judgment)

**Level 0 — Mechanistic Checks (Law):**  
Defined in `pipeline.yaml` and enforced at runtime. If a mechanistic check is **blocking**, it **always** blocks execution.  
AI reviews cannot override these failures.

**Level 1 — AI / Knowledge Checks (Judgment):**  
Derived from domain docs and failure pattern knowledge base. These produce **WARN/BLOCK** recommendations, but do not
invalidate a passing mechanistic check unless policy explicitly allows blocking.

**Override rule:** Mechanistic failures always take precedence. AI feedback can only escalate or warn, never bypass a failed law.

**Missing metadata policy:**
- If input metadata is missing (legacy sources), SVS **skips contract compatibility checks** and emits a warning.
- Semantic checks that require metadata are skipped with reason `metadata_missing`.
- For outputs, metadata is generated post-run from computed stats; absence does not block mechanistic checks.

---

### 4. Failure Policy (WARN/FAIL/SKIP) + Enforcement Mode

**Decision:** Three-tier policy with separate **enforcement mode**.  
Pre-run is always blocking; during-run and post-run are controlled by enforcement.

```yaml
# goldfish.yaml (project level defaults)
svs:
  enabled: true
  default_policy: fail
  default_enforcement: warning  # NEW: "blocking" | "warning"
  enforcement_warmup_runs: 10   # NEW: Stay in warning mode for first N runs
  check_policies:
    entropy: fail
    null_ratio: fail
    compression_ratio: warn

# pipeline.yaml (per-stage/output overrides)
stages:
  - name: tokenize
    outputs:
      tokens:
        type: npy
        schema:
          checks:
            entropy: {min: 6.0}
          enforcement: warning  # NEW: Override for this output
        svs:
          entropy: skip
          reason: "BPE produces uniform distribution"
```

**Enforcement vs Policy:**
- **Policy** (fail/warn/skip): What severity to assign when check fails
- **Enforcement** (blocking/warning): Whether to actually stop the run

| Scenario | Policy | Enforcement | Result |
|----------|--------|-------------|--------|
| entropy=5.9, threshold=6.0 | fail | warning | Log WARNING, continue run |
| entropy=5.9, threshold=6.0 | fail | blocking | Raise error, stop run |
| entropy=5.9, threshold=6.0 | warn | * | Log WARNING, continue run |

**Warm-up workflow:**
1. New pipeline starts with `enforcement: warning` (default for post-run)
2. After `enforcement_warmup_runs` successful runs, prompt user to harden
3. User explicitly sets `enforcement: blocking` when confident

**Rationale:** Don't kill a 48-hour run because entropy was 5.9 instead of 6.0. Validate thresholds in warning mode first.

---

### 5. Large Data Handling

**Decision:** Reservoir sampling (k=10000) computed IN CONTAINER before upload.

```python
def compute_output_stats(data, sample_size=10000):
    if data.size > sample_size:
        indices = reservoir_sample(data.size, sample_size)
        sample = data.flat[indices]
    else:
        sample = data.flatten()
    return {"samples_used": len(sample), "entropy": ..., "std": ...}
```

**Env var:** `GOLDFISH_STATS_SAMPLE_SIZE` (default 10000)

**Fallback:** If a check cannot be computed (unsupported dtype, missing prereqs, or oversized input),
the check is recorded as `skipped` with a reason and does not fail the stage.

---

### 6. AI Trust Boundary

**Decision:** Claude agents are trusted and receive full data access inside the container.

**Data exposure:**
- Full output data may be accessed via `goldfish.io` as needed.
- No redaction is applied in this version.
- Enterprises must supply an AI endpoint they trust.

---

### 7. Cost/Latency Control

**Decision:** Stats computation ALWAYS runs. AI review rate-limited with circuit breaker.

```yaml
# goldfish.yaml
svs:
  stats_enabled: true  # Always

  # Pre-run
  ai_pre_run_enabled: true

  # During-run
  during_run_enabled: true
  during_run_check_interval: 100  # Check every N steps
  during_run_auto_stop: false     # Auto-stop on critical alerts
  during_run_ai_review: false     # AI review during training (expensive)
  during_run_ai_checkpoint_interval: 5  # AI review every N checkpoints

  # Post-run
  ai_post_run_enabled: true
  ai_validation_stages: artifacts_only  # all | artifacts_only | critical_only

  # Limits
  rate_limit_per_hour: 60
  timeout_seconds: 30
```

**Circuit breaker:** 3 consecutive AI failures → disable for 10 minutes

---

### 8. Triple Validation Phases (Pre/During/Post)

| Phase | When | Authority | Purpose |
|-------|------|-----------|---------|
| **Pre-run** | Before execution | BLOCK | Catch code/config bugs |
| **During-run** | Periodic (every N steps) | WARN + early-stop option | Catch training divergence |
| **Post-run** | After stage completes | WARN by default; BLOCK if `enforcement: blocking` | Catch output quality issues |

**Flow:**
1. Pre-run: ERROR → BLOCK run
2. During-run: CRITICAL → early-stop if enabled, else WARN
3. Post-run: FAIL → WARN by default, BLOCK only if `enforcement: blocking`

---

### 9. Domain-Specific Checks

**Decision:** Domain profiles in `goldfish.yaml` + per-output overrides in `pipeline.yaml`.

```yaml
# goldfish.yaml
svs:
  domain: nlp_tokenizer  # Apply profile defaults project-wide
```

```yaml
# pipeline.yaml (per-output override)
stages:
  - name: tokenize
    outputs:
      tokens:
        svs:
          domain: nlp_tokenizer
          entropy: 8.0  # Further customize
```

**Built-in profiles:** `nlp_tokenizer`, `image_embeddings`, `tabular_features`, `default`

**Compatibility rule:** The selected domain profile must match the output schema kind.
If profile is incompatible (e.g., `nlp_tokenizer` on `tabular`), SVS raises a config error.

---

### 10. Reproducibility

**Decision:** Full audit logging in `svs_reviews` table.

```sql
CREATE TABLE svs_reviews (
    id INTEGER PRIMARY KEY,
    stage_run_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    review_type TEXT NOT NULL,  -- 'pre_run' | 'during_run' | 'post_run'
    model_used TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,  -- SHA256 of full prompt
    stats_json TEXT,
    response_text TEXT,
    parsed_findings TEXT,
    decision TEXT NOT NULL,  -- 'approved' | 'blocked' | 'warned'
    policy_overrides TEXT,
    reviewed_at TEXT NOT NULL,
    duration_ms INTEGER,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id)
);
```

---

### 11. SVS Feedback Transport (Core System)

**Decision:** SVS is part of the core runtime. There is no separate streaming system.
SVS feedback is surfaced via existing **run status** tools, alongside metrics and logs.

**Mechanism:**
- Container emits SVS findings during execution.
- StageExecutor aggregates findings into `stage_runs.svs_findings_json`.
- Dev‑side Claude polls with existing tools:
  - `get_run_status(...)`
  - `get_stage_run(...)`

**SVS payload (in stage_runs.svs_findings_json):**
```json
{
  "latest": {
    "phase": "during_run",
    "severity": "WARN",
    "check": "loss_divergence",
    "summary": "Loss > 10x warmup baseline"
  },
  "counts": {"ok": 8, "warn": 2, "block": 0},
  "history": [
    {"phase": "during_run", "severity": "WARN", "check": "grad_explosion", "summary": "grad_norm=150 > 100", "step": 300},
    {"phase": "during_run", "severity": "WARN", "check": "loss_divergence", "summary": "Loss > 10x warmup baseline", "step": 500}
  ]
}
```

**History notes:**
- Only WARN and BLOCK findings are stored (OK findings are just counted)
- `step` present for during-run checks, absent for post-run

---

### 12. Credential Exposure in Logs

**Decision:** Log redaction at collection time in `_finalize_stage_run()`.

```python
REDACTION_PATTERNS = [
    (r'(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s]+', r'\1=[REDACTED]'),
    (r'(?i)bearer\s+[A-Za-z0-9._-]+', 'Bearer [REDACTED]'),
    (r'sk-[a-zA-Z0-9]{20,}', '[REDACTED_API_KEY]'),
    (r'ghp_[A-Za-z0-9]{36}', '[REDACTED_GITHUB_TOKEN]'),
]
```

**Applied to:** local log files, `stage_runs.error` field, `get_stage_logs` MCP tool output

---

### 13. Check Definition Contracts

**Output checks (post-stage):**

**Computation rules:**
- All checks operate on sampled data (reservoir sampling).
- Checks declare prerequisites (dtype support, required metadata).
- If prerequisites are not met, the check is recorded as `skipped` with a reason.
  - Example: `compression_ratio` requires raw bytes; if unavailable, it is skipped.

| Check | Definition | Computation | Default Threshold |
|-------|------------|-------------|-------------------|
| `entropy` | Shannon entropy of discretized values | `scipy.stats.entropy(histogram, base=2)` | WARN if <6, FAIL if <3 |
| `vocab_utilization` | Fraction of vocab indices used | `len(unique) / vocab_size` | WARN if <0.7 |
| `compression_ratio` | Uncompressed / gzip-compressed size | `len(raw_bytes) / len(gzip(raw_bytes))` | WARN if <1.5 or >10 |
| `null_ratio` | Fraction of NaN/None values | `np.isnan(sample).mean()` | WARN if >0.01, FAIL if >0.1 |
| `unique_count` | Distinct values in sample | `len(np.unique(sample))` | Context-dependent |
| `top1_fraction` | Most common value frequency | `max(counts)/N` | WARN if >0.05 |
| `top10_fraction` | Top-10 values frequency | `sum(top10)/N` | WARN if >0.20 |

**Training checks (during-run):**

| Check | Definition | Computation | Default Threshold |
|-------|------------|-------------|-------------------|
| `loss_nan` | Loss became NaN/Inf | `np.isnan(loss) or np.isinf(loss)` | CRITICAL (auto-stop) |
| `loss_divergence` | Loss exploded after warmup | `loss > 10 * loss_at_step_100` | CRITICAL if step > 500 |
| `grad_explosion` | Gradient norm too high | `grad_norm > threshold` | WARN if >100, CRITICAL if >1000 |
| `lr_sanity` | Learning rate invalid | `lr <= 0 or lr > 1` | CRITICAL |
| `loss_plateau` | Loss hasn't improved | `best_loss unchanged for N steps` | WARN after 1000 steps |

**Prerequisites:** During-run checks require metric names defined in `svs.metric_names`.
Missing metrics → check skipped with reason `missing_metric`.

---

## Layer 1: Mechanistic Semantic Validation

**Where:** Runs in container at stage I/O boundaries (built into `goldfish.io`)

**What it catches:** schema violations, distribution anomalies

### 1.1 Output Schema Validation

Extend `pipeline.yaml` (the **contract** for this signal):

```yaml
stages:
  - name: tokenize
    config: configs/tokenize.yaml
    outputs:
      tokens:
        type: npy
        schema:
          shape: [null, "{vocab_size}"]  # Variable rows, fixed columns from config
          dtype: int32
          checks:
            # Vocabulary health
            vocab_size: {min: 10, max: 100000}
            vocab_utilization: {min: 0.7}  # 70%+ tokens used

            # Information content
            entropy: {min: 6.0}  # bits
            top1_fraction: {max: 0.05}  # No single token >5%
            top10_fraction: {max: 0.20}

            # Compression (for BPE outputs)
            compression_ratio: {min: 1.5, max: 10.0}

            # Distribution
            class_balance: {max_class: 0.6}  # No class >60%

        svs:  # Policy overrides for this output
          entropy: warn  # Don't fail, just warn
```

For JSON-heavy stages (lists/dicts), use `schema.kind: json` and `type: file`:

```yaml
stages:
  - name: render_json
    outputs:
      records:
        type: file
        schema:
          kind: json  # Accepts dict or list outputs
```

**Implementation:**

```python
# goldfish/io/__init__.py

def save_output(name: str, data: np.ndarray, **kwargs) -> None:
    """Save output with automatic validation."""

    # Load expected schema from pipeline.yaml (contract)
    schema = _load_output_schema(name)
    schema = _resolve_schema_params(schema, config=_load_stage_config())

    if schema:
        # Run mechanistic checks (shape/dtype + schema checks)
        violations = validate_output_data(data, schema)

        if violations:
            enforcement = _resolve_enforcement(schema)
            if enforcement == "blocking":
                # Fail fast with actionable error
                raise OutputSchemaViolationError(
                    f"Output '{name}' failed validation:\n"
                    + "\n".join(f"  - {v}" for v in violations)
                    + "\n\nExpected schema:\n"
                    + yaml.dump(schema)
                )
            logger.warning("SVS validation warnings for %s: %s", name, "; ".join(violations))

    # Save data
    _save_to_storage(name, data)

    # Compute and record output stats (uses reservoir sampling for large data)
    stats = compute_output_stats(data, schema)
    _record_output_metadata(name, stats)  # Stored in signal_lineage.stats_json
```

**What this catches from MLM log:**

| Issue | Check |
|-------|-------|
| Entry 1 (sine waves) | `entropy: {min: 6.0}` → 3.2 bits = FAIL |
| Entry 6 (dead market) | `std < 1e-9` → zero variance = FAIL |
| Entry 34 (98% neutral) | `max_class: 0.6` → 98% > 60% = FAIL |
| Entry 50 (no compression) | `compression_ratio: {min: 1.5}` → 1.02 = FAIL |

---

### 1.2 During-Run Training Checks

**NEW: Catch divergence before wasting hours of compute.**

```python
# In training loop (via goldfish.io.log_metric)
def log_metric(name: str, value: float, step: int):
    _record_metric(name, value, step)

    # Periodic during-run validation
    if step % DURING_RUN_CHECK_INTERVAL == 0:
        alerts = check_training_health(metrics_so_far)
        if alerts.has_critical:
            if os.environ.get("GOLDFISH_AUTO_STOP") == "1":
                raise TrainingDivergenceError(alerts.summary)
            else:
                logger.warning(f"DURING-RUN ALERT: {alerts.summary}")
```

**Mechanistic during-run checks:**
- Loss NaN/Inf detection → immediate stop
- Loss divergence (loss > 10x initial after warmup) → stop
- Gradient explosion (grad_norm > threshold) → warn or stop
- Learning rate sanity (lr went negative or exploded) → stop

**Metric naming contract (configurable):**
```yaml
svs:
  metric_names:
    loss: "loss"
    grad_norm: "grad_norm"
    lr: "learning_rate"
```
If a required metric name is missing, the corresponding check is skipped and recorded as `missing_metric`.

---

### 1.3 Tool Contract Enforcement

**Policy:** Tool contract violations are SVS checks with the same policy/enforcement rules.
Silent fallbacks require explicit opt-in.

```python
# In tools like TBPE
def run_tbpe(tokens, labels=None):
    if labels is None:
        raise ValueError("Labels are required for MI-gated BPE. Set ALLOW_FREQUENCY_BPE=1 to bypass.")

    if len(labels) != len(tokens):
        if os.environ.get("ALLOW_SILENT_FALLBACK") == "1":
            logger.warning("Label mismatch, disabling MI gating")
            labels = None
        else:
            raise ValueError(
                f"Label/token length mismatch: {len(labels)} != {len(tokens)}\n"
                "This likely means sequence-level labels mixed with token-level inputs.\n"
                "Fix your data or set ALLOW_SILENT_FALLBACK=1 to use frequency-only BPE."
            )
```

**What this catches:** Entry 10 (silent MI fallback), Entry 11 (MI values out of bounds), Entry 12 (label sync)

---

### 1.4 Stage Execution Metadata (Auto-Generated)

After every stage, stats are stored in database:

```sql
-- signal_lineage table (extended)
signal_lineage (
    stage_run_id TEXT,
    signal_name TEXT,
    signal_type TEXT,
    storage_location TEXT,
    size_bytes INTEGER,
    stats_json TEXT,  -- NEW: computed stats
    ...
)
```

Example `stats_json`:
```json
{
  "computed_at": "2025-12-25T10:32:07Z",
  "samples_used": 10000,
  "shape": [113643935],
  "vocab_size": 15034,
  "vocab_used": 12709,
  "vocab_utilization": 0.847,
  "entropy": 9.06,
  "compression_ratio": 2.71,
  "null_ratio": 0.0,
  "min": 0,
  "max": 15033,
  "mean": 7234.5,
  "std": 4102.3
}
```

---

## Layer 2: AI-Powered Semantic Validation

**Where:** Three environments (dev-side pre-run, container-side during-run, container-side post-stage)

**What it catches:** semantic coherence, hypothesis alignment, domain violations

---

### 2.0 Agent Abstraction Layer (DRY + Replaceable Providers)

**Goal:** Consolidate all AI review calls (pre‑run, during‑run, post‑run) behind a single abstraction so we can
swap Claude Code for Codex CLI (or any other assistant) without rewriting SVS logic.

#### Core Interface (Provider‑Agnostic)

```python
@dataclass
class ToolPolicy:
    permission_mode: Literal["plan", "ask", "auto"]
    allow_tools: list[str] | None = None
    deny_tools: list[str] | None = None
    mcp_servers: list[str] | None = None


@dataclass
class AgentRequest:
    mode: Literal["batch", "interactive"]
    prompt: str
    context: dict[str, Any]
    cwd: str
    model: str | None = None
    max_turns: int | None = None
    output_format: Literal["text", "json"] = "text"
    tool_policy: ToolPolicy | None = None
    timeout_seconds: int | None = None


@dataclass
class AgentResult:
    decision: Literal["approved", "blocked", "warned"] | None
    findings: list[str]
    raw_output: str
    structured_output: dict[str, Any] | None
    tool_calls: list[dict[str, Any]] | None
    duration_ms: int
    exit_code: int


class AgentProvider(Protocol):
    name: str
    def run(self, request: AgentRequest) -> AgentResult: ...
```

**Why this shape:** `prompt` is explicit; `context` stays structured; `tool_policy` captures common
permission controls; `output_format=json` enables machine parsing when supported.

#### Provider Mapping (Common CLI Shapes)

**Claude Code CLI**
- Mode: `claude -p` (batch) / interactive TUI
- Output: JSON supported via CLI flags
- Permissions + tools configured via CLI flags or config

**Codex CLI**
- Headless example (provided):
```bash
codex exec --full-auto --sandbox workspace-write \
"You are working in a Node.js monorepo with Jest tests and GitHub Actions. \
Read the repository, run the test suite, identify the minimal change needed \
to make all tests pass, implement only that change, and stop. Do not refactor \
unrelated code or files. Keep changes small and surgical."
```

**Gemini CLI**
- Interactive and batch use; supports MCP servers
- Tool access configured via CLI + MCP config

#### Container‑Side Installation (Required for During/Post‑Run Agents)

Bake CLI tools into the container image (no runtime installs):

```dockerfile
FROM node:20-bullseye

# Install agent CLIs (pin versions in real builds)
RUN npm install -g \
  @anthropic-ai/claude-code \
  @openai/codex \
  @google/gemini-cli
```

This makes all providers available inside containers for post‑run and during‑run reviews.

#### Unified Orchestrator

All SVS AI reviews call a single orchestrator:
- Builds prompt from shared templates
- Applies ToolPolicy
- Calls provider adapter (CLI or SDK)
- Parses response into `AgentResult`
- Writes audit entry (`svs_reviews`)

#### Configuration

```yaml
# goldfish.yaml
svs:
  agent_provider: claude_code  # claude_code | codex_cli | gemini_cli | null
  agent_model: claude-sonnet-4-5
  agent_timeout: 30
  agent_max_turns: 3
  agent_tools:
    pre_run: read_only
    during_run: container_full
    post_run: container_full
```

#### Implementation Plan (DRY, minimal churn)
1. **Define** `AgentRequest/AgentResult` + `AgentProvider` in `svs/agent.py`.
2. **Implement** `ClaudeCodeProvider`, `CodexCLIProvider`, `GeminiCLIProvider`, `NullProvider`.
3. **Add** a shared `ReviewOrchestrator` for pre‑run + post‑run.
4. **Wire** pre‑run review through the orchestrator (no behavior change).
5. **Add** container‑side post‑run review using the same orchestrator.

**Why this matters:** It eliminates duplicate logic, centralizes prompt/response handling, and makes the
AI backend swappable without touching SVS logic.

---

### 2.1 Dev-Side AI Reviews (Pre-Execution)

**Goal:** Catch issues before compute is wasted.

**Priority order:**
1. **Pre-run code review** (2.1.1) - HIGH: Catches syntax, config, logic bugs
2. **Config coherence review** - MEDIUM: Catches conflicts, deprecated params, type mismatches

**Principle:** Pre-run review focuses on **code and configuration coherence** only. It does **not** attempt to infer
statistical properties of generated data from source code. Those judgments belong to mechanistic output checks and
container-side AI reviews that see real artifacts.

### 2.1.2 Mechanistic Config Schema Validation (Preflight)

SVS can validate **config value types** deterministically at pipeline-parse time.
Declare per-stage config expectations in `config_schema` (optional):

```yaml
stages:
  - name: train
    config_schema:
      num_epochs: int
      lr: float
      use_amp: bool
      optimizer: str
      scheduler:
        type: str
        required: true
```

**Rules:**
- If `config_schema` is present, SVS validates the resolved stage config.
- `required: true` makes the key mandatory.
- Types supported: `int`, `float` (ints allowed), `number`, `bool`, `str`, `list`, `dict`.
- Missing non-required keys are skipped (no error).

This is **mechanistic** (no AI), fast, and runs during pipeline validation.

#### 2.1.1 Enhanced Pre-Run Code Review (Extend existing)

```python
# Update REVIEW_PROMPT in pre_run_review.py

REVIEW_PROMPT = """You are reviewing an ML experiment before execution.

## Context
**Workspace:** {workspace}
**Stages to run:** {stages_to_run}
**User Intent:** {run_reason}

## Known Failure Patterns
{failure_patterns}

## Project Current Approach
{current_approach_from_claude_md}

## Recent Run History
### Last 3 Successful Runs
{recent_successes}

### Last 5 Failed Runs
{recent_failures}

## Code & Config
{stage_sections}

## Git Diff Since Last Success
```diff
{diff_text}
```

## Your Task

Review for:
1. **Syntax/Logic bugs** - Will this code crash?
2. **Configuration issues** - Conflicts, deprecated params, missing args
3. **Hypothesis coherence** - Does the code test what user claims?
4. **Historical anti-patterns** - Does this repeat past failures?
5. **Domain violations** - Does this make sense for {domain}?

Use ERROR for blocking issues, WARNING for concerns, NOTE for suggestions.
"""
```

---

### 2.2 Container-Side AI Reviews

#### 2.2.1 During-Run AI Review (Optional, Expensive)

Triggered after every N checkpoints for long training runs:

```python
async def review_training_progress(
    metrics: dict,
    checkpoints_saved: int,
    run_reason: RunReason,
) -> TrainingReview:
    """AI review of training progress."""

    prompt = f"""
    ## Training Progress Review

    **Checkpoints saved:** {checkpoints_saved}
    **User hypothesis:** {run_reason.hypothesis}

    ## Metrics Trend
    ```yaml
    {yaml.dump(metrics)}
    ```

    ## Check for:
    1. Loss curve shape (healthy? plateaued? diverging?)
    2. Metrics trending in expected direction?
    3. Any signs this experiment should be stopped early?

    Output: CONTINUE | WARN | STOP with reasoning.
    """

    return await query_claude_agent(prompt)
```

#### 2.2.2 Post-Stage Output Review

```python
async def review_output_semantics(
    output_name: str,
    stats: dict,  # Aggregate stats (raw data available via goldfish.io)
    stage_name: str,
    run_reason: RunReason,
) -> OutputReview:
    """AI-powered semantic validation of stage output."""

    # Query database for failure patterns
    patterns = pattern_manager.get_patterns_for_stage(stage_name)
    failure_patterns_md = pattern_manager.to_markdown(patterns)

    prompt = f"""
    ## Stage Just Completed
    **Stage:** {stage_name}
    **Output:** {output_name}

    ## User's Hypothesis
    {run_reason.to_markdown()}

    ## Output Statistics (sampled)
    ```yaml
    {yaml.dump(stats)}
    ```

    You may load full output data via goldfish.io if needed.

    ## Known Failure Modes for {stage_name}
    {failure_patterns_md}

    ## Your Task
    Does this output make sense given the hypothesis?

    Output: BLOCK | WARN | OK with reasoning.
    (BLOCK is enforced only if `enforcement: blocking`.)
    """

    return await query_claude_agent(prompt)
```

---

### 2.3 Knowledge Base Architecture

**The Goldfish-Native Solution:** Structured storage (database) + generated views (markdown)

#### Database Schema

```sql
-- goldfish.db
CREATE TABLE failure_patterns (
    id TEXT PRIMARY KEY,  -- UUID
    symptom TEXT NOT NULL,
    root_cause TEXT NOT NULL,
    detection_heuristic TEXT NOT NULL,
    prevention TEXT NOT NULL,
    severity TEXT CHECK(severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    stage_type TEXT,  -- 'tokenization', 'training', 'bpe', NULL for general

    -- Provenance (link to source run)
    source_run_id TEXT REFERENCES stage_runs(id),
    source_workspace TEXT,

    -- Metadata
    created_at TEXT NOT NULL,
    last_seen_at TEXT,
    occurrence_count INTEGER DEFAULT 1,

    -- Lifecycle (NEW)
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'approved', 'rejected', 'archived')),
    confidence TEXT CHECK(confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    approved_at TEXT,
    approved_by TEXT,  -- 'librarian_agent' | 'user:{username}'
    rejection_reason TEXT,

    -- User control
    manually_edited BOOLEAN DEFAULT 0,
    enabled BOOLEAN DEFAULT 1
);

CREATE INDEX idx_failure_patterns_stage ON failure_patterns(stage_type);
CREATE INDEX idx_failure_patterns_severity ON failure_patterns(severity);
CREATE INDEX idx_failure_patterns_source_run ON failure_patterns(source_run_id);
CREATE INDEX idx_failure_patterns_status ON failure_patterns(status);
```

**Why database over flat files:**
1. **Queryable:** Filter by stage, severity, status, date
2. **Concurrent-safe:** SQLite handles locking
3. **Global scope:** Shared across workspaces (not versioned per branch)
4. **Provenance:** Link to exact run that discovered pattern
5. **Lifecycle management:** Pending → Approved → Active workflow prevents noise
6. **Still LLM-friendly:** Generate markdown on-demand (only approved patterns)

---

### 2.4 Self-Learning System

After every failed run, extract learnings and **store in database with pending status**:

> **Noise Prevention:** Auto-extraction can flood the DB with low-quality patterns from transient infrastructure errors. All auto-learned patterns start in `status: pending` and require approval before affecting future runs.

```python
async def _handle_stage_failure(
    stage_name: str,
    run_id: str,
    workspace_name: str,
    error: Exception,
    logs: str,
) -> None:
    """Extract learnings from failure and store in database."""

    # STEP 1: Strict filtering - skip transient/infrastructure errors
    if _is_transient_error(error, logs):
        logger.debug(f"Skipping pattern extraction for transient error: {error}")
        return

    analysis_prompt = f"""
    This stage failed. Extract a reusable failure pattern.

    ## Stage: {stage_name}
    ## Error: {error}
    ## Logs (last 100 lines): {logs[-100:]}

    ## STRICT CRITERIA FOR INCLUSION
    Only extract if this failure:
    1. Is REPRODUCIBLE (not a one-off infrastructure glitch)
    2. Is ACTIONABLE (there's a specific check that would catch it)
    3. Is GENERALIZABLE (would apply to other runs, not just this one)

    If this is a transient error (network timeout, disk full, permission denied),
    respond with: NOT_A_PATTERN

    Otherwise extract:
    1. **Symptom** (how it manifested)
    2. **Root cause** (what actually broke)
    3. **Detection heuristic** (how to catch earlier)
    4. **Prevention** (what to validate before running)
    5. **Severity** (CRITICAL/HIGH/MEDIUM/LOW)
    6. **Confidence** (HIGH/MEDIUM/LOW)
    """

    analysis = await query_claude_agent(analysis_prompt)

    if analysis.is_not_a_pattern:
        return

    # STEP 2: Store as PENDING - requires approval before active
    pattern_manager.record_pattern(
        symptom=analysis.symptom,
        root_cause=analysis.root_cause,
        detection_heuristic=analysis.detection,
        prevention=analysis.prevention,
        severity=analysis.severity,
        stage_type=stage_name,
        source_run_id=run_id,
        source_workspace=workspace_name,
        status="pending",  # NEW: Not active until approved
        confidence=analysis.confidence,
    )
```

**Pattern Lifecycle:**

```
pending → approved → active
    │         │
    └→ rejected (deleted or archived)
```

**Librarian Agent (Optional):**

A specialized agent periodically reviews pending patterns:

```python
async def review_pending_patterns():
    """Librarian agent: reviews and approves/rejects pending patterns."""

    pending = pattern_manager.get_pending_patterns()

    for pattern in pending:
        # Get source run context
        source_run = db.get_stage_run(pattern.source_run_id)

        prompt = f"""
        Review this auto-extracted failure pattern for quality.

        ## Pattern
        Symptom: {pattern.symptom}
        Root Cause: {pattern.root_cause}
        Detection: {pattern.detection_heuristic}
        Prevention: {pattern.prevention}
        Confidence: {pattern.confidence}

        ## Source Run
        Stage: {source_run.stage_name}
        Error: {source_run.error}
        Workspace: {source_run.workspace_name}

        ## Decision Criteria
        - APPROVE if: reproducible, actionable, generalizable
        - REJECT if: too specific, infrastructure error, low confidence
        - MERGE if: duplicate of existing pattern (specify which)

        Output: APPROVE | REJECT | MERGE:pattern_id
        """

        decision = await query_claude_agent(prompt)
        pattern_manager.update_status(pattern.id, decision)
```

**CLI for pattern management:**

```bash
# List pending patterns for manual review
goldfish list-failure-patterns --status=pending

# Approve pattern
goldfish approve-pattern pattern-abc123

# Reject pattern
goldfish reject-pattern pattern-abc123 --reason="Infrastructure error"

# Run librarian agent
goldfish review-pending-patterns
```

---

## CLI for Knowledge Management

```bash
# Query patterns
goldfish list-failure-patterns --stage=tokenization --severity=HIGH

# Show specific pattern with provenance
goldfish show-failure-pattern pattern-abc123

# Edit pattern
goldfish edit-failure-pattern pattern-abc123 --symptom="Updated description"

# Disable pattern
goldfish edit-failure-pattern pattern-abc123 --enabled=false

# Link back to source run (provenance!)
goldfish show-run $(goldfish show-failure-pattern pattern-abc123 --field=source_run_id)
```

---

## Integration Architecture

### Flow Diagram

```
User: run("workspace", stages=["tokenize", "train"])
  │
  ├─► DEV-SIDE AI REVIEWS (parallel)
  │   ├─► Pre-run code review
  │   ├─► Config coherence review
  │
  ├─► All approved?
  │   ├─ NO → Return review with errors, block execution
  │   └─ YES → Continue
  │
  ├─► Launch container
  │
  └─► CONTAINER EXECUTION
      │
      For each stage:
        │
        ├─► Load inputs
        │
        ├─► Run computation
        │     │
        │     └─► DURING-RUN CHECKS (every N steps)
        │         ├─ loss_nan, divergence, grad_explosion
        │         ├─ CRITICAL + auto_stop → raise TrainingDivergenceError
        │         └─ WARN → log, continue
        │
        ├─► save_output("tokens", data)
        │     │
        │     ├─► MECHANISTIC CHECK (schema, distribution)
        │     │   ├─ FAIL → WARN (default) or ERROR (if enforcement=blocking)
        │     │   └─ PASS → continue
        │     │
        │     ├─► Compute stats (reservoir sampling) → signal_lineage.stats_json
        │     ├─► Aggregate SVS findings → stage_runs.svs_findings_json
        │     │
        │     └─► AI OUTPUT REVIEW (if enabled)
        │         ├─► Query DB for patterns → generate markdown
        │         ├─► Check hypothesis coherence
        │         ├─► Check domain constraints
        │         │
        │         ├─ BLOCK → raise OutputSemanticError (if enforcement=blocking)
        │         ├─ WARN → log warnings, continue
        │         └─ OK → continue
        │
        └─► Record in svs_reviews table

  Stage failed?
    └─► Extract learnings → store in failure_patterns table
```

---

## Configuration

```yaml
# goldfish.yaml
svs:
  enabled: true

  # Domain profile (built-in: nlp_tokenizer, image_embeddings, tabular_features, default)
  domain: default

  # Default policy for all checks
  default_policy: fail  # fail | warn | skip

  # Per-check policy overrides
  check_policies:
    entropy: fail
    null_ratio: fail
    compression_ratio: warn
    vocab_utilization: warn

  # Mechanistic validation (always on when svs.enabled)
  enforce_output_schemas: true
  enforce_tool_contracts: true

  # Metric naming contract for during-run checks
  metric_names:
    loss: "loss"
    grad_norm: "grad_norm"
    lr: "learning_rate"

  # Pre-run AI validation
  ai_pre_run_enabled: true
  ai_config_review: false
  ai_data_gen_review: false

  # During-run validation
  during_run_enabled: true
  during_run_check_interval: 100  # Check every N steps
  during_run_auto_stop: false     # Auto-stop on critical alerts
  during_run_ai_review: false     # AI review during training (expensive)

  # Post-run AI validation
  ai_post_run_enabled: true
  ai_validation_stages: artifacts_only  # all | artifacts_only | critical_only

  # AI model settings
  review_model: claude-sonnet-4-5
  review_timeout: 30
  review_max_turns: 3
  rate_limit_per_hour: 60

  # Self-learning
  auto_learn_failures: true
```

**Environment variable overrides:**

```bash
# Enable all AI reviews
export GOLDFISH_AI_REVIEW_ENABLED=1

# Or selective
export GOLDFISH_AI_PRE_RUN=1
export GOLDFISH_AI_POST_STAGE=1
export GOLDFISH_AUTO_STOP=1  # Enable during-run auto-stop
```

---

## Database Schema Additions

```sql
-- Extend signal_lineage with stats
ALTER TABLE signal_lineage ADD COLUMN stats_json TEXT;

-- Extend stage_runs with SVS findings
ALTER TABLE stage_runs ADD COLUMN svs_findings_json TEXT;

-- SVS review audit trail
CREATE TABLE svs_reviews (
    id INTEGER PRIMARY KEY,
    stage_run_id TEXT NOT NULL,
    signal_name TEXT,
    review_type TEXT NOT NULL,  -- 'pre_run' | 'during_run' | 'post_run'
    model_used TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    stats_json TEXT,
    response_text TEXT,
    parsed_findings TEXT,
    decision TEXT NOT NULL,  -- 'approved' | 'blocked' | 'warned'
    policy_overrides TEXT,
    reviewed_at TEXT NOT NULL,
    duration_ms INTEGER,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id)
);

CREATE INDEX idx_svs_reviews_stage_run ON svs_reviews(stage_run_id);
CREATE INDEX idx_svs_reviews_type ON svs_reviews(review_type);

-- Failure patterns (already documented above)
CREATE TABLE failure_patterns (...);

```

---

## What This Catches (MLM Project Analysis)

| Category | Mechanistic | AI Dev-Side | AI Container-Side | Total |
|----------|-------------|-------------|-------------------|-------|
| **Data Pipeline Issues** | 8 | 5 | 9 | 22 / 22 (100%) |
| **Config/State Issues** | 3 | 10 | 3 | 16 / 16 (100%) |
| **Tool Assumptions** | 4 | 2 | 7 | 13 / 13 (100%) |
| **Infrastructure** | 0 | 2 | 2 | 4 / 6 (67%) |
| **Performance** | 1 | 0 | 2 | 3 / 3 (100%) |
| **Design Errors** | 0 | 1 | 2 | 3 / 3 (100%) |
| **TOTAL** | **16 (25%)** | **20 (31%)** | **25 (39%)** | **61 / 64 (95%)** |

**The 3 uncaught issues:**
- Infrastructure permission errors (not predictable)
- Novel algorithm-domain mismatches (no prior knowledge)
- One-off typos in edge case code paths

**95% coverage from a system that preserves arbitrary code flexibility.**

---

## Why This Works

1. **Preserves flexibility:** Stages are still arbitrary Python
2. **Defense in depth:** Mechanistic + AI, pre + during + post
3. **Context-aware:** Uses lineage, run history, domain knowledge
4. **Self-improving:** Learns from failures automatically
5. **Fail-fast:** Catches issues at earliest possible point
6. **Goldfish-native:** Uses database like everything else
7. **Queryable:** Can filter, search, and trace patterns back to source runs
8. **Concurrent-safe:** SQLite handles simultaneous failures
9. **Reproducible:** Full audit trail in svs_reviews table

**This is Synapse-level validation without Synapse's constraints.**

The key insight: **You don't need design-time type checking if you have runtime AI checking with rich context + structured knowledge storage.**
