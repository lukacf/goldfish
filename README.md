# Goldfish

**ML experimentation platform for AI‑assisted workflows**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![MCP Compatible](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io/)
[![AGPL-3.0 License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![CI](https://github.com/lukacf/goldfish/actions/workflows/ci.yml/badge.svg)](https://github.com/lukacf/goldfish/actions)

[Key Features](#key-features) • [Core Concepts](#core-concepts) • [Quick Start](#quick-start) • [Limitations](#limitations) • [Documentation](#documentation)

---

## The Problem

You can ask an AI agent to write ML code, but real experiments are **stateful and long‑running**:

- Runs last hours or days.
- Provenance (data/code/config) must be exact.
- Iteration requires comparing many versions.
- Context is lost when the agent restarts.

## The Solution

Goldfish is the **backend** your AI agent uses to run ML experiments reliably.  
You (human) describe the goal; the **agent** calls Goldfish MCP tools to do the work.

**You (human):**
```
“Compare three attention variants on CASP14. Use axial as baseline.”
```

**Your agent + Goldfish:**
1. Creates a workspace
2. Registers datasets with metadata
3. Writes `pipeline.yaml` + stage modules
4. Runs stages in containers
5. Versions code + config automatically
6. Tracks lineage and metrics

Goldfish handles execution, versioning, lineage, and validation. The agent writes ML code.

---

## Key Features

- **Workspace Isolation** — Each experiment runs in its own sandboxed workspace
- **Pipeline Workflows** — YAML‑defined stages with typed signals
- **Automatic Versioning** — Every run produces an immutable version tag
- **Data Lineage** — Full provenance from datasets → features → models → metrics
- **Validation System** — Schema contracts + output stats (optional, fail or warn)
- **Metrics + Logs** — Structured metrics + live logs for long runs
- **Pre‑Run Review** — Automated code review before execution (part of validation)
- **Infrastructure Abstraction** — Containers + storage are hidden from the agent
- **Context Recovery** — STATE.md helps agents resume after restarts

---

## Core Concepts

Goldfish has six core abstractions that work together:

### Workspaces = Isolated Experiments

A **workspace** is an isolated environment for one experiment. The agent can create multiple workspaces to explore different approaches in parallel without interference.

```
create_workspace("axial_attention", goal="Test axial attention on CASP14")
     ↓
Creates isolated environment
Mounts to slot w1 for file access
```

Workspaces can be **mounted** (active, files accessible), **hibernated** (stored, freeing slots), or **branched** (forked for variations).

### Versions = Immutable Snapshots

A **version** is an immutable snapshot of your experiment at a point in time. Goldfish auto-creates versions on every run, ensuring full reproducibility.

```
run("w1", stages=["train"])
     ↓
Creates version: axial_attention-v3
Captures: code, config, pipeline definition
Tracks: output locations and lineage
```

You can always return to any version: `rollback("w1", "v2")`

### Pipelines = Workflow Definitions

A **pipeline** is a YAML file defining stages and how data flows between them:

```yaml
# pipeline.yaml
stages:
  - name: preprocess
    inputs:
      raw_data: { type: dataset, dataset: casp14_proteins }
    outputs:
      features: { type: npy }
      labels: { type: npy }

  - name: train
    inputs:
      features: { from_stage: preprocess, signal: features }
      labels: { from_stage: preprocess, signal: labels }
    outputs:
      model: { type: directory }
      metrics: { type: csv }

  - name: evaluate
    inputs:
      model: { from_stage: train, signal: model }
      test_data: { type: dataset, dataset: casp14_test }
    outputs:
      report: { type: file }
```

The agent edits this file directly. Goldfish validates the DAG and wires data automatically.

Notes on `from_stage` inputs:
- Use `signal:` to point at a specific output name from the upstream stage.
- If `signal` is omitted, it defaults to the input name (e.g., input `features` pulls `preprocess.features`).

### Stages = Executable Steps

A **stage** is a Python module that runs in an isolated container:

```python
# modules/train.py
from goldfish.io import load_input, save_output

def main():
    # Inputs are automatically available from upstream stages or datasets
    features = load_input("features")  # numpy array
    labels = load_input("labels")

    # Your ML code — completely standard Python
    model = train_axial_attention(features, labels, config)
    metrics = evaluate(model, features, labels)

    # Outputs are tracked with full lineage
    save_output("model", model_dir)
    save_output("metrics", metrics_df)

if __name__ == "__main__":
    main()
```

The agent writes pure ML code. Goldfish handles containerization, input mounting, and output tracking.

### Signals = Typed Data Flow

**Signals** are typed data connectors between stages:

| Type | Format | Use Case |
|------|--------|----------|
| `dataset` | External | Registered project data (immutable) |
| `npy` | NumPy array | Features, embeddings, tensors |
| `csv` | DataFrame | Metrics, tabular data |
| `directory` | Folder | Model checkpoints, multi-file outputs |
| `file` | Single file | Configs, reports, small outputs |

Goldfish tracks every signal — you always know exactly which data produced which results.

**Dataset registration requires metadata** (format, schema, size, etc.).  
See `docs/specs/datasource-metadata.md` for the required schema.

### Pre-Run Review = Automatic Bug Detection

Before executing any stage, Goldfish automatically reviews your code using Claude to catch errors before wasting compute time:

```python
run("w1", stages=["train"], reason={
    "description": "Testing larger batch size",
    "hypothesis": "Batch size 64 will improve convergence"
})
```

**What gets reviewed:**
- Pipeline structure and data flow
- Stage modules (modules/train.py)
- Configuration files (configs/train.yaml)
- Changes since last run (git diff)

**Example review output:**
```
✗ BLOCKED: modules/train.py:12 - `learning_rate` undefined
✗ BLOCKED: modules/train.py:15 - `metrics` never assigned
⚠ WARNING: No validation split - training on full data
```

The run is blocked if ERRORs are found. Warnings are logged but don't block execution.

**Configuration:**
```yaml
# goldfish.yaml
pre_run_review:
  enabled: true              # Default: true
  timeout_seconds: 60        # API timeout
```

Requires `ANTHROPIC_API_KEY` environment variable. Skips review if not set.

### How They Work Together

```
┌─────────────────────────────────────────────────────────────────────┐
│                         WORKSPACE                                   │
│                    (isolated experiment environment)                │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                        PIPELINE                              │   │
│   │                      (pipeline.yaml)                         │   │
│   │                                                              │   │
│   │    ┌──────────┐      ┌──────────┐      ┌──────────┐         │   │
│   │    │  STAGE   │      │  STAGE   │      │  STAGE   │         │   │
│   │    │preprocess│─npy─▶│  train   │─dir─▶│ evaluate │         │   │
│   │    └──────────┘      └──────────┘      └──────────┘         │   │
│   │         ▲                                    │               │   │
│   │         │                                    ▼               │   │
│   │    ┌─────────┐                         ┌─────────┐          │   │
│   │    │ SIGNAL  │                         │ SIGNAL  │          │   │
│   │    │(dataset)│                         │ (file)  │          │   │
│   │    └─────────┘                         └─────────┘          │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│   VERSION: axial_attn-v3 (immutable snapshot, auto-created)        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start (Human + Agent)

### 1) Install + run Goldfish (human)

```bash
git clone https://github.com/lukacf/goldfish.git
cd goldfish
pip install -e ".[dev]"
```

Initialize a project in your ML repo:
```bash
cd my-ml-project
goldfish init
```

### 2) Connect your AI agent (human)

Example with Claude Code:
```bash
claude mcp add goldfish -- uv run --directory /path/to/goldfish goldfish serve
```

### 3) Ask the agent to run an experiment (human)

```
“Create a workspace and run a baseline LSTM on sales_v1.”
```

The agent will call tools like:
```
create_workspace("lstm_baseline", goal="Baseline LSTM")
mount("w1", "lstm_baseline")
register_dataset(... metadata ...)
run("w1", stages=["train"], reason={...})
```

You do not run those tools manually — the agent does.

---

## Limitations

### Compute Backends

Goldfish currently supports two execution backends:

| Backend | Stages Run On | GPU Support | Setup Required |
|---------|---------------|-------------|----------------|
| **Local** | Your machine (Docker) | If available locally | Docker installed |
| **GCE** | Google Compute Engine | H100, A100 profiles | GCP project + auth |

### GCE Requirements

To use GCE for GPU workloads:

1. **GCP Project** with Compute Engine API enabled
2. **Authentication**: Run `gcloud auth application-default login`
3. **GPU Quota**: Request quota for your desired GPU type in your zones
4. **Configuration** in `goldfish.yaml`:

```yaml
gce:
  project_id: your-gcp-project
  zones: [us-central1-a, us-central1-b]  # Zones with GPU quota
```

5. **Resource profiles** available:
   - `cpu-small`, `cpu-large` — CPU-only instances
   - `h100-spot`, `h100-on-demand` — H100 GPU
   - `a100-spot`, `a100-on-demand` — A100 GPU

### Not Yet Supported

- AWS/Azure compute backends
- Kubernetes execution
- Multi-node distributed training
- Real-time collaboration between agents

---

## Configuration

Create `goldfish.yaml` in your project root:

```yaml
project_name: my_ml_project

jobs:
  backend: local  # "local" or "gce"
  timeout: 86400  # Max stage runtime in seconds (default: 24h)

# Required only for GCE backend
gce:
  project_id: my-gcp-project
  zones: [us-central1-a, us-central1-b]
```

See [CONTRIBUTING.md](CONTRIBUTING.md#configuration-reference) for all options.

---

## Documentation

| Document | Purpose |
|----------|---------|
| [README.md](README.md) | This file — overview and quick start |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development setup, architecture, contribution guide |
| [CLAUDE.md](CLAUDE.md) | Instructions for AI agents working on this codebase |
| [docs/svs.md](docs/svs.md) | SVS design and behavior (schema contracts + checks) |
| [llms.txt](llms.txt) | Machine-readable documentation index |

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for:

- Development environment setup
- Architecture walkthrough
- Code style and testing requirements
- Pull request process

```bash
# Quick contribution workflow
git clone https://github.com/lukacf/goldfish.git
cd goldfish
pip install -e ".[dev]"
make install-hooks  # Required: sets up pre-commit hooks
make ci             # Run full test suite
```

---

## License

AGPL-3.0 — see [LICENSE](LICENSE) for details.
