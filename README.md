# Goldfish

**ML Experimentation Platform for AI Agents**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![MCP Compatible](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io/)
[![AGPL-3.0 License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![CI](https://github.com/lukacf/goldfish/actions/workflows/ci.yml/badge.svg)](https://github.com/lukacf/goldfish/actions)

[Key Features](#key-features) • [Core Concepts](#core-concepts) • [Quick Start](#quick-start) • [Limitations](#limitations) • [Documentation](#documentation)

---

## The Problem

AI agents are increasingly capable of writing ML code, but they struggle with the **stateful, iterative nature of ML research**:

- Experiments span hours or days — longer than a single conversation
- Data provenance must be tracked across dozens of pipeline runs
- Reproducibility requires exact versioning of code, data, and configuration
- Context is lost when conversations are summarized or restarted

## The Solution

Goldfish gives AI agents the infrastructure to conduct **real ML research**:

```
You: "Implement and compare three attention mechanisms for protein
      structure prediction: standard self-attention, axial attention,
      and linear attention. Use the CASP14 dataset."

AI Agent + Goldfish:
  1. Creates isolated workspace for the experiment
  2. Registers CASP14 dataset with lineage tracking
  3. Defines pipeline: preprocess → train → evaluate
  4. Implements each attention variant as a stage
  5. Runs experiments in containers
  6. Versions everything (code, config, outputs)
  7. Compares results with full provenance
  8. Can resume after any interruption
```

The agent writes pure ML code. Goldfish handles versioning, execution, and state.

---

## Key Features

- **Pre-Run Review** — Claude reviews your code before execution to catch bugs early
- **Pipeline Workflows** — Define ML workflows as YAML, run stages individually or end-to-end
- **Workspace Isolation** — Each experiment is fully isolated; run multiple experiments in parallel
- **Automatic Versioning** — Every run creates an immutable snapshot for reproducibility
- **Data Lineage** — Track the complete journey: raw data → features → models → metrics
- **SVS (Semantic Validation)** — Optional schema contracts and output stats checks to catch silent failures
- **Infrastructure Abstraction** — The agent sees ML concepts; containers and storage are hidden
- **Context Recovery** — STATE.md regenerates automatically so agents can resume after interruption

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

## Quick Start

### 1. Install Goldfish

**Option A: From Git (recommended for development)**

```bash
git clone https://github.com/lukacf/goldfish.git
cd goldfish
pip install -e ".[dev]"
```

**Option B: From PyPI**

```bash
pip install goldfish-mcp
```

### 2. Initialize a Project

```bash
cd my-ml-project
goldfish init
```

This creates:
- `.goldfish/` — Internal state and tracking database
- `goldfish.yaml` — Project configuration
- `pipeline.yaml` — Default pipeline template
- `modules/` — Directory for stage implementations

### 3. Add to Claude Code

**Local installation:**

```bash
claude mcp add goldfish -- uv run --directory /path/to/goldfish goldfish serve
```

**From Git:**

```bash
claude mcp add goldfish -- uvx --from git+https://github.com/lukacf/goldfish goldfish serve
```

Goldfish automatically uses the current working directory as the project root — no path configuration needed.

### 4. Start Experimenting

```
You: "Create a workspace for testing axial attention on protein folding"

Agent: I'll create a workspace for the axial attention experiment.

       [calls create_workspace("axial_attention", goal="...")]
       [calls mount("axial_attention", "w1")]

       Workspace 'axial_attention' created and mounted to slot w1.

       I'll set up the initial pipeline structure with preprocessing,
       training, and evaluation stages...
```

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
