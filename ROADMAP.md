# Goldfish 1.0 Roadmap

> Features required for public release as a general-purpose ML experimentation platform.

## Core Features

### 1. Metrics & Experiment Tracking Integration

Native metrics API with export to popular dashboards.

**API:**
```python
# In stage code (modules/train.py)
from goldfish.metrics import log_metric, log_metrics, log_artifact

log_metric("loss", 0.5)
log_metrics({"accuracy": 0.92, "f1": 0.88})
log_artifact("confusion_matrix", confusion_matrix_fig)
```

**Configuration:**
```yaml
# goldfish.yaml
metrics:
  backend: "wandb"  # or "mlflow", "none"
  wandb:
    project: "my-project"
    entity: "my-team"
  mlflow:
    tracking_uri: "http://localhost:5000"
```

**Scope:**
- Unified metrics API (Goldfish-native)
- W&B backend (export runs, metrics, artifacts)
- MLflow backend (export to tracking server)
- Automatic run linking (Goldfish stage run ↔ external tracker)
- Dashboard-agnostic artifact storage

---

### 2. Multi-Cloud Compute Backends

Abstract away cloud provider specifics. Users choose their infrastructure.

**Cloud Abstraction Layer (COMPLETED):**
The de-googlify refactor (2026-01-24) introduced a protocol-based cloud abstraction:
- `RunBackend`, `ObjectStorage`, `SignalBus` protocols in `cloud/protocols.py`
- `BackendCapabilities` for behavior configuration in `cloud/contracts.py`
- `AdapterFactory` for dependency injection in `cloud/factory.py`
- See [docs/CLOUD_ABSTRACTION.md](docs/CLOUD_ABSTRACTION.md) for full documentation.

**Backends:**
| Backend | Status | Use Case |
|---------|--------|----------|
| `local` | ✅ Complete | Development, small jobs (LocalRunBackend) |
| `gce` | ✅ Complete | GCP users, GPU workloads (GCERunBackend) |
| `kubernetes` | 🔲 New | Enterprise, multi-cloud, on-prem |
| `aws-batch` | 🔲 New | AWS users (implement RunBackend protocol) |
| `azure-ml` | 🔲 Future | Azure users (implement RunBackend protocol) |

**Configuration:**
```yaml
# goldfish.yaml
compute:
  default_backend: "kubernetes"

  backends:
    kubernetes:
      context: "gke_my-project_us-central1_cluster"
      namespace: "goldfish"
      storage_class: "standard"

    gce:
      project: "my-gcp-project"
      zones: ["us-central1-a", "us-central1-c"]

    aws-batch:
      region: "us-east-1"
      job_queue: "goldfish-gpu-queue"
```

**Interface:**
```python
class ComputeBackend(Protocol):
    def launch(self, run: StageRun, image: str, inputs: dict) -> str: ...
    def get_status(self, run_id: str) -> RunStatus: ...
    def get_logs(self, run_id: str, tail: int) -> str: ...
    def cancel(self, run_id: str) -> None: ...
```

---

### 3. Kubernetes Backend

First-class Kubernetes support for portable, fast execution.

**Why Kubernetes:**
- 5-15s pod startup vs 60-90s VM boot
- Native GPU scheduling via device plugins
- Works on any cloud (GKE, EKS, AKS) or on-prem
- Leverages existing enterprise infrastructure
- Built-in spot/preemptible handling

**Implementation:**
- Kubernetes Job per stage run
- PVC or GCS FUSE for input/output mounting
- ConfigMap for stage configuration
- GPU nodeSelector/tolerations from resource profiles
- Log streaming via Kubernetes API

**Resource Profiles → K8s Resources:**
```yaml
# User writes:
compute:
  profile: "gpu-small"

# Goldfish translates to:
resources:
  requests:
    nvidia.com/gpu: 1
    memory: "16Gi"
  limits:
    nvidia.com/gpu: 1
    memory: "32Gi"
nodeSelector:
  cloud.google.com/gke-accelerator: "nvidia-tesla-t4"
```

---

### 4. Configuration Flexibility

User-customizable settings without forking.

**Resource Profiles:**
```yaml
# goldfish.yaml
compute_profiles:
  # Override built-in profiles
  h100-spot:
    zones: ["us-central1-a"]  # Restrict to our quota

  # Define custom profiles
  team-gpu:
    backend: "kubernetes"
    resources:
      gpu_type: "nvidia-tesla-t4"
      gpu_count: 1
      memory: "16Gi"
      cpu: 4
    spot: true

  cpu-highmem:
    backend: "gce"
    machine_type: "n2-highmem-8"
    spot: true
```

**Defaults:**
```yaml
defaults:
  timeout_seconds: 7200
  log_sync_interval: 15
  backend: "kubernetes"

docker:
  base_image: "us-docker.pkg.dev/my-project/ml/base:latest"
  registry: "us-docker.pkg.dev/my-project/goldfish"
```

**Storage:**
```yaml
storage:
  backend: "gcs"  # or "s3", "azure", "local"
  gcs:
    bucket: "my-goldfish-artifacts"
  s3:
    bucket: "my-goldfish-artifacts"
    region: "us-east-1"
```

---

### 5. Local-First Experience

Zero cloud setup required to get started.

**What works locally:**
- `goldfish init` creates project structure
- Workspaces, pipelines, stages all work
- Local Docker execution
- SQLite database (no external deps)
- Local filesystem for artifacts

**Getting started:**
```bash
pip install goldfish-ml
goldfish init my-project
cd my-project
# Edit pipeline.yaml, write stages
goldfish run w1  # Runs in local Docker
```

**Cloud becomes opt-in:**
```yaml
# goldfish.yaml - only needed when scaling up
compute:
  default_backend: "gce"  # Switch from local
  gce:
    project: "my-project"
    # ...
```

---

### 6. Storage Backend Abstraction (COMPLETED)

Support for major cloud storage providers.

**ObjectStorage Protocol (COMPLETED):**
The `ObjectStorage` protocol in `cloud/protocols.py` provides:
- `put(uri, data)` / `get(uri)` / `exists(uri)` / `delete(uri)`
- `list_prefix(prefix)` - list objects with given prefix
- `get_local_path(uri)` - get local path if available
- Provider-agnostic `StorageURI` addressing (gs://, s3://, file://)

**Backends:**
| Backend | Status | Notes |
|---------|--------|-------|
| `local` | ✅ Complete | LocalObjectStorage (filesystem) |
| `gcs` | ✅ Complete | GCSStorage (Google Cloud Storage) |
| `s3` | 🔲 New | Implement ObjectStorage protocol |
| `azure` | 🔲 Future | Implement ObjectStorage protocol |

---

## Polish & Hardening

Consolidated improvements for production readiness:

- **Documentation**: Installation guide, quickstart tutorial, API reference
- **Security**: Secrets via env vars, credential provider abstraction
- **Examples**: Example project repository with common patterns
- **Error messages**: Clear, actionable errors for common issues
- **CLI improvements**: Better `goldfish` CLI with status, logs, cancel commands

---

### 7. Hyperparameter Sweep Abstraction

Native sweep definition with pluggable search backends.

**The insight:** Sweeps are orchestrated runs with config variations. Goldfish already has the primitives (`run()` + `config_override`). What we abstract is the **search algorithm**.

**Sweep Definition (Goldfish-native):**
```yaml
# sweeps/learning_rate_search.yaml
name: lr-search
stage: train
metric:
  name: val_accuracy
  goal: maximize

parameters:
  learning_rate:
    type: log_uniform
    min: 0.0001
    max: 0.1
  batch_size:
    type: choice
    values: [16, 32, 64, 128]
  dropout:
    type: uniform
    min: 0.1
    max: 0.5

search:
  method: bayesian    # or "grid", "random"
  max_runs: 50
  early_terminate:
    type: hyperband
    min_iter: 10
```

**Execution:**
```python
# MCP tool
sweep("w1", sweep="learning_rate_search", reason="Finding optimal LR")

# Returns sweep_id, tracks all child runs
```

**Backend abstraction:**
```python
class SweepBackend(Protocol):
    def suggest(self, sweep_config: SweepConfig, history: list[RunResult]) -> dict:
        """Return next config to try based on search method."""
        ...

    def should_stop(self, sweep_config: SweepConfig, history: list[RunResult]) -> bool:
        """Early termination decision."""
        ...
```

**Backends:**
| Backend | Status | Notes |
|---------|--------|-------|
| `random` | 🔲 Built-in | No external deps, good baseline |
| `grid` | 🔲 Built-in | Exhaustive search |
| `optuna` | 🔲 New | Bayesian optimization, TPE, CMA-ES |
| `wandb` | 🔲 New | W&B sweep agent integration |

**Why abstract:**
- Start with Optuna/W&B for smart search algorithms
- Can add native Bayesian optimization later
- Sweep history tracked in Goldfish (full provenance)
- Not locked into any single provider

**Relationship to metrics:**
- Sweeps depend on metrics abstraction (need to read `val_accuracy`)
- Sweep runs are regular stage runs (inherit all provenance)
- Results exportable to W&B/MLflow for visualization

---

## Out of Scope for 1.0

Explicitly not building:

- **Multi-user / authentication** - Goldfish is single-player
- **Web dashboard** - Use W&B/MLflow for visualization
- **Real-time collaboration** - Not the product vision
- **Model serving** - Different product category

---

## Dependencies & Sequencing

```
                    ┌─────────────────┐
                    │ Storage Backend │
                    │   Abstraction   │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      ┌──────────────┐ ┌──────────┐ ┌──────────────┐
      │  Kubernetes  │ │   AWS    │ │    Local     │
      │   Backend    │ │  Batch   │ │  Experience  │
      └──────────────┘ └──────────┘ └──────────────┘
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                    ┌─────────────────┐
                    │    Metrics      │
                    │   Abstraction   │
                    └─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Hyperparameter  │
                    │     Sweeps      │
                    └─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Configuration  │
                    │   Flexibility   │
                    └─────────────────┘
```

**Suggested order:**
1. Storage backend abstraction (foundation for everything)
2. Local-first experience (unblocks users without cloud)
3. Kubernetes backend (fastest path to multi-cloud)
4. AWS Batch backend (expands addressable market)
5. Metrics abstraction (dashboard integration)
6. Hyperparameter sweeps (depends on metrics)
7. Configuration flexibility (ongoing, can parallelize)

---

## Completed Work

- **Cloud abstraction layer** (2026-01-24): Fully implemented via de-googlify refactor
  - Protocol-based design: RunBackend, ObjectStorage, SignalBus
  - BackendCapabilities pattern for behavior configuration
  - LocalRunBackend and GCERunBackend adapters
  - LocalObjectStorage and GCSStorage adapters
  - AdapterFactory for dependency injection
  - See [docs/CLOUD_ABSTRACTION.md](docs/CLOUD_ABSTRACTION.md)
- **GCE backend**: Production-ready
- **Local Docker backend**: Production-ready with storage integration
