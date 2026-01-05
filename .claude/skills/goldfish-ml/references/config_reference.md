# Goldfish Configuration Reference

Complete schema reference for `goldfish.yaml` - the project configuration file.

## Overview

The `goldfish.yaml` file lives at the root of your project and configures:
- Project identity and paths
- GCE compute settings (for remote execution)
- GCS storage (for artifacts and datasets)
- Resource profiles (machine types and GPUs)

## Minimal Configuration

For local-only execution (Docker on local machine):

```yaml
project_name: my-ml-project
dev_repo_path: my-ml-project-dev
```

## Full Configuration

For GCE remote execution:

```yaml
# Required: Project identity
project_name: my-ml-project
dev_repo_path: my-ml-project-dev    # Relative to project parent dir

# Optional: Workspace settings
workspaces_dir: workspaces          # Default: "workspaces"
slots:                              # Default: ["w1", "w2", "w3"]
  - w1
  - w2
  - w3

# GCS Storage (required for GCE execution)
gcs:
  bucket: my-project-artifacts      # GCS bucket name
  sources_prefix: sources/          # Default: "sources/"
  artifacts_prefix: artifacts/      # Default: "artifacts/"
  snapshots_prefix: snapshots/      # Default: "snapshots/"
  datasets_prefix: datasets/        # Default: "datasets/"

# GCE Compute (required for remote execution)
gce:
  project_id: my-gcp-project        # GCP project ID

  # Optional: Artifact Registry for Docker images
  artifact_registry: us-docker.pkg.dev/my-gcp-project/goldfish

  # Optional: Zone preferences (applies to all profiles)
  zones:
    - us-central1-a
    - us-central1-b
    - us-central1-f

  # Optional: Service account
  service_account: goldfish@my-gcp-project.iam.gserviceaccount.com

  # Optional: Profile overrides and custom profiles
  profile_overrides:
    # Override built-in profile settings
    h100-spot:
      zones:
        - us-west1-a
        - us-west1-b

    # Define custom profile
    my-custom-gpu:
      machine_type: n1-standard-8
      gpu:
        type: t4
        accelerator: nvidia-tesla-t4
        count: 1
      preemptible_allowed: true
      on_demand_allowed: true
      zones:
        - us-central1-a
      boot_disk:
        type: pd-ssd
        size_gb: 200
      data_disk:
        type: pd-ssd
        size_gb: 500
        mode: rw

  # Runtime preferences
  gpu_preference:                   # Default: ["h100", "a100", "none"]
    - h100
    - a100
    - none
  preemptible_preference: spot_first  # or "on_demand_first"
  search_timeout_sec: 900           # Default: 900
  max_attempts: 150                 # Default: 150

# STATE.md settings
state_md:
  path: STATE.md                    # Default: "STATE.md"
  max_recent_actions: 15            # Default: 15

# Audit settings
audit:
  min_reason_length: 15             # Default: 15

# Job execution settings
jobs:
  backend: gce                      # "gce" or "local"
  experiments_dir: experiments      # Default: "experiments"

# SVS (Semantic Validation System) Settings
svs:
  enabled: true                     # Global master switch
  domain: default                   # default, nlp_tokenizer, image_embeddings, tabular_features
  default_policy: warn              # fail, warn, ignore
  default_enforcement: warning      # blocking, warning, silent
  
  # Statistics collection (mechanistic)
  stats_enabled: true
  
  # AI reviews
  ai_pre_run_enabled: true
  ai_post_run_enabled: true
  ai_during_run_enabled: true
  
  # During-run monitoring parameters
  ai_during_run_interval_seconds: 300
  ai_during_run_min_metrics: 200
  ai_during_run_min_log_lines: 20
  ai_during_run_max_runs_per_hour: 12
  ai_during_run_auto_stop: false
  
  # Log filtering for AI monitoring
  ai_during_run_log_filters:
    - "(ERROR|WARN|EXCEPTION|Traceback)"
    - "(CUDA|OOM|nan|inf|segfault|Killed|RuntimeError)"
    - "(loss=|ppl=|acc=|val_|train_|dir_)"
  ai_during_run_log_max_lines: 200
  ai_during_run_log_max_bytes: 16384
  ai_during_run_log_file_max_bytes: 10000000
  ai_during_run_summary_max_chars: 1200

  # Pre-run review specific configuration
  pre_run_review:
    enabled: true
    model: opus
    timeout_seconds: 120
    max_turns: 3

  # Agent configuration (shared by all AI tasks)
  agent_provider: claude_code       # claude_code, codex_cli, gemini_cli, null
  agent_model: opus
  agent_timeout: 120
  agent_max_turns: 3
  rate_limit_per_hour: 60

  # Self-learning failure patterns
  auto_learn_failures: false

# Metrics configuration
metrics:
  backend: wandb                    # wandb, local
  wandb:
    project: market-lm
    entity: my-team
    artifact_mode: file             # file or artifact

# Project invariants (enforced rules)
invariants:
  - "All training must use versioned datasets"
  - "Never delete production models"
```

## Configuration Sections

### project_name (required)

```yaml
project_name: marketlm
```

Identifies the project. Used for:
- Dev repo naming convention
- Logging and audit trails
- State file headers

### dev_repo_path (required)

```yaml
dev_repo_path: marketlm-dev
```

Path to the dev repository (relative to project's parent directory).
- Contains git history, database, and STATE.md
- User project contains only `goldfish.yaml` and `workspaces/`

### gcs (required for GCE)

```yaml
gcs:
  bucket: my-artifacts-bucket
  sources_prefix: sources/
  artifacts_prefix: artifacts/
  snapshots_prefix: snapshots/
  datasets_prefix: datasets/
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `bucket` | Yes | - | GCS bucket name (no `gs://` prefix) |
| `sources_prefix` | No | `sources/` | Path prefix for registered sources |
| `artifacts_prefix` | No | `artifacts/` | Path prefix for stage outputs |
| `snapshots_prefix` | No | `snapshots/` | Path prefix for workspace snapshots |
| `datasets_prefix` | No | `datasets/` | Path prefix for uploaded datasets |

### gce (required for remote execution)

```yaml
gce:
  project_id: my-gcp-project
  artifact_registry: us-docker.pkg.dev/my-gcp-project/goldfish
  zones:
    - us-central1-a
    - us-central1-b
  service_account: goldfish@my-gcp-project.iam.gserviceaccount.com
  profile_overrides:
    h100-spot:
      zones: [us-west1-a]
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `project_id` | Yes | - | GCP project ID (also accepts `project`) |
| `artifact_registry` | No | Auto-created | Docker image registry URL |
| `zones` | No | Per-profile | Global zone preferences |
| `service_account` | No | Default compute SA | Service account for VMs |
| `profile_overrides` | No | `{}` | Override or add profiles (also accepts `profiles`) |
| `gpu_preference` | No | `[h100, a100, none]` | GPU type priority |
| `preemptible_preference` | No | `on_demand_first` | `spot_first` or `on_demand_first` |
| `search_timeout_sec` | No | `900` | Max time searching for capacity |
| `max_attempts` | No | `150` | Max launch retry attempts |

**Note:** `project` is an alias for `project_id`, and `profiles` is an alias for `profile_overrides`.

## Built-in Resource Profiles

These profiles are available by default in `configs/<stage>.yaml`:

```yaml
compute:
  profile: h100-spot
```

| Profile | Machine Type | GPU | Spot | On-Demand | Use Case |
|---------|-------------|-----|------|-----------|----------|
| `cpu-small` | n2-standard-4 | None | Yes | Yes | Light preprocessing |
| `cpu-large` | c4-highcpu-192 | None | Yes | Yes | Heavy data processing |
| `h100-spot` | a3-highgpu-1g | H100 80GB | Yes | No | Training (cost-effective) |
| `h100-on-demand` | a3-highgpu-1g | H100 80GB | No | Yes | Critical training |
| `a100-spot` | a2-highgpu-1g | A100 40GB | Yes | No | Training alternative |
| `a100-on-demand` | a2-highgpu-1g | A100 40GB | Yes | Yes | Guaranteed availability |

### Profile Schema

When defining custom profiles in `profile_overrides`:

```yaml
gce:
  profile_overrides:
    my-custom-profile:
      machine_type: n1-standard-8      # Required: GCE machine type
      gpu:                             # Required: GPU configuration
        type: t4                       # GPU type (t4, v100, a100, h100, none)
        accelerator: nvidia-tesla-t4   # GCE accelerator name (null for none)
        count: 1                       # Number of GPUs (0 for none)
      preemptible_allowed: true        # Allow spot instances
      on_demand_allowed: true          # Allow on-demand instances
      zones:                           # Required: List of zones to try
        - us-central1-a
        - us-central1-b
      boot_disk:                       # Required: Boot disk config
        type: pd-ssd                   # pd-standard, pd-ssd, pd-balanced, hyperdisk-balanced
        size_gb: 200
      data_disk:                       # Required: Data disk config
        type: pd-ssd
        size_gb: 500
        mode: rw                       # rw or ro
```

### Overriding Built-in Profiles

Override specific fields while keeping defaults:

```yaml
gce:
  profile_overrides:
    h100-spot:
      zones:                   # Only override zones
        - us-west1-a
        - us-west1-b
      boot_disk:
        size_gb: 1000          # Bigger boot disk
```

## Common Configurations

### Basic GCE Setup

```yaml
project_name: my-project
dev_repo_path: my-project-dev

gcs:
  bucket: my-project-artifacts

gce:
  project_id: my-gcp-project
```

### Multi-Region with Custom Profiles

```yaml
project_name: trading-ml
dev_repo_path: trading-ml-dev

gcs:
  bucket: trading-ml-artifacts

gce:
  project_id: trading-gcp
  zones:
    - us-central1-a
    - us-east1-b
    - europe-west4-a

  profile_overrides:
    # High-memory preprocessing
    preprocess-large:
      machine_type: n2-highmem-32
      gpu:
        type: none
        accelerator: null
        count: 0
      preemptible_allowed: true
      on_demand_allowed: true
      zones:
        - us-central1-a
      boot_disk:
        type: pd-balanced
        size_gb: 500
      data_disk:
        type: pd-ssd
        size_gb: 2000
        mode: rw

    # Multi-GPU training
    h100-multi:
      machine_type: a3-highgpu-8g
      gpu:
        type: h100
        accelerator: nvidia-h100-80gb
        count: 8
      preemptible_allowed: true
      on_demand_allowed: false
      zones:
        - us-central1-a
      boot_disk:
        type: hyperdisk-balanced
        size_gb: 1000
      data_disk:
        type: hyperdisk-balanced
        size_gb: 4000
        mode: rw
```

## Hot Reloading

After editing `goldfish.yaml`, apply changes without restarting the MCP server:

```
reload_config()
```

Returns confirmation of loaded settings:
```json
{
  "success": true,
  "project_name": "my-project",
  "gce_configured": true,
  "gcs_configured": true
}
```

## Validation

Goldfish validates configuration on load:

1. **Required fields** - `project_name`, `dev_repo_path`
2. **GCE dependencies** - If `gce` is set, `gcs.bucket` must also be set
3. **Profile schema** - Custom profiles must have all required fields
4. **Path existence** - Dev repo path must be valid

Common errors:

| Error | Cause | Fix |
|-------|-------|-----|
| `GCS bucket required for GCE launcher` | Using GCE without GCS config | Add `gcs.bucket` |
| `GCE config requires project_id or project` | Missing project ID | Add `gce.project_id` |
| `Profile missing required field: zones` | Incomplete custom profile | Add all required profile fields |

## Environment Variables

Some settings can be overridden via environment:

| Variable | Overrides |
|----------|-----------|
| `GOLDFISH_PROJECT_ROOT` | Project root path |
| `GOOGLE_CLOUD_PROJECT` | GCE project_id (fallback) |
| `CLOUDSDK_CORE_PROJECT` | GCE project_id (fallback) |

## SVS Configuration (Semantic Validation)

The `svs` section controls code quality oversight and monitoring.

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Master switch for all SVS features |
| `domain` | `default` | Preset for checks/thresholds (see below) |
| `stats_enabled` | `true` | Enable mechanistic stats collection (entropy, nulls, etc.) |
| `ai_pre_run_enabled` | `true` | Enable AI code review before `run()` |
| `ai_post_run_enabled` | `true` | Enable AI output review after completion |
| `ai_during_run_enabled`| `true` | Enable background AI monitoring during execution |

### Domain Profiles

Domain profiles provide optimized thresholds and policies for common ML tasks. Each profile enables specific checks:

| Profile | Target Data | Typical Checks & Guarantees |
|---------|-------------|-----------------------------|
| `default` | General purpose | Validates data existence, file formats, and basic type safety. |
| `nlp_tokenizer` | Tokenizer outputs | Checks for **high entropy** (no mode collapse), **vocab utilization** (no dead tokens), and strict **null ratio** limits. |
| `image_embeddings`| Latent vectors | Monitors **variance** and **sparsity** to detect representation collapse or dead units. |
| `tabular_features`| Preprocessed CSVs | Enforces **column data types**, **missing value** thresholds, and **top-1 frequency** limits to catch data leaks. |

### During-Run Parameters

| Field | Default | Description |
|-------|---------|-------------|
| `ai_during_run_interval_seconds` | `300` | Frequency of background reviews |
| `ai_during_run_min_metrics` | `200` | Data points required before first review |
| `ai_during_run_auto_stop` | `false` | If true, SVS will termination run if critical anomaly detected |

## Metrics Configuration

Goldfish can sync metrics to external backends like Weights & Biases.

| Field | Default | Description |
|-------|---------|-------------|
| `backend` | `local` | `local` or `wandb` |
| `wandb.project` | - | W&B project name |
| `wandb.entity` | - | W&B team/user name |
| `wandb.artifact_mode` | `file` | Use `artifact` for full W&B artifact lineage |
