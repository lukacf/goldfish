# Getting Started with Goldfish

Run ML experiments with Claude Code. Automatic provenance, silent failure detection, infrastructure that stays out of your way.

---

## Why Goldfish?

You've been here before:

- **Your model was better three weeks ago.** You've made 50 changes since. Which code, config, and data produced that result? You don't know, because you didn't commit every experiment.

- **Your training run "succeeded" but produced garbage.** The preprocessing stage output zeros due to a dtype mismatch. You wasted 8 GPU hours before noticing.

- **Claude's context reset mid-experiment.** Now it doesn't remember what hypothesis it was testing, which config invariants matter, or that v23 was the best run.

- **You wanted an H100.** Instead you got a week of GCE configuration, Docker builds, GCS mounts, spot preemption handlers, and zone failover logic.

**Goldfish solves these problems:**

| Problem | Solution |
|---------|----------|
| "Which code produced that result?" | Every `run()` auto-commits and tags. `rollback("w1", "v23")` restores the exact code/config. No archaeology. |
| "My model trained on corrupted data" | SVS entropy checks catch mode collapse. Null ratio checks catch data corruption. Pre-run AI review catches logic bugs. Before GPU hours are wasted. |
| "Claude forgot what we were doing" | STATE.md persists goals, hypotheses, best results, and config invariants across context resets. `status()` recovers everything. |
| "I just want a GPU" | `profile: "h100-spot"` in your config. Goldfish handles Docker, GCS, spot pricing, and multi-zone failover. |

---

## Two Paths

| Path | Requirements | Use Case |
|------|--------------|----------|
| **Local** | Docker | Iteration, testing, CPU stages |
| **GCP** | Docker + GCP project | GPU training, production runs |

**Local path**: Skip Step 2 (GCP setup) and Step 5 (base images).

---

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Docker
- [Claude Code](https://claude.ai/code) installed
- For GCP: [gcloud CLI](https://cloud.google.com/sdk/docs/install), authenticated, billing-enabled project

---

## Step 1: Install Goldfish

```bash
git clone https://github.com/lukacf/goldfish.git ~/goldfish
cd ~/goldfish
uv pip install -e ".[dev]"
goldfish --version
```

---

## Step 2: GCP Infrastructure

> **Local only?** Skip to [Step 3](#step-3-initialize-your-project).

### Two GCP Paths

| Path | Build Images | Requirements | Use Case |
|------|--------------|--------------|----------|
| **Minimal** | Locally with Docker, push to AR | Compute + Storage + AR APIs | Don't have/want Cloud Build access |
| **Full** | Cloud Build | Compute + Storage + AR + Cloud Build APIs | Automated builds, faster GPU image builds |

Choose **Minimal** if your org restricts Cloud Build access or you prefer local builds.
Choose **Full** for convenience (Cloud Build handles Docker builds in the cloud).

### What You Need

**APIs to Enable:**

| API | Why | Required |
|-----|-----|----------|
| `compute.googleapis.com` | GCE instances run training stages | Both |
| `storage.googleapis.com` | GCS stores inputs, outputs, checkpoints | Both |
| `artifactregistry.googleapis.com` | Hosts Docker images | Both |
| `cloudbuild.googleapis.com` | Builds Docker images in cloud | Full only |

**Resources to Create:**

| Resource | Name | Purpose |
|----------|------|---------|
| GCS Bucket | `{PROJECT_ID}-goldfish-artifacts` | Stage artifacts, wheels |
| Artifact Registry | `goldfish` (Docker) | Base images |

**Region Selection:**

Cloud Build, Artifact Registry, and GCE must be in compatible regions. Common choices:

| Region | AR Location | GCE Zones | Notes |
|--------|-------------|-----------|-------|
| `us` | `us` (multi-region) | `us-central1-*`, `us-west1-*` | Default, good GPU availability |
| `europe` | `europe` (multi-region) | `europe-west1-*`, `europe-west4-*` | Required for some orgs |

Cloud Build runs in the region where triggers are created. AR multi-regions (`us`, `europe`) work across zones within that continent.

**IAM Permissions:**

| Service Account | Role | Scope | Why | Required |
|-----------------|------|-------|-----|----------|
| `{NUM}-compute@developer.gserviceaccount.com` | `roles/storage.objectAdmin` | Bucket | Read inputs, write outputs | Both |
| `{NUM}-compute@developer.gserviceaccount.com` | `roles/artifactregistry.reader` | Project | Pull Docker images | Both |
| `{NUM}@cloudbuild.gserviceaccount.com` | `roles/artifactregistry.writer` | Project | Push Docker images | Full only |
| `{NUM}@cloudbuild.gserviceaccount.com` | `roles/storage.objectViewer` | Bucket | Download wheels during builds | Full only |
| Your user account | `roles/artifactregistry.writer` | Project | Push images (local build) | Minimal only |

*`{NUM}` = your GCP project number (not project ID)*

### Setup Script

```bash
# Configuration - adjust these for your environment
export PROJECT_ID="your-project-id"
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
export REGION="us"                    # or "europe" for EU
export BUCKET_NAME="${PROJECT_ID}-goldfish-artifacts"

gcloud config set project ${PROJECT_ID}

# === MINIMAL + FULL: Enable APIs ===
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com

# === FULL ONLY: Enable Cloud Build API ===
gcloud services enable cloudbuild.googleapis.com

# === MINIMAL + FULL: Create resources ===
gsutil mb -l ${REGION} gs://${BUCKET_NAME}

gcloud artifacts repositories create goldfish \
  --repository-format=docker \
  --location=${REGION} \
  --description="Goldfish Docker images"

# === MINIMAL + FULL: Compute Engine SA permissions ===
gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://${BUCKET_NAME}

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"

# === MINIMAL ONLY: Your user needs AR write to push locally-built images ===
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="user:$(gcloud config get account)" \
  --role="roles/artifactregistry.writer"

# === FULL ONLY: Cloud Build SA permissions ===
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.objectViewer" \
  gs://${BUCKET_NAME}
```

### Verify

```bash
gcloud services list --enabled | grep -E "compute|storage|artifact"
gsutil ls gs://${BUCKET_NAME}
gcloud artifacts repositories list --location=${REGION}
```

---

## Step 3: Initialize Your Project

```bash
mkdir ~/my-ml-project && cd ~/my-ml-project
goldfish init
```

Creates:
```
my-ml-project/
├── goldfish.yaml    # Project configuration
├── pipeline.yaml    # Stage definitions
├── modules/         # Your stage code
└── configs/         # Stage configurations
```

---

## Step 4: Configure goldfish.yaml

### Local Only

```yaml
project_name: my-ml-project
dev_repo_path: my-ml-project-dev

# Optional: customize defaults
defaults:
  timeout_seconds: 3600     # 1 hour stage timeout (seconds, must be > 0)
  log_sync_interval: 10     # Log sync frequency (seconds, must be > 0)
  backend: local            # Compute backend: local, gce, kubernetes
```

> **Note**: `defaults.backend` controls where stages execute (compute). For storage configuration (where artifacts are stored), see the `storage:` section below.

### With GCP

```yaml
project_name: my-ml-project
dev_repo_path: my-ml-project-dev

gcs:
  bucket: your-project-id-goldfish-artifacts

gce:
  project_id: your-project-id
  zones: ["us-central1-a", "us-central1-b"]

# Optional: customize defaults
defaults:
  timeout_seconds: 7200     # 2 hours for GPU training (seconds, must be > 0)
  log_sync_interval: 15     # Log sync frequency (seconds, must be > 0)
  backend: gce              # Compute backend: local, gce, kubernetes

# Optional: local Docker resource limits (for backend: local)
jobs:
  container_memory: "8g"    # Docker memory limit (e.g., "4g", "8g")
  container_cpus: "4.0"     # Docker CPU limit (e.g., "2.0", "4.0")
  container_pids: 200       # Docker pids limit (e.g., 100, 200)
```

> **Note**: `defaults.backend` controls compute (where stages run). `storage.backend` controls storage (where artifacts are stored). They can be configured independently.

### Advanced: Multi-Backend Storage

Configure storage independently of compute (useful for AWS/Azure users):

```yaml
project_name: my-ml-project
dev_repo_path: my-ml-project-dev

# New unified storage configuration
storage:
  backend: "s3"  # or "gcs", "azure", "local"
  s3:
    bucket: "my-ml-artifacts"
    region: "us-east-1"

# Can still use GCE for compute with S3 for storage
gce:
  project_id: your-project-id
```

**Local storage** (for development/testing without cloud):

```yaml
storage:
  backend: "local"
  local:
    base_path: "/tmp/goldfish-artifacts"  # Local filesystem path
```

> **Note**: S3 and Azure storage adapters are coming soon. GCS and local are fully supported.

---

## Step 5: Build Base Images

> **Local only?** Skip to [Step 6](#step-6-connect-to-claude-code). Local runs use public fallback images.

Base images are built via Claude Code using the `manage_base_images` tool.

### Option A: Cloud Build (Full setup)

In Claude Code:
```
Build the GPU base image using Cloud Build:
manage_base_images(action="build", image_type="gpu", target="base", backend="cloud")
```

Cloud Build runs in the cloud - faster for GPU images, doesn't tie up your machine.

### Option B: Local Docker (Minimal setup)

In Claude Code:
```
Build the GPU base image locally and push to Artifact Registry:
manage_base_images(action="build", image_type="gpu", target="base")
manage_base_images(action="push", image_type="gpu", target="base")
```

Requires Docker running locally. GPU image build takes ~20 minutes.

### Verify

```bash
gcloud artifacts docker images list ${REGION}-docker.pkg.dev/$(gcloud config get project)/goldfish
# Should show: goldfish-base-cpu:v10, goldfish-base-gpu:v10
```

---

## Step 6: Connect to Claude Code

```bash
claude mcp add goldfish -- uv run --directory ~/goldfish goldfish serve --project ~/my-ml-project
```

This tells Claude Code:
- Run Goldfish from its installation at `~/goldfish`
- Use your ML project at `~/my-ml-project`

Verify in Claude Code:
```
> status()
```

You should see your project name and an empty workspace list.

---

## Step 7: First Run

In Claude Code:

```
Create a workspace called "hello-world", mount it, and create a simple
pipeline with one stage that outputs a numpy array. Run it locally with
cpu-small profile.
```

Claude will:
1. Create the workspace (isolated experiment environment)
2. Write the pipeline and stage code
3. Run the stage in Docker
4. The run is automatically versioned - you can reproduce it anytime

---

## Next Steps

### Essential Workflow

1. **Create workspaces** for each experiment branch
2. **Register data sources** with `register_source()`
3. **Define pipelines** with typed signal flow between stages
4. **Run stages** - every run auto-commits, creating reproducible versions
5. **Compare results** - `list_history()` shows all runs with metrics
6. **Roll back** - `rollback("w1", "v23")` restores any previous state

### GPU Training

Use compute profiles in your stage configs:

| Profile | GPU | Use Case |
|---------|-----|----------|
| `cpu-small` | - | Preprocessing, evaluation |
| `a100-spot` | A100 40GB | Training, ~70% cheaper (preemptible) |
| `h100-spot` | H100 80GB | Large models, ~70% cheaper |
| `h100-on-demand` | H100 80GB | Deadline-critical runs |

### Documentation

- [SKILL.md](../.claude/skills/goldfish-ml/SKILL.md) - Complete tool reference
- [Pipeline Guide](../.claude/skills/goldfish-ml/references/pipeline_guide.md) - Signal types and stage wiring
- [Stage Authoring](../.claude/skills/goldfish-ml/references/stage_authoring.md) - Writing training code
- [End-to-End Example](../.claude/skills/goldfish-ml/references/end_to_end_example.md) - Full LLM training walkthrough

---

## Troubleshooting

<details>
<summary><strong>Permission denied on GCS</strong></summary>

Check which account is authenticated:
```bash
gcloud auth list
```

Re-authenticate if needed:
```bash
gcloud auth application-default login
```

Verify bucket permissions:
```bash
gsutil iam get gs://${BUCKET_NAME}
```

Ensure the Compute SA has `storage.objectAdmin`:
```bash
gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://${BUCKET_NAME}
```
</details>

<details>
<summary><strong>Cloud Build fails to push images</strong></summary>

Cloud Build SA needs Artifact Registry write access:
```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```

Check Cloud Build logs in GCP Console for specific errors.
</details>

<details>
<summary><strong>GCE instance can't pull Docker images</strong></summary>

Compute SA needs Artifact Registry read access:
```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```
</details>

<details>
<summary><strong>GCE instance won't start (quota)</strong></summary>

Check your GPU quota in GCP Console (IAM & Admin > Quotas).

H100s require `NVIDIA_H100_GPUS` quota in the region. Request quota increase or try:
- Different zone (availability varies)
- Spot instances (better availability)
- A100 instead of H100
</details>

<details>
<summary><strong>Claude Code doesn't see Goldfish tools</strong></summary>

Verify the MCP connection:
```bash
claude mcp list
```

Re-add if missing:
```bash
claude mcp remove goldfish
claude mcp add goldfish -- uv run --directory ~/goldfish goldfish serve --project ~/my-ml-project
```

Restart Claude Code after adding.
</details>

---

*Questions? File an issue at [github.com/lukacf/goldfish](https://github.com/lukacf/goldfish).*
