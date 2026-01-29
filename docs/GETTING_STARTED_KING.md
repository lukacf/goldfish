# Getting Started with Goldfish at King

Run ML experiments with Claude Code on King's GCP infrastructure.

---

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Docker
- [Claude Code](https://claude.ai/code) installed
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) authenticated with your King account

---

## Step 1: Request a GCP Project

Goldfish requires a GCP project in **Zone CO, IR, or IU**. Zone CO is recommended as it's typically easier to get approved.

| Zone | Description | Approval Difficulty |
|------|-------------|---------------------|
| **CO** (Recommended) | Cloud-only, King-restricted IAM | Standard |
| **IR** | Internet-facing, restricted IAM | Standard |
| **IU** | Internet-facing, unrestricted IAM | Requires justification |

**Zone KG and PD will NOT work** - they enforce private IPs only, which blocks NVIDIA driver installation and package downloads.

### Request Process

1. Go to [GCP Project Creator](http://to.king.com/gcp-project-creator)
2. Select **"King - Other"**
3. For Zone CO:
   - "Will you be storing sensitive King data?" → **No**
   - "Will IAM access be granted to people outside King?" → **No**
4. Choose project suffix (e.g., `-dev` for development)
5. Submit and wait for approval

---

## Step 2: Configure GCP Project

Once your project is created, configure it for Goldfish.

### Set Variables

```bash
export PROJECT_ID="your-project-id"        # e.g., king-ml-experiments-dev
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
export REGION="us"                          # or "europe" for EU
export BUCKET_NAME="${PROJECT_ID}-goldfish-artifacts"

gcloud config set project ${PROJECT_ID}
```

### Enable APIs

```bash
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com
```

Optional (for Cloud Build-based image builds):
```bash
gcloud services enable cloudbuild.googleapis.com
```

### Create GCS Bucket

```bash
gsutil mb -l ${REGION} gs://${BUCKET_NAME}
```

### Grant Compute Service Account Permissions

King disables automatic IAM grants for default service accounts. You must manually grant permissions:

```bash
# Storage access for inputs/outputs
gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://${BUCKET_NAME}

# Artifact Registry access for Docker images
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

### (Optional) Create Artifact Registry

If you want to use custom base images instead of public fallbacks:

```bash
gcloud artifacts repositories create goldfish \
  --repository-format=docker \
  --location=${REGION} \
  --description="Goldfish Docker images"
```

Grant Cloud Build permission to push images (if using Cloud Build):
```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.objectViewer" \
  gs://${BUCKET_NAME}
```

---

## Step 3: Request GPU Quota

If you plan to use GPU profiles (h100-spot, a100-spot, etc.), request quota:

1. Go to [IAM & Admin > Quotas](https://console.cloud.google.com/iam-admin/quotas) in GCP Console
2. Filter by:
   - Service: `Compute Engine API`
   - Quota: `NVIDIA_H100_GPUS` or `NVIDIA_A100_GPUS`
3. Select the regions you need (e.g., `us-central1`, `europe-west4`)
4. Click "Edit Quotas" and request the number of GPUs needed
5. Provide justification: "ML model training workloads"

Quota approval typically takes 1-2 business days.

---

## Step 4: Install Goldfish

```bash
git clone https://github.com/anthropics/goldfish.git ~/goldfish
cd ~/goldfish
uv pip install -e ".[dev]"
goldfish --version
```

---

## Step 5: Initialize Your ML Project

```bash
mkdir ~/my-ml-project && cd ~/my-ml-project
goldfish init
```

This creates:
```
my-ml-project/
├── goldfish.yaml    # Project configuration
├── pipeline.yaml    # Stage definitions
├── modules/         # Your stage code
└── configs/         # Stage configurations
```

---

## Step 6: Configure goldfish.yaml

Edit `goldfish.yaml` with your King GCP settings:

```yaml
project_name: my-ml-project
dev_repo_path: my-ml-project-dev

gcs:
  bucket: king-ml-experiments-dev-goldfish-artifacts  # Your bucket name

gce:
  project_id: king-ml-experiments-dev                 # Your project ID
  zones:
    - us-central1-a
    - us-central1-b
    - us-central1-c
```

### Optional: Custom Artifact Registry

If you created an Artifact Registry:

```yaml
gce:
  project_id: king-ml-experiments-dev
  artifact_registry: us-docker.pkg.dev/king-ml-experiments-dev/goldfish
  zones:
    - us-central1-a
```

### Optional: Europe Region

For EU-based projects:

```yaml
gce:
  project_id: king-ml-experiments-dev
  artifact_registry: europe-docker.pkg.dev/king-ml-experiments-dev/goldfish
  zones:
    - europe-west4-a
    - europe-west4-b
```

---

## Step 7: Connect to Claude Code

```bash
claude mcp add goldfish -- uv run --directory ~/goldfish goldfish serve --project ~/my-ml-project
```

Verify in Claude Code:
```
> status()
```

You should see your project name and configuration.

---

## Step 8: Build Base Images (Optional)

If you configured Artifact Registry and want custom base images:

**Option A: Local Docker Build**
```
Build the GPU base image locally:
manage_base_images(action="build", image_type="gpu", target="base")
manage_base_images(action="push", image_type="gpu", target="base")
```

**Option B: Cloud Build**
```
Build using Cloud Build:
manage_base_images(action="build", image_type="gpu", target="base", backend="cloud")
```

Without custom images, Goldfish uses public fallbacks:
- CPU: `quay.io/jupyter/pytorch-notebook:python-3.11`
- GPU: `nvcr.io/nvidia/pytorch:24.01-py3`

---

## Step 9: First Run

In Claude Code:

```
Create a workspace called "hello-world", mount it, and create a simple
pipeline with one stage that outputs a numpy array. Run it with cpu-small profile.
```

For GPU training:
```
Run the train stage with h100-spot profile.
```

---

## Troubleshooting

<details>
<summary><strong>Permission denied on GCS</strong></summary>

Verify Compute SA has storage access:
```bash
gsutil iam get gs://${BUCKET_NAME} | grep compute
```

If missing, grant it:
```bash
gsutil iam ch \
  "serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://${BUCKET_NAME}
```
</details>

<details>
<summary><strong>GCE instance can't pull Docker images</strong></summary>

Verify Compute SA has Artifact Registry access:
```bash
gcloud projects get-iam-policy ${PROJECT_ID} \
  --flatten="bindings[].members" \
  --filter="bindings.role:roles/artifactregistry.reader"
```

If missing:
```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```
</details>

<details>
<summary><strong>GPU quota exceeded</strong></summary>

Check your quota:
```bash
gcloud compute regions describe us-central1 \
  --format="table(quotas.filter(metric:NVIDIA))"
```

Request more quota via GCP Console if needed.
</details>

<details>
<summary><strong>Instance fails to start (zone capacity)</strong></summary>

Goldfish automatically tries multiple zones. If all fail, try:
1. Different time of day (GPU availability varies)
2. Spot instances (`h100-spot`) have better availability than on-demand
3. Add more zones to `goldfish.yaml`
4. Try A100 instead of H100
</details>

<details>
<summary><strong>Cloud Build fails to push images</strong></summary>

Cloud Build SA needs Artifact Registry write access:
```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```
</details>

---

## King-Specific Notes

### Zone Restrictions

| Zone | Works? | Why |
|------|--------|-----|
| CO | ✅ | Full Compute Engine access, external IPs allowed |
| IR | ✅ | Full Compute Engine access, external IPs allowed |
| IU | ✅ | Full access (requires justification to create) |
| KG | ❌ | Private IPs only - can't download NVIDIA drivers |
| PD | ❌ | Private IPs only - can't download NVIDIA drivers |

### Default Service Account

King enforces `constraints/iam.automaticIamGrantsForDefaultServiceAccounts` across all zones. The default Compute SA does NOT automatically get Editor permissions. Always grant permissions explicitly (Step 2).

### Centralized Artifact Registry

King has a centralized registry at `king-registry-prod`. You can optionally use it instead of creating your own:
- Location: `europe-docker.pkg.dev/king-registry-prod/<project-name>-docker`
- Requires setup via [KCP Self-Service](https://foundry.int.king.com/docs/project/kcp-self-service-docs/latest/artifact-registry)

### Support

- **GCP Issues**: `#help-google_cloud` Slack channel
- **Cloud Build/AR Issues**: Create a [CLOUD Jira ticket](https://jira.int.midasplayer.com/browse/CLOUD)
- **GPU Quota**: Request via GCP Console, approval ~1-2 days

---

## Next Steps

- [SKILL.md](../.claude/skills/goldfish-ml/SKILL.md) - Complete tool reference
- [Pipeline Guide](../.claude/skills/goldfish-ml/references/pipeline_guide.md) - Signal types and stage wiring
- [Stage Authoring](../.claude/skills/goldfish-ml/references/stage_authoring.md) - Writing training code
