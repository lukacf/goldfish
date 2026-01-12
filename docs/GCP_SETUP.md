# GCP Setup Guide

This document outlines the Google Cloud Platform configuration required to run Goldfish.

## Required GCP Services

Enable these APIs in your GCP project:

```bash
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

## Service Accounts & IAM Permissions

Goldfish uses several service accounts. Below are the required IAM bindings.

### 1. Cloud Build Service Account

**Account**: `{PROJECT_NUMBER}@cloudbuild.gserviceaccount.com`

This account runs Docker image builds via Cloud Build.

| Resource | Role | Purpose |
|----------|------|---------|
| Project | `roles/cloudbuild.builds.builder` | Run Cloud Build jobs |
| Project | `roles/artifactregistry.writer` | Push images to Artifact Registry |
| GCS Bucket (`mlm-artifacts-*`) | `roles/storage.objectViewer` | Download pre-built wheels (e.g., FA3) |

**Grant GCS access:**
```bash
gsutil iam ch \
  "serviceAccount:{PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.objectViewer" \
  gs://{ARTIFACTS_BUCKET}
```

### 2. Compute Engine Default Service Account

**Account**: `{PROJECT_NUMBER}-compute@developer.gserviceaccount.com`

This account is used by GCE instances running Goldfish stages.

| Resource | Role | Purpose |
|----------|------|---------|
| GCS Bucket (`mlm-artifacts-*`) | `roles/storage.objectAdmin` | Read inputs, write outputs |
| Artifact Registry | `roles/artifactregistry.reader` | Pull Docker images |

### 3. Cloud Build Service Agent

**Account**: `service-{PROJECT_NUMBER}@gcp-sa-cloudbuild.iam.gserviceaccount.com`

This is a Google-managed service agent. Usually has required permissions by default.

| Resource | Role | Purpose |
|----------|------|---------|
| Project | `roles/cloudbuild.serviceAgent` | Managed by GCP |

## Artifact Registry Setup

Create a Docker repository for Goldfish images:

```bash
gcloud artifacts repositories create goldfish \
  --repository-format=docker \
  --location=us \
  --description="Goldfish Docker images"
```

## GCS Bucket Setup

Create a bucket for artifacts (inputs, outputs, wheels):

```bash
gsutil mb -l us gs://{PROJECT_ID}-goldfish-artifacts

# Grant Compute SA access
gsutil iam ch \
  "serviceAccount:{PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://{PROJECT_ID}-goldfish-artifacts

# Grant Cloud Build SA access (for downloading pre-built wheels)
gsutil iam ch \
  "serviceAccount:{PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.objectViewer" \
  gs://{PROJECT_ID}-goldfish-artifacts
```

## Pre-built Wheels

Some packages (like FlashAttention-3) require pre-built wheels because they can't be built during Docker image creation (no GPU available).

### FlashAttention-3 Wheel

**Location**: `gs://your-bucket/wheels/flash_attn_3-3.0.0b1-cp39-abi3-linux_x86_64.whl`

**Built with**:
- PyTorch 2.7.1
- CUDA 12.8
- Python 3.10 (ABI: cp39-abi3, compatible with Python 3.9+)
- Deep Learning VM image: `pytorch-2-7-cu128-ubuntu-2204-nvidia-570`

**Build time**: ~145 minutes on H100 (a3-highgpu-1g)

**Configure in goldfish.yaml**:
```yaml
docker:
  cloud_build:
    fa3_wheel_gcs: gs://your-bucket/wheels/flash_attn_3-3.0.0b1-cp39-abi3-linux_x86_64.whl
```

When `fa3_wheel_gcs` is set, GPU base image builds via Cloud Build will automatically download the wheel using Python SDK (google-cloud-storage) - no gcloud CLI required in the build image.

**Building a new FA3 wheel** (if needed):
```bash
# Create H100 spot instance with Deep Learning VM
gcloud compute instances create fa3-build-$(date +%s) \
  --project=your-gcp-project \
  --zone=us-central1-a \
  --machine-type=a3-highgpu-1g \
  --accelerator=type=nvidia-h100-80gb,count=1 \
  --image-family=pytorch-2-7-cu128-ubuntu-2204-nvidia-570 \
  --image-project=ml-images \
  --boot-disk-size=200GB \
  --maintenance-policy=TERMINATE \
  --provisioning-model=SPOT

# SSH and build (takes ~145 min)
gcloud compute ssh fa3-build-* --zone=us-central1-a --command='
  pip install ninja packaging
  git clone --depth 1 https://github.com/Dao-AILab/flash-attention.git /tmp/fa3-repo
  cd /tmp/fa3-repo/hopper
  export MAX_JOBS=20
  pip wheel . --no-build-isolation --no-deps -w /tmp/wheels/
  gsutil cp /tmp/wheels/flash_attn_3*.whl gs://your-bucket/wheels/
'

# Delete instance when done
gcloud compute instances delete fa3-build-* --zone=us-central1-a --quiet
```

## Verifying Permissions

Check Cloud Build SA permissions:
```bash
# Get project number
PROJECT_NUM=$(gcloud projects describe {PROJECT_ID} --format='value(projectNumber)')

# Check bucket IAM
gsutil iam get gs://{ARTIFACTS_BUCKET} | grep -A2 cloudbuild
```

Check Compute SA permissions:
```bash
gsutil iam get gs://{ARTIFACTS_BUCKET} | grep -A2 compute
```

## Troubleshooting

### "403 Forbidden" when Cloud Build downloads from GCS

Cloud Build SA needs `roles/storage.objectViewer` on the bucket:
```bash
gsutil iam ch \
  "serviceAccount:{PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.objectViewer" \
  gs://{ARTIFACTS_BUCKET}
```

### "Permission denied" when GCE instance accesses GCS

Compute SA needs `roles/storage.objectAdmin` on the bucket:
```bash
gsutil iam ch \
  "serviceAccount:{PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectAdmin" \
  gs://{ARTIFACTS_BUCKET}
```

### Cloud Build can't push to Artifact Registry

Cloud Build SA needs `roles/artifactregistry.writer` at project level:
```bash
gcloud projects add-iam-policy-binding {PROJECT_ID} \
  --member="serviceAccount:{PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```
