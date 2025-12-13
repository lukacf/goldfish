# Goldfish Provider System

**Provider-based architecture for pluggable execution and storage backends.**

## Overview

Goldfish uses a provider-based architecture to abstract execution and storage infrastructure. This design:

- **Decouples** core logic from cloud-specific implementations (GCP, AWS, Azure, etc.)
- **Enables extensibility** via custom provider implementations
- **Improves testability** with mock providers
- **Maintains backward compatibility** with existing configurations

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Goldfish Core                          │
│  (StageExecutor, DatasetRegistry, SourceRegistry)          │
└───────────────────┬─────────────────┬───────────────────────┘
                    │                 │
       ┌────────────┴────────┐   ┌────┴────────────┐
       │ ExecutionProvider   │   │ StorageProvider │
       └────────────┬────────┘   └────┬────────────┘
                    │                 │
       ┌────────────┼──────┐     ┌────┼────────┐
       │            │      │     │    │        │
    ┌──▼──┐   ┌────▼───┐  │  ┌──▼──┐ ┌──▼───┐ │
    │ GCE │   │ Local  │ ... │ GCS │ │Local │...
    └─────┘   └────────┘     └─────┘ └──────┘
```

## Provider Interfaces

### ExecutionProvider

Handles container image building and stage execution.

**Required Methods:**
- `build_image(image_tag, dockerfile_path, context_path, base_image) -> str`
- `launch_stage(...) -> ExecutionResult`
- `get_status(instance_id) -> ExecutionStatus`
- `get_logs(instance_id, tail) -> str`
- `cancel(instance_id) -> bool`

**Optional Methods:**
- `supports_volumes() -> bool`
- `provision_volume(volume_id, size_gb, region) -> VolumeInfo`
- `mount_volume(instance_id, volume_id, mount_path) -> bool`

### StorageProvider

Handles data upload, download, and metadata operations.

**Required Methods:**
- `upload(local_path, remote_path, metadata) -> StorageLocation`
- `download(remote_path, local_path) -> Path`
- `exists(remote_path) -> bool`
- `get_size(remote_path) -> int | None`

**Optional Methods:**
- `presign(remote_path, expiration_seconds) -> str | None`
- `get_hyperlink(remote_path) -> str | None`
- `snapshot(remote_path, snapshot_id, metadata) -> StorageLocation | None`
- `store_handle(remote_path, handle, metadata) -> bool`
- `retrieve_handle(remote_path) -> str | None`

## Built-in Providers

### GCE Execution Provider

**Name:** `gce`

**Configuration:**
```yaml
providers:
  gce:
    project_id: my-gcp-project
    zone: us-central1-a
    zones: [us-central1-a, us-west1-b]
    bucket: my-bucket
    artifact_registry: us-docker.pkg.dev/my-project/goldfish
    gpu_preference: [h100, a100, none]
    service_account: goldfish@my-project.iam.gserviceaccount.com
```

**Features:**
- Builds Docker images locally and pushes to Artifact Registry
- Launches GCE instances with capacity-aware zone search
- Supports GPU acceleration (H100, A100, etc.)
- Hyperdisk volume provisioning
- GCS-based log storage
- Console hyperlinks for debugging

### GCS Storage Provider

**Name:** `gcs`

**Configuration:**
```yaml
providers:
  gcs:
    bucket: my-bucket
    datasets_prefix: datasets
    artifacts_prefix: artifacts
    snapshots_prefix: snapshots
    sources_prefix: sources
    project_id: my-gcp-project  # For hyperlinks
```

**Features:**
- Upload/download via gsutil
- Presigned URL generation
- GCS Console hyperlinks
- Snapshot support via `gsutil cp`

### Local Execution Provider

**Name:** `local`

**Configuration:**
```yaml
providers:
  local:
    work_dir: /tmp/goldfish  # Optional, defaults to /tmp/goldfish
```

**Features:**
- Runs stages in local Docker containers
- No image registry push (local-only)
- Resource limits (memory, CPU, PIDs)
- Runs as non-root user (UID 1000)

### Local Storage Provider

**Name:** `local`

**Configuration:**
```yaml
providers:
  local:
    base_path: .goldfish/storage
    datasets_prefix: datasets
    artifacts_prefix: artifacts
    snapshots_prefix: snapshots
```

**Features:**
- Filesystem-based storage (copy operations)
- Snapshot support via directory copy
- Metadata stored as .metadata.json files

## Configuration

### New-style Provider Configuration

Explicit provider configuration (recommended):

```yaml
project_name: my-project
dev_repo_path: ../my-project-dev

jobs:
  execution_provider: gce
  storage_provider: gcs

providers:
  gce:
    project_id: my-gcp-project
    zone: us-central1-a
    bucket: my-bucket
    artifact_registry: us-docker.pkg.dev/my-project/goldfish

  gcs:
    bucket: my-bucket
    datasets_prefix: datasets
```

### Legacy Configuration (Backward Compatible)

Old-style configuration still works:

```yaml
project_name: my-project
dev_repo_path: ../my-project-dev

jobs:
  backend: gce  # Auto-maps to execution_provider: gce

gce:
  project_id: my-gcp-project
  zones: [us-central1-a]

gcs:
  bucket: my-bucket
```

**Auto-migration:**
- `backend: gce` → `execution_provider: gce`, `storage_provider: gcs`
- `backend: local` → `execution_provider: local`, `storage_provider: local`
- Legacy `gce` and `gcs` sections auto-converted to provider config

### Mixed Providers

You can mix execution and storage providers:

```yaml
jobs:
  execution_provider: local  # Run stages locally
  storage_provider: gcs      # Store data in GCS

providers:
  gcs:
    bucket: my-bucket
```

## Writing Custom Providers

### Example: S3 Storage Provider

```python
from pathlib import Path
from typing import Any
import boto3

from goldfish.providers.base import StorageProvider, StorageLocation
from goldfish.errors import GoldfishError


class S3StorageProvider(StorageProvider):
    """Storage provider for AWS S3."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)

        self.bucket = config.get("bucket")
        if not self.bucket:
            raise GoldfishError("S3 provider requires 'bucket' configuration")

        self.prefix = config.get("prefix", "goldfish")
        self.s3 = boto3.client("s3")

    def upload(
        self,
        local_path: Path,
        remote_path: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation:
        """Upload to S3."""
        key = f"{self.prefix}/{remote_path}"

        try:
            self.s3.upload_file(
                str(local_path),
                self.bucket,
                key,
                ExtraArgs={"Metadata": metadata or {}},
            )

            size = local_path.stat().st_size if local_path.is_file() else None

            return StorageLocation(
                uri=f"s3://{self.bucket}/{key}",
                size_bytes=size,
                metadata=metadata,
            )

        except Exception as e:
            raise GoldfishError(f"Failed to upload to S3: {e}") from e

    def download(self, remote_path: str, local_path: Path) -> Path:
        """Download from S3."""
        key = f"{self.prefix}/{remote_path}"

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket, key, str(local_path))
            return local_path

        except Exception as e:
            raise GoldfishError(f"Failed to download from S3: {e}") from e

    def exists(self, remote_path: str) -> bool:
        """Check if object exists in S3."""
        key = f"{self.prefix}/{remote_path}"

        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except:
            return False

    def get_size(self, remote_path: str) -> int | None:
        """Get object size from S3."""
        key = f"{self.prefix}/{remote_path}"

        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=key)
            return response.get("ContentLength")
        except:
            return None

    def presign(self, remote_path: str, expiration_seconds: int = 3600) -> str | None:
        """Generate presigned URL."""
        key = f"{self.prefix}/{remote_path}"

        try:
            return self.s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expiration_seconds,
            )
        except:
            return None
```

### Registering Custom Providers

```python
from goldfish.providers import get_storage_registry

# Register the provider
storage_registry = get_storage_registry()
storage_registry.register("s3", S3StorageProvider)
```

Then use in configuration:

```yaml
jobs:
  storage_provider: s3

providers:
  s3:
    bucket: my-s3-bucket
    prefix: goldfish-data
```

## Testing with Mock Providers

Create a simple mock provider for testing:

```python
from goldfish.providers.base import ExecutionProvider, ExecutionResult, ExecutionStatus


class MockExecutionProvider(ExecutionProvider):
    """Mock execution provider for testing."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.launched = []
        self.logs = {}

    def build_image(self, image_tag, dockerfile_path, context_path, base_image):
        return f"mock-{image_tag}"

    def launch_stage(self, image_tag, stage_run_id, **kwargs):
        self.launched.append(stage_run_id)
        return ExecutionResult(
            instance_id=stage_run_id,
            metadata={"backend": "mock"},
        )

    def get_status(self, instance_id):
        return ExecutionStatus(
            state="succeeded",
            exit_code=0,
        )

    def get_logs(self, instance_id, tail=None):
        return self.logs.get(instance_id, "Mock logs")

    def cancel(self, instance_id):
        return True


# Use in tests
from goldfish.jobs.stage_executor import StageExecutor

executor = StageExecutor(
    db=test_db,
    config=test_config,
    workspace_manager=workspace_manager,
    pipeline_manager=pipeline_manager,
    project_root=project_root,
    execution_provider=MockExecutionProvider({}),
)
```

## Migration Guide

### From Hardcoded GCE/GCS to Providers

**Before:**
```python
# stage_executor.py
if self.config.jobs.backend == "gce":
    self.gce_launcher.launch_instance(...)
elif self.config.jobs.backend == "local":
    self.local_executor.launch_container(...)
```

**After:**
```python
# stage_executor.py
result = self.execution_provider.launch_stage(...)
```

### From Direct gsutil to Storage Provider

**Before:**
```python
# datasets/registry.py
cmd = ["gsutil", "cp", str(local_path), gcs_path]
subprocess.run(cmd, check=True)
```

**After:**
```python
# datasets/registry.py
storage_location = self.storage_provider.upload(
    local_path=local_path,
    remote_path=name,
)
```

## Benefits

### Extensibility
- Add Kubernetes execution provider without modifying core code
- Support S3, Azure Blob, MinIO via new storage providers
- Mix and match providers (e.g., local execution + GCS storage)

### Testability
- Mock providers for unit tests
- No infrastructure required for testing core logic
- Verify behavior without actual cloud API calls

### Maintainability
- Clear interface contracts
- Single responsibility per provider
- Easy to update provider implementations independently

### Observability
- Hyperlinks to cloud consoles
- Provider-specific metadata in logs
- Presigned URLs for temporary data access

## FAQ

### Q: Can I use GCE execution with local storage?

Yes! Configure it like this:

```yaml
jobs:
  execution_provider: gce
  storage_provider: local

providers:
  gce:
    project_id: my-project
    zone: us-central1-a
  local:
    base_path: .goldfish/storage
```

### Q: How do I add a custom provider?

1. Implement the provider interface (ExecutionProvider or StorageProvider)
2. Register it with the appropriate registry
3. Configure it in goldfish.yaml

See "Writing Custom Providers" section for details.

### Q: What happens to my existing goldfish.yaml?

It continues to work! The system auto-migrates legacy config:

- `gce` section → `providers.gce`
- `gcs` section → `providers.gcs`
- `backend: gce` → `execution_provider: gce`

### Q: Can I still use backend field?

Yes, it's deprecated but supported for backward compatibility. We recommend migrating to `execution_provider` for clarity.

### Q: How do providers handle profile hints?

Providers receive profile hints in `launch_stage()`:

```python
profile_hints = {
    "zones": ["us-central1-a", "us-west1-b"],
    "use_capacity_search": True,
}

result = provider.launch_stage(
    ...,
    profile_hints=profile_hints,
)
```

Providers can interpret or ignore hints as needed.

### Q: What's the performance impact?

Negligible. Providers are thin wrappers around existing logic with one additional abstraction layer.

## Future Enhancements

**Planned:**
- Kubernetes execution provider
- S3 storage provider
- Azure Blob storage provider
- Provider capabilities discovery API
- Provider health checks
- Multi-region provider failover

**Under Consideration:**
- Remote Docker build via BuildKit
- Serverless execution providers (Cloud Run, Lambda)
- Distributed storage providers (Ceph, HDFS)
- Provider marketplace for community extensions

---

For implementation details, see:
- `src/goldfish/providers/base.py` - Interface definitions
- `src/goldfish/providers/gce_provider.py` - GCE implementation
- `src/goldfish/providers/gcs_provider.py` - GCS implementation
- `src/goldfish/providers/local_provider.py` - Local implementations
