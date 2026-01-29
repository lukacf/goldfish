# Cloud Abstraction Layer

> A protocol-based abstraction for cloud infrastructure, enabling Goldfish to run on any backend without code changes.

## Overview

The cloud abstraction layer isolates provider-specific code (GCP, AWS, local Docker) from Goldfish's core logic. This enables:

1. **Backend portability** - Switch from local Docker to GCE with a config change
2. **Testability** - Test full execution flows without cloud costs using local Docker
3. **Extensibility** - Add new backends (AWS ECS, Azure Container Instances) by implementing protocols

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Goldfish Core                              │
│                                                                     │
│   ┌─────────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│   │ StageExecutor   │   │ ExecutionTools   │   │ PipelineRunner │  │
│   └────────┬────────┘   └────────┬─────────┘   └───────┬────────┘  │
│            │                     │                     │            │
│            └─────────────────────┼─────────────────────┘            │
│                                  │                                  │
│                          ┌───────▼───────┐                          │
│                          │   Protocols   │◄── Backend-agnostic      │
│                          │   Contracts   │    interfaces            │
│                          └───────┬───────┘                          │
└──────────────────────────────────┼──────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
             ┌──────▼──────┐ ┌────▼────┐  ┌──────▼──────┐
             │ LocalBackend│ │GCEBackend│  │ AWSBackend │
             │   (Docker)  │ │  (GCE)   │  │   (ECS)    │
             └─────────────┘ └──────────┘  └────────────┘
                                            (future)
```

## Design Philosophy

### Protocol-Based (Structural Typing)

We use Python's `Protocol` (PEP 544) instead of abstract base classes:

```python
# Protocols define WHAT, not HOW
@runtime_checkable
class RunBackend(Protocol):
    def launch(self, spec: RunSpec) -> RunHandle: ...
    def get_status(self, handle: RunHandle) -> BackendStatus: ...
    def terminate(self, handle: RunHandle) -> None: ...
```

**Why protocols over inheritance:**

1. **No coupling** - Adapters don't inherit from a base class, just implement the interface
2. **Easier testing** - Any object with matching methods satisfies the protocol
3. **Gradual typing** - Works with duck typing while providing type safety
4. **Explicit contracts** - The protocol IS the documentation of required behavior

### Capability-Based Behavior

Different backends have different features. Instead of `if backend == "gce"` conditionals scattered through the code, we use `BackendCapabilities`:

```python
# BAD: Scattered conditionals
if backend_type == "gce":
    timeout = 3.0  # GCE needs longer timeouts
else:
    timeout = 1.0

# GOOD: Capability-driven
timeout = backend.capabilities.ack_timeout_seconds
```

This centralizes backend-specific behavior in one place (the adapter's capabilities) rather than spreading it across the codebase.

---

## Core Components

### File Structure

```
src/goldfish/cloud/
├── protocols.py      # Protocol definitions (RunBackend, ObjectStorage, etc.)
├── contracts.py      # Data types (BackendCapabilities, RunSpec, RunHandle, etc.)
├── factory.py        # AdapterFactory for dependency injection
└── adapters/
    ├── local/        # Docker-based local execution
    │   ├── run_backend.py
    │   ├── storage.py
    │   ├── identity.py
    │   └── image.py
    └── gcp/          # Google Cloud Platform
        ├── run_backend.py
        ├── storage.py
        ├── gce_launcher.py
        ├── signal_bus.py
        └── profiles.py
```

### Protocols (`protocols.py`)

| Protocol | Purpose | Key Methods |
|----------|---------|-------------|
| `RunBackend` | Compute execution | `launch()`, `get_status()`, `get_logs()`, `terminate()` |
| `ObjectStorage` | Blob storage | `put()`, `get()`, `exists()`, `delete()`, `list_prefix()` |
| `SignalBus` | Control plane messaging | `set_signal()`, `get_signal()`, `set_ack()` |
| `InstanceIdentity` | Self-discovery for instances | `get_project_id()`, `get_instance_name()`, `get_zone()` |
| `ImageBuilder` | Container image building | `build()`, `build_async()`, `get_build_status()` |
| `ImageRegistry` | Registry operations | `push()`, `pull()`, `exists()` |

### Contracts (`contracts.py`)

| Type | Purpose | Key Fields |
|------|---------|------------|
| `StorageURI` | Provider-agnostic URI | `scheme`, `bucket`, `path` |
| `RunSpec` | Launch specification | `stage_run_id`, `image`, `command`, `inputs`, `output_uri` |
| `RunHandle` | Opaque run identifier | `stage_run_id`, `backend_type`, `backend_handle` |
| `BackendStatus` | Current run status | `status: RunStatus`, `exit_code`, `termination_cause` |
| `BackendCapabilities` | Backend feature flags | See below |

---

## BackendCapabilities: The Key Pattern

`BackendCapabilities` is a dataclass that describes what a backend can do and how it behaves. This is the **central pattern** of the abstraction layer.

```python
@dataclass
class BackendCapabilities:
    # Feature flags
    supports_gpu: bool = False
    supports_spot: bool = False
    supports_preemption: bool = False
    supports_preemption_detection: bool = False
    supports_live_logs: bool = False
    supports_metrics: bool = False
    max_run_duration_hours: int | None = None

    # Timing behavior
    ack_timeout_seconds: float = 1.0
    ack_timeout_running_seconds: float = 1.0
    has_launch_delay: bool = False
    timeout_becomes_pending: bool = False

    # Messages
    logs_unavailable_message: str = "Logs not available"
    status_message_for_preparing: str = "Preparing..."

    # Zone handling
    zone_resolution_method: str = "config"  # "config" or "handle"
```

### How Capabilities Drive Behavior

Instead of backend conditionals, code checks capabilities:

```python
# Determining timeout for sync operations
def wait_for_sync(backend: RunBackend, handle: RunHandle) -> None:
    cap = backend.capabilities

    # Use capability-defined timeout
    timeout = cap.ack_timeout_seconds

    # Interpret timeout based on capability
    if timeout_exceeded:
        if cap.timeout_becomes_pending:
            # GCE: timeout means "still syncing", not failure
            return SyncStatus.PENDING
        else:
            # Local: timeout means failure
            return SyncStatus.FAILED

    # Use capability-defined message
    if logs_empty:
        return cap.logs_unavailable_message
```

### Comparing Backend Capabilities

| Capability | Local (Docker) | GCE |
|------------|---------------|-----|
| `supports_gpu` | Dynamic (nvidia-docker) | Yes |
| `supports_spot` | No (simulation only) | Yes |
| `supports_preemption` | Yes (SIGTERM) | Yes |
| `supports_preemption_detection` | Configurable | Yes |
| `supports_live_logs` | Yes (docker logs) | Yes (GCS sync) |
| `ack_timeout_seconds` | 1.0s | 3.0s |
| `has_launch_delay` | No | Yes |
| `timeout_becomes_pending` | No | Yes |
| `zone_resolution_method` | "config" | "handle" |

---

## Factory and Dependency Injection

The `AdapterFactory` creates adapters based on configuration:

```python
from goldfish.cloud.factory import AdapterFactory, get_adapter_factory

# Create factory from config
factory = get_adapter_factory(config)

# Create adapters
run_backend = factory.create_run_backend()
storage = factory.create_storage()
signal_bus = factory.create_signal_bus()
image_builder = factory.create_image_builder()
```

### Usage in Core Code

Core code receives adapters via dependency injection, never imports them directly:

```python
# GOOD: Protocol injection
class StageExecutor:
    def __init__(
        self,
        run_backend: RunBackend,
        storage: ObjectStorage,
        ...
    ):
        self.run_backend = run_backend
        self.storage = storage

# BAD: Direct adapter import in core
from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend  # NEVER do this
```

### Factory Methods

| Method | Returns | Local Impl | GCE Impl |
|--------|---------|-----------|----------|
| `create_run_backend()` | `RunBackend` | `LocalRunBackend` | `GCERunBackend` |
| `create_storage()` | `ObjectStorage` | `LocalObjectStorage` | `GCSStorage` |
| `create_signal_bus()` | `SignalBus` | `LocalMetadataBus` | `GCPSignalBus` |
| `create_identity()` | `InstanceIdentity` | `LocalIdentity` | `GCPIdentity` |
| `create_image_builder()` | `ImageBuilder` | `LocalImageBuilder` | `CloudBuildImageBuilder` |
| `create_image_registry()` | `ImageRegistry` | `LocalImageRegistry` | `ArtifactRegistryImageRegistry` |

---

## RunBackend Protocol Deep Dive

The `RunBackend` protocol is the most important abstraction. It manages compute lifecycle.

### Method Reference

```python
@runtime_checkable
class RunBackend(Protocol):
    @property
    def capabilities(self) -> BackendCapabilities:
        """Return capabilities of this backend."""

    def launch(self, spec: RunSpec) -> RunHandle:
        """Launch a new run. Returns handle for status checks."""

    def get_status(self, handle: RunHandle) -> BackendStatus:
        """Get current status. Raises NotFoundError if run doesn't exist."""

    def get_logs(self, handle: RunHandle, tail: int = 200, since: str | None = None) -> str:
        """Get logs from the run."""

    def terminate(self, handle: RunHandle) -> None:
        """Send termination signal. Idempotent."""

    def cleanup(self, handle: RunHandle) -> None:
        """Clean up resources after termination."""

    def get_zone(self, handle: RunHandle) -> str | None:
        """Get execution zone if available."""

    def get_output_dir(self, handle: RunHandle) -> Path | None:
        """Get local output dir (local backend only)."""
```

### RunSpec: What to Run

```python
@dataclass
class RunSpec:
    # Identity
    stage_run_id: str
    workspace_name: str
    stage_name: str

    # Container
    image: str
    command: list[str] | None = None
    env: dict[str, str] = field(default_factory=dict)

    # Resources
    profile: str = "cpu-small"
    machine_type: str | None = None
    gpu_count: int = 0
    gpu_type: str | None = None
    memory_gb: float = 4.0
    cpu_count: float = 2.0

    # Storage
    inputs: dict[str, StorageURI] = field(default_factory=dict)
    output_uri: StorageURI | None = None

    # Options
    spot: bool = False
    timeout_seconds: int | None = None
```

### RunHandle: Opaque Identifier

```python
@dataclass
class RunHandle:
    stage_run_id: str
    backend_type: str  # "local" or "gce"
    backend_handle: str  # Container ID or instance name
    zone: str | None = None
```

Handles are serializable for storage/recovery:

```python
# Serialize to database
data = handle.to_dict()
db.store("handle", json.dumps(data))

# Deserialize from database
data = json.loads(db.get("handle"))
handle = RunHandle.from_dict(data)  # Validates inputs for security
```

### BackendStatus: Run State

```python
@dataclass
class BackendStatus:
    status: RunStatus
    exit_code: int | None = None
    termination_cause: str | None = None  # "preemption", "oom", "timeout", "user"
    message: str | None = None
```

Status is determined from exit codes:

```python
# Helper for interpreting exit codes
BackendStatus.from_exit_code(0)    # -> COMPLETED
BackendStatus.from_exit_code(1)    # -> FAILED
BackendStatus.from_exit_code(137)  # -> TERMINATED (SIGKILL/OOM)
BackendStatus.from_exit_code(143)  # -> TERMINATED (SIGTERM/preemption)
```

---

## ObjectStorage Protocol

Abstracts blob storage across providers.

```python
@runtime_checkable
class ObjectStorage(Protocol):
    def put(self, uri: StorageURI, data: bytes) -> None: ...
    def get(self, uri: StorageURI) -> bytes: ...
    def exists(self, uri: StorageURI) -> bool: ...
    def list_prefix(self, prefix: StorageURI) -> list[StorageURI]: ...
    def delete(self, uri: StorageURI) -> None: ...
    def get_local_path(self, uri: StorageURI) -> Path | None: ...
    def download_to_file(self, uri: StorageURI, destination: Path) -> bool: ...
    def get_size(self, uri: StorageURI) -> int | None: ...
```

### StorageURI: Provider-Agnostic Addressing

```python
# Parse from string
uri = StorageURI.parse("gs://my-bucket/path/to/file.txt")
# -> StorageURI(scheme="gs", bucket="my-bucket", path="path/to/file.txt")

# Create directly
uri = StorageURI("s3", "my-bucket", "path/to/file.txt")

# Convert back to string
str(uri)  # -> "s3://my-bucket/path/to/file.txt"

# Path operations
uri.join("subdir", "file.txt")
# -> StorageURI("s3", "my-bucket", "path/to/file.txt/subdir/file.txt")
```

### Local vs Cloud Mapping

| Cloud URI | Local Path |
|-----------|------------|
| `gs://bucket/path/file.txt` | `.local_gcs/bucket/path/file.txt` |
| `s3://bucket/path/file.txt` | `.local_s3/bucket/path/file.txt` |
| `file:///tmp/data.csv` | `/tmp/data.csv` |

---

## Adding a New Backend

To add support for a new cloud provider (e.g., AWS ECS):

### 1. Create Adapter Directory

```
src/goldfish/cloud/adapters/aws/
├── __init__.py
├── run_backend.py      # Implements RunBackend
├── storage.py          # Implements ObjectStorage
├── identity.py         # Implements InstanceIdentity
└── image.py            # Implements ImageBuilder, ImageRegistry
```

### 2. Implement RunBackend

```python
# src/goldfish/cloud/adapters/aws/run_backend.py

from goldfish.cloud.contracts import (
    BackendCapabilities, BackendStatus, RunHandle, RunSpec, RunStatus
)

AWS_DEFAULT_CAPABILITIES = BackendCapabilities(
    supports_gpu=True,
    supports_spot=True,
    supports_preemption=True,
    supports_preemption_detection=True,
    supports_live_logs=True,
    supports_metrics=True,
    max_run_duration_hours=None,  # ECS has no limit
    ack_timeout_seconds=2.0,
    ack_timeout_running_seconds=2.0,
    has_launch_delay=True,
    logs_unavailable_message="Logs not yet available from CloudWatch",
    timeout_becomes_pending=True,
    status_message_for_preparing="ECS task provisioning...",
    zone_resolution_method="handle",  # Get AZ from task handle
)

class ECSRunBackend:
    """AWS ECS implementation of RunBackend."""

    def __init__(self, region: str, cluster: str, ...):
        self._ecs_client = boto3.client('ecs', region_name=region)
        self._cluster = cluster

    @property
    def capabilities(self) -> BackendCapabilities:
        return AWS_DEFAULT_CAPABILITIES

    def launch(self, spec: RunSpec) -> RunHandle:
        # Convert RunSpec to ECS TaskDefinition + RunTask call
        response = self._ecs_client.run_task(...)
        task_arn = response['tasks'][0]['taskArn']

        return RunHandle(
            stage_run_id=spec.stage_run_id,
            backend_type="ecs",
            backend_handle=task_arn,
            zone=response['tasks'][0].get('availabilityZone'),
        )

    def get_status(self, handle: RunHandle) -> BackendStatus:
        task = self._ecs_client.describe_tasks(
            cluster=self._cluster,
            tasks=[handle.backend_handle]
        )['tasks'][0]

        # Map ECS status to RunStatus
        ecs_status = task['lastStatus']
        if ecs_status == 'RUNNING':
            return BackendStatus(status=RunStatus.RUNNING)
        elif ecs_status == 'STOPPED':
            exit_code = task['containers'][0].get('exitCode', 1)
            return BackendStatus.from_exit_code(exit_code)
        # ... handle other states

    # Implement remaining methods...
```

### 3. Register in Factory

```python
# src/goldfish/cloud/factory.py

def create_run_backend(self) -> RunBackend:
    if self._backend_type == "local":
        return LocalRunBackend(...)
    elif self._backend_type == "gce":
        return GCERunBackend(...)
    elif self._backend_type == "ecs":
        from goldfish.cloud.adapters.aws.run_backend import ECSRunBackend
        return ECSRunBackend(
            region=self._config.aws.region,
            cluster=self._config.aws.cluster,
        )
    else:
        raise ValueError(f"Unknown backend: {self._backend_type}")
```

### 4. Add Configuration

```yaml
# goldfish.yaml
backend: ecs

aws:
  region: us-east-1
  cluster: goldfish-cluster
  task_role_arn: arn:aws:iam::123456789:role/goldfish-task
  execution_role_arn: arn:aws:iam::123456789:role/goldfish-execution
```

### 5. No Changes Needed Elsewhere

Because core code uses the protocol, no changes are needed in:
- `jobs/stage_executor.py`
- `server_tools/execution_tools.py`
- `state_machine/*.py`
- Any other core module

---

## Local Backend for Development

The local backend uses Docker containers to emulate cloud behavior. This enables full testing without cloud costs.

### Configuration

```yaml
# goldfish.yaml
backend: local

local:
  storage:
    root: ".local_gcs"
    consistency_delay_ms: 0

  compute:
    docker_socket: "/var/run/docker.sock"
    simulate_preemption_after_seconds: null  # Set to test preemption handling
    preemption_grace_period_seconds: 30
    zone_availability:
      local-zone-1: true
      local-zone-2: false  # Simulates capacity exhaustion

  signaling:
    metadata_file: ".local_metadata.json"
    size_limit_bytes: 262144  # 256KB (matches GCP limit)
```

### Preemption Simulation

The local backend can simulate spot instance preemption:

```yaml
local:
  compute:
    simulate_preemption_after_seconds: 60  # Preempt after 1 minute
    preemption_grace_period_seconds: 30    # Grace period before SIGKILL
```

When configured:
1. Timer starts at launch
2. After N seconds, SIGTERM sent to container
3. Container has grace period to save state
4. If still running, SIGKILL sent
5. Status shows `termination_cause: preemption`

### Zone Availability Simulation

Test multi-zone fallback without real cloud capacity issues:

```yaml
local:
  compute:
    zone_availability:
      us-central1-a: false  # Simulates capacity exhaustion
      us-central1-b: true   # Fallback zone
```

---

## Testing with Protocols

Protocols make testing straightforward:

```python
# Mock backend for unit tests
class MockRunBackend:
    def __init__(self):
        self._launches = []
        self._statuses = {}

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_gpu=False,
            ack_timeout_seconds=0.1,  # Fast for tests
        )

    def launch(self, spec: RunSpec) -> RunHandle:
        self._launches.append(spec)
        return RunHandle(
            stage_run_id=spec.stage_run_id,
            backend_type="mock",
            backend_handle="mock-123",
        )

    def get_status(self, handle: RunHandle) -> BackendStatus:
        return self._statuses.get(
            handle.stage_run_id,
            BackendStatus(status=RunStatus.RUNNING)
        )

    # ... other methods

# Use in test
def test_stage_executor_launches_run():
    mock_backend = MockRunBackend()
    executor = StageExecutor(run_backend=mock_backend, ...)

    executor.run_stage("train", workspace="w1")

    assert len(mock_backend._launches) == 1
    assert mock_backend._launches[0].stage_name == "train"
```

---

## Common Patterns

### Getting Logs Safely

```python
def get_stage_logs(backend: RunBackend, handle: RunHandle) -> str:
    try:
        return backend.get_logs(handle, tail=200)
    except NotFoundError:
        return backend.capabilities.logs_unavailable_message
```

### Waiting for Completion

```python
def wait_for_completion(
    backend: RunBackend,
    handle: RunHandle,
    timeout: float = 3600,
) -> BackendStatus:
    start = time.time()
    while time.time() - start < timeout:
        status = backend.get_status(handle)
        if status.status.is_terminal():
            return status
        time.sleep(1.0)

    return BackendStatus(
        status=RunStatus.TERMINATED,
        termination_cause="timeout",
    )
```

### Handling Zone Resolution

```python
def get_zone_for_run(backend: RunBackend, handle: RunHandle, config_zones: list[str]) -> str:
    cap = backend.capabilities

    if cap.zone_resolution_method == "handle":
        # GCE: zone is in the handle
        return backend.get_zone(handle) or config_zones[0]
    else:
        # Local: use first config zone
        return config_zones[0]
```

---

## Debugging

### Check Backend Type

```python
from goldfish.cloud.factory import get_adapter_factory

factory = get_adapter_factory(config)
print(f"Backend type: {factory.backend_type}")
# -> "local" or "gce"
```

### Inspect Capabilities

```python
backend = factory.create_run_backend()
cap = backend.capabilities

print(f"GPU support: {cap.supports_gpu}")
print(f"ACK timeout: {cap.ack_timeout_seconds}s")
print(f"Has launch delay: {cap.has_launch_delay}")
```

### Check Handle Details

```python
handle = backend.launch(spec)
print(f"Backend handle: {handle.backend_handle}")
print(f"Zone: {handle.zone}")
print(f"Serialized: {handle.to_dict()}")
```

---

## Related Documentation

- [CLAUDE.md](../CLAUDE.md) - Development guide with architecture overview
- [LOCAL_PARITY_SPEC.md](./de-googlify/LOCAL_PARITY_SPEC.md) - Local backend parity specification
- [COMPLETED.md](./de-googlify/COMPLETED.md) - De-googlify migration history
- [.rct/spec.yaml](../.rct/spec.yaml) - RCT specification for architecture

---

## Summary

The cloud abstraction layer provides:

1. **Protocols** - Define interfaces without implementation coupling
2. **Contracts** - Shared data types for cross-backend compatibility
3. **BackendCapabilities** - Centralized behavior configuration
4. **Factory** - Dependency injection for clean separation
5. **Adapters** - Provider-specific implementations behind protocols

Key principle: **Core code uses protocols, never adapters directly.**

This enables Goldfish to run ML experiments on local Docker for development, GCE for production, and potentially AWS/Azure in the future - all with zero code changes outside the adapters.
