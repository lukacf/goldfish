# Local Parity Specification

> **Phase 0.5 Deliverable:** Define how the local backend emulates GCP semantics.

## Core Principle

**Local is NOT a stub.** Local implements the FULL interface with simulation controls where physical reality doesn't exist. This validates the abstraction layer before we write AWS/Azure adapters.

If local can't implement an interface method, the interface is wrong.

---

## Capability Mapping: GCP → Local

| GCP Capability | Local Equivalent | Notes |
|----------------|------------------|-------|
| GCS bucket | Local directory (`.local_gcs/`) | Same URI semantics |
| GCE instance | Docker container | Same lifecycle states |
| Instance metadata | JSON file (with file locks) | Already implemented in `metadata/local.py` |
| Preemption | SIGTERM to container | Configurable delay |
| Zone capacity | Configurable availability map | Simulates capacity errors |
| Artifact Registry | Local Docker daemon | Standard `docker pull` |

---

## Storage Interface: Local Implementation

### URI Mapping

```
gs://bucket-name/path/to/file  →  .local_gcs/bucket-name/path/to/file
```

The local storage adapter:
1. Parses `gs://` URIs into (bucket, path) tuples
2. Maps to local filesystem under `.local_gcs/`
3. Creates parent directories automatically
4. Uses the same path semantics (trailing slashes, etc.)

### Methods to Implement

```python
from goldfish.cloud.contracts import StorageURI

class LocalObjectStorage(ObjectStorage):
    def put(self, uri: StorageURI, data: bytes) -> None:
        """Write bytes to local file."""

    def get(self, uri: StorageURI) -> bytes:
        """Read bytes from local file. Raises NotFound if missing."""

    def exists(self, uri: StorageURI) -> bool:
        """Return True if file exists."""

    def list_prefix(self, prefix: StorageURI) -> list[StorageURI]:
        """Return all URIs matching prefix."""

    def delete(self, uri: StorageURI) -> None:
        """Delete file. No-op if missing."""

    def get_local_path(self, uri: StorageURI) -> Path | None:
        """Return local path (trivial for local backend)."""
```

### Simulation Controls

```yaml
# goldfish.yaml
local:
  storage:
    root: ".local_gcs"  # Directory for emulated buckets
    consistency_delay_ms: 0  # Delay reads after writes (0 = immediate)
    size_limit_mb: null  # Optional: simulate bucket quota
```

### Consistency Simulation

When `consistency_delay_ms > 0`:
- Writes complete immediately
- Reads issued within N ms of a write to the same path wait until N ms elapsed
- This simulates eventual consistency for testing

---

## Compute Interface: Local Implementation

### Lifecycle State Mapping

| GCP Status | Local Container State | Goldfish State |
|------------|----------------------|----------------|
| PROVISIONING | docker create | PREPARING |
| STAGING | docker start (not ready) | PREPARING |
| RUNNING | Container healthy | RUNNING |
| STOPPING | docker stop issued | RUNNING |
| TERMINATED | Container exited | COMPLETED/FAILED |
| (preempted) | SIGTERM + exit | TERMINATED |

### Methods to Implement

```python
class LocalRunBackend(RunBackend):
    def launch(
        self,
        image: str,
        config: StageConfig,
        inputs: dict[str, str],  # signal_name -> uri
        env: dict[str, str],
    ) -> BackendHandle:
        """Launch container, return handle."""

    def get_status(self, handle: BackendHandle) -> BackendStatus:
        """Return current status (maps container state)."""

    def get_logs(self, handle: BackendHandle, tail: int = 200) -> str:
        """Return container logs."""

    def terminate(self, handle: BackendHandle) -> None:
        """docker stop the container."""

    def cleanup(self, handle: BackendHandle) -> None:
        """docker rm the container."""

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_gpu=True,  # If nvidia-docker installed
            supports_spot=False,  # Simulated only
            supports_preemption=True,  # Via SIGTERM
            supports_live_logs=True,
        )
```

### Preemption Simulation

```yaml
# goldfish.yaml
local:
  compute:
    simulate_preemption_after_seconds: null  # null = no preemption
    preemption_grace_period_seconds: 30  # Match GCP behavior
```

When `simulate_preemption_after_seconds` is set:
1. Launch starts a timer
2. After N seconds, send SIGTERM to container
3. Wait `preemption_grace_period_seconds`
4. If still running, SIGKILL
5. Status becomes TERMINATED with `termination_cause: preemption`

### Capacity Simulation

```yaml
# goldfish.yaml
local:
  compute:
    zone_availability:
      us-central1-a: true
      us-central1-b: false  # Simulates capacity exhaustion
      us-central1-c: true
```

When launcher tries to create container in unavailable zone:
- Raise `CapacityError`
- Launcher tries next zone
- This tests multi-zone fallback logic without real GCP

---

## Signaling Interface: Local Implementation

The local metadata bus is already implemented in `infra/metadata/local.py`.

### Current Implementation

```python
class LocalMetadataBus(MetadataBus):
    """Uses JSON file with file locking for atomicity."""

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None
    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None
    def clear_signal(self, key: str, target: str | None = None) -> None
    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None
    def get_ack(self, key: str, target: str | None = None) -> str | None
```

### Simulation Controls to Add

```yaml
# goldfish.yaml
local:
  signaling:
    metadata_file: ".local_metadata.json"
    size_limit_bytes: 262144  # 256KB per value (GCP limit)
    latency_ms: 0  # Simulated latency
```

### Size Limit Enforcement

When `size_limit_bytes` is set (default: 256KB to match GCP):
- `set_signal` validates `len(signal.model_dump_json()) <= size_limit_bytes`
- Raises `MetadataSizeLimitError` if exceeded
- This matches GCP's 256KB per-value limit

---

## Exit Code Handling

### Implementation

Local backend retrieves exit codes directly from Docker via:
```
docker inspect --format "{{.State.Status}}:{{.State.ExitCode}}" <container_id>
```

This is more direct and reliable than file-based exit code retrieval.
The `BackendStatus.from_exit_code()` helper interprets the exit code.

### Exit Code Semantics

| Container Exit | Local Backend Reports | Goldfish State |
|----------------|----------------------|----------------|
| 0 | exit_code=0 | COMPLETED |
| Non-zero | exit_code=N | FAILED |
| SIGTERM (preemption) | exit_code=143, termination_cause=preemption | TERMINATED |
| SIGKILL (OOM) | exit_code=137, termination_cause=oom | TERMINATED |
| Container removed | NotFoundError raised | TERMINATED |

---

## Directory Structure

### Current Implementation

```
project/
├── .local_gcs/                      # LocalObjectStorage root
│   └── my-bucket/
│       └── inputs/                  # Registered data sources
│           └── data.csv
├── /tmp/goldfish-stage-xxx/         # Temp output dir (per-run)
│   └── outputs/
│       └── model/
├── .local_metadata.json             # LocalMetadataBus state
└── goldfish.yaml                    # Config with local section
```

**Notes:**
- Outputs go to a temp directory, accessible via `backend.get_output_dir(handle)`
- Logs retrieved via `docker logs` command
- Exit codes retrieved via `docker inspect`
- After run completion, caller copies outputs to storage if needed

---

## Configuration Example

```yaml
# goldfish.yaml
backend: local  # or "gcp"

local:
  # Storage
  storage:
    root: ".local_gcs"
    consistency_delay_ms: 0

  # Compute
  compute:
    docker_socket: "/var/run/docker.sock"
    simulate_preemption_after_seconds: null
    zone_availability:
      local-zone-1: true

  # Signaling
  signaling:
    metadata_file: ".local_metadata.json"
    size_limit_bytes: 262144
    latency_ms: 0
```

---

## RCT-LOCAL Test Cases

These tests validate that local emulates GCP correctly:

### Storage Tests
- [ ] `RCT-LOCAL-STORAGE-1`: gs:// URI maps to local path correctly
- [ ] `RCT-LOCAL-STORAGE-2`: upload/download round-trip identical to GCS
- [ ] `RCT-LOCAL-STORAGE-3`: list_prefix returns same results as GCS
- [ ] `RCT-LOCAL-STORAGE-4`: consistency delay works when configured
- [ ] `RCT-LOCAL-STORAGE-5`: missing file raises same error type

### Metadata Tests
- [ ] `RCT-LOCAL-META-1`: 256KB size limit enforced
- [ ] `RCT-LOCAL-META-2`: ack round-trip matches GCP semantics
- [ ] `RCT-LOCAL-META-3`: signal latency simulation works
- [ ] `RCT-LOCAL-META-4`: concurrent access with file locking works

### Compute Tests
- [ ] `RCT-LOCAL-COMPUTE-1`: container lifecycle maps to GCP states
- [ ] `RCT-LOCAL-COMPUTE-2`: preemption simulation triggers SIGTERM
- [ ] `RCT-LOCAL-COMPUTE-3`: zone availability matrix respected
- [ ] `RCT-LOCAL-COMPUTE-4`: logs retrieved correctly

### Exit Code Tests
- [ ] `RCT-LOCAL-EXIT-1`: exit code 0 → COMPLETED
- [ ] `RCT-LOCAL-EXIT-2`: exit code non-zero → FAILED
- [ ] `RCT-LOCAL-EXIT-3`: SIGTERM (143) → TERMINATED + preemption
- [ ] `RCT-LOCAL-EXIT-4`: missing exit code → TERMINATED + crash

---

## Gate 0.5 Exit Criteria

- [x] Local parity specification documented (this file)
- [ ] Simulation controls designed for all capabilities
- [ ] RCT-LOCAL test cases defined (ready to implement)
- [ ] No "not supported" methods in local backend design
