# De-Googlify Cloud Abstraction Review

## Overview
The implementation of the cloud abstraction layer has been reviewed against the `main` branch state (inferred) and the provided context. The core protocols, contracts, and adapters are in place and correctly integrated into the main execution engine (`StageExecutor`).

## 1. Abstraction Layer Implementation
**Status:** ✅ **Implemented Correctly**

*   **Protocols (`src/goldfish/cloud/protocols.py`)**: Comprehensive protocols defined for `ObjectStorage`, `RunBackend`, `SignalBus`, `InstanceIdentity`, `ImageBuilder`, and `ImageRegistry`.
*   **Contracts (`src/goldfish/cloud/contracts.py`)**: Strong typing with `RunSpec`, `RunHandle`, `StorageURI`, and `BackendCapabilities`. Security validation is present in `RunHandle.from_dict`.
*   **Factory (`src/goldfish/cloud/factory.py`)**: Correctly instantiates adapters based on configuration (`local` vs `gce`).
*   **Adapters**:
    *   **GCP**: `GCERunBackend` wraps the legacy `GCELauncher`, providing a safe migration path. `CloudBuildImageBuilder` implements the `ImageBuilder` protocol.
    *   **Local**: `LocalRunBackend` re-implements container management logic (using `subprocess` calls to Docker), effectively making the legacy `LocalExecutor` redundant for the new path.

## 2. Integration Points
**Status:** ✅ **Verified**

*   **`src/goldfish/jobs/stage_executor.py`**:
    *   Fully migrated to use `AdapterFactory` and the new protocols.
    *   `_launch_container` uses `self.run_backend.launch()` and respects `BackendCapabilities`.
    *   Image building delegates to the injected `ImageBuilder` (via `DockerBuilder` wrapper).
    *   GCS access and metrics syncing use the `ObjectStorage` protocol.
*   **`src/goldfish/server_tools/execution_tools.py`**:
    *   Tools like `inspect_run`, `logs`, and `cancel` use the `RunBackend` abstraction for status checks and log retrieval.
*   **`src/goldfish/state_machine/cancel.py`**:
    *   Correctly accepts a generic `RunBackend` interface for termination, decoupling it from specific implementations.

## 3. Issues & Findings

### A. Dead Code / Duplication
*   **`src/goldfish/infra/docker_builder.py`**:
    *   **Issue**: The `DockerBuilder` class retains legacy Cloud Build logic in `_build_with_cloud_build` and `_wait_for_cloud_build` (lines ~415-680).
    *   **Impact**: This code is effectively dead because `StageExecutor` injects a `CloudBuildImageBuilder` adapter when the backend is GCE, causing `DockerBuilder.build_image` to bypass the legacy path.
    *   **Recommendation**: Remove `_build_with_cloud_build`, `_wait_for_cloud_build`, and the associated legacy fallback logic in `build_image`.

### B. Redundant Legacy Classes
*   **`src/goldfish/infra/local_executor.py`**:
    *   **Issue**: `LocalRunBackend` (`src/goldfish/cloud/adapters/local/run_backend.py`) re-implements the Docker logic rather than wrapping `LocalExecutor`.
    *   **Impact**: `LocalExecutor` is now likely unused in the main codebase (except potentially in tests or legacy scripts).
    *   **Recommendation**: Audit all references to `LocalExecutor` and verify if it can be deprecated or removed.

### C. Legacy Wrapping vs. Refactoring
*   **`src/goldfish/cloud/adapters/gcp/run_backend.py`**:
    *   **Observation**: This adapter wraps `GCELauncher`. This is a valid strategy for Phase 1, but it leaves the complex `GCELauncher` class as a critical dependency.
    *   **Recommendation**: Plan a future refactor to move logic from `GCELauncher` directly into `GCERunBackend` components to flatten the architecture.

### D. Test Coverage
*   **Status**: Good
*   **Observation**: Unit tests exist for both local and GCP adapters (`tests/unit/cloud/adapters/local/test_run_backend.py`, `tests/unit/cloud/adapters/gcp/test_run_backend.py`).
*   **Gap**: Ensure integration tests (e.g., `tests/integration/test_cloud_integration.py`) fully exercise the *factory-instantiated* paths to catch any configuration wiring issues.

## 4. Summary
The "de-googlify" abstraction layer is successfully implemented and integrated. The system is no longer tightly coupled to GCE/GCP interfaces in the core logic. The primary cleanup task remaining is to remove the duplicated Cloud Build logic in `DockerBuilder` and eventually retire `LocalExecutor`.
