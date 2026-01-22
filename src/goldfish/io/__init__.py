"""Goldfish IO Library - Storage abstraction for modules.

This library is deployed in Docker containers and provides
a simple API for modules to load inputs and save outputs
without worrying about storage backends (GCS/hyperdisk/local).

Also provides heartbeat functionality for job health monitoring.
"""

import importlib.util
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from goldfish.cloud.contracts import StorageURI
from goldfish.errors import GoldfishError
from goldfish.metrics import finish as finish_metrics
from goldfish.metrics import log_artifact, log_metric, log_metrics

if TYPE_CHECKING:
    from goldfish.cloud.protocols import ObjectStorage

logger = logging.getLogger(__name__)

_StatsJob: type[Any] | None
# Stats are optional in container images; allow graceful degradation if missing
try:
    from goldfish.io.stats import StatsJob as _StatsJob

    _HAS_STATS = True
except Exception:
    _StatsJob = None
    _HAS_STATS = False
_WARNED_MISSING_STATS = False
_WARNED_NULL_SCHEMA_INPUTS: set[str] = set()
_WARNED_NULL_SCHEMA_OUTPUTS: set[str] = set()

# Heartbeat configuration
HEARTBEAT_DIR = ".goldfish"
HEARTBEAT_FILE = "heartbeat"
_last_heartbeat_time: float = 0
_heartbeat_min_interval: float = 1.0  # Don't write more than once per second

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

try:
    import pandas as pd
except ImportError:
    pd = None


def _get_stage_config() -> dict[str, Any]:
    """Load stage configuration from environment."""
    config_json = os.environ.get("GOLDFISH_STAGE_CONFIG")
    if not config_json:
        raise RuntimeError("GOLDFISH_STAGE_CONFIG not set")
    result: dict[str, Any] = json.loads(config_json)
    return result


def _get_inputs_dir() -> Path:
    """Get inputs directory (configurable for testing)."""
    return Path(os.environ.get("GOLDFISH_INPUTS_DIR", "/mnt/inputs"))


def _get_outputs_dir() -> Path:
    """Get outputs directory (configurable for testing)."""
    return Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))


def _load_directory_for_validation(dir_path: Path, schema: dict[str, Any]) -> Any:
    """Load directory contents for schema validation.

    For tensor schemas with arrays, loads NPZ files and returns a dict-like
    object that can be validated against the schema.

    Args:
        dir_path: Path to the output directory
        schema: Schema definition from pipeline.yaml

    Returns:
        Dict-like object for validation, or None if validation should be skipped
    """
    kind = schema.get("kind")
    arrays = schema.get("arrays")

    # Only handle tensor schemas with explicit array definitions
    if kind != "tensor" or not isinstance(arrays, dict):
        logger.warning(
            "Directory output with schema kind='%s' - skipping validation (only tensor+arrays supported)",
            kind,
        )
        return None

    if np is None:
        logger.warning("NumPy not available - skipping directory schema validation")
        return None

    # Look for NPZ files in the directory
    npz_files = list(dir_path.glob("*.npz"))
    if not npz_files:
        # Also check for individual .npy files
        npy_files = list(dir_path.glob("*.npy"))
        if npy_files:
            # Load individual npy files into a dict
            result = {}
            for npy_file in npy_files:
                array_name = npy_file.stem
                if array_name in arrays:
                    result[array_name] = np.load(npy_file)
            if result:
                return result
        logger.warning(
            "No NPZ or NPY files found in directory '%s' - skipping schema validation",
            dir_path,
        )
        return None

    # Load the first NPZ file (typically there's one main one like events.npz)
    npz_path = npz_files[0]
    if len(npz_files) > 1:
        logger.debug("Multiple NPZ files found, using '%s' for validation", npz_path.name)

    try:
        # np.load returns NpzFile which is dict-like (supports __getitem__, keys())
        return np.load(npz_path, allow_pickle=False)
    except Exception as e:
        logger.warning("Failed to load NPZ '%s' for validation: %s", npz_path, e)
        return None


def load_input(name: str, format: str | None = None) -> Any:
    """Load input signal or dataset.

    Args:
        name: Input name (from pipeline.yaml)
        format: Override format (if not in config)

    Returns:
        - Python object (array, dataframe) if auto-loadable
        - Path if format requires manual loading
        - Custom loader result if specified in config

    Examples:
        # Auto-load NPY
        features = load_input("features")  # Returns np.ndarray

        # Auto-load CSV
        df = load_input("raw_data")  # Returns pd.DataFrame

        # Manual load
        path = load_input("model_dir")  # Returns Path
        model = torch.load(path / "model.pt")
    """
    # Auto-start during-run monitor on first goldfish.io call
    from goldfish.io.bootstrap import _ensure_monitor_started

    _ensure_monitor_started()

    config = _get_stage_config()
    input_config = config.get("inputs", {}).get(name)

    if not input_config:
        raise ValueError(f"Input '{name}' not defined in stage config")

    # Schema is required; null schema emits a warning (non-blocking)
    if "schema" in input_config and input_config.get("schema") is None:
        if name not in _WARNED_NULL_SCHEMA_INPUTS:
            logger.warning(
                "Input '%s': schema is null; contract validation skipped (recommended to define schema).",
                name,
            )
            _WARNED_NULL_SCHEMA_INPUTS.add(name)

    # Check for custom loader
    if "loader" in input_config:
        return _run_custom_loader(name, input_config["loader"])

    # Get input path - prefer /mnt/inputs mount, fall back to config location
    input_path = _get_inputs_dir() / name

    # For local execution, use location from config if mount doesn't exist
    if not input_path.exists() and not input_path.with_suffix(".npy").exists():
        location = input_config.get("location")
        if location:
            # Check for failed override resolution (dictionary passed as string)
            if isinstance(location, str) and location.startswith("{") and "from_" in location:
                raise ValueError(
                    f"Input '{name}' has unresolved override: {location}. "
                    "This usually means the specified run_id or signal could not be found."
                )
            # IMPORTANT: Don't use pathlib.Path() on GCS URIs - it corrupts gs:// to gs:/
            # GCS URIs should have been staged to /mnt/inputs by the infrastructure layer
            if isinstance(location, str) and location.startswith("gs://"):
                raise FileNotFoundError(
                    f"Input '{name}' not found at /mnt/inputs/{name}. "
                    f"GCS location {location} was not staged properly. "
                    "This usually means the GCE staging commands failed. "
                    "Check the staging_debug.log in the job's logs directory."
                )
            input_path = Path(location)

    # Auto-load based on format
    fmt = format or input_config.get("format", "file")

    if fmt == "npy":
        if np is None:
            raise ImportError("NumPy is required to load NPY files")
        # Try with .npy extension first
        if not str(input_path).endswith(".npy"):
            npy_path = input_path.with_suffix(".npy")
            if npy_path.exists():
                return np.load(npy_path)
        # Try exact path
        if input_path.exists() and input_path.is_file():
            return np.load(input_path)
        # Try as directory with .npy files
        if input_path.exists() and input_path.is_dir():
            npy_files = list(input_path.glob("*.npy"))
            if not npy_files:
                raise FileNotFoundError(f"No .npy files in {input_path}")
            return np.load(npy_files[0])
        raise FileNotFoundError(f"Input not found: {input_path}")

    elif fmt == "csv":
        if pd is None:
            raise ImportError("Pandas is required to load CSV files")
        # Try with .csv extension first
        if not str(input_path).endswith(".csv"):
            csv_path = input_path.with_suffix(".csv")
            if csv_path.exists():
                return pd.read_csv(csv_path)
        # Try exact path
        if input_path.exists() and input_path.is_file():
            return pd.read_csv(input_path)
        # Try as directory with .csv files
        if input_path.exists() and input_path.is_dir():
            csv_files = list(input_path.glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"No .csv files in {input_path}")
            return pd.read_csv(csv_files[0])
        raise FileNotFoundError(f"Input not found: {input_path}")

    elif fmt in ("directory", "file", "dataset"):
        # Return path for manual loading
        # "dataset" is a Goldfish registered source, treated as a directory
        if not input_path.exists():
            raise FileNotFoundError(f"Input not found: {input_path}")
        return input_path

    else:
        raise ValueError(f"Unknown format: {fmt}")


def save_output(name: str, data: Any, artifact: bool = False):
    """Save output signal or artifact.

    Args:
        name: Output name (from pipeline.yaml)
        data: Data to save or Path to existing file
        artifact: Mark as permanent artifact (vs. ephemeral signal)

    Examples:
        # Auto-save NPY
        save_output("tokens", tokens_array)

        # Auto-save CSV
        save_output("results", df)

        # Manual save
        path = get_output_path("model")
        torch.save(model, path / "model.pt")
        save_output("model", path, artifact=True)
    """
    config = _get_stage_config()
    output_config = config.get("outputs", {}).get(name)

    if not output_config:
        raise ValueError(f"Output '{name}' not defined in stage config")

    # Enforce output schema contract (SVS law) when provided
    schema = output_config.get("schema")
    if schema is None:
        if name not in _WARNED_NULL_SCHEMA_OUTPUTS:
            logger.warning(
                "Output '%s': schema is null; contract validation skipped (recommended to define schema).",
                name,
            )
            _WARNED_NULL_SCHEMA_OUTPUTS.add(name)
    if schema:
        validation_data = data
        # For directory outputs with tensor schemas, load NPZ files for validation
        if isinstance(data, Path) and data.is_dir():
            validation_data = _load_directory_for_validation(data, schema)
        elif isinstance(data, Path):
            raise GoldfishError(f"Output '{name}' schema validation requires in-memory data, got Path")

        if validation_data is not None:
            from goldfish.svs.contract import validate_output_data_against_schema

            errors = validate_output_data_against_schema(name, schema, validation_data)
            if errors:
                from goldfish.io.bootstrap import _load_svs_config

                svs_config = _load_svs_config()
                enforcement = "silent" if not svs_config.enabled else svs_config.default_enforcement
                message = f"Output '{name}' schema mismatch: " + "; ".join(errors)

                if enforcement == "blocking":
                    raise GoldfishError(message)
                if enforcement == "warning":
                    logger.warning(message)

    output_path = _get_outputs_dir() / name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Handle Path input (already saved)
    if isinstance(data, Path):
        if artifact:
            _mark_as_artifact(name)
        return

    # Auto-save based on format
    fmt = output_config.get("format", "file")

    if fmt == "npy":
        if np is None:
            raise ImportError("NumPy is required to save NPY files")
        if not str(output_path).endswith(".npy"):
            output_path = output_path.with_suffix(".npy")
        np.save(output_path, data)

    elif fmt == "csv":
        if pd is None:
            raise ImportError("Pandas is required to save CSV files")
        if not str(output_path).endswith(".csv"):
            output_path = output_path.with_suffix(".csv")
        data.to_csv(output_path, index=False)

    else:
        raise ValueError(f"Cannot auto-save format '{fmt}'. Use get_output_path() for manual saving.")

    # Enqueue stats if SVS is enabled
    from goldfish.io.bootstrap import _get_stats_queue, _svs_enabled

    if _svs_enabled():
        global _WARNED_MISSING_STATS
        if not _HAS_STATS or _StatsJob is None:
            if not _WARNED_MISSING_STATS:
                logger.warning("SVS stats unavailable: goldfish.io.stats missing in container image")
                _WARNED_MISSING_STATS = True
        else:
            try:
                stats_queue = _get_stats_queue()
                stats_queue.enqueue(
                    _StatsJob(
                        name=name,
                        path=output_path,
                        dtype=str(getattr(data, "dtype", "unknown")),
                    )
                )
            except Exception as e:
                # Stats are best-effort; don't fail the stage if enqueuing fails
                logger.warning(f"Failed to enqueue stats for {name}: {e}")

    if artifact:
        _mark_as_artifact(name)


def get_config() -> dict[str, Any]:
    """Get stage configuration.

    Returns the full stage config dict from GOLDFISH_STAGE_CONFIG.
    """
    # Auto-start during-run monitor on first goldfish.io call
    from goldfish.io.bootstrap import _ensure_monitor_started

    _ensure_monitor_started()

    return _get_stage_config()


def get_input_path(name: str) -> Path:
    """Get path to input for manual loading.

    Goldfish pre-downloads to this location if needed.
    """
    return _get_inputs_dir() / name


def get_output_path(name: str) -> Path:
    """Get path to write output for manual saving.

    For 'file' type outputs, write directly to the returned path.
    For 'directory' type outputs, call .mkdir() on the returned path first.

    Goldfish uploads from this location after stage completes.
    """
    path = _get_outputs_dir() / name
    # Create parent directory (not the path itself - for file type outputs)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _run_custom_loader(name: str, loader_config: dict) -> Any:
    """Execute custom loader function."""
    script = loader_config["script"]
    function = loader_config["function"]

    # SECURITY: Validate that script path is safe
    # Prevent path traversal, symlink attacks, and arbitrary file execution
    script_path = Path(script)

    # CRITICAL: Check for symlinks BEFORE resolving
    if script_path.is_symlink():
        raise ValueError(f"Custom loader script cannot be a symlink (security risk). Got: {script}")

    # Resolve path (but we already checked it's not a symlink)
    try:
        script_path = script_path.resolve(strict=True)
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Invalid custom loader script path: {e}") from e

    # Check if we're running in Docker (production) or tests
    app_path = Path("/app")
    if app_path.exists():
        # Running in Docker - enforce strict /app directory restriction
        app_resolved = app_path.resolve()
        try:
            script_path.relative_to(app_resolved)
        except ValueError as e:
            raise ValueError(
                f"Custom loader script must be within /app directory. Got: {script}, resolved to: {script_path}"
            ) from e

        # Additional check: script must be in loaders/ directory
        if "loaders" not in script_path.parts:
            raise ValueError(f"Custom loader script must be in loaders/ directory. Got: {script}")

        # Verify no intermediate symlinks in the resolved path
        for parent in script_path.parents:
            if parent.is_symlink() and parent != app_resolved:
                raise ValueError(f"Custom loader path contains symlink: {parent}")
    else:
        # Running in tests - just check script exists and filename is valid
        if not script_path.exists():
            raise FileNotFoundError(f"Custom loader script not found: {script}")

        # Basic security: no path traversal components
        if ".." in script_path.parts:
            raise ValueError(f"Custom loader script path contains path traversal. Got: {script}")

    # Validate function name (prevent calling private/dangerous functions)
    if not function.isidentifier() or function.startswith("_"):
        raise ValueError(
            f"Invalid function name: {function}. Must be a valid identifier and not start with underscore."
        )

    # Import custom loader
    spec = importlib.util.spec_from_file_location("custom_loader", str(script_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load custom loader from {script}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Call loader function
    loader_fn = getattr(module, function)
    input_path = get_input_path(name)
    return loader_fn(input_path)


def _mark_as_artifact(name: str):
    """Mark output as permanent artifact."""
    # Write marker file for Goldfish to detect
    marker_path = _get_outputs_dir() / ".artifacts" / name
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.touch()


# =============================================================================
# Heartbeat API - Job health monitoring
# =============================================================================


def heartbeat(message: str | None = None, force: bool = False) -> None:
    """Signal that the job is alive and working.

    Call this periodically in long-running computations to prevent
    the job from being killed by the supervisor due to inactivity.

    The heartbeat is written to a file that the supervisor monitors.
    If no heartbeat is received for a configured timeout, the job
    is considered stalled and will be terminated.

    Args:
        message: Optional status message (e.g., "Processing batch 50/100")
        force: Write even if called recently (default: rate-limited to 1/sec)

    Example:
        from goldfish.io import heartbeat

        for i, batch in enumerate(data_loader):
            heartbeat(f"Processing batch {i}/{total}")
            process(batch)

        # Or just call periodically without message
        heartbeat()
    """
    global _last_heartbeat_time

    # Rate limit to avoid excessive disk writes
    now = time.time()
    if not force and (now - _last_heartbeat_time) < _heartbeat_min_interval:
        return

    _last_heartbeat_time = now

    # Write heartbeat file
    heartbeat_path = _get_heartbeat_path()
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)

    heartbeat_data = {
        "timestamp": now,
        "iso_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "message": message,
        "pid": os.getpid(),
    }

    # Atomic write using temp file + rename
    temp_path = heartbeat_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(heartbeat_data))
    temp_path.rename(heartbeat_path)


def get_heartbeat_age() -> float | None:
    """Get seconds since last heartbeat (for monitoring).

    Returns:
        Seconds since last heartbeat, or None if no heartbeat file exists.
    """
    heartbeat_path = _get_heartbeat_path()
    if not heartbeat_path.exists():
        return None

    try:
        data = json.loads(heartbeat_path.read_text())
        last_time: float = float(data.get("timestamp", 0))
        return time.time() - last_time
    except (json.JSONDecodeError, OSError, ValueError):
        return None


def read_heartbeat() -> dict[str, Any] | None:
    """Read the current heartbeat data (for monitoring).

    Returns:
        Heartbeat dict with timestamp, message, pid, or None if not found.
    """
    heartbeat_path = _get_heartbeat_path()
    if not heartbeat_path.exists():
        return None

    try:
        data: dict[str, Any] = json.loads(heartbeat_path.read_text())
        # Add computed age
        data["age_seconds"] = time.time() - data.get("timestamp", 0)
        return data
    except (json.JSONDecodeError, OSError):
        return None


def _get_heartbeat_path() -> Path:
    """Get path to heartbeat file."""
    return _get_outputs_dir() / HEARTBEAT_DIR / HEARTBEAT_FILE


# =============================================================================
# SVS Runtime API - During-run monitoring
# =============================================================================


def runtime_log(message: str, level: str = "INFO") -> None:
    """Write a structured log line for during-run AI monitoring.

    This function serves two purposes:
    1. Writes to .goldfish/logs.txt for the SVS DuringRunMonitor (AI anomaly detection)
    2. Prints to stdout so logs appear in the logs() tool for human debugging

    The DuringRunMonitor periodically analyzes these logs to detect training anomalies
    (OOM, NaN, loss divergence) and can request early termination if critical issues
    are found.

    Args:
        message: The log message to write
        level: Log level (INFO, WARN, ERROR, etc.)

    The log file is automatically capped at 10MB to prevent disk exhaustion.
    """
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    formatted_line = f"[{timestamp}] {level}: {message}"

    # Print to stdout for human visibility via logs() tool
    print(formatted_line, flush=True)

    # Also write to .goldfish/logs.txt for AI monitoring
    outputs_dir = _get_outputs_dir()
    logs_file = outputs_dir / ".goldfish" / "logs.txt"

    try:
        logs_file.parent.mkdir(parents=True, exist_ok=True)

        # Cap log file size (10MB)
        if logs_file.exists() and logs_file.stat().st_size > 10_000_000:
            # Simple truncation: keep the last 5MB
            content = logs_file.read_text()
            half = len(content) // 2
            logs_file.write_text(content[half:])

        with open(logs_file, "a") as f:
            f.write(f"{formatted_line}\n")
    except Exception as e:
        # Best effort, don't crash the stage for logging failures
        logger.debug(f"Failed to write runtime log: {e}")


def flush_metrics() -> None:
    """Flush buffered metrics to disk for SVS visibility.

    Call this to ensure recent metrics are available for during-run AI review.
    """
    from goldfish.metrics import get_logger

    logger_inst = get_logger()
    if logger_inst:
        logger_inst.flush()


def should_stop() -> bool:
    """Check if SVS requested early termination.

    Returns:
        True if SVS DuringRunMonitor requested a stop.
    """
    from goldfish.svs.runtime import should_stop as _should_stop

    return _should_stop()


# =============================================================================
# Checkpoint API - Immediate GCS upload for resume functionality
# =============================================================================
# Storage Abstraction Layer
# =============================================================================

# Lazy-loaded storage adapter
_storage_adapter: "ObjectStorage | None" = None


def _get_storage_adapter() -> "ObjectStorage":
    """Get or create storage adapter (lazy initialization).

    The storage backend is determined by GOLDFISH_STORAGE_BACKEND environment variable:
    - "gcs" or "gce": Use Google Cloud Storage
    - "local": Use local filesystem storage
    - "s3" or "aws": Use AWS S3 (future)
    - "azure": Use Azure Blob Storage (future)

    If not set, defaults to "gcs" for backward compatibility.
    """
    global _storage_adapter
    if _storage_adapter is not None:
        return _storage_adapter

    backend = os.environ.get("GOLDFISH_STORAGE_BACKEND", "gcs").lower()

    if backend in ("gcs", "gce"):
        try:
            from goldfish.cloud.adapters.gcp.storage import GCSStorage

            _storage_adapter = GCSStorage(project=None)
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-storage not installed. "
                "Add it to your requirements.txt or set GOLDFISH_STORAGE_BACKEND=local"
            ) from e
    elif backend == "local":
        from goldfish.cloud.adapters.local.storage import LocalObjectStorage
        from goldfish.config import LocalStorageConfig

        root = Path(os.environ.get("GOLDFISH_LOCAL_STORAGE_ROOT", "/tmp/goldfish_storage"))
        config = LocalStorageConfig(consistency_delay_ms=0, size_limit_mb=None)
        _storage_adapter = LocalObjectStorage(root=root, config=config)
    else:
        raise RuntimeError(f"Unknown storage backend: {backend}. Supported: gcs, local")

    return _storage_adapter


def _parse_storage_uri(uri: str) -> tuple[str, str]:
    """Parse storage URI into (scheme+bucket, path).

    Supports:
    - gs://bucket/path -> ("gs://bucket", "path")
    - file:///path -> ("file://", "/path")
    - s3://bucket/path -> ("s3://bucket", "path") (future)
    """
    if uri.startswith("gs://"):
        parts = uri[5:].split("/", 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        return f"gs://{bucket}", path
    elif uri.startswith("file://"):
        return "file://", uri[7:]
    elif uri.startswith("s3://"):
        parts = uri[5:].split("/", 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        return f"s3://{bucket}", path
    else:
        raise ValueError(f"Invalid storage URI: {uri}")


def _get_storage_bucket() -> str | None:
    """Get storage bucket from environment.

    Returns the full bucket URI (e.g., "gs://my-bucket" or "s3://my-bucket").
    """
    # Check for generic bucket first
    bucket = os.environ.get("GOLDFISH_STORAGE_BUCKET")
    if bucket:
        return bucket.rstrip("/")

    # Fall back to GCS-specific for backward compatibility
    gcs_bucket = os.environ.get("GOLDFISH_GCS_BUCKET")
    if gcs_bucket:
        if gcs_bucket.startswith("gs://"):
            return gcs_bucket.rstrip("/")
        return f"gs://{gcs_bucket.rstrip('/')}"

    return None


def _get_run_id() -> str:
    """Get current run ID from environment."""
    run_id = os.environ.get("GOLDFISH_RUN_ID")
    if not run_id:
        raise RuntimeError("GOLDFISH_RUN_ID not set - checkpoint requires run context")
    return run_id


def _get_local_checkpoint_dir() -> Path:
    """Get local checkpoint directory for fallback."""
    return _get_outputs_dir() / ".goldfish" / "checkpoints"


def _upload_file_to_storage(local_path: Path, bucket_uri: str, blob_path: str) -> None:
    """Upload a single file to cloud storage using the abstraction layer.

    Args:
        local_path: Local file path to upload
        bucket_uri: Storage bucket URI (e.g., "gs://bucket" or "s3://bucket")
        blob_path: Path within the bucket
    """
    storage = _get_storage_adapter()
    full_uri = f"{bucket_uri}/{blob_path}"
    uri = StorageURI.parse(full_uri)
    data = local_path.read_bytes()
    storage.put(uri, data)


def _upload_dir_to_storage(local_dir: Path, bucket_uri: str, blob_prefix: str) -> None:
    """Upload a directory to cloud storage using the abstraction layer.

    Args:
        local_dir: Local directory to upload
        bucket_uri: Storage bucket URI
        blob_prefix: Prefix path within the bucket
    """
    storage = _get_storage_adapter()

    for local_file in local_dir.rglob("*"):
        if local_file.is_file():
            relative_path = local_file.relative_to(local_dir)
            blob_path = f"{blob_prefix}/{relative_path}".replace("\\", "/")
            full_uri = f"{bucket_uri}/{blob_path}"
            uri = StorageURI.parse(full_uri)
            data = local_file.read_bytes()
            storage.put(uri, data)


def _download_file_from_storage(bucket_uri: str, blob_path: str, local_path: Path) -> bool:
    """Download a single file from cloud storage. Returns True if successful.

    Args:
        bucket_uri: Storage bucket URI
        blob_path: Path within the bucket
        local_path: Local destination path
    """
    try:
        storage = _get_storage_adapter()
        full_uri = f"{bucket_uri}/{blob_path}"
        uri = StorageURI.parse(full_uri)
        if not storage.exists(uri):
            return False
        local_path.parent.mkdir(parents=True, exist_ok=True)
        data = storage.get(uri)
        local_path.write_bytes(data)
        return True
    except Exception:
        return False


def _download_dir_from_storage(bucket_uri: str, blob_prefix: str, local_dir: Path) -> bool:
    """Download all blobs with prefix to local directory. Returns True if any downloaded.

    Args:
        bucket_uri: Storage bucket URI
        blob_prefix: Prefix path within the bucket
        local_dir: Local destination directory
    """
    try:
        storage = _get_storage_adapter()
        prefix_uri = f"{bucket_uri}/{blob_prefix}"
        prefix_storage_uri = StorageURI.parse(prefix_uri)
        blob_uris = storage.list_prefix(prefix_storage_uri)
        if not blob_uris:
            return False

        local_dir.mkdir(parents=True, exist_ok=True)
        for blob_uri in blob_uris:
            blob_str = str(blob_uri)
            if blob_str.endswith("/"):
                continue  # Skip directory markers
            # Extract relative path from full URI
            relative_path = blob_str[len(prefix_uri) :].lstrip("/")
            if not relative_path:
                continue
            local_path = local_dir / relative_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            data = storage.get(blob_uri)
            local_path.write_bytes(data)
        return True
    except Exception:
        return False


def _storage_exists(bucket_uri: str, blob_path: str) -> bool:
    """Check if a blob exists in cloud storage.

    Args:
        bucket_uri: Storage bucket URI
        blob_path: Path within the bucket
    """
    try:
        storage = _get_storage_adapter()
        full_uri = f"{bucket_uri}/{blob_path}"
        uri = StorageURI.parse(full_uri)
        return storage.exists(uri)
    except Exception:
        return False


def _list_storage_prefix(bucket_uri: str, prefix: str) -> list[str]:
    """List all blob URIs with the given prefix.

    Args:
        bucket_uri: Storage bucket URI
        prefix: Prefix path within the bucket

    Returns:
        List of full URIs for matching blobs
    """
    try:
        storage = _get_storage_adapter()
        prefix_uri = f"{bucket_uri}/{prefix}"
        storage_uri = StorageURI.parse(prefix_uri)
        blob_uris = storage.list_prefix(storage_uri)
        # Convert StorageURI objects back to strings
        return [str(uri) for uri in blob_uris]
    except Exception:
        return []


# Backward compatibility aliases (deprecated - use new names)
def _upload_file_to_gcs(local_path: Path, bucket_name: str, blob_path: str) -> None:
    """Deprecated: Use _upload_file_to_storage instead."""
    _upload_file_to_storage(local_path, f"gs://{bucket_name}", blob_path)


def _upload_dir_to_gcs(local_dir: Path, bucket_name: str, blob_prefix: str) -> None:
    """Deprecated: Use _upload_dir_to_storage instead."""
    _upload_dir_to_storage(local_dir, f"gs://{bucket_name}", blob_prefix)


def _download_file_from_gcs(bucket_name: str, blob_path: str, local_path: Path) -> bool:
    """Deprecated: Use _download_file_from_storage instead."""
    return _download_file_from_storage(f"gs://{bucket_name}", blob_path, local_path)


def _download_dir_from_gcs(bucket_name: str, blob_prefix: str, local_dir: Path) -> bool:
    """Deprecated: Use _download_dir_from_storage instead."""
    return _download_dir_from_storage(f"gs://{bucket_name}", blob_prefix, local_dir)


def _blob_exists(bucket_name: str, blob_path: str) -> bool:
    """Deprecated: Use _storage_exists instead."""
    return _storage_exists(f"gs://{bucket_name}", blob_path)


def _list_blobs_with_prefix(bucket_name: str, prefix: str) -> list[str]:
    """Deprecated: Use _list_storage_prefix instead."""
    uris = _list_storage_prefix(f"gs://{bucket_name}", prefix)
    # Convert full URIs back to blob names for backward compatibility
    bucket_prefix = f"gs://{bucket_name}/"
    return [uri[len(bucket_prefix) :] if uri.startswith(bucket_prefix) else uri for uri in uris]


# Also provide _get_gcs_bucket_name for backward compatibility
def _get_gcs_bucket_name() -> str | None:
    """Deprecated: Use _get_storage_bucket instead.

    Returns just the bucket name (without gs:// prefix) for backward compatibility.
    """
    bucket_uri = _get_storage_bucket()
    if bucket_uri and bucket_uri.startswith("gs://"):
        return bucket_uri[5:]
    return None


def save_checkpoint(
    name: str,
    data: Any,
    step: int | None = None,
    local_ok: bool = False,
) -> None:
    """Save a checkpoint with immediate GCS upload.

    Use this for resume functionality on preemptible/spot instances.
    Unlike save_output() which batches uploads at stage completion,
    save_checkpoint uploads immediately to GCS.

    Args:
        name: Checkpoint name (e.g., "model", "optimizer", "training_state")
        data: Data to checkpoint. Can be:
            - Path to file or directory
            - numpy array (saved as .npy)
            - Any picklable object (saved as .pkl)
        step: Optional training step for versioned checkpoints.
              If provided, saves to {name}/step_{step}/
        local_ok: If True, save locally when GCS not configured (for local dev).
                  If False (default), raises RuntimeError without GCS.

    Raises:
        RuntimeError: If GCS bucket not configured and local_ok=False
        RuntimeError: If upload fails

    Example:
        # Save model checkpoint every 1000 steps
        if step % 1000 == 0:
            save_checkpoint("model", model_dir, step=step)

        # Save training state for exact resume
        save_checkpoint("training_state", {
            "step": step,
            "optimizer_state": optimizer.state_dict(),
            "rng_state": torch.get_rng_state(),
        })
    """
    import shutil
    import tempfile

    bucket_name = _get_gcs_bucket_name()
    run_id = _get_run_id()

    # Build GCS blob path
    if step is not None:
        blob_prefix = f"checkpoints/{run_id}/{name}/step_{step}"
    else:
        blob_prefix = f"checkpoints/{run_id}/{name}"

    # Handle different data types
    source_path: Path
    is_temp = False
    is_dir = False

    if isinstance(data, Path):
        source_path = data
        is_dir = data.is_dir()
    elif np is not None and isinstance(data, np.ndarray):
        # Save numpy array to temp file
        temp_dir = Path(tempfile.mkdtemp())
        source_path = temp_dir / f"{name}.npy"
        np.save(source_path, data)
        is_temp = True
        blob_prefix = f"{blob_prefix}.npy"
    else:
        # Pickle other objects
        import pickle

        temp_dir = Path(tempfile.mkdtemp())
        source_path = temp_dir / f"{name}.pkl"
        with open(source_path, "wb") as f:
            pickle.dump(data, f)
        is_temp = True
        blob_prefix = f"{blob_prefix}.pkl"

    try:
        if bucket_name:
            # Upload to GCS immediately using Python client
            try:
                if is_dir:
                    _upload_dir_to_gcs(source_path, bucket_name, blob_prefix)
                else:
                    _upload_file_to_gcs(source_path, bucket_name, blob_prefix)
                logger.info(f"Checkpoint '{name}' uploaded to gs://{bucket_name}/{blob_prefix}")
            except Exception as e:
                raise RuntimeError(f"Checkpoint upload failed: {e}") from e

        elif local_ok:
            # Local fallback
            local_dir = _get_local_checkpoint_dir()
            if step is not None:
                local_path = local_dir / name / f"step_{step}"
            else:
                local_path = local_dir / name

            local_path.parent.mkdir(parents=True, exist_ok=True)

            if is_dir:
                if local_path.exists():
                    shutil.rmtree(local_path)
                shutil.copytree(source_path, local_path)
            else:
                shutil.copy2(source_path, local_path)

            logger.info(f"Checkpoint '{name}' saved locally to {local_path}")

        else:
            raise RuntimeError(
                "GCS bucket not configured for checkpoints. "
                "Set GOLDFISH_GCS_BUCKET or use local_ok=True for local dev."
            )

    finally:
        # Clean up temp files
        if is_temp and source_path.parent.exists():
            shutil.rmtree(source_path.parent, ignore_errors=True)


def load_checkpoint(
    name: str,
    step: int | None = None,
    run_id: str | None = None,
) -> Path | None:
    """Load a checkpoint from GCS or local storage.

    Use this at stage start to resume from a previous checkpoint.

    Args:
        name: Checkpoint name to load
        step: Specific step to load. If None, loads the base checkpoint.
        run_id: Run ID to load from. If None, uses current run.
                Use this to resume from a previous run's checkpoint.

    Returns:
        Path to downloaded checkpoint (file or directory), or None if not found.

    Example:
        # Try to resume from previous checkpoint
        ckpt_path = load_checkpoint("model", run_id="stage-previous123")
        if ckpt_path:
            model.load_state_dict(torch.load(ckpt_path / "model.pt"))
            start_step = load_checkpoint("training_state")["step"]
        else:
            start_step = 0
    """
    bucket_name = _get_gcs_bucket_name()
    target_run_id = run_id or _get_run_id()

    # Check local first (for local dev or already-downloaded checkpoints)
    local_dir = _get_local_checkpoint_dir()
    if step is not None:
        local_path = local_dir / name / f"step_{step}"
    else:
        local_path = local_dir / name

    if local_path.exists():
        return local_path

    # Try GCS if bucket configured
    if not bucket_name:
        return None

    if step is not None:
        blob_prefix = f"checkpoints/{target_run_id}/{name}/step_{step}"
    else:
        blob_prefix = f"checkpoints/{target_run_id}/{name}"

    # Try exact path first, then with common extensions
    for suffix in ["", ".npy", ".pkl"]:
        test_path = f"{blob_prefix}{suffix}"

        # Check if it's a single file
        if _blob_exists(bucket_name, test_path):
            local_file = local_path.with_suffix(suffix) if suffix else local_path
            if _download_file_from_gcs(bucket_name, test_path, local_file):
                logger.info(f"Checkpoint '{name}' downloaded to {local_file}")
                return local_file

        # Check if it's a directory (has blobs with this prefix)
        blobs = _list_blobs_with_prefix(bucket_name, test_path + "/")
        if blobs:
            if _download_dir_from_gcs(bucket_name, test_path, local_path):
                logger.info(f"Checkpoint '{name}' downloaded to {local_path}")
                return local_path

    return None


def list_checkpoints(run_id: str | None = None) -> dict[str, dict]:
    """List available checkpoints for a run.

    Args:
        run_id: Run ID to list checkpoints for. If None, uses current run.

    Returns:
        Dict mapping checkpoint names to info including available steps.
        Example: {"model": {"steps": [1000, 2000, 3000]}, "optimizer": {"steps": []}}
    """
    import re

    bucket_name = _get_gcs_bucket_name()
    target_run_id = run_id or _get_run_id()

    result: dict[str, dict] = {}

    # Check local checkpoints
    local_dir = _get_local_checkpoint_dir()
    if local_dir.exists():
        for item in local_dir.iterdir():
            if item.is_dir():
                steps = []
                for sub in item.iterdir():
                    if sub.is_dir() and sub.name.startswith("step_"):
                        try:
                            step_num = int(sub.name.replace("step_", ""))
                            steps.append(step_num)
                        except ValueError:
                            pass
                result[item.name] = {"steps": sorted(steps), "location": "local"}

    # Check GCS if configured
    if bucket_name:
        prefix = f"checkpoints/{target_run_id}/"
        blobs = _list_blobs_with_prefix(bucket_name, prefix)

        for blob_name in blobs:
            if not blob_name:
                continue
            # Parse checkpoint name from path
            # checkpoints/run-id/model/file.pt or checkpoints/run-id/model/step_1000/file.pt
            relative = blob_name[len(prefix) :]
            parts = relative.split("/")
            if not parts or not parts[0]:
                continue

            ckpt_name = parts[0]
            # Strip extensions for single-file checkpoints
            if "." in ckpt_name and "/" not in relative[len(ckpt_name) :]:
                ckpt_name = ckpt_name.rsplit(".", 1)[0]

            if ckpt_name not in result:
                result[ckpt_name] = {"steps": [], "location": "gcs"}

            # Check for step directories
            step_match = re.search(r"step_(\d+)", relative)
            if step_match:
                step_num = int(step_match.group(1))
                if step_num not in result[ckpt_name]["steps"]:
                    result[ckpt_name]["steps"].append(step_num)
                    result[ckpt_name]["steps"].sort()

    return result


# =============================================================================
# Metrics API - Re-export for convenience
# =============================================================================

__all__ = [
    # IO functions
    "load_input",
    "save_output",
    "get_config",
    "get_input_path",
    "get_output_path",
    # Checkpoint functions (immediate GCS upload for resume)
    "save_checkpoint",
    "load_checkpoint",
    "list_checkpoints",
    # Heartbeat functions
    "heartbeat",
    "get_heartbeat_age",
    "read_heartbeat",
    # SVS Runtime functions
    "runtime_log",
    "flush_metrics",
    "should_stop",
    # Metrics functions
    "log_metric",
    "log_metrics",
    "log_artifact",
    "finish_metrics",
]
