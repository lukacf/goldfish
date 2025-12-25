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
from pathlib import Path
from typing import Any

from goldfish.metrics import finish as finish_metrics
from goldfish.metrics import log_artifact, log_metric, log_metrics

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
    config = _get_stage_config()
    input_config = config.get("inputs", {}).get(name)

    if not input_config:
        raise ValueError(f"Input '{name}' not defined in stage config")

    # Check for custom loader
    if "loader" in input_config:
        return _run_custom_loader(name, input_config["loader"])

    # Get input path (Goldfish pre-downloads to this location)
    input_path = _get_inputs_dir() / name

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
    return _get_stage_config()


def get_input_path(name: str) -> Path:
    """Get path to input for manual loading.

    Goldfish pre-downloads to this location if needed.
    """
    return _get_inputs_dir() / name


def get_output_path(name: str) -> Path:
    """Get path to write output for manual saving.

    Goldfish uploads from this location after stage completes.
    """
    path = _get_outputs_dir() / name
    path.mkdir(parents=True, exist_ok=True)
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
# Metrics API - Re-export for convenience
# =============================================================================

__all__ = [
    # IO functions
    "load_input",
    "save_output",
    "get_config",
    "get_input_path",
    "get_output_path",
    # Heartbeat functions
    "heartbeat",
    "get_heartbeat_age",
    "read_heartbeat",
    # Metrics functions
    "log_metric",
    "log_metrics",
    "log_artifact",
    "finish_metrics",
]
