"""Goldfish IO Library - Storage abstraction for modules.

This library is deployed in Docker containers and provides
a simple API for modules to load inputs and save outputs
without worrying about storage backends (GCS/hyperdisk/local).
"""

import os
import json
from pathlib import Path
from typing import Any, Optional
import importlib.util

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None


def _get_stage_config() -> dict:
    """Load stage configuration from environment."""
    config_json = os.environ.get("GOLDFISH_STAGE_CONFIG")
    if not config_json:
        raise RuntimeError("GOLDFISH_STAGE_CONFIG not set")
    return json.loads(config_json)


def _get_inputs_dir() -> Path:
    """Get inputs directory (configurable for testing)."""
    return Path(os.environ.get("GOLDFISH_INPUTS_DIR", "/mnt/inputs"))


def _get_outputs_dir() -> Path:
    """Get outputs directory (configurable for testing)."""
    return Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))


def load_input(name: str, format: Optional[str] = None) -> Any:
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

    elif fmt in ("directory", "file"):
        # Return path for manual loading
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
        raise ValueError(
            f"Cannot auto-save format '{fmt}'. "
            f"Use get_output_path() for manual saving."
        )

    if artifact:
        _mark_as_artifact(name)


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
    # Prevent path traversal and arbitrary file execution
    script_path = Path(script).resolve()

    # Check if we're running in Docker (production) or tests
    app_path = Path("/app")
    if app_path.exists():
        # Running in Docker - enforce strict /app directory restriction
        try:
            script_path.relative_to(app_path.resolve())
        except ValueError:
            raise ValueError(
                f"Custom loader script must be within /app directory. "
                f"Got: {script}"
            )

        # Additional check: script must be in loaders/ directory
        if "loaders" not in script_path.parts:
            raise ValueError(
                f"Custom loader script must be in loaders/ directory. "
                f"Got: {script}"
            )
    else:
        # Running in tests - just check script exists and filename is valid
        if not script_path.exists():
            raise FileNotFoundError(f"Custom loader script not found: {script}")

        # Basic security: no path traversal components
        if ".." in script_path.parts:
            raise ValueError(
                f"Custom loader script path contains path traversal. "
                f"Got: {script}"
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
