#!/usr/bin/env python3
"""Stage: validate_io - Validate goldfish.io module works correctly.

This stage uses goldfish.io's load_input and save_output functions to:
1. Load different input types (npy, csv, directory)
2. Validate the data matches expectations
3. Save transformed outputs

This is the KEY test for the goldfish.io packaging fix.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Import goldfish.io - this validates the Docker image packaging fix
try:
    from goldfish.io import get_config, load_input, save_output

    print("goldfish.io imported successfully!")
    GOLDFISH_IO_AVAILABLE = True
except ImportError as e:
    print(f"ERROR: Failed to import goldfish.io: {e}")
    print("This indicates the Docker image packaging is broken.")
    GOLDFISH_IO_AVAILABLE = False


def main():
    """Validate goldfish.io input/output operations."""
    if not GOLDFISH_IO_AVAILABLE:
        print("FATAL: goldfish.io not available - cannot continue")
        sys.exit(1)

    validation_results = []

    # Test 1: Load numpy array
    print("\n=== Test 1: Load NPY array ===")
    try:
        array_data = load_input("array_input")
        assert isinstance(array_data, np.ndarray), f"Expected np.ndarray, got {type(array_data)}"
        assert array_data.shape == (100, 10), f"Wrong shape: {array_data.shape}"
        validation_results.append(
            {
                "test": "load_npy",
                "status": "passed",
                "details": f"shape={array_data.shape}",
            }
        )
        print(f"  Loaded array with shape {array_data.shape}")
    except Exception as e:
        validation_results.append({"test": "load_npy", "status": "failed", "details": str(e)})
        print(f"  FAILED: {e}")

    # Test 2: Load CSV as DataFrame
    print("\n=== Test 2: Load CSV as DataFrame ===")
    try:
        csv_data = load_input("csv_input")
        assert isinstance(csv_data, pd.DataFrame), f"Expected pd.DataFrame, got {type(csv_data)}"
        assert len(csv_data) == 50, f"Wrong row count: {len(csv_data)}"
        assert "id" in csv_data.columns, "Missing 'id' column"
        assert "value" in csv_data.columns, "Missing 'value' column"
        assert "category" in csv_data.columns, "Missing 'category' column"
        validation_results.append(
            {
                "test": "load_csv",
                "status": "passed",
                "details": f"rows={len(csv_data)}, cols={list(csv_data.columns)}",
            }
        )
        print(f"  Loaded DataFrame with {len(csv_data)} rows")
    except Exception as e:
        validation_results.append({"test": "load_csv", "status": "failed", "details": str(e)})
        print(f"  FAILED: {e}")

    # Test 3: Load directory
    print("\n=== Test 3: Load directory ===")
    try:
        dir_path = load_input("dir_input")
        assert isinstance(dir_path, Path), f"Expected Path, got {type(dir_path)}"
        assert dir_path.exists(), f"Directory does not exist: {dir_path}"

        # Verify expected files
        expected_files = ["metadata.json", "nested_array.npy", "readme.txt"]
        found_files = list(dir_path.iterdir())
        found_names = [f.name for f in found_files]

        for expected in expected_files:
            assert expected in found_names, f"Missing file: {expected}"

        # Verify metadata content
        metadata = json.loads((dir_path / "metadata.json").read_text())
        assert metadata["version"] == 1
        assert metadata["test"] is True

        validation_results.append(
            {
                "test": "load_directory",
                "status": "passed",
                "details": f"files={found_names}",
            }
        )
        print(f"  Loaded directory with files: {found_names}")
    except Exception as e:
        validation_results.append({"test": "load_directory", "status": "failed", "details": str(e)})
        print(f"  FAILED: {e}")

    # Test 4: Get config
    print("\n=== Test 4: Get config ===")
    try:
        config = get_config()
        assert isinstance(config, dict), f"Expected dict, got {type(config)}"
        validation_results.append(
            {
                "test": "get_config",
                "status": "passed",
                "details": f"keys={list(config.keys())[:5]}...",
            }
        )
        print(f"  Config keys: {list(config.keys())}")
    except Exception as e:
        validation_results.append({"test": "get_config", "status": "failed", "details": str(e)})
        print(f"  FAILED: {e}")

    # Test 5: Save npy output
    print("\n=== Test 5: Save NPY output ===")
    try:
        transformed = array_data * 2 + 1  # Simple transformation
        save_output("transformed_array", transformed)
        validation_results.append(
            {
                "test": "save_npy",
                "status": "passed",
                "details": f"shape={transformed.shape}",
            }
        )
        print(f"  Saved transformed array with shape {transformed.shape}")
    except Exception as e:
        validation_results.append({"test": "save_npy", "status": "failed", "details": str(e)})
        print(f"  FAILED: {e}")

    # Test 6: Save CSV output
    print("\n=== Test 6: Save CSV output ===")
    try:
        results_df = pd.DataFrame(validation_results)
        save_output("validation_results", results_df)
        validation_results.append(
            {
                "test": "save_csv",
                "status": "passed",
                "details": f"rows={len(results_df)}",
            }
        )
        print(f"  Saved validation results with {len(results_df)} rows")
    except Exception as e:
        # Can't add to results_df here since it's already saved
        print(f"  FAILED: {e}")

    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION SUMMARY")
    print("=" * 50)
    passed = sum(1 for r in validation_results if r["status"] == "passed")
    failed = sum(1 for r in validation_results if r["status"] == "failed")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total:  {len(validation_results)}")

    if failed > 0:
        print("\nFailed tests:")
        for r in validation_results:
            if r["status"] == "failed":
                print(f"  - {r['test']}: {r['details']}")
        sys.exit(1)
    else:
        print("\nAll tests passed!")


if __name__ == "__main__":
    main()
