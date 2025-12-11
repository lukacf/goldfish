#!/usr/bin/env python3
"""Stage: generate_test_data - Generate test data for I/O validation.

This stage creates test data WITHOUT using goldfish.io, writing directly
to /mnt/outputs to ensure the output staging works correctly.
"""

from pathlib import Path

import numpy as np
import pandas as pd


def main():
    """Generate test data for I/O validation."""
    output_dir = Path("/mnt/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate numpy array
    print("Generating numpy array...")
    test_array = np.random.randn(100, 10).astype(np.float32)
    np.save(output_dir / "test_array.npy", test_array)
    print(f"  Shape: {test_array.shape}")
    print(f"  Mean: {test_array.mean():.4f}")
    print(f"  Std: {test_array.std():.4f}")

    # Generate CSV data
    print("Generating CSV data...")
    test_df = pd.DataFrame(
        {
            "id": range(50),
            "value": np.random.randn(50),
            "category": np.random.choice(["A", "B", "C"], 50),
        }
    )
    test_df.to_csv(output_dir / "test_csv.csv", index=False)
    print(f"  Rows: {len(test_df)}")
    print(f"  Columns: {list(test_df.columns)}")

    # Generate directory output
    print("Generating directory output...")
    dir_output = output_dir / "test_directory"
    dir_output.mkdir(exist_ok=True)

    # Create multiple files in directory
    (dir_output / "metadata.json").write_text('{"version": 1, "test": true}')
    np.save(dir_output / "nested_array.npy", np.array([1, 2, 3, 4, 5]))
    (dir_output / "readme.txt").write_text("Test directory for I/O validation")

    print("  Files created: metadata.json, nested_array.npy, readme.txt")

    print("\nGenerate test data completed successfully!")


if __name__ == "__main__":
    main()
