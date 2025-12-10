"""Preprocess raw data: normalize and split into train/test sets.

Input: raw_data (X.npy, y.npy)
Output: processed (X_train, X_test, y_train, y_test)
"""

from pathlib import Path

import numpy as np


def main():
    """Preprocess data and create train/test split."""
    # Load raw data
    input_dir = Path("/mnt/inputs/raw_data")
    X = np.load(input_dir / "X.npy")
    y = np.load(input_dir / "y.npy")

    print(f"Loaded data: X shape {X.shape}, y shape {y.shape}")

    # Normalize features (already in [0,1] but ensure it)
    X_normalized = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-8)

    # Train/test split (80/20)
    n_train = int(0.8 * len(X))

    # Use deterministic split
    np.random.seed(42)
    indices = np.random.permutation(len(X))

    train_idx = indices[:n_train]
    test_idx = indices[n_train:]

    X_train = X_normalized[train_idx]
    X_test = X_normalized[test_idx]
    y_train = y[train_idx]
    y_test = y[test_idx]

    # Save outputs
    output_dir = Path("/mnt/outputs/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    np.save(output_dir / "X_train.npy", X_train)
    np.save(output_dir / "X_test.npy", X_test)
    np.save(output_dir / "y_train.npy", y_train)
    np.save(output_dir / "y_test.npy", y_test)

    print(f"Train set: {len(X_train)} samples")
    print(f"Test set: {len(X_test)} samples")
    print(f"Train class distribution: {np.bincount(y_train)}")
    print(f"Test class distribution: {np.bincount(y_test)}")


if __name__ == "__main__":
    main()
