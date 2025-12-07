"""Generate synthetic image-like classification data.

Creates 1000 samples of 28x28 "images" (784 features) with 10 classes.
Data is randomly generated but deterministic (seeded).
"""

import numpy as np
from pathlib import Path


def main():
    """Generate synthetic training data."""
    # Set seed for reproducibility
    np.random.seed(42)

    # Generate synthetic data
    n_samples = 1000
    n_features = 28 * 28  # 784 features (like MNIST)
    n_classes = 10

    # Generate random features (normalized between 0 and 1)
    X = np.random.rand(n_samples, n_features).astype(np.float32)

    # Generate random labels
    y = np.random.randint(0, n_classes, size=n_samples).astype(np.int32)

    # Add some structure to make it learnable
    # Classes 0-4 have higher values in first half of features
    # Classes 5-9 have higher values in second half of features
    for i in range(n_samples):
        if y[i] < 5:
            X[i, :392] += 0.3  # Boost first half
        else:
            X[i, 392:] += 0.3  # Boost second half

    # Clip to [0, 1]
    X = np.clip(X, 0, 1)

    # Save outputs
    output_dir = Path("/mnt/outputs/raw_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    np.save(output_dir / "X.npy", X)
    np.save(output_dir / "y.npy", y)

    print(f"Generated {n_samples} samples with {n_features} features")
    print(f"X shape: {X.shape}, y shape: {y.shape}")
    print(f"Class distribution: {np.bincount(y)}")


if __name__ == "__main__":
    main()
