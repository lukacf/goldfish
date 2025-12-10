"""Train a classifier on preprocessed data.

Uses scikit-learn LogisticRegression for simplicity.
Hyperparameters can be controlled via environment variables.

Input: processed (X_train.npy, y_train.npy)
Output: model (model.pkl, metadata.json)
"""

import json
import os
import pickle
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression


def main():
    """Train classifier."""
    # Load training data
    input_dir = Path("/mnt/inputs/processed")
    X_train = np.load(input_dir / "X_train.npy")
    y_train = np.load(input_dir / "y_train.npy")

    print(f"Loaded training data: X shape {X_train.shape}, y shape {y_train.shape}")

    # Get hyperparameters from environment
    max_iter = int(os.getenv("MAX_ITER", "100"))
    C = float(os.getenv("C", "1.0"))

    print(f"Training with max_iter={max_iter}, C={C}")

    # Train model
    model = LogisticRegression(
        max_iter=max_iter,
        C=C,
        random_state=42,
        n_jobs=-1,
        verbose=1,
    )

    model.fit(X_train, y_train)

    # Compute training accuracy
    train_accuracy = model.score(X_train, y_train)
    print(f"Training accuracy: {train_accuracy:.4f}")

    # Save model
    output_dir = Path("/mnt/outputs/model")
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    # Save metadata
    metadata = {
        "max_iter": max_iter,
        "C": C,
        "train_accuracy": float(train_accuracy),
        "n_samples": int(len(X_train)),
        "n_features": int(X_train.shape[1]),
        "n_classes": int(len(np.unique(y_train))),
    }

    with open(output_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print("Training complete - model saved")


if __name__ == "__main__":
    main()
