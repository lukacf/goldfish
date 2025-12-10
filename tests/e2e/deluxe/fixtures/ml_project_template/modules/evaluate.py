"""Evaluate trained model on test set.

Input:
  - processed (X_test.npy, y_test.npy)
  - model (model.pkl)
Output: metrics (metrics.json, confusion_matrix.npy)
"""

import json
import pickle
from pathlib import Path

import numpy as np
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix


def main():
    """Evaluate model on test set."""
    # Load test data
    processed_dir = Path("/mnt/inputs/processed")
    X_test = np.load(processed_dir / "X_test.npy")
    y_test = np.load(processed_dir / "y_test.npy")

    print(f"Loaded test data: X shape {X_test.shape}, y shape {y_test.shape}")

    # Load model
    model_dir = Path("/mnt/inputs/model")
    with open(model_dir / "model.pkl", "rb") as f:
        model = pickle.load(f)

    print("Loaded trained model")

    # Make predictions
    y_pred = model.predict(X_test)

    # Compute metrics
    accuracy = accuracy_score(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    print(f"Test accuracy: {accuracy:.4f}")
    print(f"Confusion matrix shape: {conf_matrix.shape}")

    # Get classification report
    report = classification_report(y_test, y_pred, output_dict=True)

    # Save outputs
    output_dir = Path("/mnt/outputs/metrics")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save confusion matrix
    np.save(output_dir / "confusion_matrix.npy", conf_matrix)

    # Save metrics
    metrics = {
        "test_accuracy": float(accuracy),
        "n_test_samples": int(len(X_test)),
        "classification_report": report,
    }

    with open(output_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("Evaluation complete - metrics saved")
    print(f"Test Accuracy: {accuracy:.4f}")


if __name__ == "__main__":
    main()
