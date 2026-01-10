# Stage Module Development Guide

Complete guide to writing stage modules for Goldfish ML pipelines.

## Stage Module Structure

Each stage corresponds to a Python module:

```
workspace/
├── requirements.txt      # Python dependencies (CRITICAL!)
├── pipeline.yaml         # Stage definitions and data flow
├── modules/
│   ├── preprocess.py     # Stage: preprocess
│   ├── train.py          # Stage: train
│   └── evaluate.py       # Stage: evaluate
├── configs/
│   ├── preprocess.yaml
│   └── train.yaml
└── loaders/              # Optional custom data loaders
```

## Pre-built Base Images

Goldfish automatically uses pre-built Docker images with common ML libraries:

| Profile Type | Base Image | Included Libraries |
|--------------|------------|-------------------|
| CPU (`cpu-small`, `cpu-large`) | `goldfish-base-cpu:v5` | numpy, pandas, scikit-learn, torch, matplotlib, seaborn |
| GPU (`h100-*`, `a100-*`) | `goldfish-base-gpu:v5` | Same as CPU + CUDA 12.8 + PyTorch 2.9.1 + FlashAttention-3 |

**No setup required** - the appropriate base image is automatically selected based on your stage's compute profile.

For custom packages, see [Custom Docker Images (Advanced)](#custom-docker-images-advanced).

## Requirements.txt (Optional)

Use `requirements.txt` only for project-specific dependencies not in the base images:

```txt
# requirements.txt - Only project-specific extras
my-custom-package>=1.0
specialized-ml-lib>=2.0
```

Most ML workloads don't need a `requirements.txt` at all since common libraries are pre-installed.

## Basic Template

```python
#!/usr/bin/env python3
"""Stage: train - Train the prediction model."""

from goldfish.io import load_input, save_output


def main():
    # 1. Load inputs (from /mnt/inputs/)
    features = load_input("features")
    labels = load_input("labels")

    # 2. Your ML logic
    model = train_model(features, labels)

    # 3. Save outputs (to /mnt/outputs/)
    save_output("model", model_path)


if __name__ == "__main__":
    main()
```

## The goldfish.io Module

Goldfish provides I/O helpers for stages running in containers.

### load_input(name, format) -> Any

```python
def load_input(name: str, format: str | None = None) -> np.ndarray | pd.DataFrame | Path
```

Load an input signal by name.

```python
from goldfish.io import load_input

# Auto-load based on signal type
features = load_input("features")

# Optional: Override format
df = load_input("raw_data", format="csv")
```

**Parameters:**
- `name`: Signal name from `pipeline.yaml`
- `format`: (Optional) Explicit format override (`npy`, `csv`, `directory`, `file`, `dataset`)

**Input mapping:**
| Signal Type | Returned Type |
|-------------|---------------|
| `npy` | `np.ndarray` |
| `csv` | `pd.DataFrame` |
| `directory` | `pathlib.Path` |
| `file` | `pathlib.Path` |
| `dataset` | `pathlib.Path` to the dataset directory |

---

### save_output(name, data, artifact) -> Path | None

```python
def save_output(name: str, data: Any, artifact: bool = False) -> Path | None
```

Save an output signal or artifact. **Only supports `npy` and `csv` formats.**

```python
from goldfish.io import save_output

# Save numpy array (format: npy)
save_output("embeddings", embeddings)

# Save pandas DataFrame (format: csv)
save_output("metrics", metrics_df)

# Save permanent artifact (promoted to registry)
save_output("model", "/path/to/model", artifact=True)
```

**Parameters:**
- `name`: Output name from `pipeline.yaml`
- `data`: Object to save (numpy array or pandas DataFrame)
- `artifact`: (Optional) If `True`, marks this output as a permanent artifact for registry promotion

**Supported Formats:**
| Format | Data Type | Auto-save |
|--------|-----------|-----------|
| `npy` | `np.ndarray` | Yes |
| `csv` | `pd.DataFrame` | Yes |
| `file` | - | No, use `get_output_path()` |
| `directory` | - | No, use `get_output_path()` |

**For `file` or `directory` formats, use `get_output_path()` instead:**
```python
# WRONG - will raise ValueError for file/directory formats
save_output("my_file", data)  # ValueError!

# CORRECT - use get_output_path() for manual saving
path = get_output_path("my_file")  # Returns directory Path
with open(path / "data.bin", "wb") as f:  # Write file inside directory
    f.write(data)
```

---

### get_input_path(name) -> Path

```python
def get_input_path(name: str) -> Path
```

Get the physical local path for an input. **Takes only 1 argument.** Useful for manual loading with libraries like `torch` or `cv2`.

```python
from goldfish.io import get_input_path
import torch

path = get_input_path("checkpoint")  # Returns Path to /mnt/inputs/checkpoint/
model = torch.load(path / "model.pt")
```

---

### get_output_path(name) -> Path

```python
def get_output_path(name: str) -> Path
```

Get the physical local path to write outputs. **Takes only 1 argument** - the output name from pipeline.yaml. Returns a `Path` object. Goldfish will automatically upload content from this path after the stage completes.

```python
from goldfish.io import get_output_path

# Single file output (type: file)
path = get_output_path("encoded_data")  # Returns Path, e.g., /mnt/outputs/encoded_data/
with open(path / "data.bin", "wb") as f:
    f.write(binary_data)

# Multi-file output (type: directory)
path = get_output_path("plots")  # Returns Path, e.g., /mnt/outputs/plots/
plt.savefig(path / "loss.png")
plt.savefig(path / "accuracy.png")
```

**Common mistake:** Do NOT pass a filename as second argument:
```python
# WRONG - get_output_path takes only 1 argument!
path = get_output_path("data", "data.bin")  # TypeError!

# CORRECT
path = get_output_path("data")
with open(path / "data.bin", "wb") as f:
    ...
```

---

### get_config()

Access stage configuration.

```python
from goldfish.io import get_config

config = get_config()
learning_rate = config.get("learning_rate", 0.001)
batch_size = config.get("batch_size", 32)
```

## SVS Runtime API (Monitoring)

The SVS Runtime API allows stages to interact with background AI monitoring.

### runtime_log(message, level)

Write a structured log line for both AI monitoring and human debugging.

```python
from goldfish.io import runtime_log

# Signal progress to the AI monitor
runtime_log("Gradient norm is stable, training looks healthy")

# Log warnings the AI should notice
runtime_log("Loss is flatlining, considering early stop", level="WARN")
```

**Dual Purpose:**
1. **AI Monitoring**: Logs are written to `.goldfish/logs.txt` where the `DuringRunMonitor` analyzes them for anomalies (OOM, NaN, loss divergence) and can request early termination.
2. **Human Visibility**: Logs are also printed to stdout, so they appear in the `logs()` tool for real-time debugging.

**When to use:**
- Use `runtime_log()` for important status updates you want both AI and humans to see
- Use regular `print()` for verbose debug output that doesn't need AI analysis

### should_stop()

Check if the background AI monitor has requested early termination.

```python
from goldfish.io import should_stop

for epoch in range(epochs):
    # ... training logic ...
    
    if should_stop():
        print("SVS requested early termination. Saving best model and exiting.")
        save_output("model", best_model)
        break
```

### flush_metrics()

Manually trigger a flush of metrics to disk to make them available for background review.

```python
from goldfish.io import flush_metrics

# Use before should_stop() to ensure AI sees latest data
flush_metrics()
if should_stop():
    # ...
```

## Container Environment

Stages run in Docker containers with specific mount points:

```
/mnt/
├── inputs/          # Read-only inputs
│   ├── features/    # Input signals mounted here
│   └── labels/
├── outputs/         # Write outputs here
│   ├── model/
│   └── metrics/
├── code/            # Workspace code (read-only)
│   ├── modules/
│   └── configs/
└── config.yaml      # Merged config file
```

## Complete Examples

### Preprocessing Stage

```python
#!/usr/bin/env python3
"""Stage: preprocess - Clean and transform raw data."""

import numpy as np
import pandas as pd
from goldfish.io import load_input, save_output, get_config


def main():
    # Load config
    config = get_config()
    normalize = config.get("normalize", True)
    fill_na = config.get("fill_na", "mean")

    # Load raw dataset
    df = load_input("raw_data")  # From registered dataset

    # Clean data
    if fill_na == "mean":
        df = df.fillna(df.mean())
    elif fill_na == "zero":
        df = df.fillna(0)

    # Split features and labels
    feature_cols = [c for c in df.columns if c != "target"]
    X = df[feature_cols].values
    y = df["target"].values

    # Normalize if configured
    if normalize:
        mean = X.mean(axis=0)
        std = X.std(axis=0) + 1e-8
        X = (X - mean) / std

    # Save outputs
    save_output("features", X)
    save_output("labels", y)

    print(f"Processed {len(df)} samples")
    print(f"Features shape: {X.shape}")


if __name__ == "__main__":
    main()
```

### Training Stage

```python
#!/usr/bin/env python3
"""Stage: train - Train LSTM model."""

import numpy as np
import torch
import torch.nn as nn
from pathlib import Path
from goldfish.io import load_input, save_output, get_config


class LSTMModel(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        return self.fc(lstm_out[:, -1, :])


def main():
    # Load config
    config = get_config()
    hidden_dim = config.get("hidden_dim", 64)
    learning_rate = config.get("learning_rate", 0.001)
    epochs = config.get("epochs", 100)
    batch_size = config.get("batch_size", 32)

    # Load inputs
    features = load_input("features")
    labels = load_input("labels")

    # Convert to tensors
    X = torch.FloatTensor(features)
    y = torch.FloatTensor(labels)

    # Create model
    model = LSTMModel(
        input_dim=X.shape[-1],
        hidden_dim=hidden_dim,
        output_dim=1
    )

    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.MSELoss()

    # Training loop
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        outputs = model(X.unsqueeze(1))
        loss = criterion(outputs.squeeze(), y)
        loss.backward()
        optimizer.step()

        if epoch % 10 == 0:
            print(f"Epoch {epoch}: Loss = {loss.item():.4f}")

    # Save model checkpoint
    model_dir = Path("/mnt/outputs/model")
    model_dir.mkdir(exist_ok=True)
    torch.save(model.state_dict(), model_dir / "model.pt")
    torch.save({
        "hidden_dim": hidden_dim,
        "input_dim": X.shape[-1],
        "output_dim": 1
    }, model_dir / "config.pt")

    save_output("model", model_dir)

    print(f"Training complete. Final loss: {loss.item():.4f}")


if __name__ == "__main__":
    main()
```

### Evaluation Stage

```python
#!/usr/bin/env python3
"""Stage: evaluate - Evaluate model performance."""

import numpy as np
import pandas as pd
import torch
from pathlib import Path
from goldfish.io import load_input, save_output, get_config


def main():
    config = get_config()
    threshold = config.get("threshold", 0.5)

    # Load model
    model_dir = load_input("model")
    model_config = torch.load(model_dir / "config.pt")

    # Recreate model architecture
    from train import LSTMModel  # Import from training module
    model = LSTMModel(**model_config)
    model.load_state_dict(torch.load(model_dir / "model.pt"))
    model.eval()

    # Load test data
    features = load_input("features")
    labels = load_input("labels")

    # Run inference
    X = torch.FloatTensor(features).unsqueeze(1)
    with torch.no_grad():
        predictions = model(X).squeeze().numpy()

    # Calculate metrics
    mse = np.mean((predictions - labels) ** 2)
    mae = np.mean(np.abs(predictions - labels))
    rmse = np.sqrt(mse)

    # Direction accuracy (for time series)
    pred_direction = np.diff(predictions) > 0
    true_direction = np.diff(labels) > 0
    direction_acc = np.mean(pred_direction == true_direction)

    # Save metrics
    metrics_df = pd.DataFrame([{
        "mse": mse,
        "mae": mae,
        "rmse": rmse,
        "direction_accuracy": direction_acc,
        "samples": len(labels)
    }])
    save_output("metrics", metrics_df)

    # Save predictions
    save_output("predictions", predictions)

    print(f"Evaluation Results:")
    print(f"  MSE: {mse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  Direction Accuracy: {direction_acc:.2%}")


if __name__ == "__main__":
    main()
```

## Stage Config Files

Corresponding config files in `configs/`:

### configs/preprocess.yaml

```yaml
# Data preprocessing settings
normalize: true
fill_na: mean
train_split: 0.8
random_seed: 42

# Compute profile (CPU is sufficient for preprocessing)
compute:
  profile: cpu-small
```

### configs/train.yaml

```yaml
# Model architecture
hidden_dim: 128
num_layers: 2
dropout: 0.2

# Training hyperparameters
learning_rate: 0.001
batch_size: 64
epochs: 200
early_stopping_patience: 20

# Compute profile (GPU for training)
compute:
  profile: h100-spot

# Environment variables
environment:
  CUDA_VISIBLE_DEVICES: "0"
  WANDB_PROJECT: forex-prediction
```

### configs/evaluate.yaml

```yaml
# Evaluation settings
threshold: 0.5
metrics:
  - mse
  - mae
  - rmse
  - direction_accuracy

# Compute profile
compute:
  profile: cpu-small
```

## Logging Best Practices

### Use print() for Progress

```python
print(f"Loading data...")
print(f"Processed {n} samples")
print(f"Epoch {epoch}: loss={loss:.4f}")
print(f"Final accuracy: {acc:.2%}")
```

Logs are captured and available via `logs(run_id)`.

### Metrics API (Preferred)

Use the Metrics API for scalars and artifacts. It is structured, validated, and persisted in `.goldfish/metrics.jsonl`.

```python
from goldfish.metrics import log_metric, log_metrics, log_artifact, finish

for step in range(epochs):
    loss = train_step(...)
    acc = evaluate(...)

    log_metric("train/loss", loss, step=step)
    log_metrics({"train/acc": acc, "train/lr": lr}, step=step)

log_artifact("model", "model.pt")  # relative to outputs dir
finish()  # safe to call multiple times
```

Notes:
- A given metric name should consistently use either `step=None` or `step=int`.
  Mixed step modes are skipped with a warning (no crash).
- `timestamp` accepts ISO 8601 strings or Unix float seconds (returned as ISO 8601 UTC strings).
- Bool values are rejected; use 0/1.
- Artifact paths must be relative to outputs dir (absolute paths and symlinks are rejected).
- Unique metric names are capped per run (default 10,000).
- Live sync: `get_run_metrics` will attempt a best-effort live sync for running runs.

### Progress Indicators

```python
from tqdm import tqdm

for batch in tqdm(dataloader, desc="Training"):
    # Training code
    pass
```

## Error Handling

### Graceful Failures

```python
def main():
    try:
        features = load_input("features")
    except FileNotFoundError as e:
        print(f"ERROR: Required input 'features' not found: {e}")
        raise SystemExit(1)

    try:
        model = train_model(features)
    except Exception as e:
        print(f"ERROR: Training failed: {e}")
        import traceback
        traceback.print_exc()
        raise SystemExit(1)

    save_output("model", model)
    print("SUCCESS: Training completed")
```

### Validation

```python
def main():
    features = load_input("features")

    # Validate inputs
    if len(features) == 0:
        raise ValueError("Empty features array")

    if np.isnan(features).any():
        raise ValueError("Features contain NaN values")

    # Continue with processing
```

## Custom Docker Images (Advanced)

Goldfish manages **two layers** of Docker images via `manage_base_images()`:

1. **Base images** (`goldfish-base-gpu`, `goldfish-base-cpu`) - foundation with CUDA, PyTorch, FlashAttention-3
2. **Project images** (`{project}-gpu`, `{project}-cpu`) - extend base with project-specific packages

### Prerequisites: Base Images

Base images must exist in Artifact Registry before project images can be built. Check status:

```python
manage_base_images(action="list")
# → base_images: shows goldfish-base-* status
# → project_images: shows {project}-* status
```

If base images don't exist (first-time setup), build and push them:

Two build backends are available:
- `backend="local"` (default): Uses local Docker daemon
- `backend="cloud"`: Uses Google Cloud Build (recommended for GPU images - faster, doesn't tie up local machine)

```python
# Build goldfish base GPU image on Cloud Build (recommended, ~15-20 min)
result = manage_base_images(action="build", image_type="gpu", target="base", backend="cloud")
# Returns immediately with build_id - poll with get_build_status(result["build_id"])

# Or build locally (ties up machine but works without GCP)
manage_base_images(action="build", image_type="gpu", target="base", wait=True)

# Push to Artifact Registry (required for GCE runs)
manage_base_images(action="push", image_type="gpu", target="base")

# CPU image (~5 min)
manage_base_images(action="build", image_type="cpu", target="base", backend="cloud")
manage_base_images(action="push", image_type="cpu", target="base")
```

**Cloud Build Requirements:**
- `gce.project_id` must be set in goldfish.yaml
- The service account used by Cloud Build needs Artifact Registry write permission on the repository:
  ```bash
  # Check which service account Cloud Build uses
  # (usually: {PROJECT_NUMBER}-compute@developer.gserviceaccount.com)
  gcloud builds list --project=YOUR_PROJECT_ID --limit=1 --format="value(serviceAccount)"

  # Grant repository-level permission (one-time setup)
  gcloud artifacts repositories add-iam-policy-binding goldfish \
    --location=us \
    --project=YOUR_PROJECT_ID \
    --member="serviceAccount:YOUR_SERVICE_ACCOUNT" \
    --role="roles/artifactregistry.writer"
  ```

### Option 1: Config-Based Packages (Recommended)

Add extra pip packages via `goldfish.yaml` without writing a Dockerfile:

```yaml
# goldfish.yaml
docker:
  extra_packages:
    gpu:
      - triton
    cpu:
      - lightgbm
```

Then build and push the project image:

```python
# Check current images and customizations
manage_base_images(action="list")

# View effective Dockerfile (base + your packages)
manage_base_images(action="inspect", image_type="gpu")

# Build project image (target="project" is default)
manage_base_images(action="build", image_type="gpu", wait=True)

# Push to Artifact Registry
manage_base_images(action="push", image_type="gpu")
```

### Option 2: Custom Dockerfile (Full Control)

For system-level dependencies or major customizations, place a `Dockerfile.gpu` or `Dockerfile.cpu` in your project root:

```dockerfile
# Dockerfile.gpu (in project root)
FROM goldfish-base-gpu:v5

# System dependencies
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Additional Python packages
RUN pip install --no-cache-dir my-custom-package

WORKDIR /app
```

Then build and push:

```python
manage_base_images(action="build", image_type="gpu", wait=True)
manage_base_images(action="push", image_type="gpu")
```

### Image Management Workflow

```python
# Check if rebuild is needed
manage_base_images(action="check")
# → Shows: needs_rebuild, needs_push, has_customization

# Start a Cloud Build (recommended for GPU)
result = manage_base_images(action="build", image_type="gpu", backend="cloud")
# Returns immediately with build_id

# Monitor build progress
status = get_build_status(result["build_id"])
# → Shows: status, logs_uri (Cloud Build logs URL), error

# Or run locally and wait
manage_base_images(action="build", image_type="gpu", wait=True)
# → Shows: status, logs_tail, image_tag
```

### Base Images

| Type | Base Image | Included |
|------|------------|----------|
| GPU | `goldfish-base-gpu:v5` | CUDA 12.8 + PyTorch 2.9.1 + PyTorch + FlashAttention-3 + numpy/pandas/scikit-learn |
| CPU | `goldfish-base-cpu:v5` | PyTorch (CPU) + numpy/pandas/scikit-learn |

### Target Parameter

- `target="base"` - Build/push goldfish-base-{cpu,gpu} foundation images
- `target="project"` (default) - Build/push {project}-{cpu,gpu} images

**Note:** Most workloads only need `requirements.txt` for simple pip packages. Use custom images for system dependencies or when you need specific base image customizations.

## Testing Stages Locally

Test stages before running in Goldfish:

```python
# test_train.py
import tempfile
import numpy as np
from pathlib import Path

# Create mock inputs
with tempfile.TemporaryDirectory() as tmpdir:
    input_dir = Path(tmpdir) / "inputs"
    output_dir = Path(tmpdir) / "outputs"
    input_dir.mkdir()
    output_dir.mkdir()

    # Create test data
    np.save(input_dir / "features.npy", np.random.randn(100, 10))
    np.save(input_dir / "labels.npy", np.random.randn(100))

    # Set environment for goldfish.io
    import os
    os.environ["GOLDFISH_INPUT_DIR"] = str(input_dir)
    os.environ["GOLDFISH_OUTPUT_DIR"] = str(output_dir)

    # Run stage
    from modules.train import main
    main()

    # Verify outputs
    assert (output_dir / "model").exists()
```

## Common Patterns

### Checkpointing During Training

```python
def main():
    model = create_model()
    best_loss = float("inf")

    for epoch in range(epochs):
        loss = train_epoch(model)

        if loss < best_loss:
            best_loss = loss
            save_checkpoint(model, "best")
            print(f"New best model: loss={loss:.4f}")

    # Final save
    save_output("model", checkpoint_dir)
```

### Resumable Training

```python
def main():
    config = get_config()

    # Check for existing checkpoint
    checkpoint_path = Path("/mnt/inputs/checkpoint")
    if checkpoint_path.exists():
        model, start_epoch = load_checkpoint(checkpoint_path)
        print(f"Resuming from epoch {start_epoch}")
    else:
        model = create_model()
        start_epoch = 0

    # Continue training
    for epoch in range(start_epoch, config["epochs"]):
        train_epoch(model)
```

### Multi-GPU Training

```python
def main():
    import torch.distributed as dist

    # Initialize distributed training
    dist.init_process_group(backend="nccl")
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)

    model = create_model().cuda()
    model = torch.nn.parallel.DistributedDataParallel(model)

    # Training loop
    train(model)
```

## Heartbeat API (Long-Running Jobs)

For long-running computations, use the heartbeat API to signal that your job is alive. This prevents the job from being terminated due to inactivity.

### Basic Usage

```python
from goldfish.io import heartbeat

def main():
    for i, batch in enumerate(data_loader):
        # Signal that we're alive (with optional status message)
        heartbeat(f"Processing batch {i}/{total}")

        # Your computation (may take minutes without log output)
        process_batch(batch)
```

### Why Use Heartbeats?

Goldfish monitors job health via the supervisor. If a job appears stalled (no activity), it may be terminated to prevent wasted compute costs. Heartbeats explicitly signal that your job is working even during:

- Long computations with no log output
- Large data transfers
- GPU operations that block for extended periods
- Any operation that takes minutes without printing

### Heartbeat Functions

```python
from goldfish.io import heartbeat, get_heartbeat_age, read_heartbeat

# Signal alive (rate-limited to 1/sec automatically)
heartbeat()

# With status message
heartbeat("Training epoch 5/100")

# Force write (bypass rate limiting)
heartbeat(message="Critical checkpoint", force=True)

# Check heartbeat age (for debugging)
age = get_heartbeat_age()  # Returns seconds since last heartbeat

# Read full heartbeat data
data = read_heartbeat()
# Returns: {"timestamp": 1234567890, "message": "...", "pid": 1234, "age_seconds": 5.2}
```

### When to Call heartbeat()

| Scenario | Recommendation |
|----------|----------------|
| Training loop | Call every epoch or every N batches |
| Data loading | Call periodically during large loads |
| Model evaluation | Call between evaluation steps |
| Checkpointing | Call before and after saves |
| Any 1+ minute operation | Call at least every 5 minutes |

### Example: Training with Heartbeats

```python
from goldfish.io import load_input, save_output, get_config, heartbeat

def main():
    config = get_config()
    features = load_input("features")
    labels = load_input("labels")

    model = create_model()

    heartbeat("Starting training")

    for epoch in range(config["epochs"]):
        # Heartbeat every epoch
        heartbeat(f"Epoch {epoch}/{config['epochs']}")

        for batch_idx, (X, y) in enumerate(data_loader):
            # For very large datasets, heartbeat every N batches
            if batch_idx % 100 == 0:
                heartbeat(f"Epoch {epoch}, batch {batch_idx}")

            train_step(model, X, y)

        # Heartbeat before potentially slow checkpoint
        heartbeat(f"Saving checkpoint after epoch {epoch}")
        save_checkpoint(model)

    heartbeat("Training complete, saving final model")
    save_output("model", model_path)
```

### Cost Protection Layers

Goldfish implements 4 layers of cost protection for GCE instances:

1. **Self-Deletion Trap** - Instance deletes itself on any exit (success, failure, signal)
2. **Watchdog Timeout** - Optional hard limit via `max_runtime_seconds`
3. **Daemon Monitoring** - Checks for orphaned instances every 60s
4. **Heartbeat Supervisor** - Monitors heartbeat file, terminates stalled jobs

Configure in stage config:
```yaml
compute:
  profile: h100-spot
  max_runtime_seconds: 14400     # 4 hour hard limit
  heartbeat_timeout_seconds: 600  # 10 min without heartbeat = stalled
```
