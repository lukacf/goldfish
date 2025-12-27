# End-to-End Example: Training a Language Model

A complete worked example showing the full Goldfish workflow from project setup to model training.

## Scenario

Train a 1B parameter language model on tokenized text data using an H100 GPU.

## Step 1: Project Setup

First, ensure `goldfish.yaml` is configured:

```yaml
# goldfish.yaml
project_name: marketlm
dev_repo_path: marketlm-dev

gcs:
  bucket: mlm-artifacts-bucket

gce:
  project_id: my-gcp-project
```

Verify configuration:
```
reload_config()
```

## Step 2: Register Dataset

Register the tokenized training data:

```
register_dataset(
    name="v37-tokens",
    source="gs://mlm-artifacts-bucket/unified-v37/tokens/",
    description="Tokenized training data from unified-v37 preprocessing",
    format="directory"
)
```

Verify it's available:
```
list_sources()
get_source("v37-tokens")
```

## Step 3: Create Workspace

```
create_workspace(
    name="1b-8k-lm",
    goal="Train 1B parameter LM with 8096 context on V37 tokens"
)

mount(slot="w1", workspace="1b-8k-lm", reason="Starting LM training experiment")
```

## Step 4: Create Pipeline

Create `workspaces/w1/pipeline.yaml`:

```yaml
name: 1b-8k-lm
description: 1B parameter LM with 8096 context on V37 tokens

stages:
  - name: train
    inputs:
      tokens:
        type: dataset
        dataset: v37-tokens
        schema:
          kind: tensor
          shape: [null]
          dtype: int32
    outputs:
      model:
        type: directory
        schema: null
      training_log:
        type: csv
        schema: null
```

## Step 5: Create Stage Config

Create `workspaces/w1/configs/train.yaml`:

```yaml
# Model architecture
d_model: 2048
n_layers: 24
n_heads: 16
d_ff: 8192
dropout: 0.1
context_len: 8096

# Training settings
batch_size: 2
grad_accum: 16
epochs: 20
lr: 1e-4
warmup_steps: 2000
weight_decay: 0.1

# Compute profile
compute:
  profile: h100-spot
  gpu_count: 1

# Checkpointing
save_every_n_epochs: 1
eval_every_n_steps: 500

# Logging
wandb_project: market-lm-v37
wandb_name: 1b-8k-lm
```

## Step 6: Create Stage Module

Create `workspaces/w1/modules/train.py`:

```python
#!/usr/bin/env python3
"""Stage: train - Train language model."""

import torch
import torch.nn as nn
from pathlib import Path
from goldfish.io import load_input, save_output, get_config


class LanguageModel(nn.Module):
    def __init__(self, vocab_size, d_model, n_layers, n_heads, d_ff, context_len, dropout):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, d_model)
        self.pos_embedding = nn.Embedding(context_len, d_model)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_ff,
            dropout=dropout,
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        self.output = nn.Linear(d_model, vocab_size)

    def forward(self, x):
        seq_len = x.size(1)
        positions = torch.arange(seq_len, device=x.device).unsqueeze(0)

        x = self.embedding(x) + self.pos_embedding(positions)
        x = self.transformer(x)
        return self.output(x)


def main():
    config = get_config()

    # Load tokenized data
    tokens_path = load_input("tokens")
    train_data = torch.load(tokens_path / "train.pt")
    val_data = torch.load(tokens_path / "val.pt")
    vocab_size = int(open(tokens_path / "vocab_size.txt").read())

    print(f"Loaded {len(train_data)} training samples")
    print(f"Vocab size: {vocab_size}")

    # Create model
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = LanguageModel(
        vocab_size=vocab_size,
        d_model=config["d_model"],
        n_layers=config["n_layers"],
        n_heads=config["n_heads"],
        d_ff=config["d_ff"],
        context_len=config["context_len"],
        dropout=config["dropout"]
    ).to(device)

    param_count = sum(p.numel() for p in model.parameters())
    print(f"Model parameters: {param_count:,} ({param_count/1e9:.2f}B)")

    # Training setup
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=float(config["lr"]),
        weight_decay=config["weight_decay"]
    )
    criterion = nn.CrossEntropyLoss()

    # Training loop
    model.train()
    training_log = []

    for epoch in range(config["epochs"]):
        total_loss = 0
        for batch_idx, batch in enumerate(train_data):
            batch = batch.to(device)
            inputs, targets = batch[:, :-1], batch[:, 1:]

            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs.reshape(-1, vocab_size), targets.reshape(-1))
            loss.backward()
            optimizer.step()

            total_loss += loss.item()

            if batch_idx % config["eval_every_n_steps"] == 0:
                print(f"Epoch {epoch}, Step {batch_idx}: loss={loss.item():.4f}")

        avg_loss = total_loss / len(train_data)
        training_log.append({"epoch": epoch, "loss": avg_loss})
        print(f"Epoch {epoch} complete: avg_loss={avg_loss:.4f}")

        # Save checkpoint
        if (epoch + 1) % config["save_every_n_epochs"] == 0:
            checkpoint_dir = Path("/mnt/outputs/model")
            checkpoint_dir.mkdir(exist_ok=True)
            torch.save({
                "epoch": epoch,
                "model_state_dict": model.state_dict(),
                "optimizer_state_dict": optimizer.state_dict(),
                "config": config
            }, checkpoint_dir / f"checkpoint_epoch_{epoch}.pt")

    # Save final model
    model_dir = Path("/mnt/outputs/model")
    model_dir.mkdir(exist_ok=True)
    torch.save(model.state_dict(), model_dir / "model.pt")
    torch.save(config, model_dir / "config.pt")
    save_output("model", model_dir)

    # Save training log
    import pandas as pd
    log_df = pd.DataFrame(training_log)
    save_output("training_log", log_df)

    print("Training complete!")


if __name__ == "__main__":
    main()
```

## Step 7: Run Training

```
run("w1", stages=["train"], reason="Training 1B LM on V37 tokens")
```

Returns:
```json
{
  "runs": [{
    "stage_run_id": "stage-abc123",
    "workspace": "1b-8k-lm",
    "version": "v1",
    "stage": "train",
    "profile": "h100-spot",
    "status": "running",
    "inputs": {
      "tokens": "gs://mlm-artifacts-bucket/unified-v37/tokens/"
    }
  }]
}
```

## Step 8: Monitor Progress

Check status:
```
get_run("stage-abc123")
```

View logs:
```
logs("stage-abc123", tail=100)
```

List all running jobs:
```
list_runs(status="running")
```

## Step 9: After Completion

Get outputs:
```
get_outputs("stage-abc123")
```

Returns:
```json
{
  "outputs": [
    {"name": "model", "type": "directory", "location": "gs://mlm-artifacts-bucket/artifacts/..."},
    {"name": "training_log", "type": "csv", "location": "gs://mlm-artifacts-bucket/artifacts/..."}
  ]
}
```

Check lineage:
```
get_run_provenance("stage-abc123")
```

## Step 10: Save and Continue

Create version:
```
save_version("w1", "Training complete - 1B model trained on V37")
```

Hibernate workspace:
```
hibernate("w1", "Completed 1B-8k LM training experiment")
```

## Full Tool Sequence Summary

```
1. reload_config()                                    # Verify config
2. register_dataset("v37-tokens", ...)               # Register data
3. create_workspace("1b-8k-lm", goal="...")          # Create workspace
4. mount("w1", "1b-8k-lm", reason="...")             # Mount to slot
5. [Create files: pipeline.yaml, configs/train.yaml, modules/train.py]
6. run("w1", stages=["train"], reason="...")         # Launch training
7. logs("stage-abc123", tail=100)                    # Monitor
8. get_run("stage-abc123")                           # Check status
9. get_outputs("stage-abc123")                       # Get results
10. save_version("w1", "Training complete")          # Save progress
11. hibernate("w1", "Done with experiment")          # Clean up
```

## Common Variations

### Run All Pipeline Stages

```
run("w1")  # Runs all stages in pipeline order
```

### Resume Failed Run

```
# Check what failed
get_run("stage-abc123")
logs("stage-abc123", tail=500)

# Fix code, then re-run
run("w1", stages=["train"], reason="Fixed OOM error, reduced batch size")
```

### Promote Output to Dataset

After training, make the model available as a dataset for other workspaces:

```
promote_artifact(
    job_id="stage-abc123",
    output_name="model",
    source_name="1b-8k-lm-model-v1",
    reason="Promote trained model for evaluation experiments"
)
```

### Branch for Experiment Variation

```
create_workspace("1b-8k-lm-larger", goal="Try 2B params instead")
mount("w2", "1b-8k-lm-larger", reason="Experimenting with larger model")

# Copy and modify configs
# Run new experiment
```
