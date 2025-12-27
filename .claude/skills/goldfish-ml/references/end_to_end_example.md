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
run("w1", stages=["train"], reason={
    "description": "Training 1B LM on V37 tokens",
    "hypothesis": "1B params should achieve < 3.0 perplexity",
    "approach": "Standard transformer training with AdamW",
    "min_result": "Perplexity under 4.0"
})
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

## Step 10: Save and Tag Milestone

Create version and tag it as a milestone:
```
save_version("w1", "Training complete - 1B model trained on V37")
# Returns version="v1"

# Tag this as a significant milestone for easy reference
tag_version("1b-8k-lm", "v1", "initial-training-complete")
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
6. run("w1", stages=["train"], reason={...})         # Launch training (structured reason)
7. logs("stage-abc123", tail=100)                    # Monitor
8. get_run("stage-abc123")                           # Check status
9. get_outputs("stage-abc123")                       # Get results
10. save_version("w1", "Training complete")          # Save progress
11. tag_version("1b-8k-lm", "v1", "baseline")        # Tag milestone
12. hibernate("w1", "Done with experiment")          # Clean up
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
run("w1", stages=["train"], reason={
    "description": "Fixed OOM error, reduced batch size",
    "approach": "Reduced batch size from 4 to 2"
})
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

---

## Extended Example: Iterative Experimentation with Tags and Pruning

This shows a realistic ML workflow over multiple iterations, demonstrating how to use tags and pruning to manage experiment history.

### Phase 1: Initial Development (Many Failures)

```
# Day 1-3: Getting the basics working
mount("w1", "1b-8k-lm", reason="Starting LM development iteration")

# First attempt: broken imports
run("w1", reason={"description": "Initial training attempt"})
# FAILED: ImportError in modules/train.py

# Fix imports, try again
run("w1", reason={"description": "Fixed imports, retry training"})
# FAILED: Shape mismatch in attention layer

# Fix architecture
run("w1", reason={"description": "Fixed attention dimensions"})
# FAILED: NaN loss after 100 steps

# Lower learning rate
run("w1", reason={
    "description": "Lower learning rate to prevent NaN",
    "approach": "Reduced lr from 1e-3 to 1e-4"
})
# SUCCESS: Training runs, but loss stuck at 8.0

# ... many more iterations ...
# At this point we have v1 through v15, all failed/suboptimal
```

### Phase 2: First Working Model

```
# Finally working! (v16)
run("w1", reason={
    "description": "Added warmup and gradient clipping",
    "hypothesis": "Will prevent early instability",
    "approach": "2000 warmup steps, clip at 1.0"
})
# SUCCESS: Loss decreasing properly!

# Mark this milestone
save_version("w1", "First working training configuration")
# Returns v16

tag_version("1b-8k-lm", "v16", "first-working")
```

### Phase 3: Optimization Iterations

```
# Now iterate on the working baseline
# v17: Try larger batch
run("w1", reason={
    "description": "Testing larger batch size",
    "hypothesis": "Batch 4 might improve convergence"
})
# FAILED: OOM

# v18: Gradient accumulation instead
run("w1", reason={
    "description": "Gradient accumulation for effective batch 4",
    "approach": "batch_size=2, grad_accum=2"
})
# Slightly worse

# v19-v30: More experiments, mixed results
# ...

# v31: Best configuration found!
run("w1", reason={
    "description": "Combined best settings from experiments",
    "hypothesis": "Should achieve lowest perplexity yet",
    "approach": "lr=5e-5, batch=2, accum=4, warmup=4000",
    "goal": "Perplexity under 3.0"
})
# SUCCESS: Perplexity 2.87!

save_version("w1", "Best model configuration found")
tag_version("1b-8k-lm", "v31", "best-model")
```

### Phase 4: Clean Up History

```
# Now we have v1-v31 but only v16 and v31 matter
# List what we've tagged
list_tags("1b-8k-lm")
# Returns: [{"version": "v16", "tag_name": "first-working"},
#           {"version": "v31", "tag_name": "best-model"}]

# Prune all the noise before first working version
prune_before_tag("1b-8k-lm", "first-working",
    reason="Pruning all failed initial attempts before first working config")
# Prunes v1-v15 (15 versions)

# Prune the iterations between milestones
prune_versions("1b-8k-lm", "v17", "v30",
    reason="Pruning optimization iterations between milestones")
# Prunes v17-v30 (14 versions)

# Check cleanup status
get_pruned_count("1b-8k-lm")
# Returns: {"count": 29}

# Now get_workspace_lineage shows clean history:
# - v16 "first-working"
# - v31 "best-model" (current)
# (29 versions pruned)
```

### Phase 5: Continue Development

```
# Version numbering continues normally
run("w1", reason={
    "description": "Testing attention head reduction",
    "hypothesis": "16 heads might be overkill"
})
# Creates v32 (not v3!)

# Tag more milestones as you go
tag_version("1b-8k-lm", "v35", "production-candidate")

# Later, clean up again
prune_versions("1b-8k-lm", "v32", "v34",
    reason="Pruning failed experiments after best-model")
```

### Summary: Tags and Pruning Workflow

```
# Tag milestones as you discover them
tag_version(workspace, version, "meaningful-name")

# Prune noise periodically
prune_before_tag(workspace, "milestone", reason="...")  # Clean start
prune_versions(workspace, "v_start", "v_end", reason="...")  # Between milestones

# Check status
list_tags(workspace)  # What's significant
get_pruned_count(workspace)  # How much noise hidden

# Restore if needed (pruning is reversible)
unprune_version(workspace, "v5")  # If you need to review old work
```

### Key Benefits

1. **Clean context**: Claude sees only significant versions in STATE.md
2. **Preserved history**: Pruned versions still exist for audit/recovery
3. **Protected milestones**: Tagged versions cannot be accidentally pruned
4. **Retroactive tagging**: Discover milestones after the fact
5. **Continuous numbering**: v50 is always v50, even if v1-v49 are pruned