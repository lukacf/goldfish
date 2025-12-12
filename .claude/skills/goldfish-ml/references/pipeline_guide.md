# Pipeline YAML Specification

Complete guide to writing Goldfish pipeline.yaml files that define ML workflows.

## File Locations

Goldfish looks for pipelines in these locations:
```
workspace/
├── pipeline.yaml           # Default pipeline
└── pipelines/
    ├── training.yaml       # Named pipeline
    └── evaluation.yaml     # Another named pipeline
```

Run specific pipelines with: `run("w1", pipeline="training")`

## Basic Structure

```yaml
# Optional metadata
name: my-pipeline
description: Pipeline description

# Required: list of stages
stages:
  - name: stage_name
    inputs:
      input_name:
        type: signal_type
        # Type-specific fields
    outputs:
      output_name:
        type: signal_type
```

## Signal Types

### Dataset (External Data)

Input-only signal referencing registered datasets.

```yaml
inputs:
  raw_data:
    type: dataset
    dataset: sales_v1      # Must be registered via register_dataset()
```

### NPY (NumPy Arrays)

For arrays, embeddings, tensors.

```yaml
# As input from upstream stage
inputs:
  features:
    type: npy
    from_stage: preprocess
    signal: features

# As output
outputs:
  embeddings:
    type: npy
```

### CSV (Tabular Data)

For pandas DataFrames and tabular data.

```yaml
inputs:
  metrics:
    type: csv
    from_stage: evaluate
    signal: results

outputs:
  predictions:
    type: csv
```

### Directory (Multi-File Outputs)

For model checkpoints, multi-file artifacts.

```yaml
outputs:
  model:
    type: directory     # Entire directory saved

inputs:
  checkpoint:
    type: directory
    from_stage: train
    signal: model
```

### File (Single Files)

For configs, small outputs, single artifacts.

```yaml
outputs:
  config:
    type: file

inputs:
  schema:
    type: file
    from_stage: setup
    signal: config
```

## Complete Example

```yaml
name: forex-prediction
description: EUR/USD price prediction pipeline

stages:
  # Stage 1: Data preprocessing
  - name: preprocess
    inputs:
      raw_ticks:
        type: dataset
        dataset: eurusd_ticks_v3    # Registered dataset
    outputs:
      features:
        type: npy
      labels:
        type: npy
      metadata:
        type: file

  # Stage 2: Model training
  - name: train
    inputs:
      features:
        type: npy
        from_stage: preprocess
        signal: features
      labels:
        type: npy
        from_stage: preprocess
        signal: labels
    outputs:
      model:
        type: directory
      training_log:
        type: csv

  # Stage 3: Evaluation
  - name: evaluate
    inputs:
      model:
        type: directory
        from_stage: train
        signal: model
      features:
        type: npy
        from_stage: preprocess
        signal: features
      labels:
        type: npy
        from_stage: preprocess
        signal: labels
    outputs:
      metrics:
        type: csv
      predictions:
        type: npy

  # Stage 4: Report generation
  - name: report
    inputs:
      metrics:
        type: csv
        from_stage: evaluate
        signal: metrics
      training_log:
        type: csv
        from_stage: train
        signal: training_log
    outputs:
      report:
        type: file
```

## Signal Wiring Rules

### Rule 1: Datasets are Always External

Datasets must be registered before use:
```yaml
# Wrong - can't define dataset inline
inputs:
  data:
    type: dataset
    path: /some/path    # Invalid!

# Correct - reference registered dataset
inputs:
  data:
    type: dataset
    dataset: my_registered_dataset
```

### Rule 2: Stage Outputs Must Match Downstream Inputs

Types must be compatible:
```yaml
# Stage A outputs npy
outputs:
  features:
    type: npy

# Stage B must expect npy from A
inputs:
  features:
    type: npy          # Must match!
    from_stage: stage_a
    signal: features
```

### Rule 3: Signal Names Must Be Unique Per Stage

Within a stage, input and output names must be unique:
```yaml
# Wrong - duplicate name
inputs:
  data:
    type: dataset
    dataset: train_data
outputs:
  data:           # Same name as input - ambiguous!
    type: npy

# Correct - unique names
inputs:
  raw_data:
    type: dataset
    dataset: train_data
outputs:
  processed_data:
    type: npy
```

### Rule 4: No Circular Dependencies

Stages form a DAG (Directed Acyclic Graph):
```yaml
# Wrong - circular dependency
stages:
  - name: A
    inputs:
      x:
        from_stage: B    # A depends on B
  - name: B
    inputs:
      y:
        from_stage: A    # B depends on A - CYCLE!

# Correct - acyclic
stages:
  - name: A
    outputs: ...
  - name: B
    inputs:
      from_stage: A      # One direction only
```

## Advanced Features

### Multiple Inputs from Same Stage

A stage can consume multiple outputs from an upstream stage:
```yaml
- name: train
  inputs:
    features:
      type: npy
      from_stage: preprocess
      signal: features
    labels:
      type: npy
      from_stage: preprocess
      signal: labels
```

### Diamond Dependencies

Multiple stages can consume the same upstream output:
```yaml
stages:
  - name: preprocess
    outputs:
      features:
        type: npy

  - name: train_lstm
    inputs:
      features:
        from_stage: preprocess
        signal: features

  - name: train_transformer
    inputs:
      features:
        from_stage: preprocess    # Same source
        signal: features
```

### Optional Artifacts

Mark outputs as artifacts for auto-registration:
```yaml
outputs:
  model:
    type: directory
    artifact: true      # Auto-promote to source after run
```

### Storage Hints

Provide storage hints for optimization:
```yaml
outputs:
  large_embeddings:
    type: npy
    storage: gcs          # gcs, hyperdisk, local
```

## Stage Config Files

Each stage has a corresponding config file:

```
workspace/
├── pipeline.yaml
└── configs/
    ├── preprocess.yaml
    ├── train.yaml
    └── evaluate.yaml
```

### Config File Structure

```yaml
# configs/train.yaml

# General settings
batch_size: 32
learning_rate: 0.001
epochs: 100

# Compute profile
compute:
  profile: h100-spot    # Resource profile

# Environment
environment:
  WANDB_PROJECT: my-project
  CUDA_VISIBLE_DEVICES: "0"
```

### Config Override at Runtime

Override configs without editing files:
```python
run("w1", stages=["train"], config_override={
    "learning_rate": 0.0001,
    "epochs": 200
})
```

For multiple stages:
```python
run("w1", config_override={
    "train": {"learning_rate": 0.001},
    "evaluate": {"threshold": 0.5}
})
```

## Validation

Goldfish validates pipelines automatically:

1. **Stage files exist**: `modules/{stage}.py` and `configs/{stage}.yaml`
2. **Signal types match**: Upstream outputs compatible with downstream inputs
3. **No cycles**: DAG validation
4. **Datasets exist**: Referenced datasets are registered
5. **Names are valid**: Alphanumeric + underscore

Validation errors are reported when:
- Calling `run()`
- Calling `get_workspace()` (includes pipeline info)

## Best Practices

1. **Name stages by action**: `preprocess`, `train`, `evaluate`, `export`
2. **Use meaningful signal names**: `features`, `model`, `predictions`, not `output1`
3. **Keep pipelines simple**: 3-5 stages is typical
4. **Document with description field**: Explain the pipeline purpose
5. **Version datasets**: `sales_v1`, `sales_v2` not just `sales`
6. **Separate concerns**: One stage = one task

## Common Patterns

### Training Pipeline

```yaml
stages:
  - name: preprocess
  - name: train
  - name: evaluate
```

### Feature Engineering Pipeline

```yaml
stages:
  - name: extract_features
  - name: normalize
  - name: select_features
  - name: validate
```

### Inference Pipeline

```yaml
stages:
  - name: load_model
  - name: preprocess
  - name: predict
  - name: postprocess
```

### Ensemble Pipeline

```yaml
stages:
  - name: preprocess
  - name: train_model_a
  - name: train_model_b
  - name: train_model_c
  - name: ensemble
  - name: evaluate
```
