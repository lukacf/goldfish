# Goldfish Rust SDK

Rust SDK for writing Goldfish ML pipeline stages.

## Overview

This crate provides the Rust API for Goldfish ML pipeline stages, with:

- **Input/Output**: Type-safe loading and saving of signals (tensors, CSVs, JSON)
- **Schema Validation**: Automatic validation against pipeline-defined schemas
- **Statistics**: Background computation of output statistics for SVS
- **Security**: Path traversal protection, proper error handling

## Quick Start

```rust
use goldfish_rust::{init, load_input, save_output, OutputData};

fn main() -> goldfish_rust::Result<()> {
    // Initialize Goldfish (auto-finalizes on drop)
    let _guard = init();

    // Load input
    let features = load_input("features", None)?;

    // Process data
    let processed = match features {
        OutputData::TensorF32(arr) => {
            // Transform...
            OutputData::TensorF32(arr)
        }
        _ => features,
    };

    // Save output
    save_output("processed", processed, false)?;

    Ok(())
}
```

## Module Structure

```
goldfish_rust/
├── config/     # Configuration from environment variables
├── error/      # Typed error handling with thiserror
├── io/         # Input/output operations
│   ├── npy     # NPY file handling (v1/v2/v3)
│   ├── npz     # NPZ archive handling
│   └── path    # Path validation (security)
├── logging/    # Runtime logging and heartbeat
├── schema/     # Schema types and validation
└── stats/      # Background statistics computation
```

## Features

### Input/Output

```rust
use goldfish_rust::{load_input, save_output, OutputData};

// Load by name (format from config)
let data = load_input("features", None)?;

// Override format
let csv_data = load_input("data", Some("csv"))?;

// Save with validation
save_output("output", OutputData::TensorF32(arr), false)?;
```

Supported formats: `npy`, `csv`, `json`, `directory`, `file`, `dataset`

Multi-array tensor outputs (`OutputData::MultiTensor`) are auto-saved when the output
format is `directory` (or left as default). Each array is written as `<name>.npy`
under the output directory.

### Schema Validation

Schemas are defined in `pipeline.yaml` and validated automatically:

```yaml
outputs:
  features:
    type: npy
    schema:
      kind: tensor
      dtype: float32
      shape: [null, 128]  # null = any size
```

### Statistics for SVS

When enabled, statistics are computed in background threads:

```rust
// Automatic for save_output() calls
// Manual for custom outputs:
use goldfish_rust::stats::enqueue_stats;
enqueue_stats("custom_output", &path, "float32");
```

### Logging

```rust
use goldfish_rust::{runtime_log, heartbeat, should_stop, log_metric};

// Structured logging (to stdout + .goldfish/logs.txt)
runtime_log("Processing batch 50/100", "INFO");

// Heartbeat for long computations
for i in 0..100 {
    heartbeat(Some(&format!("Batch {}/100", i)), false);
    // ... process batch ...

    // Check for early termination
    if should_stop() {
        break;
    }
}

// Log metrics
log_metric("loss", 0.5, Some(100));
```

### Error Handling

Strongly-typed errors for proper handling:

```rust
use goldfish_rust::{GoldfishError, ConfigError, IoError};

match result {
    Err(GoldfishError::Config(ConfigError::UndefinedInput { name })) => {
        // Handle missing input
    }
    Err(GoldfishError::Io(IoError::FileNotFound { path })) => {
        // Handle missing file
    }
    Err(GoldfishError::PathSecurity(_)) => {
        // Path traversal attempt
    }
    _ => {}
}
```

## Security

### Path Traversal Protection

All input/output names are validated to prevent path traversal:

```rust
// These will error:
load_input("../../../etc/passwd", None);  // PathTraversal
load_input("foo/bar", None);              // PathTraversal
load_input("foo\0bar", None);             // InvalidCharacters
```

### Safe Defaults

- SVS validation defaults to "warning" mode (logs but doesn't fail)
- Statistics computation has timeouts to prevent blocking
- Mutex operations use proper error handling (no panics)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GOLDFISH_STAGE_CONFIG` | Stage configuration JSON | Required |
| `GOLDFISH_SVS_CONFIG` | SVS configuration JSON | `{"enabled": true, "default_enforcement": "warning"}` |
| `GOLDFISH_INPUTS_DIR` | Input files directory | `/mnt/inputs` |
| `GOLDFISH_OUTPUTS_DIR` | Output files directory | `/mnt/outputs` |
| `GOLDFISH_SVS_STATS_ENABLED` | Enable stats computation | `false` |

## Dependencies

- `ndarray` + `ndarray-npy`: Tensor operations
- `polars`: DataFrames
- `thiserror`: Error handling
- `serde` + `serde_json`: Serialization
- `chrono`: Timestamps

## Testing

```bash
cargo test          # Run all tests
cargo test --doc    # Doc tests only
```

## License

MIT
