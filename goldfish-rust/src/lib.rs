//! Goldfish Rust SDK for ML pipeline stages.
//!
//! This crate provides the Rust API for writing Goldfish ML pipeline stages.
//! It handles input/output data loading and saving, schema validation,
//! statistics computation, and integration with the SVS (Semantic Validation System).
//!
//! # Quick Start
//!
//! ```no_run
//! use goldfish_rust::{init, load_input, save_output, OutputData, GoldfishError};
//!
//! fn main() -> Result<(), GoldfishError> {
//!     let _guard = init();
//!     let features = load_input("features", None)?;
//!     // Process data...
//!     save_output("processed", features, false)?;
//!     Ok(())
//! }
//! ```
//!
//! # Security
//!
//! This SDK includes security protections:
//! - Path traversal prevention (rejects `..`, `/`, `\` in names)
//! - NPZ decompression bomb protection (1GB limit per entry)
//! - NPY header size limits (1MB max)
//!
//! # Modules
//!
//! - [`config`] - Configuration loading from environment
//! - [`error`] - Error types for all operations
//! - [`io`] - Input/output operations with format handling
//! - [`logging`] - Structured logging, heartbeat, and metrics
//! - [`schema`] - Schema validation for outputs
//! - [`stats`] - Statistics computation for SVS

// Declare modules
pub mod config;
pub mod error;
pub mod io;
pub mod logging;
pub mod schema;
pub mod stats;

// Re-export commonly used items at crate root for convenience
pub use config::{get_config, get_inputs_dir, get_outputs_dir, get_svs_config, StageConfig, SVSConfig};
// Re-export clear_config_cache for testing (marked doc hidden)
#[doc(hidden)]
pub use config::clear_config_cache;
pub use error::{ConfigError, GoldfishError, IoError, PathSecurityError, Result, SchemaError, StatsError};
pub use io::{
    get_input_path, get_output_path, load_input, load_npz, load_npz_array, save_output,
    validate_path_component, NpzFile, OutputData,
};
pub use logging::{heartbeat, log_artifact, log_metric, log_metrics, runtime_log, should_stop};
pub use schema::{validate_output_data_against_schema, ArraySchema, Dim, Schema};
pub use stats::{enqueue_stats, finalize_svs, StatsEntry};

/// RAII guard for automatic SVS finalization.
///
/// When dropped, this guard calls `finalize_svs` with a 10-second timeout
/// to ensure all statistics are written even if the stage exits unexpectedly.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::init;
///
/// fn main() {
///     let _guard = init(); // SVS will be finalized when _guard drops
///     // ... stage code ...
/// }
/// ```
pub struct GoldfishGuard {
    _private: (),
}

impl Drop for GoldfishGuard {
    fn drop(&mut self) {
        if let Err(e) = stats::finalize_svs(Some(10)) {
            eprintln!("[goldfish] Warning: Failed to finalize SVS: {}", e);
        }
    }
}

/// Initialize the Goldfish SDK.
///
/// Returns a guard that automatically finalizes SVS on drop.
/// This ensures statistics are written even if the stage panics.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::init;
///
/// fn main() -> Result<(), goldfish_rust::GoldfishError> {
///     let _guard = init();
///
///     // Your stage code here...
///
///     Ok(())
/// } // SVS finalized automatically when _guard drops
/// ```
#[must_use]
pub fn init() -> GoldfishGuard {
    let _ = env_logger::try_init();
    GoldfishGuard { _private: () }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_api_accessible() {
        // Verify that public API items are properly exported
        let _: fn(&str, Option<&str>) -> Result<OutputData> = load_input;
        let _: fn(&str, OutputData, bool) -> Result<()> = save_output;
        let _: fn(&str) -> Result<std::path::PathBuf> = get_input_path;
        let _: fn(&str) -> Result<std::path::PathBuf> = get_output_path;
        let _: fn(&str) -> Result<()> = validate_path_component;
        let _: fn(&str, &str) = runtime_log;
        let _: fn() -> bool = should_stop;
    }

    #[test]
    fn test_error_types_accessible() {
        // Verify error types are properly exported
        let _err: GoldfishError = GoldfishError::Config(ConfigError::UndefinedInput {
            name: "test".to_string(),
        });
        assert!(_err.to_string().contains("test"));
    }

    #[test]
    fn test_schema_types_accessible() {
        // Verify schema types are properly exported
        let schema = Schema {
            kind: Some("tensor".to_string()),
            dtype: Some("float32".to_string()),
            ..Default::default()
        };
        assert_eq!(schema.kind.as_deref(), Some("tensor"));
    }
}
