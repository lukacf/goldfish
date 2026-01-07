//! Error types for Goldfish Rust library.
//!
//! This module provides strongly-typed errors that allow callers to handle
//! different failure modes appropriately. Uses `thiserror` for ergonomic
//! error definition.

use std::path::PathBuf;
use thiserror::Error;

/// Main error type for Goldfish operations.
#[derive(Error, Debug)]
pub enum GoldfishError {
    /// Configuration-related errors.
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Input/output errors.
    #[error("IO error: {0}")]
    Io(#[from] IoError),

    /// Schema validation errors.
    #[error("Schema validation error: {0}")]
    Schema(#[from] SchemaError),

    /// Stats computation errors.
    #[error("Stats error: {0}")]
    Stats(#[from] StatsError),

    /// Path security errors.
    #[error("Path security error: {0}")]
    PathSecurity(#[from] PathSecurityError),
}

// Implement From for std::io::Error to allow ? operator
impl From<std::io::Error> for GoldfishError {
    fn from(e: std::io::Error) -> Self {
        GoldfishError::Io(IoError::StdIo(e))
    }
}

// Implement From for serde_json::Error to allow ? operator
impl From<serde_json::Error> for GoldfishError {
    fn from(e: serde_json::Error) -> Self {
        GoldfishError::Io(IoError::JsonError(e))
    }
}

/// Configuration-related errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Environment variable not set.
    #[error("Environment variable '{name}' not set")]
    EnvVarNotSet { name: String },

    /// Configuration cache mutex poisoned (prior panic).
    #[error("Configuration cache poisoned - this indicates a prior panic")]
    CachePoisoned,

    /// Failed to parse configuration JSON.
    #[error("Failed to parse {config_name}: {source}")]
    ParseError {
        config_name: String,
        #[source]
        source: serde_json::Error,
    },

    /// Input not defined in stage config.
    #[error("Input '{name}' not defined in stage config")]
    UndefinedInput { name: String },

    /// Output not defined in stage config.
    #[error("Output '{name}' not defined in stage config")]
    UndefinedOutput { name: String },
}

/// Input/output errors.
#[derive(Error, Debug)]
pub enum IoError {
    /// File not found.
    #[error("File not found: {path}")]
    FileNotFound { path: PathBuf },

    /// Unsupported format.
    #[error("Unsupported format '{format}' for {context}. Supported: {supported}")]
    UnsupportedFormat {
        format: String,
        context: String,
        supported: String,
    },

    /// NPY file error.
    #[error("NPY error in {path}: {message}")]
    NpyError { path: PathBuf, message: String },

    /// NPZ file error.
    #[error("NPZ error in {path}: {message}")]
    NpzError { path: PathBuf, message: String },

    /// CSV error (Polars).
    #[error("CSV error: {0}")]
    CsvError(#[from] polars::prelude::PolarsError),

    /// CSV parsing error (streaming reader).
    #[error("CSV parse error: {0}")]
    CsvParseError(String),

    /// JSON error.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Standard IO error.
    #[error("IO error: {0}")]
    StdIo(#[from] std::io::Error),

    /// Format requires specific data type.
    #[error("Format '{format}' requires {expected} data, got {actual}")]
    DataTypeMismatch {
        format: String,
        expected: String,
        actual: String,
    },

    /// Cannot auto-save format.
    #[error("Cannot auto-save format '{format}'. Use get_output_path() for manual saving.")]
    CannotAutoSave { format: String },

    /// Too many arrays in MultiTensor output.
    #[error("Too many arrays ({count}) in output. Maximum allowed: {max}")]
    TooManyArrays { count: usize, max: usize },
}

/// Schema validation errors.
#[derive(Error, Debug)]
pub enum SchemaError {
    /// Schema validation failed with multiple errors.
    #[error("Output '{name}' schema mismatch: {errors}")]
    ValidationFailed { name: String, errors: String },

    /// Schema validation requires in-memory data.
    #[error("Output '{name}' schema validation requires in-memory data, got file Path")]
    RequiresInMemoryData { name: String },

    /// Missing expected array in multi-tensor output.
    #[error("Output '{name}' missing expected array '{array_name}'")]
    MissingArray { name: String, array_name: String },

    /// Dtype mismatch.
    #[error("Output '{name}' dtype mismatch: expected {expected}, got {actual}")]
    DtypeMismatch {
        name: String,
        expected: String,
        actual: String,
    },

    /// Shape mismatch.
    #[error("Output '{name}' shape mismatch at dim {dim}: expected {expected}, got {actual}")]
    ShapeMismatch {
        name: String,
        dim: usize,
        expected: String,
        actual: usize,
    },

    /// Rank mismatch.
    #[error("Output '{name}' rank mismatch: expected {expected}, got {actual}")]
    RankMismatch {
        name: String,
        expected: i64,
        actual: usize,
    },
}

/// Stats computation errors.
#[derive(Error, Debug)]
pub enum StatsError {
    /// Stats computation timed out.
    #[error("Stats computation timed out after {timeout_secs} seconds")]
    Timeout { timeout_secs: u64 },

    /// Failed to compute stats for file.
    #[error("Failed to compute stats for {path}: {message}")]
    ComputationFailed { path: PathBuf, message: String },

    /// Thread panicked during stats computation.
    #[error("Stats thread panicked: {message}")]
    ThreadPanic { message: String },
}

/// Path security errors (path traversal, etc.).
#[derive(Error, Debug)]
pub enum PathSecurityError {
    /// Path traversal attempt detected.
    #[error("Path traversal detected: '{path}' escapes root directory")]
    PathTraversal { path: String },

    /// Path contains invalid characters.
    #[error("Path contains invalid characters: '{path}'")]
    InvalidCharacters { path: String },

    /// Symlink detected where not allowed.
    #[error("Symlink not allowed: '{path}'")]
    SymlinkNotAllowed { path: PathBuf },
}

/// Result type alias for Goldfish operations.
pub type Result<T> = std::result::Result<T, GoldfishError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GoldfishError::Config(ConfigError::UndefinedInput {
            name: "features".to_string(),
        });
        assert!(err.to_string().contains("features"));
        assert!(err.to_string().contains("not defined"));
    }

    #[test]
    fn test_path_security_error() {
        let err = PathSecurityError::PathTraversal {
            path: "../../../etc/passwd".to_string(),
        };
        assert!(err.to_string().contains("traversal"));
    }

    #[test]
    fn test_schema_error_details() {
        let err = SchemaError::DtypeMismatch {
            name: "output".to_string(),
            expected: "float32".to_string(),
            actual: "int64".to_string(),
        };
        assert!(err.to_string().contains("float32"));
        assert!(err.to_string().contains("int64"));
    }
}
