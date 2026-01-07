//! Configuration types and loading for Goldfish stages.
//!
//! This module handles loading configuration from environment variables
//! and provides strongly-typed access to stage configuration.

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::error::{ConfigError, GoldfishError, Result};
use crate::schema::Schema;

/// Data format for signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalFormat {
    /// NumPy array format (.npy).
    Npy,
    /// CSV tabular format.
    Csv,
    /// JSON format.
    Json,
    /// Directory containing multiple files.
    Directory,
    /// Single file (default).
    #[default]
    File,
    /// Dataset reference.
    Dataset,
}

impl SignalFormat {
    /// Convert format to string for error messages.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalFormat::Npy => "npy",
            SignalFormat::Csv => "csv",
            SignalFormat::Json => "json",
            SignalFormat::Directory => "directory",
            SignalFormat::File => "file",
            SignalFormat::Dataset => "dataset",
        }
    }
}

/// SVS enforcement mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnforcementMode {
    /// Blocking mode: validation failures are errors.
    Blocking,
    /// Warning mode: validation failures are logged but don't fail.
    #[default]
    Warning,
    /// Silent mode: validation failures are ignored.
    Silent,
}

impl std::fmt::Display for EnforcementMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnforcementMode::Blocking => write!(f, "blocking"),
            EnforcementMode::Warning => write!(f, "warning"),
            EnforcementMode::Silent => write!(f, "silent"),
        }
    }
}

impl std::fmt::Display for SignalFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl EnforcementMode {
    /// Check if this mode is blocking.
    #[must_use]
    pub fn is_blocking(&self) -> bool {
        matches!(self, EnforcementMode::Blocking)
    }

    /// Check if this mode should log warnings.
    #[must_use]
    pub fn should_warn(&self) -> bool {
        matches!(self, EnforcementMode::Warning)
    }
}

/// Stage configuration loaded from GOLDFISH_STAGE_CONFIG.
#[derive(Deserialize, Debug, Clone)]
pub struct StageConfig {
    /// Output signal configurations.
    pub outputs: HashMap<String, OutputConfig>,
    /// Input signal configurations.
    pub inputs: HashMap<String, InputConfig>,
}

/// Configuration for an input signal.
#[derive(Deserialize, Debug, Clone)]
pub struct InputConfig {
    /// Data format (npy, csv, json, directory, file, dataset).
    #[serde(default)]
    pub format: Option<SignalFormat>,
    /// Optional schema for validation.
    pub schema: Option<Schema>,
    /// Location override (for local execution).
    pub location: Option<String>,
}

/// Configuration for an output signal.
#[derive(Deserialize, Debug, Clone)]
pub struct OutputConfig {
    /// Data format for saving.
    #[serde(default)]
    pub format: Option<SignalFormat>,
    /// Schema for validation.
    pub schema: Option<Schema>,
}

/// SVS (Semantic Validation System) configuration.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SVSConfig {
    /// Whether SVS is enabled.
    pub enabled: bool,
    /// Default enforcement mode.
    #[serde(default)]
    pub default_enforcement: EnforcementMode,
}

impl Default for SVSConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_enforcement: EnforcementMode::Warning,
        }
    }
}

// Cached configuration to avoid repeated JSON parsing.
// Using Arc to avoid cloning on every access - just clone the Arc pointer.
static CONFIG_CACHE: Lazy<RwLock<Option<Arc<StageConfig>>>> = Lazy::new(|| RwLock::new(None));
static SVS_CONFIG_CACHE: Lazy<RwLock<Option<Arc<SVSConfig>>>> = Lazy::new(|| RwLock::new(None));
static INPUTS_DIR_CACHE: Lazy<RwLock<Option<Arc<PathBuf>>>> = Lazy::new(|| RwLock::new(None));
static OUTPUTS_DIR_CACHE: Lazy<RwLock<Option<Arc<PathBuf>>>> = Lazy::new(|| RwLock::new(None));

/// Get the stage configuration.
///
/// Configuration is parsed once from GOLDFISH_STAGE_CONFIG and cached.
/// Returns an `Arc` to avoid expensive cloning of the config HashMap.
///
/// # Errors
///
/// Returns an error if:
/// - GOLDFISH_STAGE_CONFIG environment variable is not set
/// - The JSON cannot be parsed as StageConfig
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::config::get_config;
///
/// let config = get_config()?;
/// println!("Inputs: {:?}", config.inputs.keys().collect::<Vec<_>>());
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn get_config() -> Result<Arc<StageConfig>> {
    // Check cache first - Arc::clone is cheap (just increments refcount)
    {
        let cache = CONFIG_CACHE
            .read()
            .map_err(|_| GoldfishError::Config(ConfigError::CachePoisoned))?;
        if let Some(ref config) = *cache {
            return Ok(Arc::clone(config));
        }
    }

    // Parse and cache
    let config_json = std::env::var("GOLDFISH_STAGE_CONFIG").map_err(|_| {
        GoldfishError::Config(ConfigError::EnvVarNotSet {
            name: "GOLDFISH_STAGE_CONFIG".to_string(),
        })
    })?;

    let config: StageConfig =
        serde_json::from_str(&config_json).map_err(|e| ConfigError::ParseError {
            config_name: "GOLDFISH_STAGE_CONFIG".to_string(),
            source: e,
        })?;

    // Wrap in Arc and cache
    let config = Arc::new(config);
    if let Ok(mut cache) = CONFIG_CACHE.write() {
        *cache = Some(Arc::clone(&config));
    }

    Ok(config)
}

/// Get the SVS configuration.
///
/// Returns default values if GOLDFISH_SVS_CONFIG is not set or invalid.
/// This function never fails - it falls back to defaults.
/// Returns an `Arc` to avoid cloning the config on every access.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::config::get_svs_config;
///
/// let svs = get_svs_config();
/// if svs.enabled {
///     println!("SVS enforcement: {}", svs.default_enforcement);
/// }
/// ```
pub fn get_svs_config() -> Arc<SVSConfig> {
    // Check cache first - Arc::clone is cheap
    if let Ok(cache) = SVS_CONFIG_CACHE.read() {
        if let Some(ref config) = *cache {
            return Arc::clone(config);
        }
    }

    // Parse or use defaults
    let config: SVSConfig = std::env::var("GOLDFISH_SVS_CONFIG")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    // Wrap in Arc and cache
    let config = Arc::new(config);
    if let Ok(mut cache) = SVS_CONFIG_CACHE.write() {
        *cache = Some(Arc::clone(&config));
    }

    config
}

/// Get the inputs directory path.
///
/// Defaults to /mnt/inputs if GOLDFISH_INPUTS_DIR is not set.
/// The result is cached for efficiency. Returns `PathBuf` for API compatibility,
/// but internally uses `Arc` to minimize allocations on repeated calls.
#[must_use]
pub fn get_inputs_dir() -> PathBuf {
    // Check cache first - Arc::clone is cheap (just increments refcount)
    if let Ok(cache) = INPUTS_DIR_CACHE.read() {
        if let Some(ref path) = *cache {
            // Deref the Arc to get PathBuf - this is still a clone but unavoidable
            // for API compatibility. The Arc prevents repeated env var lookups.
            return (**path).clone();
        }
    }

    let path = Arc::new(PathBuf::from(
        std::env::var("GOLDFISH_INPUTS_DIR").unwrap_or_else(|_| "/mnt/inputs".to_string()),
    ));

    let result = (*path).clone();
    if let Ok(mut cache) = INPUTS_DIR_CACHE.write() {
        *cache = Some(path);
    }

    result
}

/// Get the outputs directory path.
///
/// Defaults to /mnt/outputs if GOLDFISH_OUTPUTS_DIR is not set.
/// The result is cached for efficiency. Returns `PathBuf` for API compatibility,
/// but internally uses `Arc` to minimize allocations on repeated calls.
#[must_use]
pub fn get_outputs_dir() -> PathBuf {
    // Check cache first - Arc::clone is cheap (just increments refcount)
    if let Ok(cache) = OUTPUTS_DIR_CACHE.read() {
        if let Some(ref path) = *cache {
            // Deref the Arc to get PathBuf - this is still a clone but unavoidable
            // for API compatibility. The Arc prevents repeated env var lookups.
            return (**path).clone();
        }
    }

    let path = Arc::new(PathBuf::from(
        std::env::var("GOLDFISH_OUTPUTS_DIR").unwrap_or_else(|_| "/mnt/outputs".to_string()),
    ));

    let result = (*path).clone();
    if let Ok(mut cache) = OUTPUTS_DIR_CACHE.write() {
        *cache = Some(path);
    }

    result
}

/// Check if SVS stats collection is enabled.
#[must_use]
pub fn svs_stats_enabled() -> bool {
    std::env::var("GOLDFISH_SVS_STATS_ENABLED")
        .map(|v| ["true", "1", "yes"].contains(&v.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Clear all configuration caches.
///
/// Useful for testing when environment variables are changed.
#[doc(hidden)]
pub fn clear_config_cache() {
    if let Ok(mut cache) = CONFIG_CACHE.write() {
        *cache = None;
    }
    if let Ok(mut cache) = SVS_CONFIG_CACHE.write() {
        *cache = None;
    }
    if let Ok(mut cache) = INPUTS_DIR_CACHE.write() {
        *cache = None;
    }
    if let Ok(mut cache) = OUTPUTS_DIR_CACHE.write() {
        *cache = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    fn with_env<F, R>(key: &str, value: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        clear_config_cache();
        let old = env::var(key).ok();
        env::set_var(key, value);
        clear_config_cache();
        let result = f();
        clear_config_cache();
        match old {
            Some(v) => env::set_var(key, v),
            None => env::remove_var(key),
        }
        result
    }

    fn without_env<F, R>(key: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        clear_config_cache();
        let old = env::var(key).ok();
        env::remove_var(key);
        clear_config_cache();
        let result = f();
        clear_config_cache();
        if let Some(v) = old {
            env::set_var(key, v);
        }
        result
    }

    #[test]
    #[serial]
    fn test_get_config_missing_env() {
        without_env("GOLDFISH_STAGE_CONFIG", || {
            let result = get_config();
            assert!(result.is_err());
            if let Err(GoldfishError::Config(ConfigError::EnvVarNotSet { name })) = result {
                assert_eq!(name, "GOLDFISH_STAGE_CONFIG");
            } else {
                panic!("Expected EnvVarNotSet error");
            }
        });
    }

    #[test]
    #[serial]
    fn test_get_config_valid() {
        let config_json = r#"{"inputs": {}, "outputs": {}}"#;
        with_env("GOLDFISH_STAGE_CONFIG", config_json, || {
            let config = get_config().unwrap();
            assert!(config.inputs.is_empty());
            assert!(config.outputs.is_empty());
        });
    }

    #[test]
    #[serial]
    fn test_get_config_invalid_json() {
        with_env("GOLDFISH_STAGE_CONFIG", "not json", || {
            let result = get_config();
            assert!(result.is_err());
        });
    }

    #[test]
    #[serial]
    fn test_get_svs_config_default() {
        without_env("GOLDFISH_SVS_CONFIG", || {
            let config = get_svs_config();
            assert!(config.enabled);
            assert_eq!(config.default_enforcement, EnforcementMode::Warning);
        });
    }

    #[test]
    fn test_signal_format_as_str() {
        assert_eq!(SignalFormat::Npy.as_str(), "npy");
        assert_eq!(SignalFormat::Csv.as_str(), "csv");
        assert_eq!(SignalFormat::Json.as_str(), "json");
        assert_eq!(SignalFormat::Directory.as_str(), "directory");
        assert_eq!(SignalFormat::File.as_str(), "file");
        assert_eq!(SignalFormat::Dataset.as_str(), "dataset");
    }

    #[test]
    fn test_enforcement_mode_methods() {
        assert!(EnforcementMode::Blocking.is_blocking());
        assert!(!EnforcementMode::Warning.is_blocking());
        assert!(!EnforcementMode::Silent.is_blocking());

        assert!(!EnforcementMode::Blocking.should_warn());
        assert!(EnforcementMode::Warning.should_warn());
        assert!(!EnforcementMode::Silent.should_warn());
    }

    #[test]
    fn test_signal_format_serde() {
        // Test JSON deserialization
        let json = r#"{"format": "npy"}"#;
        #[derive(Deserialize)]
        struct TestConfig {
            format: SignalFormat,
        }
        let cfg: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.format, SignalFormat::Npy);
    }

    #[test]
    fn test_enforcement_mode_serde() {
        let json = r#"{"enabled": true, "default_enforcement": "blocking"}"#;
        let cfg: SVSConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.default_enforcement, EnforcementMode::Blocking);
    }

    #[test]
    #[serial]
    fn test_get_inputs_dir_default() {
        without_env("GOLDFISH_INPUTS_DIR", || {
            let path = get_inputs_dir();
            assert_eq!(path, PathBuf::from("/mnt/inputs"));
        });
    }

    #[test]
    #[serial]
    fn test_get_inputs_dir_custom() {
        with_env("GOLDFISH_INPUTS_DIR", "/custom/inputs", || {
            let path = get_inputs_dir();
            assert_eq!(path, PathBuf::from("/custom/inputs"));
        });
    }

    #[test]
    #[serial]
    fn test_svs_stats_enabled() {
        without_env("GOLDFISH_SVS_STATS_ENABLED", || {
            assert!(!svs_stats_enabled());
        });

        with_env("GOLDFISH_SVS_STATS_ENABLED", "true", || {
            assert!(svs_stats_enabled());
        });

        with_env("GOLDFISH_SVS_STATS_ENABLED", "1", || {
            assert!(svs_stats_enabled());
        });

        with_env("GOLDFISH_SVS_STATS_ENABLED", "false", || {
            assert!(!svs_stats_enabled());
        });
    }
}
