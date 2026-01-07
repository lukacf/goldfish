//! Input/output operations for Goldfish stages.
//!
//! This module provides functions to load inputs and save outputs with proper
//! format handling, path validation, and schema enforcement.

pub mod npy;
mod npz;
mod path;

pub use npy::{load_npy_from_reader, load_npy_typed, read_npy_header, save_npy, NpyHeader};
pub use npz::{load_npz, load_npz_array, NpzFile};
pub use path::{get_input_path, get_output_path, validate_path_component};

use ndarray::ArrayD;
use polars::prelude::{CsvWriter, SerReader, SerWriter};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::{get_config, get_inputs_dir, get_outputs_dir, get_svs_config, EnforcementMode, SignalFormat};
use crate::error::{ConfigError, GoldfishError, IoError, Result, SchemaError};
use crate::schema::{validate_output_data_against_schema, Schema};
use crate::stats::enqueue_stats;

/// Maximum number of arrays allowed in MultiTensor outputs or directory validation.
/// Prevents memory exhaustion from malicious NPZ files with thousands of arrays.
const MAX_MULTI_ARRAY_COUNT: usize = 100;

/// Output data types that can be saved.
#[derive(Clone, Debug)]
pub enum OutputData {
    /// 32-bit float tensor.
    TensorF32(ArrayD<f32>),
    /// 64-bit float tensor.
    TensorF64(ArrayD<f64>),
    /// 64-bit integer tensor.
    TensorI64(ArrayD<i64>),
    /// 32-bit integer tensor.
    TensorI32(ArrayD<i32>),
    /// 8-bit unsigned integer tensor (for images).
    TensorU8(ArrayD<u8>),
    /// JSON value.
    Json(serde_json::Value),
    /// Polars DataFrame.
    Tabular(polars::prelude::DataFrame),
    /// Path to existing file/directory.
    Path(PathBuf),
    /// Multiple tensors keyed by name.
    MultiTensor(HashMap<String, OutputData>),
}

impl OutputData {
    /// Get the dtype string for this data type.
    #[must_use]
    pub fn dtype_str(&self) -> &'static str {
        match self {
            OutputData::TensorF32(_) => "float32",
            OutputData::TensorF64(_) => "float64",
            OutputData::TensorI64(_) => "int64",
            OutputData::TensorI32(_) => "int32",
            OutputData::TensorU8(_) => "uint8",
            OutputData::Json(_) => "json",
            OutputData::Tabular(_) => "tabular",
            OutputData::Path(_) => "path",
            OutputData::MultiTensor(_) => "multi",
        }
    }

    /// Try to extract as f32 tensor reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use goldfish_rust::io::OutputData;
    /// use ndarray::ArrayD;
    ///
    /// let data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0f32));
    /// assert!(data.as_tensor_f32().is_some());
    /// assert!(data.as_tensor_f64().is_none());
    /// ```
    #[must_use]
    pub fn as_tensor_f32(&self) -> Option<&ArrayD<f32>> {
        match self {
            OutputData::TensorF32(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to extract as f64 tensor reference.
    #[must_use]
    pub fn as_tensor_f64(&self) -> Option<&ArrayD<f64>> {
        match self {
            OutputData::TensorF64(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to extract as i64 tensor reference.
    #[must_use]
    pub fn as_tensor_i64(&self) -> Option<&ArrayD<i64>> {
        match self {
            OutputData::TensorI64(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to extract as i32 tensor reference.
    #[must_use]
    pub fn as_tensor_i32(&self) -> Option<&ArrayD<i32>> {
        match self {
            OutputData::TensorI32(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to extract as u8 tensor reference.
    #[must_use]
    pub fn as_tensor_u8(&self) -> Option<&ArrayD<u8>> {
        match self {
            OutputData::TensorU8(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to extract as JSON value reference.
    #[must_use]
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            OutputData::Json(val) => Some(val),
            _ => None,
        }
    }

    /// Try to extract as DataFrame reference.
    #[must_use]
    pub fn as_tabular(&self) -> Option<&polars::prelude::DataFrame> {
        match self {
            OutputData::Tabular(df) => Some(df),
            _ => None,
        }
    }

    /// Try to extract as Path reference.
    #[must_use]
    pub fn as_path(&self) -> Option<&PathBuf> {
        match self {
            OutputData::Path(p) => Some(p),
            _ => None,
        }
    }

    /// Try to take ownership of f32 tensor.
    ///
    /// Returns `Err(self)` if not a TensorF32, allowing recovery.
    ///
    /// # Examples
    ///
    /// ```
    /// use goldfish_rust::io::OutputData;
    /// use ndarray::ArrayD;
    ///
    /// let data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0f32));
    /// let arr = data.into_tensor_f32().expect("expected f32 tensor");
    /// assert_eq!(arr.shape(), &[10]);
    /// ```
    pub fn into_tensor_f32(self) -> std::result::Result<ArrayD<f32>, OutputData> {
        match self {
            OutputData::TensorF32(arr) => Ok(arr),
            other => Err(other),
        }
    }

    /// Try to take ownership of f64 tensor.
    pub fn into_tensor_f64(self) -> std::result::Result<ArrayD<f64>, OutputData> {
        match self {
            OutputData::TensorF64(arr) => Ok(arr),
            other => Err(other),
        }
    }

    /// Try to take ownership of i64 tensor.
    pub fn into_tensor_i64(self) -> std::result::Result<ArrayD<i64>, OutputData> {
        match self {
            OutputData::TensorI64(arr) => Ok(arr),
            other => Err(other),
        }
    }

    /// Try to take ownership of DataFrame.
    pub fn into_tabular(self) -> std::result::Result<polars::prelude::DataFrame, OutputData> {
        match self {
            OutputData::Tabular(df) => Ok(df),
            other => Err(other),
        }
    }

    /// Check if this is a tensor type.
    #[must_use]
    pub fn is_tensor(&self) -> bool {
        matches!(
            self,
            OutputData::TensorF32(_)
                | OutputData::TensorF64(_)
                | OutputData::TensorI64(_)
                | OutputData::TensorI32(_)
                | OutputData::TensorU8(_)
        )
    }
}

/// Load an input signal.
///
/// # Arguments
///
/// * `name` - Input name as defined in the stage config
/// * `format` - Optional format override
///
/// # Errors
///
/// Returns an error if:
/// - The input is not defined in stage config
/// - The input file does not exist
/// - The format is unsupported
/// - The file cannot be parsed
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::load_input;
///
/// // Auto-load based on config format
/// let features = load_input("features", None)?;
///
/// // Override format
/// let raw = load_input("data", Some("csv"))?;
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn load_input(name: &str, format: Option<&str>) -> Result<OutputData> {
    // Validate path component to prevent traversal
    validate_path_component(name)?;

    let config = get_config()?;
    let in_config = config
        .inputs
        .get(name)
        .ok_or_else(|| GoldfishError::Config(ConfigError::UndefinedInput { name: name.to_string() }))?;

    let input_path = get_inputs_dir().join(name);

    // Parse format override or use config format, defaulting to File
    let fmt = match format {
        Some(s) => parse_format(s)?,
        None => in_config.format.unwrap_or(SignalFormat::File),
    };

    match fmt {
        SignalFormat::Npy => {
            let p = find_file_with_extension(&input_path, "npy")?;
            load_npy_typed(&p)
        }
        SignalFormat::Csv => {
            let p = find_file_with_extension(&input_path, "csv")?;
            let df = polars::prelude::CsvReader::from_path(&p)
                .map_err(IoError::CsvError)?
                .finish()
                .map_err(IoError::CsvError)?;
            Ok(OutputData::Tabular(df))
        }
        SignalFormat::Json => {
            let p = find_file_with_extension(&input_path, "json")?;
            let s = std::fs::read_to_string(&p)?;
            let val = serde_json::from_str(&s)?;
            Ok(OutputData::Json(val))
        }
        SignalFormat::Directory | SignalFormat::File | SignalFormat::Dataset => {
            if !input_path.exists() {
                return Err(GoldfishError::Io(IoError::FileNotFound { path: input_path }));
            }
            Ok(OutputData::Path(input_path))
        }
    }
}

/// Parse a format string to SignalFormat enum.
fn parse_format(s: &str) -> Result<SignalFormat> {
    match s.to_lowercase().as_str() {
        "npy" => Ok(SignalFormat::Npy),
        "csv" => Ok(SignalFormat::Csv),
        "json" => Ok(SignalFormat::Json),
        "directory" => Ok(SignalFormat::Directory),
        "file" => Ok(SignalFormat::File),
        "dataset" => Ok(SignalFormat::Dataset),
        _ => Err(GoldfishError::Io(IoError::UnsupportedFormat {
            format: s.to_string(),
            context: "format override".to_string(),
            supported: "npy, csv, json, directory, file, dataset".to_string(),
        })),
    }
}

/// Save an output signal.
///
/// # Arguments
///
/// * `name` - Output name as defined in the stage config
/// * `data` - Data to save
/// * `artifact` - Mark as permanent artifact
///
/// # Errors
///
/// Returns an error if:
/// - The output is not defined in stage config
/// - Schema validation fails in blocking mode
/// - The format is unsupported for auto-save
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::{save_output, OutputData};
/// use ndarray::ArrayD;
///
/// let arr = ArrayD::from_elem(vec![100, 50], 1.0f32);
/// save_output("features", OutputData::TensorF32(arr), false)?;
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn save_output(name: &str, data: OutputData, artifact: bool) -> Result<()> {
    // Validate path component to prevent traversal
    validate_path_component(name)?;

    let config = get_config()?;
    let out_config = config
        .outputs
        .get(name)
        .ok_or_else(|| GoldfishError::Config(ConfigError::UndefinedOutput { name: name.to_string() }))?;

    let svs_config = get_svs_config();

    // Schema validation
    // Use Cow pattern to avoid cloning - only load_directory creates new data
    if let Some(schema) = &out_config.schema {
        // For directory outputs, we need to load data for validation
        // For in-memory data, we validate directly (no clone)
        let loaded_data: Option<OutputData>;
        let validation_data: Option<&OutputData> = match &data {
            OutputData::Path(p) if p.is_file() => {
                return Err(GoldfishError::Schema(SchemaError::RequiresInMemoryData {
                    name: name.to_string(),
                }));
            }
            OutputData::Path(p) if p.is_dir() => {
                loaded_data = load_directory_for_validation(p, schema);
                loaded_data.as_ref()
            }
            _ => Some(&data), // No clone - use reference directly
        };

        if let Some(val_data) = validation_data {
            let val_errors = validate_output_data_against_schema(name, schema, val_data);

            if !val_errors.is_empty() {
                let enforcement = if !svs_config.enabled {
                    EnforcementMode::Silent
                } else {
                    svs_config.default_enforcement
                };

                let msg = format!("Output '{}' schema mismatch: {}", name, val_errors.join("; "));

                if enforcement.is_blocking() {
                    return Err(GoldfishError::Schema(SchemaError::ValidationFailed {
                        name: name.to_string(),
                        errors: val_errors.join("; "),
                    }));
                } else if enforcement.should_warn() {
                    crate::logging::runtime_log(&msg, "WARN");
                }
                // Silent mode: do nothing
            }
        }
    }

    // Save based on format
    let output_path = get_outputs_dir().join(name);

    match &data {
        OutputData::Path(_) => {
            // Already saved, just validate schema was done above
        }
        _ => {
            let fmt = out_config.format.unwrap_or(SignalFormat::File);

            match fmt {
                SignalFormat::Npy => {
                    let path = if output_path.extension().is_none() {
                        output_path.with_extension("npy")
                    } else {
                        output_path.clone()
                    };

                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    save_npy(&path, &data)?;
                    enqueue_stats(name, &path, data.dtype_str());
                }
                SignalFormat::Csv => {
                    let path = if output_path.extension().is_none() {
                        output_path.with_extension("csv")
                    } else {
                        output_path.clone()
                    };

                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    // Extract DataFrame to pass mutable reference
                    if let OutputData::Tabular(mut df) = data {
                        save_csv(&path, &mut df)?;
                        enqueue_stats(name, &path, "tabular");
                    } else {
                        return Err(GoldfishError::Io(IoError::DataTypeMismatch {
                            format: "csv".to_string(),
                            expected: "Tabular".to_string(),
                            actual: data.dtype_str().to_string(),
                        }));
                    }
                    return Ok(());
                }
                SignalFormat::Directory | SignalFormat::File => {
                    // Multi-tensor outputs can be auto-saved as a directory of .npy files.
                    if let OutputData::MultiTensor(arrays) = &data {
                        // Security: Limit number of arrays to prevent memory exhaustion
                        if arrays.len() > MAX_MULTI_ARRAY_COUNT {
                            return Err(GoldfishError::Io(IoError::TooManyArrays {
                                count: arrays.len(),
                                max: MAX_MULTI_ARRAY_COUNT,
                            }));
                        }
                        std::fs::create_dir_all(&output_path)?;
                        for (arr_name, arr_data) in arrays {
                            // Prevent path traversal in array names.
                            validate_path_component(arr_name)?;
                            let arr_path = output_path.join(format!("{}.npy", arr_name));
                            save_npy(&arr_path, arr_data)?;
                            // Store stats per array using a namespaced key.
                            let stats_name = format!("{}.{}", name, arr_name);
                            enqueue_stats(&stats_name, &arr_path, arr_data.dtype_str());
                        }
                    }
                    // Manual save expected for non-multi outputs
                }
                SignalFormat::Json | SignalFormat::Dataset => {
                    return Err(GoldfishError::Io(IoError::CannotAutoSave {
                        format: fmt.as_str().to_string(),
                    }));
                }
            }
        }
    }

    if artifact {
        mark_as_artifact(name)?;
    }

    Ok(())
}

/// Save data as CSV.
///
/// Takes mutable reference to DataFrame to avoid cloning.
/// Uses buffered writer for improved performance on large DataFrames.
fn save_csv(path: &Path, df: &mut polars::prelude::DataFrame) -> Result<()> {
    let file = std::fs::File::create(path)?;
    // Use 64KB buffer for significantly faster writes on large DataFrames
    let buf_writer = std::io::BufWriter::with_capacity(64 * 1024, file);
    let mut writer = CsvWriter::new(buf_writer);
    writer.finish(df).map_err(|e| IoError::CsvError(e))?;
    Ok(())
}

/// Load directory contents for schema validation.
///
/// Supports both NPZ files and individual NPY files.
fn load_directory_for_validation(dir: &Path, schema: &Schema) -> Option<OutputData> {
    let arrays_schema = schema.arrays.as_ref()?;

    // Try NPZ files first (Python primary path)
    for npz_file in dir.read_dir().ok()?.flatten() {
        let path = npz_file.path();
        if path.extension().and_then(|e| e.to_str()) == Some("npz") {
            if let Ok(mut npz) = load_npz(&path) {
                let mut actual_arrays = HashMap::new();
                for arr_name in arrays_schema.keys() {
                    // Use take() to move ownership instead of cloning
                    if let Some(arr) = npz.take(arr_name) {
                        actual_arrays.insert(arr_name.clone(), arr);
                    }
                }
                if !actual_arrays.is_empty() {
                    return Some(OutputData::MultiTensor(actual_arrays));
                }
            }
        }
    }

    // Fallback to individual NPY files
    let mut actual_arrays = HashMap::new();
    for arr_name in arrays_schema.keys() {
        let npy_path = dir.join(format!("{}.npy", arr_name));
        if npy_path.exists() {
            if let Ok(data) = load_npy_typed(&npy_path) {
                actual_arrays.insert(arr_name.clone(), data);
            }
        }
    }

    if !actual_arrays.is_empty() {
        Some(OutputData::MultiTensor(actual_arrays))
    } else {
        // Return empty MultiTensor to trigger missing-array validation.
        Some(OutputData::MultiTensor(HashMap::new()))
    }
}

/// Mark output as artifact.
fn mark_as_artifact(name: &str) -> Result<()> {
    let marker = get_outputs_dir().join(".artifacts").join(name);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(marker)?;
    Ok(())
}

/// Find file with optional extension.
fn find_file_with_extension(base: &Path, ext: &str) -> Result<PathBuf> {
    if base.exists() {
        return Ok(base.to_path_buf());
    }

    let with_ext = base.with_extension(ext);
    if with_ext.exists() {
        return Ok(with_ext);
    }

    // Try as directory containing files with extension
    if base.is_dir() {
        for entry in base.read_dir()? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                return Ok(path);
            }
        }
    }

    Err(GoldfishError::Io(IoError::FileNotFound {
        path: base.to_path_buf(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::clear_config_cache;
    use ndarray::ArrayD;
    use serial_test::serial;
    use tempfile::tempdir;

    fn with_config<F, R>(inputs_dir: &str, outputs_dir: &str, config_json: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        clear_config_cache();

        let old_inputs = std::env::var("GOLDFISH_INPUTS_DIR").ok();
        let old_outputs = std::env::var("GOLDFISH_OUTPUTS_DIR").ok();
        let old_config = std::env::var("GOLDFISH_STAGE_CONFIG").ok();

        std::env::set_var("GOLDFISH_INPUTS_DIR", inputs_dir);
        std::env::set_var("GOLDFISH_OUTPUTS_DIR", outputs_dir);
        std::env::set_var("GOLDFISH_STAGE_CONFIG", config_json);
        clear_config_cache();

        let result = f();

        clear_config_cache();
        match old_inputs {
            Some(v) => std::env::set_var("GOLDFISH_INPUTS_DIR", v),
            None => std::env::remove_var("GOLDFISH_INPUTS_DIR"),
        }
        match old_outputs {
            Some(v) => std::env::set_var("GOLDFISH_OUTPUTS_DIR", v),
            None => std::env::remove_var("GOLDFISH_OUTPUTS_DIR"),
        }
        match old_config {
            Some(v) => std::env::set_var("GOLDFISH_STAGE_CONFIG", v),
            None => std::env::remove_var("GOLDFISH_STAGE_CONFIG"),
        }

        result
    }

    fn with_config_and_svs<F, R>(
        inputs_dir: &str,
        outputs_dir: &str,
        config_json: &str,
        svs_config_json: &str,
        f: F,
    ) -> R
    where
        F: FnOnce() -> R,
    {
        clear_config_cache();

        let old_inputs = std::env::var("GOLDFISH_INPUTS_DIR").ok();
        let old_outputs = std::env::var("GOLDFISH_OUTPUTS_DIR").ok();
        let old_config = std::env::var("GOLDFISH_STAGE_CONFIG").ok();
        let old_svs = std::env::var("GOLDFISH_SVS_CONFIG").ok();

        std::env::set_var("GOLDFISH_INPUTS_DIR", inputs_dir);
        std::env::set_var("GOLDFISH_OUTPUTS_DIR", outputs_dir);
        std::env::set_var("GOLDFISH_STAGE_CONFIG", config_json);
        std::env::set_var("GOLDFISH_SVS_CONFIG", svs_config_json);
        clear_config_cache();

        let result = f();

        clear_config_cache();
        match old_inputs {
            Some(v) => std::env::set_var("GOLDFISH_INPUTS_DIR", v),
            None => std::env::remove_var("GOLDFISH_INPUTS_DIR"),
        }
        match old_outputs {
            Some(v) => std::env::set_var("GOLDFISH_OUTPUTS_DIR", v),
            None => std::env::remove_var("GOLDFISH_OUTPUTS_DIR"),
        }
        match old_config {
            Some(v) => std::env::set_var("GOLDFISH_STAGE_CONFIG", v),
            None => std::env::remove_var("GOLDFISH_STAGE_CONFIG"),
        }
        match old_svs {
            Some(v) => std::env::set_var("GOLDFISH_SVS_CONFIG", v),
            None => std::env::remove_var("GOLDFISH_SVS_CONFIG"),
        }

        result
    }

    #[test]
    fn test_output_data_dtype_str() {
        assert_eq!(
            OutputData::TensorF32(ArrayD::from_elem(vec![1], 0.0)).dtype_str(),
            "float32"
        );
        assert_eq!(
            OutputData::TensorF64(ArrayD::from_elem(vec![1], 0.0)).dtype_str(),
            "float64"
        );
        assert_eq!(
            OutputData::TensorU8(ArrayD::from_elem(vec![1], 0u8)).dtype_str(),
            "uint8"
        );
    }

    #[test]
    fn test_find_file_with_extension() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.npy");
        std::fs::write(&file_path, b"test").unwrap();

        // Exact path
        let found = find_file_with_extension(&file_path, "npy").unwrap();
        assert_eq!(found, file_path);

        // Without extension
        let base = dir.path().join("test");
        let found = find_file_with_extension(&base, "npy").unwrap();
        assert_eq!(found, file_path);
    }

    #[test]
    #[serial]
    fn test_load_input_npy() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        // Create test NPY file
        let arr = ArrayD::from_elem(vec![10, 20], 1.5f32);
        let npy_path = inputs_dir.join("features.npy");
        ndarray_npy::write_npy(&npy_path, &arr).unwrap();

        let config = r#"{
            "inputs": {"features": {"format": "npy"}},
            "outputs": {}
        }"#;

        with_config(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            || {
                let loaded = load_input("features", None).unwrap();
                if let OutputData::TensorF32(loaded_arr) = loaded {
                    assert_eq!(loaded_arr.shape(), &[10, 20]);
                } else {
                    panic!("Expected TensorF32");
                }
            },
        );
    }

    #[test]
    #[serial]
    fn test_load_input_undefined_error() {
        let dir = tempdir().unwrap();
        let config = r#"{"inputs": {}, "outputs": {}}"#;

        with_config(
            dir.path().to_str().unwrap(),
            dir.path().to_str().unwrap(),
            config,
            || {
                let result = load_input("nonexistent", None);
                assert!(result.is_err());
                if let Err(GoldfishError::Config(crate::error::ConfigError::UndefinedInput {
                    name,
                })) = result
                {
                    assert_eq!(name, "nonexistent");
                } else {
                    panic!("Expected UndefinedInput error");
                }
            },
        );
    }

    #[test]
    #[serial]
    fn test_load_input_path_traversal_rejected() {
        let dir = tempdir().unwrap();
        let config = r#"{"inputs": {"../evil": {}}, "outputs": {}}"#;

        with_config(
            dir.path().to_str().unwrap(),
            dir.path().to_str().unwrap(),
            config,
            || {
                let result = load_input("../evil", None);
                assert!(result.is_err());
                // Should be rejected by path validation
            },
        );
    }

    #[test]
    #[serial]
    fn test_save_output_npy() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        let config = r#"{
            "inputs": {},
            "outputs": {"features": {"format": "npy"}}
        }"#;

        with_config(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            || {
                let arr = ArrayD::from_elem(vec![5, 10], 2.0f32);
                save_output("features", OutputData::TensorF32(arr), false).unwrap();

                // Verify file was created
                let saved_path = outputs_dir.join("features.npy");
                assert!(saved_path.exists());

                // Verify we can read it back
                let loaded =
                    ndarray_npy::read_npy::<_, ArrayD<f32>>(&saved_path).unwrap();
                assert_eq!(loaded.shape(), &[5, 10]);
            },
        );
    }

    #[test]
    #[serial]
    fn test_save_output_multi_tensor_directory_autosave() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        let config = r#"{
            "inputs": {},
            "outputs": {
                "model": {
                    "format": "directory",
                    "schema": {
                        "kind": "tensor",
                        "arrays": {
                            "weights": {"shape": [2], "dtype": "float32"},
                            "bias": {"shape": [2], "dtype": "float32"}
                        }
                    }
                }
            }
        }"#;

        with_config(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            || {
                let mut arrays = HashMap::new();
                arrays.insert(
                    "weights".to_string(),
                    OutputData::TensorF32(ArrayD::from_elem(vec![2], 1.0f32)),
                );
                arrays.insert(
                    "bias".to_string(),
                    OutputData::TensorF32(ArrayD::from_elem(vec![2], 0.5f32)),
                );

                let data = OutputData::MultiTensor(arrays);
                save_output("model", data, false).unwrap();

                let model_dir = outputs_dir.join("model");
                let weights_path = model_dir.join("weights.npy");
                let bias_path = model_dir.join("bias.npy");

                assert!(weights_path.exists(), "weights.npy should be saved");
                assert!(bias_path.exists(), "bias.npy should be saved");

                let weights = ndarray_npy::read_npy::<_, ArrayD<f32>>(&weights_path).unwrap();
                let bias = ndarray_npy::read_npy::<_, ArrayD<f32>>(&bias_path).unwrap();

                assert_eq!(weights.shape(), &[2]);
                assert_eq!(bias.shape(), &[2]);
            },
        );
    }

    #[test]
    #[serial]
    fn test_save_output_undefined_error() {
        let dir = tempdir().unwrap();
        let config = r#"{"inputs": {}, "outputs": {}}"#;

        with_config(
            dir.path().to_str().unwrap(),
            dir.path().to_str().unwrap(),
            config,
            || {
                let arr = ArrayD::from_elem(vec![5], 1.0f32);
                let result = save_output("nonexistent", OutputData::TensorF32(arr), false);
                assert!(result.is_err());
                if let Err(GoldfishError::Config(crate::error::ConfigError::UndefinedOutput {
                    name,
                })) = result
                {
                    assert_eq!(name, "nonexistent");
                } else {
                    panic!("Expected UndefinedOutput error");
                }
            },
        );
    }

    #[test]
    #[serial]
    fn test_save_output_path_traversal_rejected() {
        let dir = tempdir().unwrap();
        let config = r#"{"inputs": {}, "outputs": {"../evil": {"format": "npy"}}}"#;

        with_config(
            dir.path().to_str().unwrap(),
            dir.path().to_str().unwrap(),
            config,
            || {
                let arr = ArrayD::from_elem(vec![5], 1.0f32);
                let result = save_output("../evil", OutputData::TensorF32(arr), false);
                assert!(result.is_err());
                // Should be rejected by path validation
            },
        );
    }

    #[test]
    #[serial]
    fn test_directory_validation_missing_arrays_blocking() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        let config = r#"{
            "inputs": {},
            "outputs": {
                "model": {
                    "format": "directory",
                    "schema": {
                        "kind": "tensor",
                        "arrays": {
                            "weights": {"shape": [2], "dtype": "float32"}
                        }
                    }
                }
            }
        }"#;
        let svs_config = r#"{"enabled": true, "default_enforcement": "blocking"}"#;

        with_config_and_svs(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            svs_config,
            || {
                let model_dir = outputs_dir.join("model");
                std::fs::create_dir_all(&model_dir).unwrap();

                let result = save_output("model", OutputData::Path(model_dir), false);
                assert!(result.is_err());
                if let Err(GoldfishError::Schema(SchemaError::ValidationFailed { .. })) = result {
                    // Expected
                } else {
                    panic!("Expected schema validation failure for missing arrays");
                }
            },
        );
    }

    #[test]
    #[serial]
    fn test_load_input_json() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        // Create test JSON file
        let json_path = inputs_dir.join("config.json");
        std::fs::write(&json_path, r#"{"key": "value"}"#).unwrap();

        let config = r#"{
            "inputs": {"config": {"format": "json"}},
            "outputs": {}
        }"#;

        with_config(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            || {
                let loaded = load_input("config", None).unwrap();
                if let OutputData::Json(val) = loaded {
                    assert_eq!(val["key"], "value");
                } else {
                    panic!("Expected Json");
                }
            },
        );
    }

    #[test]
    #[serial]
    fn test_load_input_directory() {
        let dir = tempdir().unwrap();
        let inputs_dir = dir.path().join("inputs");
        let outputs_dir = dir.path().join("outputs");
        std::fs::create_dir_all(&inputs_dir).unwrap();
        std::fs::create_dir_all(&outputs_dir).unwrap();

        // Create test directory
        let data_dir = inputs_dir.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let config = r#"{
            "inputs": {"data": {"format": "directory"}},
            "outputs": {}
        }"#;

        with_config(
            inputs_dir.to_str().unwrap(),
            outputs_dir.to_str().unwrap(),
            config,
            || {
                let loaded = load_input("data", None).unwrap();
                if let OutputData::Path(p) = loaded {
                    assert!(p.exists());
                    assert!(p.is_dir());
                } else {
                    panic!("Expected Path");
                }
            },
        );
    }

    #[test]
    fn test_output_data_typed_extractors() {
        // Test as_* reference extractors
        let f32_data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0f32));
        assert!(f32_data.as_tensor_f32().is_some());
        assert!(f32_data.as_tensor_f64().is_none());
        assert!(f32_data.as_json().is_none());
        assert!(f32_data.is_tensor());

        let f64_data = OutputData::TensorF64(ArrayD::from_elem(vec![10], 1.0f64));
        assert!(f64_data.as_tensor_f64().is_some());
        assert!(f64_data.as_tensor_f32().is_none());
        assert!(f64_data.is_tensor());

        let json_data = OutputData::Json(serde_json::json!({"key": "value"}));
        assert!(json_data.as_json().is_some());
        assert!(json_data.as_tensor_f32().is_none());
        assert!(!json_data.is_tensor());

        let path_data = OutputData::Path(PathBuf::from("/tmp/test"));
        assert!(path_data.as_path().is_some());
        assert!(!path_data.is_tensor());
    }

    #[test]
    fn test_output_data_into_extractors() {
        // Test into_* ownership extractors
        let f32_data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0f32));
        let arr = f32_data.into_tensor_f32().expect("should extract f32");
        assert_eq!(arr.shape(), &[10]);

        // Test that wrong type returns Err with original data
        let f64_data = OutputData::TensorF64(ArrayD::from_elem(vec![5], 2.0f64));
        let result = f64_data.into_tensor_f32();
        assert!(result.is_err());
        // Can recover the original data
        let recovered = result.unwrap_err();
        assert_eq!(recovered.dtype_str(), "float64");
    }
}
