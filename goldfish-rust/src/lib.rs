use anyhow::{anyhow, Result};
use chrono::Utc;
use ndarray::ArrayD;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use once_cell::sync::Lazy;
use std::sync::Mutex;
use polars::prelude::{SerReader, SerWriter};
use std::io::Write;

// === Core Structs ===

#[derive(Deserialize, Debug, Clone)]
pub struct StageConfig {
    pub outputs: HashMap<String, OutputConfig>,
    pub inputs: HashMap<String, InputConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct InputConfig {
    pub format: Option<String>,
    pub schema: Option<Schema>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OutputConfig {
    pub format: Option<String>,
    pub schema: Option<Schema>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SVSConfig {
    pub enabled: bool,
    pub default_enforcement: String, // "blocking" | "warning" | "silent"
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Schema {
    pub kind: Option<String>,        // "tensor" | "tabular" | "json" | "file"
    pub shape: Option<Vec<Dim>>,     // dim can be int or wildcard
    pub rank: Option<i64>,
    pub dtype: Option<String>,
    pub columns: Option<Vec<String>>,
    pub dtypes: Option<HashMap<String, String>>,
    pub arrays: Option<HashMap<String, ArraySchema>>,
    pub primary_array: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ArraySchema {
    pub shape: Option<Vec<Dim>>,
    pub dtype: Option<String>,
    pub role: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Dim {
    Int(i64),
    Null,
}

impl Dim {
    pub fn matches(&self, val: i64) -> bool {
        match self {
            Dim::Null => true,
            Dim::Int(-1) => true,
            Dim::Int(i) => *i == val,
        }
    }
}

#[derive(Clone)]
pub enum OutputData {
    TensorF32(ArrayD<f32>),
    TensorF64(ArrayD<f64>),
    TensorI64(ArrayD<i64>),
    TensorI32(ArrayD<i32>),
    Json(serde_json::Value),
    Tabular(polars::prelude::DataFrame),
    Path(PathBuf),
    MultiTensor(HashMap<String, OutputData>),
}

impl OutputData {
    pub fn dtype_str(&self) -> String {
        match self {
            OutputData::TensorF32(_) => "float32".to_string(),
            OutputData::TensorF64(_) => "float64".to_string(),
            OutputData::TensorI64(_) => "int64".to_string(),
            OutputData::TensorI32(_) => "int32".to_string(),
            OutputData::Json(_) => "json".to_string(),
            OutputData::Tabular(_) => "tabular".to_string(),
            OutputData::Path(_) => "path".to_string(),
            OutputData::MultiTensor(_) => "multi".to_string(),
        }
    }
}

// === Automatic Finalization ===

pub struct GoldfishGuard;

impl Drop for GoldfishGuard {
    fn drop(&mut self) {
        let _ = finalize_svs();
    }
}

/// Initialize Goldfish and return a guard that auto-finalizes SVS on drop.
pub fn init() -> GoldfishGuard {
    env_logger::init();
    GoldfishGuard
}

// === Configuration Access ===

pub fn get_config() -> Result<StageConfig> {
    let config_json = std::env::var("GOLDFISH_STAGE_CONFIG")
        .map_err(|_| anyhow!("GOLDFISH_STAGE_CONFIG environment variable not set"))?;
    serde_json::from_str(&config_json).map_err(|e| anyhow!("Failed to parse GOLDFISH_STAGE_CONFIG: {}", e))
}

pub fn get_svs_config() -> SVSConfig {
    std::env::var("GOLDFISH_SVS_CONFIG")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(SVSConfig {
            enabled: true,
            default_enforcement: "warning".to_string(),
        })
}

pub fn get_inputs_dir() -> PathBuf {
    PathBuf::from(std::env::var("GOLDFISH_INPUTS_DIR").unwrap_or_else(|_| "/mnt/inputs".to_string()))
}

pub fn get_outputs_dir() -> PathBuf {
    PathBuf::from(std::env::var("GOLDFISH_OUTPUTS_DIR").unwrap_or_else(|_| "/mnt/outputs".to_string()))
}

// === Path Helpers ===

pub fn get_input_path(name: &str) -> PathBuf {
    get_inputs_dir().join(name)
}

pub fn get_output_path(name: &str) -> PathBuf {
    let path = get_outputs_dir().join(name);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::create_dir_all(&path);
    path
}

// === Logging & Monitoring ===

const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

pub fn runtime_log(message: &str, level: &str) {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let formatted = format!("[{}] {}: {}", timestamp, level, message);
    
    // Print to stdout for serial/logs tool
    println!("{}", formatted);
    
    // Append to logs.txt
    let log_file = get_outputs_dir().join(".goldfish").join("logs.txt");
    
    // Enforce size cap
    if let Ok(metadata) = std::fs::metadata(&log_file) {
        if metadata.len() >= MAX_LOG_SIZE {
            return;
        }
    }

    if let Some(parent) = log_file.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
    {
        let _ = writeln!(file, "{}", formatted);
    }
}

pub fn should_stop() -> bool {
    get_outputs_dir().join(".goldfish").join("stop_requested").exists()
}

pub fn heartbeat(message: Option<&str>, force: bool) {
    static LAST_HEARTBEAT: Lazy<Mutex<f64>> = Lazy::new(|| Mutex::new(0.0));
    
    let now = Utc::now().timestamp() as f64;
    if !force {
        let mut last = LAST_HEARTBEAT.lock().unwrap();
        if now - *last < 1.0 {
            return;
        }
        *last = now;
    }
    
    let hb_dir = get_outputs_dir().join(".goldfish");
    let hb_file = hb_dir.join("heartbeat");
    let _ = std::fs::create_dir_all(&hb_dir);
    
    let data = serde_json::json!({
        "timestamp": now,
        "iso_time": Utc::now().to_rfc3339(),
        "message": message,
        "pid": std::process::id(),
    });
    
    // Atomic write using tempfile and rename
    if let Ok(mut temp) = tempfile::NamedTempFile::new_in(&hb_dir) {
        if let Ok(json_str) = serde_json::to_string(&data) {
            if write!(temp, "{}", json_str).is_ok() {
                let _ = temp.persist(&hb_file);
            }
        }
    }
}

// === Metrics API ===

pub fn log_metric(name: &str, value: f64, step: Option<i64>) {
    let mut metrics = HashMap::new();
    metrics.insert(name.to_string(), value);
    log_metrics(metrics, step);
}

pub fn log_metrics(metrics: HashMap<String, f64>, step: Option<i64>) {
    let now = Utc::now();
    let mut entries = Vec::new();
    
    for (name, value) in metrics {
        entries.push(serde_json::json!({
            "type": "metric",
            "name": name,
            "value": value,
            "step": step,
            "timestamp": now.to_rfc3339(),
        }));
    }
    
    append_metrics(entries);
}

pub fn log_artifact(name: &str, path: &str) {
    let entry = serde_json::json!({
        "type": "artifact",
        "name": name,
        "path": path,
        "timestamp": Utc::now().to_rfc3339(),
    });
    append_metrics(vec![entry]);
}

fn append_metrics(entries: Vec<serde_json::Value>) {
    let metrics_file = get_outputs_dir().join(".goldfish").join("metrics.jsonl");
    if let Some(parent) = metrics_file.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&metrics_file)
    {
        for entry in entries {
            if let Ok(line) = serde_json::to_string(&entry) {
                let _ = writeln!(file, "{}", line);
            }
        }
    }
}

// === Primary API ===

pub fn load_input(name: &str, format: Option<&str>) -> Result<OutputData> {
    let config = get_config()?;
    let in_config = config.inputs.get(name)
        .ok_or_else(|| anyhow!("Input '{}' not defined in stage config", name))?;
        
    let input_path = get_input_path(name);
    let fmt = format.or(in_config.format.as_deref()).unwrap_or("file");
    
    match fmt {
        "npy" => {
            let p = if input_path.exists() { input_path } else { input_path.with_extension("npy") };
            if !p.exists() { return Err(anyhow!("Input file not found: {:?}", p)); }
            
            // Try loading common types, preserving dtype
            if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f32>>(&p) {
                return Ok(OutputData::TensorF32(arr));
            }
            if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f64>>(&p) {
                return Ok(OutputData::TensorF64(arr));
            }
            if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<i64>>(&p) {
                return Ok(OutputData::TensorI64(arr));
            }
            if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<i32>>(&p) {
                return Ok(OutputData::TensorI32(arr));
            }
            Err(anyhow!("Unsupported NPY dtype in {:?}", p))
        },
        "csv" => {
            let p = if input_path.exists() { input_path } else { input_path.with_extension("csv") };
            let df = polars::prelude::CsvReader::from_path(p)?.finish()?;
            Ok(OutputData::Tabular(df))
        },
        "json" => {
            let p = if input_path.exists() { input_path } else { input_path.with_extension("json") };
            let s = std::fs::read_to_string(p)?;
            let val = serde_json::from_str(&s)?;
            Ok(OutputData::Json(val))
        },
        "directory" | "file" | "dataset" => {
            Ok(OutputData::Path(input_path))
        },
        _ => Err(anyhow!("Unknown format: {}", fmt)),
    }
}

pub fn save_output(name: &str, data: OutputData, artifact: bool) -> Result<()> {
    let config = get_config()?;
    let out_config = config.outputs.get(name)
        .ok_or_else(|| anyhow!("Output '{}' not defined in stage config", name))?;
        
    let svs_config = get_svs_config();
    
    if let Some(schema) = &out_config.schema {
        let mut validation_errors = Vec::new();
        
        match &data {
            OutputData::Path(p) if p.is_file() => {
                return Err(anyhow!("Output '{}' schema validation requires in-memory data, got file Path", name));
            },
            OutputData::Path(p) if p.is_dir() => {
                if let Some(val_data) = load_directory_for_validation(p, schema) {
                    validation_errors = validate_output_data_against_schema(name, schema, &val_data);
                }
            },
            _ => {
                validation_errors = validate_output_data_against_schema(name, schema, &data);
            }
        }
        
        if !validation_errors.is_empty() {
            let enforcement = if !svs_config.enabled { "silent" } else { &svs_config.default_enforcement };
            let msg = format!("Output '{}' schema mismatch: {}", name, validation_errors.join("; "));
            
            match enforcement.as_ref() {
                "blocking" => return Err(anyhow!(msg)),
                "warning" => runtime_log(&msg, "WARN"),
                _ => {},
            }
        }
    }

    let output_path = get_outputs_dir().join(name);
    
    match &data {
        OutputData::Path(_) => {},
        _ => {
            let fmt = out_config.format.as_deref().unwrap_or("file");
            match fmt {
                "npy" => {
                    let path = if output_path.extension().is_none() { output_path.with_extension("npy") } else { output_path.clone() };
                    save_npy(&path, &data)?;
                    enqueue_stats(name, &path, &data.dtype_str());
                },
                "csv" => {
                    let path = if output_path.extension().is_none() { output_path.with_extension("csv") } else { output_path.clone() };
                    save_csv(&path, &data)?;
                    enqueue_stats(name, &path, "tabular");
                },
                _ => {
                    if fmt != "directory" && fmt != "file" {
                        return Err(anyhow!("Cannot auto-save format '{}'. Use get_output_path() for manual saving.", fmt));
                    }
                }
            }
        }
    }
    
    if artifact {
        mark_as_artifact(name)?;
    }
    
    Ok(())
}

fn save_npy(path: &Path, data: &OutputData) -> Result<()> {
    match data {
        OutputData::TensorF32(arr) => ndarray_npy::write_npy(path, arr)?,
        OutputData::TensorF64(arr) => ndarray_npy::write_npy(path, arr)?,
        OutputData::TensorI64(arr) => ndarray_npy::write_npy(path, arr)?,
        OutputData::TensorI32(arr) => ndarray_npy::write_npy(path, arr)?,
        _ => return Err(anyhow!("NPY format requires Tensor data")),
    }
    Ok(())
}

fn save_csv(path: &Path, data: &OutputData) -> Result<()> {
    if let OutputData::Tabular(ref df) = data {
        let mut df_mut = df.clone();
        let file = std::fs::File::create(path)?;
        polars::prelude::CsvWriter::new(file).finish(&mut df_mut)?;
    } else {
        return Err(anyhow!("CSV format requires Tabular data"));
    }
    Ok(())
}

fn mark_as_artifact(name: &str) -> Result<()> {
    let marker = get_outputs_dir().join(".artifacts").join(name);
    if let Some(parent) = marker.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::File::create(marker)?;
    Ok(())
}

// === Schema Validation ===

pub fn validate_output_data_against_schema(name: &str, schema: &Schema, data: &OutputData) -> Vec<String> {
    let mut errors = Vec::new();

    // Support multi-array schemas
    if let Some(arrays) = &schema.arrays {
        if let OutputData::MultiTensor(actual_arrays) = data {
            for (arr_name, arr_schema) in arrays {
                if let Some(actual_arr) = actual_arrays.get(arr_name) {
                    let sub_schema = Schema {
                        kind: Some("tensor".to_string()),
                        shape: arr_schema.shape.clone(),
                        dtype: arr_schema.dtype.clone(),
                        rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
                    };
                    let sub_errors = validate_output_data_against_schema(arr_name, &sub_schema, actual_arr);
                    for e in sub_errors {
                        errors.push(format!("Array '{}' in {}: {}", arr_name, name, e));
                    }
                } else {
                    errors.push(format!("Output '{}' missing expected array '{}'", name, arr_name));
                }
            }
            return errors;
        } else if let Some(primary) = &schema.primary_array {
             // If schema has multiple arrays but we got a single tensor, check if it matches primary
             if let Some(primary_schema) = arrays.get(primary) {
                 let sub_schema = Schema {
                    kind: Some("tensor".to_string()),
                    shape: primary_schema.shape.clone(),
                    dtype: primary_schema.dtype.clone(),
                    rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
                };
                return validate_output_data_against_schema(name, &sub_schema, data);
             }
        }
    }
    
    match schema.kind.as_deref() {
        Some("json") => {
            if let OutputData::Json(val) = data {
                if !val.is_object() && !val.is_array() {
                    errors.push(format!("Output '{}' kind=json requires object or array, got scalar", name));
                }
            } else {
                errors.push(format!("Output '{}' kind=json requires Json data", name));
            }
        },
        Some("tabular") | _ if schema.columns.is_some() || schema.dtypes.is_some() => {
            if let OutputData::Tabular(df) = data {
                if let Some(expected_cols) = &schema.columns {
                    let actual_cols: Vec<_> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
                    if expected_cols != &actual_cols {
                        errors.push(format!("Output '{}' column mismatch. Expected {:?}, got {:?}", name, expected_cols, actual_cols));
                    }
                }
                if let Some(expected_dtypes) = &schema.dtypes {
                    for (col, expected_dt) in expected_dtypes {
                        match df.column(col) {
                            Ok(c) => {
                                let actual_dt = format!("{:?}", c.dtype()).to_lowercase();
                                // Strict equality for dtypes
                                if actual_dt != expected_dt.to_lowercase() {
                                    errors.push(format!("Output '{}' column '{}' dtype mismatch. Expected {}, got {}", name, col, expected_dt, actual_dt));
                                }
                            },
                            Err(_) => errors.push(format!("Output '{}' missing required column '{}'", name, col)),
                        }
                    }
                }
            } else {
                errors.push(format!("Output '{}' kind=tabular requires Tabular data", name));
            }
        },
        _ => {
            match data {
                OutputData::TensorF32(arr) => validate_tensor(name, schema, arr.shape(), "float32", &mut errors),
                OutputData::TensorF64(arr) => validate_tensor(name, schema, arr.shape(), "float64", &mut errors),
                OutputData::TensorI64(arr) => validate_tensor(name, schema, arr.shape(), "int64", &mut errors),
                OutputData::TensorI32(arr) => validate_tensor(name, schema, arr.shape(), "int32", &mut errors),
                _ => {
                    if schema.kind.as_deref() == Some("tensor") {
                        errors.push(format!("Output '{}' kind=tensor requires Tensor data", name));
                    }
                }
            }
        }
    }
    
    errors
}

fn validate_tensor(name: &str, schema: &Schema, actual_shape: &[usize], actual_dtype: &str, errors: &mut Vec<String>) {
    if let Some(expected_dtype) = &schema.dtype {
        if expected_dtype != actual_dtype {
            errors.push(format!("Output '{}' dtype mismatch. Expected {}, got {}", name, expected_dtype, actual_dtype));
        }
    }
    
    if let Some(expected_rank) = schema.rank {
        if actual_shape.len() as i64 != expected_rank {
            errors.push(format!("Output '{}' rank mismatch. Expected {}, got {}", name, expected_rank, actual_shape.len()));
        }
    }
    
    if let Some(expected_shape) = &schema.shape {
        if expected_shape.len() != actual_shape.len() {
            errors.push(format!("Output '{}' shape length mismatch. Expected {}, got {}", name, expected_shape.len(), actual_shape.len()));
        } else {
            for (i, (exp, &act)) in expected_shape.iter().zip(actual_shape.iter()).enumerate() {
                if !exp.matches(act as i64) {
                    errors.push(format!("Output '{}' shape mismatch at dim {}. Expected {:?}, got {}", name, i, exp, act));
                }
            }
        }
    }
}

// === Stats tracking ===

#[derive(Serialize, Default, Clone)]
pub struct StatsEntry {
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub samples_used: usize,
    pub total_elements: usize,
    pub entropy: f64,
    pub null_ratio: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
}

static STATS_CACHE: Lazy<Mutex<HashMap<String, StatsEntry>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn finalize_svs() -> Result<()> {
    let stats = STATS_CACHE.lock().unwrap();
    let manifest = serde_json::json!({
        "version": 1,
        "stats": *stats,
    });
    
    let path = get_outputs_dir().join(".goldfish").join("svs_stats.json");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(path, serde_json::to_string_pretty(&manifest)?)?;
    Ok(())
}

fn enqueue_stats(name: &str, path: &Path, dtype: &str) {
    if std::env::var("GOLDFISH_SVS_STATS_ENABLED").as_deref() != Ok("true") {
        return;
    }
    
    let name = name.to_string();
    let path = path.to_path_buf();
    let dtype = dtype.to_string();
    
    // Compute stats in background thread to avoid blocking stage code
    std::thread::spawn(move || {
        if let Ok(entry) = compute_stats(&path, &dtype) {
            let mut cache = STATS_CACHE.lock().unwrap();
            cache.insert(name, entry);
        }
    });
}

fn compute_stats(path: &Path, dtype: &str) -> Result<StatsEntry> {
    use rand::seq::SliceRandom;
    
    // Load as f32 for stats computation (best effort for all types)
    let samples: Vec<f32> = if dtype == "tabular" {
        let df = polars::prelude::CsvReader::from_path(path)?.finish()?;
        // Just use first numeric column for now
        let mut s = Vec::new();
        for col in df.get_columns() {
            if col.dtype().is_numeric() {
                if let Ok(ca) = col.f32() {
                    s = ca.into_no_null_iter().collect();
                    break;
                }
            }
        }
        s
    } else {
        // Try loading NPY as different types and convert to f32
        if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f32>>(path) {
            arr.iter().cloned().collect()
        } else if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f64>>(path) {
            arr.iter().map(|&x| x as f32).collect()
        } else if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<i64>>(path) {
            arr.iter().map(|&x| x as f32).collect()
        } else {
            return Err(anyhow!("Unsupported dtype for stats: {}", dtype));
        }
    };

    let total_elements = samples.len();
    let sample_size = std::cmp::min(10000, total_elements);
    let mut rng = rand::thread_rng();
    let samples: Vec<_> = if total_elements > sample_size {
        samples.choose_multiple(&mut rng, sample_size).cloned().collect()
    } else {
        samples
    };
    
    if samples.is_empty() {
        return Ok(StatsEntry { total_elements, ..Default::default() });
    }
    
    let mut min = f32::MAX;
    let mut max = f32::MIN;
    let mut sum = 0.0;
    let mut null_count = 0;
    for &s in &samples {
        if s.is_nan() { null_count += 1; continue; }
        if s < min { min = s; }
        if s > max { max = s; }
        sum += s as f64;
    }
    let mean = sum / (samples.len() - null_count) as f64;
    let mut var_sum = 0.0;
    for &s in &samples {
        if !s.is_nan() { var_sum += (s as f64 - mean).powi(2); }
    }
    let std = (var_sum / (samples.len() - null_count) as f64).sqrt();
    let entropy = compute_entropy(&samples);
    
    Ok(StatsEntry { 
        mean, std, min: min as f64, max: max as f64, 
        samples_used: samples.len(), total_elements, 
        entropy, null_ratio: null_count as f64 / samples.len() as f64, 
        dtype: Some(dtype.to_string()) 
    })
}

fn compute_entropy(samples: &[f32]) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut counts = HashMap::new();
    for &s in samples {
        let key = (s * 1000.0) as i32;
        *counts.entry(key).or_insert(0) += 1;
    }
    let mut ent = 0.0;
    let n = samples.len() as f64;
    for &count in counts.values() {
        let p = count as f64 / n;
        ent -= p * p.log2();
    }
    ent
}

fn load_directory_for_validation(dir: &Path, schema: &Schema) -> Option<OutputData> {
    // Implement multi-array directory loading
    if let Some(arrays) = &schema.arrays {
        let mut actual_arrays = HashMap::new();
        for name in arrays.keys() {
            let p = dir.join(format!("{}.npy", name));
            if p.exists() {
                // Best effort load as f32 for validation
                if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f32>>(&p) {
                    actual_arrays.insert(name.clone(), OutputData::TensorF32(arr));
                }
            }
        }
        if !actual_arrays.is_empty() {
            return Some(OutputData::MultiTensor(actual_arrays));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn setup_env() {
        std::env::set_var("GOLDFISH_STAGE_CONFIG", json!({
            "inputs": {
                "test_in": {"format": "npy", "schema": {"kind": "tensor", "dtype": "float32"}}
            },
            "outputs": {
                "test_out": {"format": "npy", "schema": {"kind": "tensor", "dtype": "float32", "shape": [null, 10]}}
            }
        }).to_string());
        
        let out_dir = tempfile::tempdir().unwrap();
        std::env::set_var("GOLDFISH_OUTPUTS_DIR", out_dir.path().to_str().unwrap());
    }

    #[test]
    fn test_tensor_validation() {
        let schema = Schema {
            kind: Some("tensor".to_string()),
            dtype: Some("float32".to_string()),
            shape: Some(vec![Dim::Null, Dim::Int(10)]),
            rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
        };
        
        // Correct data
        let data = OutputData::TensorF32(ndarray::ArrayD::from_elem(vec![5, 10], 1.0));
        let errs = validate_output_data_against_schema("test", &schema, &data);
        assert!(errs.is_empty());
        
        // Dtype mismatch
        let data_f64 = OutputData::TensorF64(ndarray::ArrayD::from_elem(vec![5, 10], 1.0));
        let errs = validate_output_data_against_schema("test", &schema, &data_f64);
        assert!(!errs.is_empty());
        assert!(errs[0].contains("dtype mismatch"));
        
        // Shape length mismatch
        let data_rank3 = OutputData::TensorF32(ndarray::ArrayD::from_elem(vec![5, 10, 1], 1.0));
        let errs = validate_output_data_against_schema("test", &schema, &data_rank3);
        assert!(!errs.is_empty());
        assert!(errs[0].contains("shape length mismatch"));
    }

    #[test]
    fn test_json_validation() {
        let schema = Schema {
            kind: Some("json".to_string()),
            dtype: None, shape: None, rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
        };
        
        let data = OutputData::Json(json!({"a": 1}));
        let errs = validate_output_data_against_schema("test", &schema, &data);
        assert!(errs.is_empty());
        
        let data_scalar = OutputData::Json(json!(1));
        let errs = validate_output_data_against_schema("test", &schema, &data_scalar);
        assert!(!errs.is_empty());
        assert!(errs[0].contains("requires object or array"));
    }

    #[test]
    fn test_multi_array_validation() {
        let mut arrays = HashMap::new();
        arrays.insert("weights".to_string(), ArraySchema {
            shape: Some(vec![Dim::Int(100), Dim::Int(100)]),
            dtype: Some("float32".to_string()),
            role: None,
        });
        arrays.insert("bias".to_string(), ArraySchema {
            shape: Some(vec![Dim::Int(100)]),
            dtype: Some("float32".to_string()),
            role: None,
        });

        let schema = Schema {
            kind: Some("directory".to_string()),
            arrays: Some(arrays),
            primary_array: Some("weights".to_string()),
            shape: None, rank: None, dtype: None, columns: None, dtypes: None,
        };

        // Case 1: MultiTensor input
        let mut actual_arrays = HashMap::new();
        actual_arrays.insert("weights".to_string(), OutputData::TensorF32(ArrayD::from_elem(vec![100, 100], 1.0)));
        actual_arrays.insert("bias".to_string(), OutputData::TensorF32(ArrayD::from_elem(vec![100], 1.0)));
        let data = OutputData::MultiTensor(actual_arrays);
        let errs = validate_output_data_against_schema("test", &schema, &data);
        assert!(errs.is_empty());

        // Case 2: Missing array
        let mut missing_arrays = HashMap::new();
        missing_arrays.insert("weights".to_string(), OutputData::TensorF32(ArrayD::from_elem(vec![100, 100], 1.0)));
        let data_missing = OutputData::MultiTensor(missing_arrays);
        let errs = validate_output_data_against_schema("test", &schema, &data_missing);
        assert!(!errs.is_empty());
        assert!(errs[0].contains("missing expected array 'bias'"));

        // Case 3: Single tensor matching primary
        let data_single = OutputData::TensorF32(ArrayD::from_elem(vec![100, 100], 1.0));
        let errs = validate_output_data_against_schema("test", &schema, &data_single);
        assert!(errs.is_empty());
    }
}
