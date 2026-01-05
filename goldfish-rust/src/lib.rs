use anyhow::{anyhow, Result};
use chrono::Utc;
use ndarray::ArrayD;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex, Condvar};
use polars::prelude::{SerReader, SerWriter};
use std::io::{Write, Read, Seek, SeekFrom};

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

// === Thread Management for Async Stats ===

struct StatsTracker {
    in_flight: Mutex<usize>,
    cond: Condvar,
}

impl StatsTracker {
    fn increment(&self) {
        let mut count = self.in_flight.lock().unwrap();
        *count += 1;
    }

    fn decrement(&self) {
        let mut count = self.in_flight.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.cond.notify_all();
        }
    }

    fn wait_for_all(&self) {
        let mut count = self.in_flight.lock().unwrap();
        while *count > 0 {
            count = self.cond.wait(count).unwrap();
        }
    }
}

static TRACKER: Lazy<Arc<StatsTracker>> = Lazy::new(|| Arc::new(StatsTracker {
    in_flight: Mutex::new(0),
    cond: Condvar::new(),
}));

// === Automatic Finalization ===

pub struct GoldfishGuard;

impl Drop for GoldfishGuard {
    fn drop(&mut self) {
        let _ = finalize_svs();
    }
}

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
    println!("{}", formatted);
    
    let log_file = get_outputs_dir().join(".goldfish").join("logs.txt");
    if let Ok(metadata) = std::fs::metadata(&log_file) {
        if metadata.len() >= MAX_LOG_SIZE { return; }
    }

    if let Some(parent) = log_file.parent() { let _ = std::fs::create_dir_all(parent); }
    
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&log_file) {
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
        if now - *last < 1.0 { return; }
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
    
    if let Ok(mut temp) = tempfile::NamedTempFile::new_in(&hb_dir) {
        if let Ok(json_str) = serde_json::to_string(&data) {
            if write!(temp, "{}", json_str).is_ok() { let _ = temp.persist(&hb_file); }
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
            "type": "metric", "name": name, "value": value, "step": step, "timestamp": now.to_rfc3339(),
        }));
    }
    append_metrics(entries);
}

pub fn log_artifact(name: &str, path: &str) {
    let entry = serde_json::json!({
        "type": "artifact", "name": name, "path": path, "timestamp": Utc::now().to_rfc3339(),
    });
    append_metrics(vec![entry]);
}

fn append_metrics(entries: Vec<serde_json::Value>) {
    let metrics_file = get_outputs_dir().join(".goldfish").join("metrics.jsonl");
    if let Some(parent) = metrics_file.parent() { let _ = std::fs::create_dir_all(parent); }
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&metrics_file) {
        for entry in entries {
            if let Ok(line) = serde_json::to_string(&entry) { let _ = writeln!(file, "{}", line); }
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
            load_npy_typed(&p)
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
        "directory" | "file" | "dataset" => Ok(OutputData::Path(input_path)),
        _ => Err(anyhow!("Unknown format: {}", fmt)),
    }
}

fn load_npy_typed(p: &Path) -> Result<OutputData> {
    if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f32>>(p) { return Ok(OutputData::TensorF32(arr)); }
    if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<f64>>(p) { return Ok(OutputData::TensorF64(arr)); }
    if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<i64>>(p) { return Ok(OutputData::TensorI64(arr)); }
    if let Ok(arr) = ndarray_npy::read_npy::<_, ArrayD<i32>>(p) { return Ok(OutputData::TensorI32(arr)); }
    Err(anyhow!("Unsupported NPY dtype in {:?}", p))
}

pub fn save_output(name: &str, data: OutputData, artifact: bool) -> Result<()> {
    let config = get_config()?;
    let out_config = config.outputs.get(name)
        .ok_or_else(|| anyhow!("Output '{}' not defined in stage config", name))?;
        
    let svs_config = get_svs_config();
    
    if let Some(schema) = &out_config.schema {
        let mut val_errors = Vec::new();
        match &data {
            OutputData::Path(p) if p.is_file() => return Err(anyhow!("Output '{}' schema validation requires in-memory data, got file Path", name)),
            OutputData::Path(p) if p.is_dir() => {
                if let Some(val_data) = load_directory_for_validation(p, schema) {
                    val_errors = validate_output_data_against_schema(name, schema, &val_data);
                }
            },
            _ => val_errors = validate_output_data_against_schema(name, schema, &data),
        }
        
        if !val_errors.is_empty() {
            let enforcement = if !svs_config.enabled { "silent" } else { &svs_config.default_enforcement };
            let msg = format!("Output '{}' schema mismatch: {}", name, val_errors.join("; "));
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
                _ => if fmt != "directory" && fmt != "file" {
                    return Err(anyhow!("Cannot auto-save format '{}'. Use get_output_path() for manual saving.", fmt));
                }
            }
        }
    }
    if artifact { mark_as_artifact(name)?; }
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
        Ok(())
    } else { Err(anyhow!("CSV format requires Tabular data")) }
}

fn mark_as_artifact(name: &str) -> Result<()> {
    let marker = get_outputs_dir().join(".artifacts").join(name);
    if let Some(parent) = marker.parent() { let _ = std::fs::create_dir_all(parent); }
    std::fs::File::create(marker)?;
    Ok(())
}

// === Schema Validation ===

pub fn validate_output_data_against_schema(name: &str, schema: &Schema, data: &OutputData) -> Vec<String> {
    let mut errors = Vec::new();

    // Support multi-array schemas (only for kind=tensor)
    if schema.kind.as_deref() == Some("tensor") {
        if let Some(arrays) = &schema.arrays {
            if let OutputData::MultiTensor(actual_arrays) = data {
                for (arr_name, arr_schema) in arrays {
                    if let Some(actual_arr) = actual_arrays.get(arr_name) {
                        let sub_schema = Schema {
                            kind: Some("tensor".to_string()), shape: arr_schema.shape.clone(), dtype: arr_schema.dtype.clone(),
                            rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
                        };
                        let sub_errors = validate_output_data_against_schema(arr_name, &sub_schema, actual_arr);
                        for e in sub_errors { errors.push(format!("Array '{}' in {}: {}", arr_name, name, e)); }
                    } else {
                        errors.push(format!("Output '{}' missing expected array '{}'", name, arr_name));
                    }
                }
                return errors;
            } else {
                // Fallback for single tensor: use primary_array or first available definition
                let target_name = schema.primary_array.as_deref().or_else(|| arrays.keys().next().map(|s| s.as_str()));
                if let Some(tn) = target_name {
                    if let Some(target_schema) = arrays.get(tn) {
                        let sub_schema = Schema {
                            kind: Some("tensor".to_string()), shape: target_schema.shape.clone(), dtype: target_schema.dtype.clone(),
                            rank: None, columns: None, dtypes: None, arrays: None, primary_array: None,
                        };
                        return validate_output_data_against_schema(name, &sub_schema, data);
                    }
                }
            }
        }
    }
    
    match schema.kind.as_deref() {
        Some("json") => {
            if let OutputData::Json(val) = data {
                if !val.is_object() && !val.is_array() { errors.push(format!("Output '{}' kind=json requires object or array, got scalar", name)); }
            } else { errors.push(format!("Output '{}' kind=json requires Json data", name)); }
        },
        Some("tabular") | _ if schema.columns.is_some() || schema.dtypes.is_some() => {
            if let OutputData::Tabular(df) = data {
                if let Some(expected_cols) = &schema.columns {
                    let actual_cols: Vec<_> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
                    if expected_cols != &actual_cols { errors.push(format!("Output '{}' column mismatch. Expected {:?}, got {:?}", name, expected_cols, actual_cols)); }
                }
                if let Some(expected_dtypes) = &schema.dtypes {
                    for (col, expected_dt) in expected_dtypes {
                        match df.column(col) {
                            Ok(c) => {
                                let actual_dt = format!("{:?}", c.dtype()).to_lowercase();
                                if actual_dt != expected_dt.to_lowercase() {
                                    errors.push(format!("Output '{}' column '{}' dtype mismatch. Expected {}, got {}", name, col, expected_dt, actual_dt));
                                }
                            },
                            Err(_) => errors.push(format!("Output '{}' missing required column '{}'", name, col)),
                        }
                    }
                }
            } else { errors.push(format!("Output '{}' kind=tabular requires Tabular data", name)); }
        },
        _ => {
            match data {
                OutputData::TensorF32(arr) => validate_tensor(name, schema, arr.shape(), "float32", &mut errors),
                OutputData::TensorF64(arr) => validate_tensor(name, schema, arr.shape(), "float64", &mut errors),
                OutputData::TensorI64(arr) => validate_tensor(name, schema, arr.shape(), "int64", &mut errors),
                OutputData::TensorI32(arr) => validate_tensor(name, schema, arr.shape(), "int32", &mut errors),
                _ => if schema.kind.as_deref() == Some("tensor") { errors.push(format!("Output '{}' kind=tensor requires Tensor data", name)); }
            }
        }
    }
    errors
}

fn validate_tensor(name: &str, schema: &Schema, actual_shape: &[usize], actual_dtype: &str, errors: &mut Vec<String>) {
    if let Some(expected_dtype) = &schema.dtype {
        if expected_dtype != actual_dtype { errors.push(format!("Output '{}' dtype mismatch. Expected {}, got {}", name, expected_dtype, actual_dtype)); }
    }
    if let Some(expected_rank) = schema.rank {
        if actual_shape.len() as i64 != expected_rank { errors.push(format!("Output '{}' rank mismatch. Expected {}, got {}", name, expected_rank, actual_shape.len())); }
    }
    if let Some(expected_shape) = &schema.shape {
        if expected_shape.len() != actual_shape.len() {
            errors.push(format!("Output '{}' shape length mismatch. Expected {}, got {}", name, expected_shape.len(), actual_shape.len()));
        } else {
            for (i, (exp, &act)) in expected_shape.iter().zip(actual_shape.iter()).enumerate() {
                if !exp.matches(act as i64) { errors.push(format!("Output '{}' shape mismatch at dim {}. Expected {:?}, got {}", name, i, exp, act)); }
            }
        }
    }
}

// === Stats tracking ===

#[derive(Serialize, Default, Clone)]
pub struct StatsEntry {
    pub mean: f64, pub std: f64, pub min: f64, pub max: f64,
    pub samples_used: usize, pub total_elements: usize,
    pub entropy: f64, pub null_ratio: f64,
    #[serde(skip_serializing_if = "Option::is_none")] pub dtype: Option<String>,
}

static STATS_CACHE: Lazy<Mutex<HashMap<String, StatsEntry>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn finalize_svs() -> Result<()> {
    TRACKER.wait_for_all();
    let stats = STATS_CACHE.lock().unwrap();
    let manifest = serde_json::json!({ "version": 1, "stats": *stats });
    let path = get_outputs_dir().join(".goldfish").join("svs_stats.json");
    if let Some(parent) = path.parent() { let _ = std::fs::create_dir_all(parent); }
    std::fs::write(path, serde_json::to_string_pretty(&manifest)?)?;
    Ok(())
}

fn enqueue_stats(name: &str, path: &Path, dtype: &str) {
    let enabled = std::env::var("GOLDFISH_SVS_STATS_ENABLED").unwrap_or_default().to_lowercase();
    if !["true", "1", "yes"].contains(&enabled.as_str()) { return; }
    
    let name = name.to_string();
    let path = path.to_path_buf();
    let dtype = dtype.to_string();
    let tracker = Arc::clone(&TRACKER);
    
    tracker.increment();
    std::thread::spawn(move || {
        if let Ok(entry) = compute_stats(&path, &dtype) {
            let mut cache = STATS_CACHE.lock().unwrap();
            cache.insert(name, entry);
        }
        tracker.decrement();
    });
}

fn compute_stats(path: &Path, dtype: &str) -> Result<StatsEntry> {
    use rand::seq::SliceRandom;
    let mut samples: Vec<f32> = Vec::new();
    let total_elements: usize;

    if dtype == "tabular" {
        let df = polars::prelude::CsvReader::from_path(path)?.finish()?;
        for col in df.get_columns() {
            if col.dtype().is_numeric() {
                if let Ok(ca) = col.f32() { samples = ca.into_no_null_iter().collect(); break; }
            }
        }
        total_elements = samples.len();
    } else {
        // Memory-safe sampling for NPY: read header, then seek to random positions
        let mut file = std::fs::File::open(path)?;
        let header = read_npy_header(&mut file)?;
        total_elements = header.size;
        let sample_size = std::cmp::min(10000, total_elements);
        let mut rng = rand::thread_rng();
        
        let mut indices: Vec<usize> = (0..total_elements).collect();
        indices.shuffle(&mut rng);
        indices.truncate(sample_size);
        indices.sort_unstable();

        for idx in indices {
            file.seek(SeekFrom::Start(header.data_offset + (idx * header.word_size) as u64))?;
            samples.push(read_f32_sample(&mut file, &header.dtype)?);
        }
    }

    if samples.is_empty() { return Ok(StatsEntry { total_elements, ..Default::default() }); }
    
    let mut min = f32::MAX; let mut max = f32::MIN; let mut sum = 0.0; let mut null_count = 0;
    for &s in &samples {
        if s.is_nan() { null_count += 1; continue; }
        if s < min { min = s; }
        if s > max { max = s; }
        sum += s as f64;
    }
    let mean = sum / (samples.len() - null_count) as f64;
    let mut var_sum = 0.0;
    for &s in &samples { if !s.is_nan() { var_sum += (s as f64 - mean).powi(2); } }
    let std = (var_sum / (samples.len() - null_count) as f64).sqrt();
    let entropy = compute_entropy(&samples);
    
    Ok(StatsEntry { 
        mean, std, min: min as f64, max: max as f64, samples_used: samples.len(), total_elements, 
        entropy, null_ratio: null_count as f64 / samples.len() as f64, dtype: Some(dtype.to_string()) 
    })
}

struct NpyHeader { data_offset: u64, word_size: usize, dtype: String, size: usize }

fn read_npy_header<R: Read + Seek>(reader: &mut R) -> Result<NpyHeader> {
    let mut magic = [0u8; 6];
    reader.read_exact(&mut magic)?;
    if &magic != b"\x93NUMPY" { return Err(anyhow!("Invalid NPY magic")); }
    let mut version = [0u8; 2];
    reader.read_exact(&mut version)?;
    let mut header_len_bytes = [0u8; 2];
    reader.read_exact(&mut header_len_bytes)?;
    let header_len = u16::from_le_bytes(header_len_bytes) as usize;
    let mut header_bytes = vec![0u8; header_len];
    reader.read_exact(&mut header_bytes)?;
    let header_str = String::from_utf8_lossy(&header_bytes);
    
    let dtype = if header_str.contains("'f4'") { "f4" } else if header_str.contains("'f8'") { "f8" } 
                else if header_str.contains("'i8'") { "i8" } else if header_str.contains("'i4'") { "i4" }
                else { "f4" };
    let word_size = match dtype { "f8" | "i8" => 8, _ => 4 };
    
    // Extract shape to get total size (simple heuristic)
    let size = if let Some(start) = header_str.find('(') {
        if let Some(end) = header_str[start..].find(')') {
            header_str[start+1..start+end].split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .product()
        } else { 1 }
    } else { 1 };

    Ok(NpyHeader { data_offset: 10 + header_len as u64, word_size, dtype: dtype.to_string(), size })
}

fn read_f32_sample<R: Read>(reader: &mut R, dtype: &str) -> Result<f32> {
    match dtype {
        "f4" => { let mut b = [0u8; 4]; reader.read_exact(&mut b)?; Ok(f32::from_le_bytes(b)) },
        "f8" => { let mut b = [0u8; 8]; reader.read_exact(&mut b)?; Ok(f64::from_le_bytes(b) as f32) },
        "i8" => { let mut b = [0u8; 8]; reader.read_exact(&mut b)?; Ok(i64::from_le_bytes(b) as f32) },
        "i4" => { let mut b = [0u8; 4]; reader.read_exact(&mut b)?; Ok(i32::from_le_bytes(b) as f32) },
        _ => Err(anyhow!("Unsupported dtype")),
    }
}

fn compute_entropy(samples: &[f32]) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut counts = HashMap::new();
    for &s in samples { let key = (s * 1000.0) as i32; *counts.entry(key).or_insert(0) += 1; }
    let mut ent = 0.0; let n = samples.len() as f64;
    for &count in counts.values() { let p = count as f64 / n; ent -= p * p.log2(); }
    ent
}

fn load_directory_for_validation(dir: &Path, schema: &Schema) -> Option<OutputData> {
    if let Some(arrays) = &schema.arrays {
        // Try NPZ first
        for name in arrays.keys() {
            let p = dir.join(format!("{}.npz", name));
            if p.exists() { /* NPZ loading is complex, fallback to NPY files in dir */ }
        }
        let mut actual_arrays = HashMap::new();
        for name in arrays.keys() {
            let p = dir.join(format!("{}.npy", name));
            if p.exists() { if let Ok(data) = load_npy_typed(&p) { actual_arrays.insert(name.clone(), data); } }
        }
        if !actual_arrays.is_empty() { return Some(OutputData::MultiTensor(actual_arrays)); }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_array_validation_fallback() {
        let mut arrays = HashMap::new();
        arrays.insert("weights".to_string(), ArraySchema { shape: Some(vec![Dim::Int(10)]), dtype: Some("float32".to_string()), role: None });
        let schema = Schema { kind: Some("tensor".to_string()), arrays: Some(arrays), primary_array: None, shape: None, rank: None, dtype: None, columns: None, dtypes: None };
        let data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0));
        let errs = validate_output_data_against_schema("test", &schema, &data);
        assert!(errs.is_empty());
    }

    #[test]
    fn test_async_stats_flush() {
        std::env::set_var("GOLDFISH_SVS_STATS_ENABLED", "true");
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("GOLDFISH_OUTPUTS_DIR", dir.path().to_str().unwrap());
        let npy_path = dir.path().join("test.npy");
        let arr = ArrayD::<f32>::from_elem(vec![100], 1.0);
        ndarray_npy::write_npy(&npy_path, &arr).unwrap();
        
        enqueue_stats("test", &npy_path, "float32");
        finalize_svs().unwrap(); // Should wait for thread
        
        let stats_path = dir.path().join(".goldfish").join("svs_stats.json");
        assert!(stats_path.exists());
    }
}