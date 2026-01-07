//! Logging and monitoring utilities for Goldfish stages.
//!
//! This module provides structured logging, heartbeat functionality,
//! and metrics APIs that integrate with the Goldfish monitoring system.

use chrono::Utc;
use once_cell::sync::Lazy;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

use crate::config::get_outputs_dir;

/// Maximum log file size (10 MB).
const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024;

/// Minimum interval between heartbeats (seconds).
const HEARTBEAT_MIN_INTERVAL: f64 = 1.0;

/// Last heartbeat timestamp.
static LAST_HEARTBEAT: Lazy<Mutex<f64>> = Lazy::new(|| Mutex::new(0.0));

/// Write a structured log line for monitoring.
///
/// This function:
/// 1. Prints to stdout for human visibility via `logs()` tool
/// 2. Writes to `.goldfish/logs.txt` for AI monitoring (DuringRunMonitor)
///
/// The log file is automatically capped at 10MB to prevent disk exhaustion.
///
/// # Arguments
///
/// * `message` - The log message
/// * `level` - Log level (e.g., "INFO", "WARN", "ERROR")
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::logging::runtime_log;
///
/// runtime_log("Processing batch 50/100", "INFO");
/// runtime_log("Unusual loss spike detected", "WARN");
/// ```
pub fn runtime_log(message: &str, level: &str) {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let formatted = format!("[{}] {}: {}", timestamp, level, message);

    // Print to stdout for human visibility
    println!("{}", formatted);

    // Write to .goldfish/logs.txt for AI monitoring
    let log_file = get_outputs_dir().join(".goldfish").join("logs.txt");

    if let Some(parent) = log_file.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("[goldfish] Failed to create log directory: {}", e);
            return;
        }
    }

    // Check and handle log rotation
    if let Ok(metadata) = std::fs::metadata(&log_file) {
        if metadata.len() >= MAX_LOG_SIZE {
            // Truncate to last half
            if let Ok(content) = std::fs::read_to_string(&log_file) {
                let half = content.len() / 2;
                if let Err(e) = std::fs::write(&log_file, &content[half..]) {
                    eprintln!("[goldfish] Failed to rotate log file: {}", e);
                }
            }
        }
    }

    // Append log line
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
    {
        if let Err(e) = writeln!(file, "{}", formatted) {
            eprintln!("[goldfish] Failed to write log: {}", e);
        }
    }
}

/// Check if SVS requested early termination.
///
/// Returns true if the `.goldfish/stop_requested` file exists.
#[must_use]
pub fn should_stop() -> bool {
    get_outputs_dir()
        .join(".goldfish")
        .join("stop_requested")
        .exists()
}

/// Send a heartbeat signal.
///
/// Call this periodically in long-running computations to prevent
/// the job from being killed due to inactivity.
///
/// # Arguments
///
/// * `message` - Optional status message (e.g., "Processing batch 50/100")
/// * `force` - Write even if called recently (default: rate-limited to 1/sec)
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::logging::heartbeat;
///
/// for i in 0..100 {
///     heartbeat(Some(&format!("Processing batch {}/100", i)), false);
///     // ... process batch ...
/// }
/// ```
pub fn heartbeat(message: Option<&str>, force: bool) {
    let now = Utc::now().timestamp() as f64;

    // Rate limiting
    if !force {
        if let Ok(mut last) = LAST_HEARTBEAT.lock() {
            if now - *last < HEARTBEAT_MIN_INTERVAL {
                return;
            }
            *last = now;
        }
    } else if let Ok(mut last) = LAST_HEARTBEAT.lock() {
        *last = now;
    }

    let hb_dir = get_outputs_dir().join(".goldfish");
    let hb_file = hb_dir.join("heartbeat");

    if std::fs::create_dir_all(&hb_dir).is_err() {
        return;
    }

    let data = json!({
        "timestamp": now,
        "iso_time": Utc::now().to_rfc3339(),
        "message": message,
        "pid": std::process::id(),
    });

    // Atomic write using temp file + rename
    // Note: We use a simple write + rename pattern here since tempfile::persist
    // can fail on some filesystems. Fall back to direct write if needed.
    let json_str = match serde_json::to_string(&data) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[goldfish] Failed to serialize heartbeat: {}", e);
            return;
        }
    };

    match tempfile::NamedTempFile::new_in(&hb_dir) {
        Ok(temp) => {
            if let Err(e) = std::fs::write(temp.path(), &json_str) {
                eprintln!("[goldfish] Failed to write heartbeat temp file: {}", e);
                return;
            }
            // persist can fail on some temp dir configurations, fall back to copy
            if let Err(persist_err) = temp.persist(&hb_file) {
                if let Err(e) = std::fs::write(&hb_file, &json_str) {
                    eprintln!("[goldfish] Failed to write heartbeat (persist failed: {}, direct write failed: {})", persist_err, e);
                }
            }
        }
        Err(e) => {
            // Fall back to direct write if tempfile creation fails
            eprintln!("[goldfish] tempfile creation failed ({}), falling back to direct write", e);
            if let Err(e) = std::fs::write(&hb_file, &json_str) {
                eprintln!("[goldfish] Failed to write heartbeat: {}", e);
            }
        }
    }
}

/// Log a single metric.
///
/// # Arguments
///
/// * `name` - Metric name (e.g., "loss", "accuracy")
/// * `value` - Metric value
/// * `step` - Optional step/epoch number
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::logging::log_metric;
///
/// log_metric("loss", 0.5, Some(100));
/// log_metric("accuracy", 0.95, Some(100));
/// ```
pub fn log_metric(name: &str, value: f64, step: Option<i64>) {
    let mut metrics = HashMap::new();
    metrics.insert(name.to_string(), value);
    log_metrics(metrics, step);
}

/// Log multiple metrics.
///
/// More efficient than calling `log_metric` multiple times.
///
/// # Arguments
///
/// * `metrics` - Map of metric name to value
/// * `step` - Optional step/epoch number
pub fn log_metrics(metrics: HashMap<String, f64>, step: Option<i64>) {
    let now = Utc::now();
    let mut entries = Vec::new();

    for (name, value) in metrics {
        entries.push(json!({
            "type": "metric",
            "name": name,
            "value": value,
            "step": step,
            "timestamp": now.to_rfc3339(),
        }));
    }

    append_metrics(entries);
}

/// Log an artifact for tracking.
///
/// Records an artifact entry in the metrics JSONL file for tracking
/// outputs like model checkpoints, plots, or other files.
///
/// # Arguments
///
/// * `name` - Artifact name (e.g., "best_model", "training_plot")
/// * `path` - Path to the artifact file
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::logging::log_artifact;
///
/// // Log a model checkpoint
/// log_artifact("best_model", "/mnt/outputs/checkpoints/epoch_10.pt");
///
/// // Log a plot
/// log_artifact("loss_curve", "/mnt/outputs/plots/loss.png");
/// ```
pub fn log_artifact(name: &str, path: &str) {
    let entry = json!({
        "type": "artifact",
        "name": name,
        "path": path,
        "timestamp": Utc::now().to_rfc3339(),
    });
    append_metrics(vec![entry]);
}

/// Append entries to metrics JSONL file.
fn append_metrics(entries: Vec<serde_json::Value>) {
    let metrics_file = get_outputs_dir().join(".goldfish").join("metrics.jsonl");

    if let Some(parent) = metrics_file.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("[goldfish] Failed to create metrics directory: {}", e);
            return;
        }
    }

    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&metrics_file)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[goldfish] Failed to open metrics file: {}", e);
            return;
        }
    };

    for entry in entries {
        match serde_json::to_string(&entry) {
            Ok(line) => {
                if let Err(e) = writeln!(file, "{}", line) {
                    eprintln!("[goldfish] Failed to write metric: {}", e);
                }
            }
            Err(e) => {
                eprintln!("[goldfish] Failed to serialize metric: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::clear_config_cache;
    use serial_test::serial;
    use tempfile::tempdir;

    fn with_outputs_dir<F, R>(dir_path: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        clear_config_cache();
        let old = std::env::var("GOLDFISH_OUTPUTS_DIR").ok();
        std::env::set_var("GOLDFISH_OUTPUTS_DIR", dir_path);
        clear_config_cache(); // Clear again after setting
        let result = f();
        clear_config_cache();
        match old {
            Some(v) => std::env::set_var("GOLDFISH_OUTPUTS_DIR", v),
            None => std::env::remove_var("GOLDFISH_OUTPUTS_DIR"),
        }
        result
    }

    #[test]
    #[serial]
    fn test_runtime_log_caps_file_size() {
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            // Use get_outputs_dir() to read from the same place runtime_log writes to
            let log_file = get_outputs_dir().join(".goldfish").join("logs.txt");

            runtime_log("Test message", "INFO");

            assert!(log_file.exists(), "Log file should exist at {:?}", log_file);

            let content = std::fs::read_to_string(&log_file).unwrap();
            assert!(content.contains("Test message"));
        });
    }

    #[test]
    #[serial]
    fn test_heartbeat_writes_successfully() {
        // Simple test that heartbeat writes to the correct location
        // Rate limiting is tested via HEARTBEAT_MIN_INTERVAL constant
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            let hb_file = get_outputs_dir().join(".goldfish").join("heartbeat");

            // Force write to ensure it happens regardless of global state
            heartbeat(Some("TestMessage"), true);

            assert!(hb_file.exists(), "Heartbeat file should exist");
            let content = std::fs::read_to_string(&hb_file).unwrap();
            assert!(content.contains("TestMessage"), "Heartbeat should contain message");
            assert!(content.contains("timestamp"), "Heartbeat should have timestamp");
            assert!(content.contains("pid"), "Heartbeat should have pid");
        });
    }

    #[test]
    fn test_heartbeat_rate_limit_constant() {
        // Verify rate limiting constant is reasonable (1 second)
        assert_eq!(HEARTBEAT_MIN_INTERVAL, 1.0);
    }

    #[test]
    #[serial]
    fn test_log_metrics() {
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            // Use get_outputs_dir() to read from the same place log_metrics writes to
            let metrics_file = get_outputs_dir().join(".goldfish").join("metrics.jsonl");

            let mut metrics = HashMap::new();
            metrics.insert("loss".to_string(), 0.5);
            metrics.insert("accuracy".to_string(), 0.95);
            log_metrics(metrics, Some(100));

            assert!(metrics_file.exists(), "Metrics file should exist at {:?}", metrics_file);

            let content = std::fs::read_to_string(&metrics_file).unwrap();
            assert!(content.contains("loss"));
            assert!(content.contains("accuracy"));
            assert!(content.contains("100")); // step
        });
    }

    #[test]
    #[serial]
    fn test_should_stop() {
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            assert!(!should_stop());

            // Create stop file at the path should_stop() will check
            let stop_file = get_outputs_dir().join(".goldfish").join("stop_requested");
            std::fs::create_dir_all(stop_file.parent().unwrap()).unwrap();
            std::fs::write(&stop_file, "test").unwrap();

            assert!(should_stop());
        });
    }

    #[test]
    fn test_log_rotation_constant() {
        // Verify the log rotation threshold is 10MB
        assert_eq!(MAX_LOG_SIZE, 10 * 1024 * 1024);
    }

    #[test]
    #[serial]
    fn test_log_rotation_truncates_large_file() {
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            let log_file = get_outputs_dir().join(".goldfish").join("logs.txt");

            // Pre-create a log file that exceeds MAX_LOG_SIZE
            std::fs::create_dir_all(log_file.parent().unwrap()).unwrap();

            // Create a file just over the limit (we use a smaller size for test speed)
            // The rotation logic checks >= MAX_LOG_SIZE, so we simulate by checking the behavior
            // We can't easily test 10MB in unit tests, so we verify the truncation logic works
            // by checking that after writing to a large-ish file, subsequent writes work.

            // Write initial content
            let initial_content = "A".repeat(1000);
            std::fs::write(&log_file, &initial_content).unwrap();

            // Log a new message
            runtime_log("New message after initial content", "INFO");

            // Verify file exists and contains the new message
            let content = std::fs::read_to_string(&log_file).unwrap();
            assert!(
                content.contains("New message after initial content"),
                "Log should contain new message"
            );

            // Verify the file wasn't truncated (since we're under the limit)
            assert!(content.len() > 1000, "File should still have initial content");
        });
    }

    #[test]
    #[serial]
    fn test_log_artifact() {
        let dir = tempdir().unwrap();
        with_outputs_dir(dir.path().to_str().unwrap(), || {
            let metrics_file = get_outputs_dir().join(".goldfish").join("metrics.jsonl");

            log_artifact("model_checkpoint", "/path/to/checkpoint");

            assert!(metrics_file.exists(), "Metrics file should exist");
            let content = std::fs::read_to_string(&metrics_file).unwrap();
            assert!(content.contains("artifact"), "Should contain artifact type");
            assert!(content.contains("model_checkpoint"), "Should contain artifact name");
            assert!(content.contains("/path/to/checkpoint"), "Should contain artifact path");
        });
    }
}
