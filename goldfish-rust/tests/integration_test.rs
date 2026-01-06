//! Integration tests for the Goldfish Rust SDK.
//!
//! These tests verify end-to-end behavior with real files and environment setup.

use goldfish_rust::{OutputData, runtime_log, heartbeat, log_metric, should_stop, clear_config_cache};
use ndarray::ArrayD;
use serial_test::serial;
use tempfile::tempdir;

/// Helper to run a test with custom environment variables.
/// Clears config cache to ensure env var changes are picked up.
fn with_env<F, R>(vars: &[(&str, &str)], f: F) -> R
where
    F: FnOnce() -> R,
{
    // Clear config cache first to ensure fresh env var reads
    clear_config_cache();

    // Save old values
    let old_values: Vec<_> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(*k).ok()))
        .collect();

    // Set new values
    for (k, v) in vars {
        std::env::set_var(*k, *v);
    }

    // Clear config cache again after setting new values
    clear_config_cache();

    let result = f();

    // Clear config cache before restoring
    clear_config_cache();

    // Restore old values
    for (k, old) in old_values {
        match old {
            Some(v) => std::env::set_var(k, v),
            None => std::env::remove_var(k),
        }
    }

    result
}

#[test]
#[serial]
fn test_logging_runtime_log() {
    let dir = tempdir().unwrap();

    with_env(&[("GOLDFISH_OUTPUTS_DIR", dir.path().to_str().unwrap())], || {
        runtime_log("Test message 1", "INFO");
        runtime_log("Test message 2", "WARN");

        let log_file = dir.path().join(".goldfish").join("logs.txt");
        assert!(log_file.exists(), "Log file should exist");

        let content = std::fs::read_to_string(&log_file).unwrap();
        assert!(content.contains("Test message 1"));
        assert!(content.contains("Test message 2"));
        assert!(content.contains("INFO"));
        assert!(content.contains("WARN"));
    });
}

#[test]
#[serial]
fn test_logging_heartbeat() {
    let dir = tempdir().unwrap();

    with_env(&[("GOLDFISH_OUTPUTS_DIR", dir.path().to_str().unwrap())], || {
        // Reset rate limiter by forcing
        heartbeat(Some("Test heartbeat"), true);

        let hb_file = dir.path().join(".goldfish").join("heartbeat");
        assert!(hb_file.exists(), "Heartbeat file should exist");

        let content = std::fs::read_to_string(&hb_file).unwrap();
        assert!(content.contains("Test heartbeat"));
        assert!(content.contains("timestamp"));
    });
}

#[test]
#[serial]
fn test_logging_metrics() {
    let dir = tempdir().unwrap();

    with_env(&[("GOLDFISH_OUTPUTS_DIR", dir.path().to_str().unwrap())], || {
        log_metric("loss", 0.5, Some(100));
        log_metric("accuracy", 0.95, Some(100));

        let metrics_file = dir.path().join(".goldfish").join("metrics.jsonl");
        assert!(metrics_file.exists(), "Metrics file should exist");

        let content = std::fs::read_to_string(&metrics_file).unwrap();
        assert!(content.contains("loss"));
        assert!(content.contains("accuracy"));
        assert!(content.contains("0.5"));
        assert!(content.contains("0.95"));
    });
}

#[test]
#[serial]
fn test_should_stop_sentinel() {
    let dir = tempdir().unwrap();

    with_env(&[("GOLDFISH_OUTPUTS_DIR", dir.path().to_str().unwrap())], || {
        // Initially should not stop
        assert!(!should_stop());

        // Create stop sentinel
        let stop_file = dir.path().join(".goldfish").join("stop_requested");
        std::fs::create_dir_all(stop_file.parent().unwrap()).unwrap();
        std::fs::write(&stop_file, "SVS requested stop").unwrap();

        // Now should stop
        assert!(should_stop());
    });
}

#[test]
fn test_output_data_dtype_str() {
    let f32_data = OutputData::TensorF32(ArrayD::from_elem(vec![5], 1.0f32));
    assert_eq!(f32_data.dtype_str(), "float32");

    let f64_data = OutputData::TensorF64(ArrayD::from_elem(vec![5], 1.0f64));
    assert_eq!(f64_data.dtype_str(), "float64");

    let i64_data = OutputData::TensorI64(ArrayD::from_elem(vec![5], 1i64));
    assert_eq!(i64_data.dtype_str(), "int64");

    let i32_data = OutputData::TensorI32(ArrayD::from_elem(vec![5], 1i32));
    assert_eq!(i32_data.dtype_str(), "int32");

    let u8_data = OutputData::TensorU8(ArrayD::from_elem(vec![5], 1u8));
    assert_eq!(u8_data.dtype_str(), "uint8");

    let json_data = OutputData::Json(serde_json::json!({"key": "value"}));
    assert_eq!(json_data.dtype_str(), "json");

    let path_data = OutputData::Path("/tmp/test".into());
    assert_eq!(path_data.dtype_str(), "path");
}

#[test]
fn test_output_data_tensor_extractors() {
    let f32_data = OutputData::TensorF32(ArrayD::from_elem(vec![5], 1.0f32));
    assert!(f32_data.as_tensor_f32().is_some());
    assert!(f32_data.as_tensor_f64().is_none());
    assert!(f32_data.is_tensor());

    let json_data = OutputData::Json(serde_json::json!({"key": "value"}));
    assert!(!json_data.is_tensor());
}

#[test]
fn test_output_data_into_tensor() {
    let f32_data = OutputData::TensorF32(ArrayD::from_elem(vec![5], 1.0f32));
    let result = f32_data.into_tensor_f32();
    assert!(result.is_ok());

    let f64_data = OutputData::TensorF64(ArrayD::from_elem(vec![5], 2.0f64));
    let result = f64_data.into_tensor_f32();
    assert!(result.is_err());
    // Recover the original data
    let recovered = result.unwrap_err();
    assert_eq!(recovered.dtype_str(), "float64");
}
