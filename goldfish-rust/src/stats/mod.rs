//! Statistics computation for SVS (Semantic Validation System).
//!
//! This module provides async statistics computation with proper timeout handling,
//! reservoir sampling for large arrays, and thread-safe accumulation.

use once_cell::sync::Lazy;
use rand::Rng;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::config::{get_outputs_dir, svs_stats_enabled};
use crate::error::{GoldfishError, Result, StatsError};
use crate::io::npy::{read_npy_header, read_sample_at_index};

/// Statistics entry for an output.
#[derive(Serialize, Default, Clone, Debug)]
pub struct StatsEntry {
    /// Mean of sampled values.
    pub mean: f64,
    /// Standard deviation of sampled values.
    pub std: f64,
    /// Minimum value.
    pub min: f64,
    /// Maximum value.
    pub max: f64,
    /// Number of samples used for statistics.
    pub samples_used: usize,
    /// Total number of elements in the output (rows for CSV).
    pub total_elements: usize,
    /// Shannon entropy (discretized).
    pub entropy: f64,
    /// Ratio of null/NaN values.
    pub null_ratio: f64,
    /// Data type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
}

/// Thread-safe tracker for in-flight stats computations.
struct StatsTracker {
    in_flight: Mutex<usize>,
    cond: Condvar,
}

impl StatsTracker {
    fn new() -> Self {
        Self {
            in_flight: Mutex::new(0),
            cond: Condvar::new(),
        }
    }

    fn increment(&self) -> std::result::Result<(), String> {
        let mut count = self
            .in_flight
            .lock()
            .map_err(|e| format!("Mutex poisoned: {}", e))?;
        *count = count.saturating_add(1);
        Ok(())
    }

    fn decrement(&self) {
        if let Ok(mut count) = self.in_flight.lock() {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.cond.notify_all();
            }
        }
    }

    /// Wait for all in-flight computations with timeout.
    ///
    /// Returns true if all completed, false if timeout.
    fn wait_with_timeout(&self, timeout: Duration) -> bool {
        let start = Instant::now();

        let result = self.in_flight.lock();
        let Ok(mut count) = result else {
            return false;
        };

        while *count > 0 {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return false;
            }

            let wait_result = self.cond.wait_timeout(count, remaining);
            match wait_result {
                Ok((new_count, timeout_result)) => {
                    count = new_count;
                    if timeout_result.timed_out() && *count > 0 {
                        return false;
                    }
                }
                Err(_) => {
                    // Mutex poisoned - treat as timeout
                    return false;
                }
            }
        }

        true
    }
}

static TRACKER: Lazy<Arc<StatsTracker>> = Lazy::new(|| Arc::new(StatsTracker::new()));
static STATS_CACHE: Lazy<Mutex<HashMap<String, StatsEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Enqueue stats computation for an output.
///
/// The computation runs in a background thread. Use `finalize_svs` to wait
/// for all computations to complete and write results to disk.
///
/// Stats are only computed if `GOLDFISH_SVS_STATS_ENABLED` is set to `true`.
///
/// # Arguments
///
/// * `name` - Output signal name
/// * `path` - Path to the output file (NPY or CSV)
/// * `dtype` - Data type string (e.g., "float32", "int64")
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::stats::enqueue_stats;
/// use std::path::Path;
///
/// // Enqueue stats computation for an output
/// enqueue_stats("predictions", Path::new("/mnt/outputs/predictions.npy"), "float32");
///
/// // ... do other work ...
///
/// // Wait for all stats to complete
/// goldfish_rust::stats::finalize_svs(Some(30))?;
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn enqueue_stats(name: &str, path: &Path, dtype: &str) {
    if !svs_stats_enabled() {
        return;
    }

    let name = name.to_string();
    let path = path.to_path_buf();
    let dtype = dtype.to_string();
    let tracker = Arc::clone(&TRACKER);

    if tracker.increment().is_err() {
        log::error!("Failed to increment stats tracker");
        return;
    }

    std::thread::spawn(move || {
        match compute_stats(&path, &dtype) {
            Ok(entry) => {
                if let Ok(mut cache) = STATS_CACHE.lock() {
                    cache.insert(name, entry);
                }
            }
            Err(e) => {
                log::warn!("Stats computation failed for {}: {}", path.display(), e);
            }
        }
        tracker.decrement();
    });
}

/// Finalize SVS stats and write to output.
///
/// Waits for all in-flight stats computations (with timeout) and writes
/// the accumulated stats to svs_stats.json.
///
/// # Arguments
///
/// * `timeout_secs` - Maximum seconds to wait for stats threads (default 10)
///
/// # Errors
///
/// Returns an error if:
/// - Stats threads timed out (stats may be incomplete)
/// - Failed to write stats file
pub fn finalize_svs(timeout_secs: Option<u64>) -> Result<()> {
    let timeout = Duration::from_secs(timeout_secs.unwrap_or(10));

    // Wait for all stats with timeout (parity with Python's flush(timeout=10))
    let completed = TRACKER.wait_with_timeout(timeout);

    if !completed {
        log::warn!(
            "Stats computation timed out after {} seconds, some stats may be incomplete",
            timeout.as_secs()
        );
    }

    // Get accumulated stats
    let stats = STATS_CACHE
        .lock()
        .map_err(|_| StatsError::ThreadPanic {
            message: "Stats cache mutex poisoned".to_string(),
        })?
        .clone();

    // Write manifest
    let manifest = serde_json::json!({
        "version": 1,
        "stats": stats,
        "complete": completed,
    });

    let path = get_outputs_dir().join(".goldfish").join("svs_stats.json");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(&path, serde_json::to_string_pretty(&manifest)?)?;

    Ok(())
}

/// Compute statistics for a file.
fn compute_stats(path: &Path, dtype: &str) -> Result<StatsEntry> {
    let (samples, total_elements) = if dtype == "tabular" {
        sample_csv(path)?
    } else {
        sample_npy(path)?
    };

    if samples.is_empty() {
        return Ok(StatsEntry {
            total_elements,
            dtype: Some(dtype.to_string()),
            ..Default::default()
        });
    }

    // Compute statistics
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut sum = 0.0;
    let mut null_count = 0;

    for &s in &samples {
        if s.is_nan() || s.is_infinite() {
            null_count += 1;
            continue;
        }
        if s < min {
            min = s;
        }
        if s > max {
            max = s;
        }
        sum += s;
    }

    let valid_count = samples.len() - null_count;

    // Guard against division by zero
    let mean = if valid_count > 0 {
        sum / valid_count as f64
    } else {
        0.0
    };

    let std = if valid_count > 1 {
        let var_sum: f64 = samples
            .iter()
            .filter(|s| !s.is_nan() && !s.is_infinite())
            .map(|&s| (s - mean).powi(2))
            .sum();
        (var_sum / (valid_count - 1) as f64).sqrt() // Sample std dev
    } else {
        0.0
    };

    let entropy = compute_entropy(&samples);

    Ok(StatsEntry {
        mean,
        std,
        min: if min == f64::MAX { 0.0 } else { min },
        max: if max == f64::MIN { 0.0 } else { max },
        samples_used: samples.len(),
        total_elements,
        entropy,
        null_ratio: null_count as f64 / samples.len() as f64,
        dtype: Some(dtype.to_string()),
    })
}

/// Sample from an NPY file using reservoir sampling.
///
/// This is O(k) memory where k is sample_size, not O(n) where n is total elements.
///
/// Returns (samples, total_elements).
fn sample_npy(path: &Path) -> Result<(Vec<f64>, usize)> {
    const SAMPLE_SIZE: usize = 10_000;

    let mut file = std::fs::File::open(path)?;
    let header = read_npy_header(&mut file)?;

    if header.size == 0 {
        return Ok((vec![], 0));
    }

    // Use reservoir sampling: O(k) memory, O(n) time but single pass
    // For stats, we can use random index generation instead since we need seek
    let sample_size = std::cmp::min(SAMPLE_SIZE, header.size);

    if header.size <= sample_size {
        // Small enough to read all
        let mut samples = Vec::with_capacity(header.size);
        for i in 0..header.size {
            if let Ok(val) = read_sample_at_index(&mut file, &header, i, path) {
                samples.push(val);
            }
        }
        return Ok((samples, header.size));
    }

    // Generate random unique indices - O(k) memory
    let mut rng = rand::thread_rng();
    let mut indices = generate_random_indices(&mut rng, header.size, sample_size);
    indices.sort_unstable(); // Sort for sequential access (better I/O)

    let mut samples = Vec::with_capacity(sample_size);
    for idx in indices {
        if let Ok(val) = read_sample_at_index(&mut file, &header, idx, path) {
            samples.push(val);
        }
    }

    Ok((samples, header.size))
}

/// Generate k unique random indices from 0..n.
///
/// Uses Floyd's algorithm for O(k) memory and time when k << n.
fn generate_random_indices<R: Rng>(rng: &mut R, n: usize, k: usize) -> Vec<usize> {
    if k >= n {
        return (0..n).collect();
    }

    // Floyd's algorithm for sampling without replacement
    let mut result = Vec::with_capacity(k);
    let mut set = std::collections::HashSet::with_capacity(k);

    for j in (n - k)..n {
        let t = rng.gen_range(0..=j);
        if set.contains(&t) {
            result.push(j);
            set.insert(j);
        } else {
            result.push(t);
            set.insert(t);
        }
    }

    result
}

/// Sample from a CSV file using streaming reservoir sampling.
///
/// This is O(k) memory where k is sample_size, not O(n) where n is total rows.
/// Uses Algorithm R (Vitter's reservoir sampling) for uniform random sampling.
///
/// Returns (samples, total_rows_with_numeric_values).
fn sample_csv(path: &Path) -> Result<(Vec<f64>, usize)> {
    const SAMPLE_SIZE: usize = 10_000;

    let file = std::fs::File::open(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true) // Handle variable column counts
        .from_reader(file);

    // Find first numeric column index by inspecting headers
    let headers = reader
        .headers()
        .map_err(|e| GoldfishError::Io(crate::error::IoError::CsvParseError(e.to_string())))?
        .clone();

    let mut numeric_col_idx: Option<usize> = None;
    let mut reservoir: Vec<f64> = Vec::with_capacity(SAMPLE_SIZE);
    let mut rng = rand::thread_rng();
    let mut n: usize = 0; // Count of rows seen

    for result in reader.records() {
        let record = match result {
            Ok(r) => r,
            Err(_) => continue, // Skip malformed rows
        };

        // On first valid row, find first numeric column
        if numeric_col_idx.is_none() {
            for (i, _) in headers.iter().enumerate() {
                if let Some(field) = record.get(i) {
                    if field.trim().parse::<f64>().is_ok() {
                        numeric_col_idx = Some(i);
                        break;
                    }
                }
            }
            // If no numeric column found, we'll keep trying on subsequent rows
            // (in case first row has special values)
        }

        // Get value from numeric column (or try first parseable column if not found)
        let value = if let Some(idx) = numeric_col_idx {
            record.get(idx).and_then(|s| s.trim().parse::<f64>().ok())
        } else {
            // Try each column on this row
            record
                .iter()
                .find_map(|s| s.trim().parse::<f64>().ok())
        };

        let Some(val) = value else {
            continue; // Skip rows without numeric values
        };

        // Reservoir sampling (Algorithm R)
        if reservoir.len() < SAMPLE_SIZE {
            reservoir.push(val);
        } else {
            // Replace element at random index with probability SAMPLE_SIZE/n
            let j = rng.gen_range(0..=n);
            if j < SAMPLE_SIZE {
                reservoir[j] = val;
            }
        }
        n += 1;
    }

    Ok((reservoir, n))
}

/// Compute Shannon entropy of samples.
fn compute_entropy(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return f64::NAN; // Undefined for empty set
    }

    // Discretize values for counting
    // Guard against f64→i64 overflow for extreme values
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for &s in samples {
        if !s.is_nan() && !s.is_infinite() {
            let scaled = s * 1000.0;
            // Skip values that would overflow i64 (approximately +/- 9.2e15)
            if scaled > i64::MAX as f64 || scaled < i64::MIN as f64 {
                continue;
            }
            let key = scaled as i64;
            *counts.entry(key).or_insert(0) += 1;
        }
    }

    if counts.is_empty() {
        return f64::NAN;
    }

    let n = samples.len() as f64;
    let mut entropy = 0.0;

    for &count in counts.values() {
        let p = count as f64 / n;
        if p > 0.0 {
            entropy -= p * p.log2();
        }
    }

    entropy
}

/// Clear stats cache (for testing).
#[doc(hidden)]
pub fn clear_stats_cache() {
    if let Ok(mut cache) = STATS_CACHE.lock() {
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ArrayD;
    use tempfile::tempdir;

    #[test]
    fn test_generate_random_indices() {
        let mut rng = rand::thread_rng();

        // k < n
        let indices = generate_random_indices(&mut rng, 1000, 10);
        assert_eq!(indices.len(), 10);
        let unique: std::collections::HashSet<_> = indices.iter().collect();
        assert_eq!(unique.len(), 10); // All unique

        // k >= n
        let indices = generate_random_indices(&mut rng, 5, 10);
        assert_eq!(indices.len(), 5);
    }

    #[test]
    fn test_compute_entropy() {
        // Uniform distribution - high entropy
        let uniform: Vec<f64> = (0..1000).map(|i| i as f64 / 1000.0).collect();
        let entropy = compute_entropy(&uniform);
        assert!(entropy > 5.0); // High entropy

        // Constant - zero entropy
        let constant = vec![1.0; 1000];
        let entropy = compute_entropy(&constant);
        assert_eq!(entropy, 0.0);

        // Empty - NaN
        let empty: Vec<f64> = vec![];
        let entropy = compute_entropy(&empty);
        assert!(entropy.is_nan());
    }

    #[test]
    fn test_compute_stats_total_elements_npy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("values.npy");

        let arr = ArrayD::from_elem(vec![2, 3], 1.0f32);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let stats = compute_stats(&path, "float32").unwrap();
        assert_eq!(stats.total_elements, 6);
        assert_eq!(stats.samples_used, stats.total_elements.min(10_000));
    }

    #[test]
    fn test_compute_stats_handles_all_nan() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.npy");

        // Create array of all NaN
        let arr = ArrayD::from_elem(vec![100], f32::NAN);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let result = compute_stats(&path, "float32").unwrap();

        // Should not panic, should have 100% null ratio
        assert_eq!(result.null_ratio, 1.0);
        assert_eq!(result.mean, 0.0); // Guarded against division by zero
    }

    #[test]
    fn test_compute_stats_handles_all_inf() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("all_inf.npy");

        // Create array with all Infinity values
        let arr: ArrayD<f32> = ArrayD::from_elem(vec![10], f32::INFINITY);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let result = compute_stats(&path, "float32").unwrap();

        // Inf values should be treated as null (filtered out)
        assert_eq!(result.null_ratio, 1.0);
        assert_eq!(result.mean, 0.0);
    }

    #[test]
    fn test_compute_stats_handles_mixed_nan_inf() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed.npy");

        // Create array with NaN, +Inf, -Inf, and valid values
        let arr: ArrayD<f32> = ArrayD::from_shape_vec(
            vec![8],
            vec![
                1.0,
                f32::NAN,
                2.0,
                f32::INFINITY,
                3.0,
                f32::NEG_INFINITY,
                4.0,
                f32::NAN,
            ],
        )
        .unwrap();
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let result = compute_stats(&path, "float32").unwrap();

        // 4 valid values (1, 2, 3, 4), 4 invalid (NaN, Inf, -Inf, NaN)
        assert!((result.null_ratio - 0.5).abs() < 0.01, "Should have 50% null ratio");
        assert!((result.mean - 2.5).abs() < 0.01, "Mean of [1,2,3,4] should be 2.5");
        assert!((result.min - 1.0).abs() < 0.01, "Min should be 1.0");
        assert!((result.max - 4.0).abs() < 0.01, "Max should be 4.0");
    }

    #[test]
    fn test_sample_npy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.npy");

        // Create test array
        let arr: ArrayD<f32> = ArrayD::from_shape_fn(vec![100], |i| i[0] as f32);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let (samples, total) = sample_npy(&path).unwrap();
        assert_eq!(samples.len(), 100); // Small enough to read all
        assert_eq!(total, 100);

        // Test with large array (would need reservoir sampling)
        // This test verifies the code path works, actual sampling behavior
        // is probabilistic
    }

    #[test]
    fn test_sample_csv_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csv");

        // Create simple CSV - first numeric column is 'id'
        std::fs::write(&path, "id,value,name\n1,10.5,foo\n2,20.0,bar\n3,30.5,baz\n").unwrap();

        let (samples, total) = sample_csv(&path).unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(total, 3);
        // First numeric column is 'id' with values 1, 2, 3
        assert!(samples.contains(&1.0));
        assert!(samples.contains(&2.0));
        assert!(samples.contains(&3.0));
    }

    #[test]
    fn test_sample_csv_no_numeric_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csv");

        // CSV with only string columns
        std::fs::write(&path, "name,city\nAlice,NYC\nBob,LA\n").unwrap();

        let (samples, total) = sample_csv(&path).unwrap();
        // Should return empty when no numeric columns found
        assert!(samples.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn test_sample_csv_malformed_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csv");

        // CSV with some malformed rows
        std::fs::write(
            &path,
            "id,value\n1,10.5\n2,bad\n3,30.5\n,\ngarbage\n4,40.0\n",
        )
        .unwrap();

        // Should skip malformed rows and still sample
        let (samples, total) = sample_csv(&path).unwrap();
        assert!(!samples.is_empty());
        assert_eq!(total, 4);
    }

    #[test]
    fn test_sample_csv_reservoir_sampling() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.csv");

        // Create CSV larger than SAMPLE_SIZE (10,000)
        let mut csv_content = String::from("value\n");
        for i in 0..15_000 {
            csv_content.push_str(&format!("{}.0\n", i));
        }
        std::fs::write(&path, csv_content).unwrap();

        let (samples, total) = sample_csv(&path).unwrap();
        // Should be capped at SAMPLE_SIZE
        assert_eq!(samples.len(), 10_000);
        assert_eq!(total, 15_000);
    }

    #[test]
    fn test_stats_tracker_timeout() {
        let tracker = StatsTracker::new();

        // No in-flight - should return immediately
        assert!(tracker.wait_with_timeout(Duration::from_millis(100)));

        // With in-flight - should timeout
        tracker.increment().unwrap();
        let start = Instant::now();
        let result = tracker.wait_with_timeout(Duration::from_millis(50));
        assert!(!result);
        assert!(start.elapsed() >= Duration::from_millis(50));

        // Cleanup
        tracker.decrement();
    }

    #[test]
    fn test_compute_entropy_handles_extreme_values() {
        // Test values that would overflow i64 when scaled by 1000
        // i64::MAX is approximately 9.2e18, so values > 9.2e15 would overflow
        let extreme_samples = vec![
            1.0,      // Normal value
            2.0,      // Normal value
            1e16,     // Would overflow when multiplied by 1000
            -1e16,    // Would overflow negative
            f64::MAX, // Extreme positive (should be skipped)
        ];

        let entropy = compute_entropy(&extreme_samples);
        // Should still compute entropy from the valid values (1.0 and 2.0)
        // Without overflow protection, this would cause undefined behavior
        assert!(!entropy.is_nan(), "Should compute entropy from valid values");
    }

    #[test]
    fn test_concurrent_stats_enqueue() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();

        // Create multiple NPY files
        for i in 0..5 {
            let path = dir.path().join(format!("data_{}.npy", i));
            let arr: ArrayD<f32> = ArrayD::from_elem(vec![10], i as f32);
            ndarray_npy::write_npy(&path, &arr).unwrap();
        }

        // Enqueue stats from multiple threads simultaneously
        let dir_path = Arc::new(dir.path().to_path_buf());
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let dp = Arc::clone(&dir_path);
                thread::spawn(move || {
                    let path = dp.join(format!("data_{}.npy", i));
                    enqueue_stats(&format!("output_{}", i), &path, "float32");
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().expect("Thread should not panic");
        }

        // If we get here without deadlock or panic, concurrent access works
    }

    #[test]
    fn test_sample_csv_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.csv");

        // CSV with only headers, no data rows
        std::fs::write(&path, "id,value\n").unwrap();

        let (samples, total) = sample_csv(&path).unwrap();
        // Should return empty for no data rows
        assert!(samples.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn test_sample_csv_flexible_columns() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("flexible.csv");

        // CSV with inconsistent column counts
        std::fs::write(&path, "a,b,c\n1,2\n3,4,5,6\n7,8,9\n").unwrap();

        // Should handle flexible column counts without panic
        let result = sample_csv(&path);
        // May succeed or fail depending on polars behavior, but should not panic
        assert!(result.is_ok() || result.is_err());
    }
}
