//! NPZ file format handling.
//!
//! This module provides functions to read NumPy .npz files (ZIP archives of .npy files).
//! Uses in-memory Cursor for efficiency (no temp files).
//!
//! # Security
//!
//! This module implements several security measures:
//! - **Zip-slip protection**: Rejects entries with path traversal attempts
//! - **Decompression bomb protection**: Limits entry size to prevent OOM
//! - **Fail-fast on malicious content**: Returns error instead of silently skipping

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::Path;

use crate::error::{GoldfishError, IoError, Result};
use crate::io::npy::load_npy_from_reader;
use crate::io::OutputData;

/// Maximum decompressed size per NPZ entry (1 GB).
/// Protects against zip bomb attacks.
const MAX_ENTRY_SIZE: u64 = 1_000_000_000;

/// A loaded NPZ file containing multiple arrays.
#[derive(Debug)]
pub struct NpzFile {
    arrays: HashMap<String, OutputData>,
}

impl NpzFile {
    /// Get an array by name (returns reference to avoid cloning).
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&OutputData> {
        self.arrays.get(name)
    }

    /// Get an array by name, taking ownership (consumes the array).
    #[must_use]
    pub fn take(&mut self, name: &str) -> Option<OutputData> {
        self.arrays.remove(name)
    }

    /// Get all array names.
    #[must_use]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.arrays.keys()
    }

    /// Check if array exists.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.arrays.contains_key(name)
    }

    /// Get number of arrays.
    #[must_use]
    pub fn len(&self) -> usize {
        self.arrays.len()
    }

    /// Check if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    /// Consume the NpzFile and return all arrays.
    #[must_use]
    pub fn into_arrays(self) -> HashMap<String, OutputData> {
        self.arrays
    }
}

/// Load an NPZ file.
///
/// NPZ files are ZIP archives containing .npy files. Each array in the archive
/// is loaded and can be accessed by name.
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be opened
/// - The ZIP archive is invalid
/// - Any contained .npy file cannot be parsed
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::load_npz;
///
/// let npz = load_npz("data.npz")?;
/// if let Some(weights) = npz.get("weights") {
///     // Use weights...
/// }
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn load_npz<P: AsRef<Path>>(path: P) -> Result<NpzFile> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| {
        GoldfishError::Io(IoError::NpzError {
            path: path.to_path_buf(),
            message: format!("Failed to open ZIP archive: {}", e),
        })
    })?;

    let mut arrays = HashMap::new();

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).map_err(|e| {
            GoldfishError::Io(IoError::NpzError {
                path: path.to_path_buf(),
                message: format!("Failed to read ZIP entry {}: {}", i, e),
            })
        })?;

        let name = entry.name().to_string();

        // Only process .npy files
        if !name.ends_with(".npy") {
            continue;
        }

        // SECURITY: Validate zip entry path to prevent zip-slip attacks
        // Fail-fast on malicious content - don't silently skip
        if name.contains("..") || name.starts_with('/') || name.contains('\\') || name.contains(':')
        {
            return Err(GoldfishError::Io(IoError::NpzError {
                path: path.to_path_buf(),
                message: format!(
                    "Security violation: NPZ entry '{}' contains path traversal attempt",
                    name
                ),
            }));
        }

        // SECURITY: Check decompressed size to prevent zip bombs
        let uncompressed_size = entry.size();
        if uncompressed_size > MAX_ENTRY_SIZE {
            return Err(GoldfishError::Io(IoError::NpzError {
                path: path.to_path_buf(),
                message: format!(
                    "Security violation: NPZ entry '{}' exceeds max size ({} > {} bytes)",
                    name, uncompressed_size, MAX_ENTRY_SIZE
                ),
            }));
        }

        // Strip .npy extension for the array name
        let array_name = name.trim_end_matches(".npy").to_string();

        // Read entry into memory with pre-allocated buffer
        let mut buffer = Vec::with_capacity(uncompressed_size as usize);
        entry.read_to_end(&mut buffer).map_err(|e| {
            GoldfishError::Io(IoError::NpzError {
                path: path.to_path_buf(),
                message: format!("Failed to read entry '{}': {}", name, e),
            })
        })?;

        let cursor = Cursor::new(buffer);
        let context = format!("{}:{}", path.display(), array_name);

        // Load the NPY array from memory - propagate errors
        let data = load_npy_from_reader(cursor, &context)?;
        arrays.insert(array_name, data);
    }

    Ok(NpzFile { arrays })
}

/// Load a specific array from an NPZ file.
///
/// More efficient than loading the entire NPZ if you only need one array.
/// Uses in-memory Cursor for efficient reading without temp files.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::load_npz_array;
///
/// let weights = load_npz_array("model.npz", "weights")?;
/// // Use weights...
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn load_npz_array<P: AsRef<Path>>(path: P, array_name: &str) -> Result<OutputData> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| {
        GoldfishError::Io(IoError::NpzError {
            path: path.to_path_buf(),
            message: format!("Failed to open ZIP archive: {}", e),
        })
    })?;

    let entry_name = format!("{}.npy", array_name);

    let mut entry = archive.by_name(&entry_name).map_err(|e| {
        GoldfishError::Io(IoError::NpzError {
            path: path.to_path_buf(),
            message: format!("Array '{}' not found in NPZ: {}", array_name, e),
        })
    })?;

    // SECURITY: Check decompressed size to prevent zip bombs
    let uncompressed_size = entry.size();
    if uncompressed_size > MAX_ENTRY_SIZE {
        return Err(GoldfishError::Io(IoError::NpzError {
            path: path.to_path_buf(),
            message: format!(
                "Security violation: array '{}' exceeds max size ({} > {} bytes)",
                array_name, uncompressed_size, MAX_ENTRY_SIZE
            ),
        }));
    }

    // Read into memory with pre-allocated buffer
    let mut buffer = Vec::with_capacity(uncompressed_size as usize);
    entry.read_to_end(&mut buffer)?;

    let cursor = Cursor::new(buffer);
    let context = format!("{}:{}", path.display(), array_name);

    load_npy_from_reader(cursor, &context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ArrayD;
    use std::io::Write;
    use tempfile::tempdir;

    fn create_test_npz(path: &Path, arrays: &[(&str, ArrayD<f32>)]) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        for (name, arr) in arrays {
            // Write array to temp npy file
            let temp_dir = tempdir().unwrap();
            let npy_path = temp_dir.path().join(format!("{}.npy", name));
            ndarray_npy::write_npy(&npy_path, arr).unwrap();

            // Add to zip
            let options = zip::write::FileOptions::default();
            zip.start_file(format!("{}.npy", name), options).unwrap();
            let npy_data = std::fs::read(&npy_path).unwrap();
            zip.write_all(&npy_data).unwrap();
        }

        zip.finish().unwrap();
    }

    #[test]
    fn test_load_npz() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("test.npz");

        // Create test NPZ
        let arr1 = ArrayD::from_elem(vec![10], 1.0f32);
        let arr2 = ArrayD::from_elem(vec![5, 5], 2.0f32);
        create_test_npz(&npz_path, &[("weights", arr1), ("bias", arr2)]);

        // Load and verify
        let npz = load_npz(&npz_path).unwrap();
        assert_eq!(npz.len(), 2);
        assert!(npz.contains("weights"));
        assert!(npz.contains("bias"));

        if let Some(OutputData::TensorF32(weights)) = npz.get("weights") {
            assert_eq!(weights.shape(), &[10]);
        } else {
            panic!("weights not found or wrong type");
        }

        if let Some(OutputData::TensorF32(bias)) = npz.get("bias") {
            assert_eq!(bias.shape(), &[5, 5]);
        } else {
            panic!("bias not found or wrong type");
        }

        // Test take() for ownership transfer
        let mut npz2 = load_npz(&npz_path).unwrap();
        if let Some(OutputData::TensorF32(taken)) = npz2.take("weights") {
            assert_eq!(taken.shape(), &[10]);
        }
        assert!(!npz2.contains("weights")); // Should be removed after take
    }

    #[test]
    fn test_load_npz_array() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("test.npz");

        let arr = ArrayD::from_elem(vec![10], 1.0f32);
        create_test_npz(&npz_path, &[("data", arr)]);

        let data = load_npz_array(&npz_path, "data").unwrap();
        if let OutputData::TensorF32(arr) = data {
            assert_eq!(arr.shape(), &[10]);
        } else {
            panic!("Wrong type");
        }
    }

    #[test]
    fn test_load_npz_rejects_path_traversal() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("malicious.npz");

        // Create NPZ with path traversal attempt
        let file = std::fs::File::create(&npz_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        // Add malicious entry
        let options = zip::write::FileOptions::default();
        zip.start_file("../../../etc/passwd.npy", options).unwrap();
        // Write a minimal valid NPY header
        let npy_header = b"\x93NUMPY\x01\x00\x16\x00{'descr': '<f4', 'fortran_order': False, 'shape': (1,), }";
        zip.write_all(npy_header).unwrap();
        zip.write_all(&[0u8; 4]).unwrap(); // One f32 element

        zip.finish().unwrap();

        let result = load_npz(&npz_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Security violation"));
    }

    #[test]
    fn test_load_npz_rejects_backslash_paths() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("windows_malicious.npz");

        let file = std::fs::File::create(&npz_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        let options = zip::write::FileOptions::default();
        zip.start_file("..\\..\\etc\\passwd.npy", options).unwrap();
        let npy_header = b"\x93NUMPY\x01\x00\x16\x00{'descr': '<f4', 'fortran_order': False, 'shape': (1,), }";
        zip.write_all(npy_header).unwrap();
        zip.write_all(&[0u8; 4]).unwrap();

        zip.finish().unwrap();

        let result = load_npz(&npz_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Security violation"));
    }

    #[test]
    fn test_load_npz_rejects_colon_paths() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("colon_malicious.npz");

        let file = std::fs::File::create(&npz_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        let options = zip::write::FileOptions::default();
        zip.start_file("C:Windows\\System32.npy", options).unwrap();
        let npy_header = b"\x93NUMPY\x01\x00\x16\x00{'descr': '<f4', 'fortran_order': False, 'shape': (1,), }";
        zip.write_all(npy_header).unwrap();
        zip.write_all(&[0u8; 4]).unwrap();

        zip.finish().unwrap();

        let result = load_npz(&npz_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Security violation"));
    }

    #[test]
    fn test_decompression_bomb_constant() {
        // Verify the MAX_ENTRY_SIZE constant is set to 1GB
        // This protects against decompression bomb attacks where a small compressed
        // file expands to a huge size
        assert_eq!(MAX_ENTRY_SIZE, 1_000_000_000, "MAX_ENTRY_SIZE should be 1GB");
    }

    #[test]
    fn test_load_npz_within_size_limits() {
        // Create a valid NPZ and verify it loads successfully
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("valid.npz");

        // Use the proper NPZ creation helper
        let arr = ArrayD::from_elem(vec![100], 1.0f32);
        create_test_npz(&npz_path, &[("data", arr)]);

        // This should succeed (well within 1GB limit)
        let result = load_npz(&npz_path);
        assert!(result.is_ok(), "Valid NPZ should load successfully: {:?}", result.err());
    }

    #[test]
    fn test_load_npz_rejects_absolute_path() {
        let dir = tempdir().unwrap();
        let npz_path = dir.path().join("absolute_path.npz");

        let file = std::fs::File::create(&npz_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        let options = zip::write::FileOptions::default();
        zip.start_file("/etc/passwd.npy", options).unwrap();
        let npy_header = b"\x93NUMPY\x01\x00\x16\x00{'descr': '<f4', 'fortran_order': False, 'shape': (1,), }";
        zip.write_all(npy_header).unwrap();
        zip.write_all(&[0u8; 4]).unwrap();

        zip.finish().unwrap();

        let result = load_npz(&npz_path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Security violation"), "Should reject absolute paths: {}", err_msg);
    }
}
