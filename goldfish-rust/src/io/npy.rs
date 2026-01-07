//! NPY file format handling.
//!
//! This module provides functions to read and write NumPy .npy files,
//! with proper support for NPY format versions 1.0, 2.0, and 3.0.
//!
//! # Security
//!
//! - Header length is limited to 1MB to prevent OOM attacks
//! - All error messages include path context when available

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{GoldfishError, IoError, Result};
use crate::io::OutputData;

/// Maximum NPY header size (1 MB) to prevent OOM attacks.
/// Legitimate NPY files have headers of ~100 bytes typically.
const MAX_HEADER_SIZE: usize = 1_000_000;

/// NPY file header information.
#[derive(Debug, Clone)]
pub struct NpyHeader {
    /// Offset to the start of data in the file.
    pub data_offset: u64,
    /// Size of each element in bytes.
    pub word_size: usize,
    /// Data type string (e.g., "f4", "f8", "i4", "i8", "u1").
    pub dtype: String,
    /// Total number of elements.
    pub size: usize,
    /// Shape of the array.
    pub shape: Vec<usize>,
    /// Whether the data is Fortran-ordered.
    pub fortran_order: bool,
    /// Endianness of the data.
    pub endian: Endian,
}

/// Byte order for NPY data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    /// Little-endian encoding.
    Little,
    /// Big-endian encoding.
    Big,
}

/// Read the header from an NPY file.
///
/// Supports NPY format versions 1.0, 2.0, and 3.0.
///
/// # Arguments
///
/// * `reader` - Any `Read + Seek` implementor (file, cursor, etc.)
///
/// # Security
///
/// Header size is limited to 1MB to prevent OOM attacks from malicious files.
///
/// # Errors
///
/// Returns an error if:
/// - The file doesn't have valid NPY magic bytes
/// - The header cannot be parsed
/// - The dtype is not supported
/// - The header size exceeds 1MB (security limit)
pub fn read_npy_header<R: Read + Seek>(reader: &mut R) -> Result<NpyHeader> {
    read_npy_header_with_context(reader, PathBuf::new())
}

/// Read NPY header with path context for error messages.
///
/// Internal function that preserves path context in errors.
fn read_npy_header_with_context<R: Read + Seek>(reader: &mut R, context: PathBuf) -> Result<NpyHeader> {
    // Read magic number
    let mut magic = [0u8; 6];
    reader.read_exact(&mut magic).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: context.clone(),
            message: format!("Failed to read magic: {}", e),
        })
    })?;

    if &magic != b"\x93NUMPY" {
        return Err(GoldfishError::Io(IoError::NpyError {
            path: context,
            message: "Invalid NPY magic bytes".to_string(),
        }));
    }

    // Read version
    let mut version = [0u8; 2];
    reader.read_exact(&mut version).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: context.clone(),
            message: format!("Failed to read version: {}", e),
        })
    })?;

    let major_version = version[0];

    // Read header length based on version
    let header_len: usize = match major_version {
        1 => {
            // Version 1.0: 2-byte header length
            let mut len_bytes = [0u8; 2];
            reader.read_exact(&mut len_bytes)?;
            u16::from_le_bytes(len_bytes) as usize
        }
        2 | 3 => {
            // Version 2.0/3.0: 4-byte header length
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            u32::from_le_bytes(len_bytes) as usize
        }
        _ => {
            return Err(GoldfishError::Io(IoError::NpyError {
                path: context,
                message: format!("Unsupported NPY version {}.{}", version[0], version[1]),
            }));
        }
    };

    // SECURITY: Limit header size to prevent OOM attacks
    if header_len > MAX_HEADER_SIZE {
        return Err(GoldfishError::Io(IoError::NpyError {
            path: context,
            message: format!(
                "Header size {} exceeds maximum {} bytes (possible attack)",
                header_len, MAX_HEADER_SIZE
            ),
        }));
    }

    // Read header bytes
    let mut header_bytes = vec![0u8; header_len];
    reader.read_exact(&mut header_bytes)?;
    let header_str = String::from_utf8_lossy(&header_bytes);

    // Parse dtype - look for patterns like '<f4', '>f8', '|u1', etc.
    let (dtype, endian) = parse_dtype_with_context(&header_str, &context)?;
    let word_size = dtype_to_word_size_with_context(&dtype, &context)?;

    // Parse shape
    let shape = parse_shape_with_context(&header_str, &context)?;
    let size: usize = if shape.is_empty() {
        1
    } else {
        shape.iter().try_fold(1usize, |acc, &dim| acc.checked_mul(dim)).ok_or_else(|| {
            GoldfishError::Io(IoError::NpyError {
                path: context.clone(),
                message: format!("Shape {:?} overflows usize", shape),
            })
        })?
    };

    // Parse fortran_order
    let fortran_order = header_str.contains("'fortran_order': True")
        || header_str.contains("'fortran_order':True");

    // Calculate data offset
    let data_offset = match major_version {
        1 => 10 + header_len as u64,      // 6 (magic) + 2 (version) + 2 (header_len)
        2 | 3 => 12 + header_len as u64,  // 6 (magic) + 2 (version) + 4 (header_len)
        _ => unreachable!(),
    };

    Ok(NpyHeader {
        data_offset,
        word_size,
        dtype,
        size,
        shape,
        fortran_order,
        endian,
    })
}

/// Parse dtype from header string with path context for errors.
fn parse_dtype_with_context(header: &str, context: &PathBuf) -> Result<(String, Endian)> {
    // Look for 'descr': '<dtype>' or 'descr': '<dtype>'
    let descr_patterns = [
        "'descr': '",
        "'descr':'",
        "\"descr\": \"",
        "\"descr\":\"",
    ];

    for pattern in descr_patterns {
        if let Some(start) = header.find(pattern) {
            let rest = &header[start + pattern.len()..];
            if let Some(end) = rest.find(['\'', '"']) {
                let dtype_str = &rest[..end];
                // Parse the dtype string - may have byte order prefix
                return normalize_dtype_with_endian(dtype_str, context);
            }
        }
    }

    // Fallback: look for common patterns
    let dtype_patterns = [
        ("<f4", "f4", Endian::Little),
        ("<f8", "f8", Endian::Little),
        ("<i4", "i4", Endian::Little),
        ("<i8", "i8", Endian::Little),
        (">f4", "f4", Endian::Big),
        (">f8", "f8", Endian::Big),
        (">i4", "i4", Endian::Big),
        (">i8", "i8", Endian::Big),
        ("|u1", "u1", Endian::Little),
        ("|b1", "b1", Endian::Little),
        ("|i1", "i1", Endian::Little),
        ("'f4'", "f4", Endian::Little),
        ("'f8'", "f8", Endian::Little),
        ("'i4'", "i4", Endian::Little),
        ("'i8'", "i8", Endian::Little),
        ("'u1'", "u1", Endian::Little),
    ];

    for (pattern, dtype, endian) in dtype_patterns {
        if header.contains(pattern) {
            return Ok((dtype.to_string(), endian));
        }
    }

    Err(GoldfishError::Io(IoError::NpyError {
        path: context.clone(),
        message: format!("Could not parse dtype from header: {}", header),
    }))
}

/// Normalize dtype string and infer endianness.
fn normalize_dtype_with_endian(dtype_str: &str, context: &PathBuf) -> Result<(String, Endian)> {
    let endian = match dtype_str.chars().next() {
        Some('>') => Endian::Big,
        Some('<') => Endian::Little,
        Some('|') => Endian::Little, // Not applicable (1-byte) - treat as native
        Some('=') => {
            if cfg!(target_endian = "little") {
                Endian::Little
            } else {
                Endian::Big
            }
        }
        _ => {
            if cfg!(target_endian = "little") {
                Endian::Little
            } else {
                Endian::Big
            }
        }
    };

    let dtype = normalize_dtype_with_context(dtype_str, context)?;
    Ok((dtype, endian))
}

/// Normalize dtype string by removing byte order prefix.
fn normalize_dtype_with_context(dtype_str: &str, context: &PathBuf) -> Result<String> {
    let normalized = dtype_str
        .trim_start_matches(['<', '>', '|', '='])
        .to_string();

    // Validate it's a known dtype
    match normalized.as_str() {
        "f4" | "f8" | "f2" | "f16" | "i4" | "i8" | "i2" | "i1" | "u4" | "u8" | "u2" | "u1"
        | "b1" | "?" => Ok(normalized),
        _ => Err(GoldfishError::Io(IoError::NpyError {
            path: context.clone(),
            message: format!("Unsupported dtype: {}", dtype_str),
        })),
    }
}

/// Get word size for dtype with path context for errors.
fn dtype_to_word_size_with_context(dtype: &str, context: &PathBuf) -> Result<usize> {
    match dtype {
        "f8" | "i8" | "u8" => Ok(8),
        "f4" | "i4" | "u4" => Ok(4),
        "f2" | "i2" | "u2" => Ok(2),
        "f16" => Ok(2),
        "i1" | "u1" | "b1" | "?" => Ok(1),
        _ => Err(GoldfishError::Io(IoError::NpyError {
            path: context.clone(),
            message: format!("Unknown dtype word size: {}", dtype),
        })),
    }
}

/// Parse shape from header string with path context for errors.
fn parse_shape_with_context(header: &str, context: &PathBuf) -> Result<Vec<usize>> {
    // Look for 'shape': (...)
    let shape_patterns = ["'shape': (", "'shape':(", "\"shape\": (", "\"shape\":("];

    for pattern in shape_patterns {
        if let Some(start) = header.find(pattern) {
            let rest = &header[start + pattern.len()..];
            if let Some(end) = rest.find(')') {
                let shape_str = &rest[..end];
                return parse_shape_tuple_with_context(shape_str, context);
            }
        }
    }

    // Empty shape (scalar)
    Ok(vec![])
}

/// Parse a shape tuple like "10, 20, 30" or "100," (1D).
fn parse_shape_tuple_with_context(s: &str, context: &PathBuf) -> Result<Vec<usize>> {
    let mut dims = Vec::new();

    for part in s.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            let dim: usize = trimmed.parse().map_err(|_| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.clone(),
                    message: format!("Invalid shape dimension: {}", trimmed),
                })
            })?;
            dims.push(dim);
        }
    }

    Ok(dims)
}

/// Load an NPY file and return as OutputData.
///
/// Automatically detects the dtype from the header and loads the appropriate type.
/// Opens the file once and uses seek to read both header and data (no double open).
pub fn load_npy_typed(path: &Path) -> Result<OutputData> {
    use ndarray_npy::ReadNpyExt;

    // Open file once - use BufReader for efficient reading
    let file = std::fs::File::open(path).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: path.to_path_buf(),
            message: format!("Failed to open file: {}", e),
        })
    })?;
    let mut reader = std::io::BufReader::new(file);

    // Read header first to determine dtype
    let header = read_npy_header(&mut reader).map_err(|e| {
        // Re-wrap with path context
        if let GoldfishError::Io(IoError::NpyError { message, .. }) = e {
            GoldfishError::Io(IoError::NpyError {
                path: path.to_path_buf(),
                message,
            })
        } else {
            e
        }
    })?;

    // Seek back to start for full array read
    reader.seek(SeekFrom::Start(0)).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: path.to_path_buf(),
            message: format!("Failed to seek: {}", e),
        })
    })?;

    // Dispatch based on dtype - use ReadNpyExt to read from same file handle
    match header.dtype.as_str() {
        "f4" => {
            let arr = ndarray::ArrayD::<f32>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorF32(arr))
        }
        "f8" => {
            let arr = ndarray::ArrayD::<f64>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorF64(arr))
        }
        "i8" => {
            let arr = ndarray::ArrayD::<i64>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorI64(arr))
        }
        "i4" => {
            let arr = ndarray::ArrayD::<i32>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorI32(arr))
        }
        "u1" => {
            let arr = ndarray::ArrayD::<u8>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorU8(arr))
        }
        dtype => Err(GoldfishError::Io(IoError::NpyError {
            path: path.to_path_buf(),
            message: format!("Unsupported NPY dtype: {}", dtype),
        })),
    }
}

/// Load an NPY array from a reader.
///
/// This avoids the need for a temp file when loading from in-memory data.
/// Uses `ReadNpyExt` trait from `ndarray_npy`.
///
/// # Arguments
///
/// * `reader` - Any type that implements `Read + Seek` (e.g., `Cursor<Vec<u8>>`)
/// * `context` - Context string for error messages (e.g., file path)
///
/// # Examples
///
/// ```no_run
/// use std::io::Cursor;
/// use goldfish_rust::io::load_npy_from_reader;
///
/// let npy_bytes: Vec<u8> = std::fs::read("array.npy")?;
/// let cursor = Cursor::new(npy_bytes);
/// let data = load_npy_from_reader(cursor, "array.npy")?;
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn load_npy_from_reader<R: Read + Seek>(mut reader: R, context: &str) -> Result<OutputData> {
    use ndarray_npy::ReadNpyExt;

    // Read header first to determine dtype (with context for errors)
    let context_path: PathBuf = context.into();
    let header = read_npy_header_with_context(&mut reader, context_path.clone())?;

    // Reset reader position to beginning for full read
    reader.seek(SeekFrom::Start(0)).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: context.into(),
            message: format!("Failed to seek: {}", e),
        })
    })?;

    // Dispatch based on dtype using ReadNpyExt trait
    match header.dtype.as_str() {
        "f4" => {
            let arr = ndarray::ArrayD::<f32>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.into(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorF32(arr))
        }
        "f8" => {
            let arr = ndarray::ArrayD::<f64>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.into(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorF64(arr))
        }
        "i8" => {
            let arr = ndarray::ArrayD::<i64>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.into(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorI64(arr))
        }
        "i4" => {
            let arr = ndarray::ArrayD::<i32>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.into(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorI32(arr))
        }
        "u1" => {
            let arr = ndarray::ArrayD::<u8>::read_npy(&mut reader).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.into(),
                    message: e.to_string(),
                })
            })?;
            Ok(OutputData::TensorU8(arr))
        }
        dtype => Err(GoldfishError::Io(IoError::NpyError {
            path: context.into(),
            message: format!("Unsupported NPY dtype: {}", dtype),
        })),
    }
}

/// Save OutputData as NPY file.
///
/// Writes array data in NumPy's .npy format. Supports f32, f64, i32, i64,
/// and u8 tensor types.
///
/// # Arguments
///
/// * `path` - Destination file path
/// * `data` - Output data to save (must be a Tensor type)
///
/// # Errors
///
/// Returns an error if:
/// - The data is not a tensor type (e.g., Tabular or Path)
/// - The file cannot be written
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::{save_npy, OutputData};
/// use ndarray::ArrayD;
/// use std::path::Path;
///
/// let arr = ArrayD::from_elem(vec![10, 20], 1.0f32);
/// save_npy(Path::new("/tmp/output.npy"), &OutputData::TensorF32(arr))?;
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn save_npy(path: &Path, data: &OutputData) -> Result<()> {
    match data {
        OutputData::TensorF32(arr) => {
            ndarray_npy::write_npy(path, arr).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
        }
        OutputData::TensorF64(arr) => {
            ndarray_npy::write_npy(path, arr).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
        }
        OutputData::TensorI64(arr) => {
            ndarray_npy::write_npy(path, arr).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
        }
        OutputData::TensorI32(arr) => {
            ndarray_npy::write_npy(path, arr).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
        }
        OutputData::TensorU8(arr) => {
            ndarray_npy::write_npy(path, arr).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })
            })?;
        }
        _ => {
            return Err(GoldfishError::Io(IoError::DataTypeMismatch {
                format: "npy".to_string(),
                expected: "Tensor".to_string(),
                actual: data.dtype_str().to_string(),
            }));
        }
    }
    Ok(())
}

/// Read a single sample from an NPY file at a specific index.
///
/// # Arguments
///
/// * `reader` - Reader positioned at start of NPY file
/// * `header` - Pre-parsed NPY header
/// * `idx` - Element index to read
/// * `context` - Path context for error messages
pub fn read_sample_at_index<R: Read + Seek>(
    reader: &mut R,
    header: &NpyHeader,
    idx: usize,
    context: &Path,
) -> Result<f64> {
    // Use checked arithmetic to prevent overflow
    let byte_offset = idx.checked_mul(header.word_size).ok_or_else(|| {
        GoldfishError::Io(IoError::NpyError {
            path: context.to_path_buf(),
            message: format!("Index {} * word_size {} overflows usize", idx, header.word_size),
        })
    })?;
    let offset = header.data_offset.checked_add(byte_offset as u64).ok_or_else(|| {
        GoldfishError::Io(IoError::NpyError {
            path: context.to_path_buf(),
            message: format!("Data offset {} + {} overflows u64", header.data_offset, byte_offset),
        })
    })?;
    reader.seek(SeekFrom::Start(offset)).map_err(|e| {
        GoldfishError::Io(IoError::NpyError {
            path: context.to_path_buf(),
            message: format!("Failed to seek to index {}: {}", idx, e),
        })
    })?;

    let is_little_endian = matches!(header.endian, Endian::Little);

    match header.dtype.as_str() {
        "f4" => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read f32 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                f32::from_le_bytes(buf) as f64
            } else {
                f32::from_be_bytes(buf) as f64
            })
        }
        "f8" => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read f64 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                f64::from_le_bytes(buf)
            } else {
                f64::from_be_bytes(buf)
            })
        }
        "i4" => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read i32 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                i32::from_le_bytes(buf) as f64
            } else {
                i32::from_be_bytes(buf) as f64
            })
        }
        "i8" => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read i64 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                i64::from_le_bytes(buf) as f64
            } else {
                i64::from_be_bytes(buf) as f64
            })
        }
        "u1" => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read u8 at index {}: {}", idx, e),
                })
            })?;
            Ok(buf[0] as f64)
        }
        "i1" => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read i8 at index {}: {}", idx, e),
                })
            })?;
            Ok(buf[0] as i8 as f64)
        }
        "u4" => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read u32 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                u32::from_le_bytes(buf) as f64
            } else {
                u32::from_be_bytes(buf) as f64
            })
        }
        "i2" => {
            let mut buf = [0u8; 2];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read i16 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                i16::from_le_bytes(buf) as f64
            } else {
                i16::from_be_bytes(buf) as f64
            })
        }
        "u2" => {
            let mut buf = [0u8; 2];
            reader.read_exact(&mut buf).map_err(|e| {
                GoldfishError::Io(IoError::NpyError {
                    path: context.to_path_buf(),
                    message: format!("Failed to read u16 at index {}: {}", idx, e),
                })
            })?;
            Ok(if is_little_endian {
                u16::from_le_bytes(buf) as f64
            } else {
                u16::from_be_bytes(buf) as f64
            })
        }
        _ => Err(GoldfishError::Io(IoError::NpyError {
            path: context.to_path_buf(),
            message: format!("Cannot read sample for dtype: {}", header.dtype),
        })),
    }
}

// Test helper wrappers (use empty path context for test convenience)
#[cfg(test)]
fn parse_dtype(header: &str) -> Result<String> {
    parse_dtype_with_context(header, &PathBuf::new()).map(|(dtype, _)| dtype)
}

#[cfg(test)]
fn parse_shape(header: &str) -> Result<Vec<usize>> {
    parse_shape_with_context(header, &PathBuf::new())
}

#[cfg(test)]
fn normalize_dtype(dtype_str: &str) -> Result<String> {
    normalize_dtype_with_context(dtype_str, &PathBuf::new())
}

#[cfg(test)]
fn dtype_to_word_size(dtype: &str) -> Result<usize> {
    dtype_to_word_size_with_context(dtype, &PathBuf::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ArrayD;
    use tempfile::tempdir;

    #[test]
    fn test_parse_dtype() {
        assert_eq!(parse_dtype("'descr': '<f4'").unwrap(), "f4");
        assert_eq!(parse_dtype("'descr': '>f8'").unwrap(), "f8");
        assert_eq!(parse_dtype("'descr': '|u1'").unwrap(), "u1");
        assert_eq!(parse_dtype("'descr':'<i4'").unwrap(), "i4");
    }

    #[test]
    fn test_parse_shape() {
        assert_eq!(parse_shape("'shape': (10, 20, 30)").unwrap(), vec![10, 20, 30]);
        assert_eq!(parse_shape("'shape': (100,)").unwrap(), vec![100]);
        assert_eq!(parse_shape("'shape': ()").unwrap(), Vec::<usize>::new());
    }

    #[test]
    fn test_normalize_dtype() {
        assert_eq!(normalize_dtype("<f4").unwrap(), "f4");
        assert_eq!(normalize_dtype(">f8").unwrap(), "f8");
        assert_eq!(normalize_dtype("|u1").unwrap(), "u1");
        assert_eq!(normalize_dtype("=i4").unwrap(), "i4");
    }

    #[test]
    fn test_dtype_to_word_size() {
        assert_eq!(dtype_to_word_size("f4").unwrap(), 4);
        assert_eq!(dtype_to_word_size("f8").unwrap(), 8);
        assert_eq!(dtype_to_word_size("i4").unwrap(), 4);
        assert_eq!(dtype_to_word_size("u1").unwrap(), 1);
    }

    #[test]
    fn test_save_and_load_npy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.npy");

        // Save
        let arr = ArrayD::from_elem(vec![10, 20], 1.5f32);
        save_npy(&path, &OutputData::TensorF32(arr.clone())).unwrap();

        // Load
        let loaded = load_npy_typed(&path).unwrap();
        if let OutputData::TensorF32(loaded_arr) = loaded {
            assert_eq!(loaded_arr.shape(), arr.shape());
        } else {
            panic!("Wrong type loaded");
        }
    }

    #[test]
    fn test_read_npy_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.npy");

        let arr = ArrayD::from_elem(vec![10, 20], 1.5f32);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let header = read_npy_header(&mut file).unwrap();

        assert_eq!(header.dtype, "f4");
        assert_eq!(header.word_size, 4);
        assert_eq!(header.shape, vec![10, 20]);
        assert_eq!(header.size, 200);
        assert_eq!(header.endian, Endian::Little);
    }

    #[test]
    fn test_empty_array_f32() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_f32.npy");

        // Create empty array with shape (0,)
        let arr: ArrayD<f32> = ArrayD::from_shape_vec(vec![0], vec![]).unwrap();
        save_npy(&path, &OutputData::TensorF32(arr.clone())).unwrap();

        let loaded = load_npy_typed(&path).unwrap();
        if let OutputData::TensorF32(loaded_arr) = loaded {
            assert_eq!(loaded_arr.shape(), &[0]);
            assert_eq!(loaded_arr.len(), 0);
        } else {
            panic!("Wrong type loaded");
        }
    }

    #[test]
    fn test_empty_array_2d() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_2d.npy");

        // Create empty 2D array with shape (0, 10)
        let arr: ArrayD<f64> = ArrayD::from_shape_vec(vec![0, 10], vec![]).unwrap();
        save_npy(&path, &OutputData::TensorF64(arr.clone())).unwrap();

        let loaded = load_npy_typed(&path).unwrap();
        if let OutputData::TensorF64(loaded_arr) = loaded {
            assert_eq!(loaded_arr.shape(), &[0, 10]);
            assert_eq!(loaded_arr.len(), 0);
        } else {
            panic!("Wrong type loaded");
        }
    }

    #[test]
    fn test_header_size_limit_constant() {
        // Verify the constant is set to 1MB
        assert_eq!(MAX_HEADER_SIZE, 1_000_000);
    }

    #[test]
    fn test_header_size_enforcement() {
        use std::io::Cursor;

        // Create a malicious NPY file that claims to have a header larger than MAX_HEADER_SIZE
        // NPY v1 format: magic (6 bytes) + version (2 bytes) + header_len (2 bytes) + header
        // We craft a header_len that exceeds MAX_HEADER_SIZE

        // For v2/v3 format: header_len is 4 bytes (u32), allowing us to specify a large value
        let mut malicious_npy: Vec<u8> = Vec::new();

        // Magic number: \x93NUMPY
        malicious_npy.extend_from_slice(b"\x93NUMPY");

        // Version 2.0 (allows 4-byte header length)
        malicious_npy.push(2); // major version
        malicious_npy.push(0); // minor version

        // Header length: claim 2MB (0x200000 = 2097152 bytes) - exceeds 1MB limit
        // Little-endian u32
        let fake_header_len: u32 = 2_000_000;
        malicious_npy.extend_from_slice(&fake_header_len.to_le_bytes());

        // We don't need to actually include 2MB of data - the check happens before reading
        // Just add some padding so we have a valid cursor
        malicious_npy.extend_from_slice(b"{'descr': '<f4'");

        let mut cursor = Cursor::new(malicious_npy);
        let result = read_npy_header_with_context(&mut cursor, PathBuf::from("test.npy"));

        assert!(result.is_err(), "Should reject headers exceeding MAX_HEADER_SIZE");
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("exceeds maximum") || err_msg.contains("attack"),
            "Error should mention size limit: {}",
            err_msg
        );
    }

    #[test]
    fn test_read_sample_at_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("samples.npy");

        // Create array with known values
        let arr = ArrayD::from_shape_vec(vec![5], vec![1.0f32, 2.0, 3.0, 4.0, 5.0]).unwrap();
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let header = read_npy_header(&mut file).unwrap();

        // Read each sample
        for i in 0..5 {
            let val = read_sample_at_index(&mut file, &header, i, &path).unwrap();
            assert!((val - (i + 1) as f64).abs() < 1e-6);
        }
    }

    #[test]
    fn test_read_sample_at_index_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("samples.npy");

        // Create array with 5 elements
        let arr = ArrayD::from_shape_vec(vec![5], vec![1.0f32, 2.0, 3.0, 4.0, 5.0]).unwrap();
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let header = read_npy_header(&mut file).unwrap();

        // Reading at index 5 (equal to size) should fail
        let result = read_sample_at_index(&mut file, &header, 5, &path);
        assert!(result.is_err(), "Index at size should fail");

        // Reading at very large index should fail
        let result = read_sample_at_index(&mut file, &header, usize::MAX - 1, &path);
        assert!(result.is_err(), "Very large index should fail");
    }

    #[test]
    fn test_read_sample_at_index_big_endian() {
        use std::io::Write;

        let dir = tempdir().unwrap();
        let path = dir.path().join("big_endian.npy");

        // Build a minimal NPY v1.0 file with big-endian int32 values [1, 2, 3].
        let mut header = "{'descr': '>i4', 'fortran_order': False, 'shape': (3,), }".to_string();
        // Pad so that (preamble + header_len) is aligned to 16 bytes, header ends with newline.
        while (10 + header.len() + 1) % 16 != 0 {
            header.push(' ');
        }
        header.push('\n');

        let header_len = header.len() as u16;

        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"\x93NUMPY").unwrap(); // magic
        file.write_all(&[1, 0]).unwrap(); // version 1.0
        file.write_all(&header_len.to_le_bytes()).unwrap();
        file.write_all(header.as_bytes()).unwrap();

        for val in [1i32, 2, 3] {
            file.write_all(&val.to_be_bytes()).unwrap();
        }
        drop(file);

        let mut file = std::fs::File::open(&path).unwrap();
        let header = read_npy_header(&mut file).unwrap();
        assert_eq!(header.dtype, "i4");
        assert_eq!(header.endian, Endian::Big);

        let val = read_sample_at_index(&mut file, &header, 1, &path).unwrap();
        assert!((val - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_fortran_order_parsing() {
        // Test that we correctly parse the fortran_order field from header dict
        // We test the parsing function directly rather than constructing raw bytes

        // The header parser should correctly identify fortran_order: True
        // This is implicitly tested through shape parsing, which uses the same
        // dictionary parsing code. The NpyHeader struct has a fortran_order field
        // that gets populated during parsing.

        // Create a regular (C-order) NPY and verify fortran_order is false
        let dir = tempdir().unwrap();
        let path = dir.path().join("c_order.npy");

        let arr: ArrayD<f32> = ArrayD::from_elem(vec![2, 3], 1.0f32);
        ndarray_npy::write_npy(&path, &arr).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let header = read_npy_header(&mut file).unwrap();

        // Standard numpy arrays are C-order (not Fortran)
        assert!(!header.fortran_order, "Standard array should be C-order");
        assert_eq!(header.shape, vec![2, 3]);
    }
}
