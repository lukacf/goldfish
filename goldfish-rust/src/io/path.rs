//! Path validation and helpers for secure file operations.
//!
//! This module provides path validation to prevent path traversal attacks
//! and other security issues.

use std::path::PathBuf;

use crate::config::{get_inputs_dir, get_outputs_dir};
use crate::error::{GoldfishError, PathSecurityError, Result};

/// Validate a path component to prevent path traversal.
///
/// # Security
///
/// This function prevents path traversal attacks by rejecting:
/// - Components containing ".."
/// - Components containing path separators ("/" or "\")
/// - Empty components
/// - Components with null bytes
///
/// # Errors
///
/// Returns `PathSecurityError::PathTraversal` if the component is invalid.
///
/// # Examples
///
/// ```
/// use goldfish_rust::io::validate_path_component;
///
/// // Valid
/// assert!(validate_path_component("features").is_ok());
/// assert!(validate_path_component("model_v1").is_ok());
///
/// // Invalid - path traversal
/// assert!(validate_path_component("../etc/passwd").is_err());
/// assert!(validate_path_component("foo/bar").is_err());
/// ```
pub fn validate_path_component(name: &str) -> Result<()> {
    // Check for empty
    if name.is_empty() {
        return Err(GoldfishError::PathSecurity(PathSecurityError::PathTraversal {
            path: name.to_string(),
        }));
    }

    // Check for null bytes
    if name.contains('\0') {
        return Err(GoldfishError::PathSecurity(
            PathSecurityError::InvalidCharacters {
                path: name.to_string(),
            },
        ));
    }

    // Check for path separators
    if name.contains('/') || name.contains('\\') {
        return Err(GoldfishError::PathSecurity(PathSecurityError::PathTraversal {
            path: name.to_string(),
        }));
    }

    // Check for parent directory reference
    if name == ".." || name.starts_with("../") || name.ends_with("/..") || name.contains("/../") {
        return Err(GoldfishError::PathSecurity(PathSecurityError::PathTraversal {
            path: name.to_string(),
        }));
    }

    // Additional check: the component itself shouldn't be ".."
    if name == "." {
        return Err(GoldfishError::PathSecurity(PathSecurityError::PathTraversal {
            path: name.to_string(),
        }));
    }

    Ok(())
}

/// Get the path to an input file/directory.
///
/// # Security
///
/// This function validates the name to prevent path traversal attacks.
/// The name must be a simple filename without path separators.
///
/// # Errors
///
/// Returns `PathSecurityError::PathTraversal` if the name contains path
/// traversal sequences.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::get_input_path;
///
/// let path = get_input_path("features")?;
/// // path is now /mnt/inputs/features (or custom GOLDFISH_INPUTS_DIR)
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn get_input_path(name: &str) -> Result<PathBuf> {
    validate_path_component(name)?;
    Ok(get_inputs_dir().join(name))
}

/// Get the path to an output file/directory.
///
/// Creates the directory if it doesn't exist.
///
/// # Security
///
/// This function validates the name to prevent path traversal attacks.
/// The name must be a simple filename without path separators.
///
/// # Errors
///
/// Returns `PathSecurityError::PathTraversal` if the name contains path
/// traversal sequences.
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::io::get_output_path;
///
/// let path = get_output_path("model")?;
/// // path is now /mnt/outputs/model (or custom GOLDFISH_OUTPUTS_DIR)
/// // Directory is created if it doesn't exist
/// # Ok::<(), goldfish_rust::error::GoldfishError>(())
/// ```
pub fn get_output_path(name: &str) -> Result<PathBuf> {
    validate_path_component(name)?;

    let path = get_outputs_dir().join(name);

    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            // Only warn, don't fail - parent might already exist
            log::debug!("Could not create parent directory {:?}: {}", parent, e);
        }
    }

    // Create the output directory
    if let Err(e) = std::fs::create_dir_all(&path) {
        // Only warn if it's not already a file (valid for file outputs)
        if !path.exists() {
            log::warn!("Could not create output directory {:?}: {}", path, e);
        }
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path_component_valid() {
        assert!(validate_path_component("features").is_ok());
        assert!(validate_path_component("model_v1").is_ok());
        assert!(validate_path_component("my-data").is_ok());
        assert!(validate_path_component("data.npy").is_ok());
        assert!(validate_path_component("123").is_ok());
    }

    #[test]
    fn test_validate_path_component_empty() {
        let result = validate_path_component("");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_path_component_path_traversal() {
        // Direct parent reference
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component(".").is_err());

        // With slashes
        assert!(validate_path_component("../etc/passwd").is_err());
        assert!(validate_path_component("foo/bar").is_err());
        assert!(validate_path_component("/etc/passwd").is_err());
        assert!(validate_path_component("foo\\bar").is_err());
    }

    #[test]
    fn test_validate_path_component_null_byte() {
        assert!(validate_path_component("foo\0bar").is_err());
    }

    #[test]
    fn test_get_input_path_rejects_traversal() {
        let result = get_input_path("../../../etc/passwd");
        assert!(result.is_err());

        let result = get_input_path("foo/bar");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_output_path_rejects_traversal() {
        let result = get_output_path("../../../tmp/evil");
        assert!(result.is_err());

        let result = get_output_path("foo/bar");
        assert!(result.is_err());
    }
}
