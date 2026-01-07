//! Schema types and validation for Goldfish outputs.
//!
//! This module defines schema structures for tensor, tabular, and JSON outputs,
//! and provides validation functions to verify outputs match their schemas.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{GoldfishError, SchemaError};
use crate::io::OutputData;

/// Schema definition for output validation.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub struct Schema {
    /// Kind of output: "tensor", "tabular", "json", "file".
    pub kind: Option<String>,
    /// Expected shape (for tensors). Each dimension can be a fixed value or wildcard.
    pub shape: Option<Vec<Dim>>,
    /// Expected rank (number of dimensions).
    pub rank: Option<i64>,
    /// Expected dtype (e.g., "float32", "int64").
    pub dtype: Option<String>,
    /// Expected column names (for tabular).
    pub columns: Option<Vec<String>>,
    /// Expected dtypes per column (for tabular).
    pub dtypes: Option<HashMap<String, String>>,
    /// Multi-array schema definitions (for tensor outputs with multiple arrays).
    pub arrays: Option<HashMap<String, ArraySchema>>,
    /// Primary array name for fallback validation.
    pub primary_array: Option<String>,
}

/// Schema for individual arrays in multi-array outputs.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub struct ArraySchema {
    /// Expected shape.
    pub shape: Option<Vec<Dim>>,
    /// Expected dtype.
    pub dtype: Option<String>,
    /// Role of this array (e.g., "primary", "auxiliary").
    pub role: Option<String>,
}

/// Dimension specification: can be a fixed integer, wildcard (null), or -1 (any).
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Dim {
    /// Fixed dimension value.
    Int(i64),
    /// Wildcard - any value is accepted.
    Null,
}

impl Dim {
    /// Check if a value matches this dimension specification.
    ///
    /// - `Dim::Null` matches any value (wildcard)
    /// - `Dim::Int(-1)` matches any value (alternative wildcard)
    /// - `Dim::Int(n)` matches only if `val == n`
    #[must_use]
    pub fn matches(&self, val: i64) -> bool {
        match self {
            Dim::Null => true,
            Dim::Int(-1) => true,
            Dim::Int(i) => *i == val,
        }
    }
}

impl std::fmt::Display for Dim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dim::Null => write!(f, "*"),
            Dim::Int(n) => write!(f, "{}", n),
        }
    }
}

/// Validate output data against a schema.
///
/// Returns a list of validation error messages. Empty list means validation passed.
///
/// # Arguments
///
/// * `name` - Output name for error messages
/// * `schema` - Schema to validate against
/// * `data` - Output data to validate
///
/// # Examples
///
/// ```no_run
/// use goldfish_rust::schema::{validate_output_data_against_schema, Schema, Dim};
/// use goldfish_rust::io::OutputData;
/// use ndarray::ArrayD;
///
/// let schema = Schema {
///     kind: Some("tensor".to_string()),
///     dtype: Some("float32".to_string()),
///     shape: Some(vec![Dim::Int(100), Dim::Null]),
///     ..Default::default()
/// };
///
/// let data = OutputData::TensorF32(ArrayD::from_elem(vec![100, 50], 1.0));
/// let errors = validate_output_data_against_schema("features", &schema, &data);
/// assert!(errors.is_empty());
/// ```
#[must_use]
pub fn validate_output_data_against_schema(
    name: &str,
    schema: &Schema,
    data: &OutputData,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Handle multi-array tensor schemas
    if schema.kind.as_deref() == Some("tensor") {
        if let Some(arrays) = &schema.arrays {
            if let OutputData::MultiTensor(actual_arrays) = data {
                // Validate each expected array
                for (arr_name, arr_schema) in arrays {
                    if let Some(actual_arr) = actual_arrays.get(arr_name) {
                        let sub_schema = Schema {
                            kind: Some("tensor".to_string()),
                            shape: arr_schema.shape.clone(),
                            dtype: arr_schema.dtype.clone(),
                            ..Default::default()
                        };
                        let sub_errors =
                            validate_output_data_against_schema(arr_name, &sub_schema, actual_arr);
                        for e in sub_errors {
                            errors.push(format!("Array '{}' in {}: {}", arr_name, name, e));
                        }
                    } else {
                        errors.push(format!(
                            "Output '{}' missing expected array '{}'",
                            name, arr_name
                        ));
                    }
                }
                return errors;
            } else {
                // Fallback for single tensor with multi-array schema
                let target_name = schema
                    .primary_array
                    .as_deref()
                    .or_else(|| arrays.keys().next().map(|s| s.as_str()));

                if let Some(tn) = target_name {
                    if let Some(target_schema) = arrays.get(tn) {
                        let sub_schema = Schema {
                            kind: Some("tensor".to_string()),
                            shape: target_schema.shape.clone(),
                            dtype: target_schema.dtype.clone(),
                            ..Default::default()
                        };
                        return validate_output_data_against_schema(name, &sub_schema, data);
                    }
                }
            }
        }
    }

    // Standard validation based on kind
    match schema.kind.as_deref() {
        Some("json") => {
            if let OutputData::Json(val) = data {
                if !val.is_object() && !val.is_array() {
                    errors.push(format!(
                        "Output '{}' kind=json requires object or array, got scalar",
                        name
                    ));
                }
            } else {
                errors.push(format!("Output '{}' kind=json requires Json data", name));
            }
        }
        Some("tabular") | _ if schema.columns.is_some() || schema.dtypes.is_some() => {
            if let OutputData::Tabular(df) = data {
                // Validate columns
                if let Some(expected_cols) = &schema.columns {
                    let actual_cols: Vec<String> =
                        df.get_column_names().iter().map(|s| s.to_string()).collect();
                    if expected_cols != &actual_cols {
                        errors.push(format!(
                            "Output '{}' column mismatch. Expected {:?}, got {:?}",
                            name, expected_cols, actual_cols
                        ));
                    }
                }
                // Validate column dtypes
                if let Some(expected_dtypes) = &schema.dtypes {
                    for (col, expected_dt) in expected_dtypes {
                        match df.column(col) {
                            Ok(c) => {
                                let actual_dt = format!("{:?}", c.dtype()).to_lowercase();
                                if actual_dt != expected_dt.to_lowercase() {
                                    errors.push(format!(
                                        "Output '{}' column '{}' dtype mismatch. Expected {}, got {}",
                                        name, col, expected_dt, actual_dt
                                    ));
                                }
                            }
                            Err(_) => {
                                errors.push(format!(
                                    "Output '{}' missing required column '{}'",
                                    name, col
                                ));
                            }
                        }
                    }
                }
            } else {
                errors.push(format!(
                    "Output '{}' kind=tabular requires Tabular data",
                    name
                ));
            }
        }
        _ => {
            // Tensor validation
            match data {
                OutputData::TensorF32(arr) => {
                    validate_tensor(name, schema, arr.shape(), "float32", &mut errors);
                }
                OutputData::TensorF64(arr) => {
                    validate_tensor(name, schema, arr.shape(), "float64", &mut errors);
                }
                OutputData::TensorI64(arr) => {
                    validate_tensor(name, schema, arr.shape(), "int64", &mut errors);
                }
                OutputData::TensorI32(arr) => {
                    validate_tensor(name, schema, arr.shape(), "int32", &mut errors);
                }
                OutputData::TensorU8(arr) => {
                    validate_tensor(name, schema, arr.shape(), "uint8", &mut errors);
                }
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

/// Validate tensor shape and dtype against schema.
fn validate_tensor(
    name: &str,
    schema: &Schema,
    actual_shape: &[usize],
    actual_dtype: &str,
    errors: &mut Vec<String>,
) {
    // Check dtype
    if let Some(expected_dtype) = &schema.dtype {
        if expected_dtype != actual_dtype {
            errors.push(format!(
                "Output '{}' dtype mismatch. Expected {}, got {}",
                name, expected_dtype, actual_dtype
            ));
        }
    }

    // Check rank
    if let Some(expected_rank) = schema.rank {
        if actual_shape.len() as i64 != expected_rank {
            errors.push(format!(
                "Output '{}' rank mismatch. Expected {}, got {}",
                name,
                expected_rank,
                actual_shape.len()
            ));
        }
    }

    // Check shape
    if let Some(expected_shape) = &schema.shape {
        if expected_shape.len() != actual_shape.len() {
            errors.push(format!(
                "Output '{}' shape length mismatch. Expected {}, got {}",
                name,
                expected_shape.len(),
                actual_shape.len()
            ));
        } else {
            for (i, (exp, &act)) in expected_shape.iter().zip(actual_shape.iter()).enumerate() {
                if !exp.matches(act as i64) {
                    errors.push(format!(
                        "Output '{}' shape mismatch at dim {}. Expected {}, got {}",
                        name, i, exp, act
                    ));
                }
            }
        }
    }
}

/// Convert validation errors to a GoldfishError if not empty.
pub fn errors_to_result(name: &str, errors: Vec<String>) -> Result<(), GoldfishError> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(GoldfishError::Schema(SchemaError::ValidationFailed {
            name: name.to_string(),
            errors: errors.join("; "),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ArrayD;

    #[test]
    fn test_dim_matches() {
        assert!(Dim::Null.matches(100));
        assert!(Dim::Null.matches(0));
        assert!(Dim::Int(-1).matches(100));
        assert!(Dim::Int(-1).matches(0));
        assert!(Dim::Int(10).matches(10));
        assert!(!Dim::Int(10).matches(11));
    }

    #[test]
    fn test_validate_tensor_shape() {
        let schema = Schema {
            kind: Some("tensor".to_string()),
            shape: Some(vec![Dim::Int(10), Dim::Null]),
            dtype: Some("float32".to_string()),
            ..Default::default()
        };

        let data = OutputData::TensorF32(ArrayD::from_elem(vec![10, 50], 1.0));
        let errors = validate_output_data_against_schema("test", &schema, &data);
        assert!(errors.is_empty(), "Unexpected errors: {:?}", errors);
    }

    #[test]
    fn test_validate_tensor_dtype_mismatch() {
        let schema = Schema {
            kind: Some("tensor".to_string()),
            dtype: Some("float32".to_string()),
            ..Default::default()
        };

        let data = OutputData::TensorF64(ArrayD::from_elem(vec![10], 1.0));
        let errors = validate_output_data_against_schema("test", &schema, &data);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("dtype mismatch"));
    }

    #[test]
    fn test_validate_tensor_shape_mismatch() {
        let schema = Schema {
            kind: Some("tensor".to_string()),
            shape: Some(vec![Dim::Int(10), Dim::Int(20)]),
            ..Default::default()
        };

        let data = OutputData::TensorF32(ArrayD::from_elem(vec![10, 30], 1.0));
        let errors = validate_output_data_against_schema("test", &schema, &data);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("shape mismatch"));
    }

    #[test]
    fn test_validate_multi_tensor() {
        let mut arrays = HashMap::new();
        arrays.insert(
            "weights".to_string(),
            ArraySchema {
                shape: Some(vec![Dim::Int(10)]),
                dtype: Some("float32".to_string()),
                role: None,
            },
        );
        arrays.insert(
            "bias".to_string(),
            ArraySchema {
                shape: Some(vec![Dim::Int(5)]),
                dtype: Some("float32".to_string()),
                role: None,
            },
        );

        let schema = Schema {
            kind: Some("tensor".to_string()),
            arrays: Some(arrays),
            ..Default::default()
        };

        let mut data_arrays = HashMap::new();
        data_arrays.insert(
            "weights".to_string(),
            OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0)),
        );
        data_arrays.insert(
            "bias".to_string(),
            OutputData::TensorF32(ArrayD::from_elem(vec![5], 0.5)),
        );

        let data = OutputData::MultiTensor(data_arrays);
        let errors = validate_output_data_against_schema("model", &schema, &data);
        assert!(errors.is_empty(), "Unexpected errors: {:?}", errors);
    }

    #[test]
    fn test_validate_multi_tensor_missing_array() {
        let mut arrays = HashMap::new();
        arrays.insert(
            "weights".to_string(),
            ArraySchema {
                shape: Some(vec![Dim::Int(10)]),
                dtype: None,
                role: None,
            },
        );
        arrays.insert(
            "bias".to_string(),
            ArraySchema {
                shape: Some(vec![Dim::Int(5)]),
                dtype: None,
                role: None,
            },
        );

        let schema = Schema {
            kind: Some("tensor".to_string()),
            arrays: Some(arrays),
            ..Default::default()
        };

        let mut data_arrays = HashMap::new();
        data_arrays.insert(
            "weights".to_string(),
            OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0)),
        );
        // Missing "bias"

        let data = OutputData::MultiTensor(data_arrays);
        let errors = validate_output_data_against_schema("model", &schema, &data);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("missing expected array 'bias'"));
    }

    #[test]
    fn test_validate_single_tensor_with_arrays_schema_fallback() {
        let mut arrays = HashMap::new();
        arrays.insert(
            "weights".to_string(),
            ArraySchema {
                shape: Some(vec![Dim::Int(10)]),
                dtype: Some("float32".to_string()),
                role: None,
            },
        );

        let schema = Schema {
            kind: Some("tensor".to_string()),
            arrays: Some(arrays),
            primary_array: Some("weights".to_string()),
            ..Default::default()
        };

        // Single tensor should validate against primary_array schema
        let data = OutputData::TensorF32(ArrayD::from_elem(vec![10], 1.0));
        let errors = validate_output_data_against_schema("test", &schema, &data);
        assert!(errors.is_empty(), "Unexpected errors: {:?}", errors);
    }

    #[test]
    fn test_validate_json() {
        let schema = Schema {
            kind: Some("json".to_string()),
            ..Default::default()
        };

        let data = OutputData::Json(serde_json::json!({"key": "value"}));
        let errors = validate_output_data_against_schema("config", &schema, &data);
        assert!(errors.is_empty());

        let scalar_data = OutputData::Json(serde_json::json!("just a string"));
        let errors = validate_output_data_against_schema("config", &schema, &scalar_data);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("requires object or array"));
    }
}
