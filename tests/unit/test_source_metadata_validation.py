"""Tests for strict source metadata validation."""

import pytest

from goldfish.validation import InvalidSourceMetadataError, parse_source_metadata, validate_source_metadata


def _valid_npy_metadata() -> dict:
    return {
        "schema_version": 1,
        "description": "Token ids for wiki_lm_v3 dataset (unit test).",
        "source": {
            "format": "npy",
            "size_bytes": 1234,
            "created_at": "2025-12-24T12:00:00Z",
        },
        "schema": {
            "kind": "tensor",
            "arrays": {
                "features": {
                    "role": "features",
                    "shape": [10, 3],
                    "dtype": "float32",
                    "feature_names": {"kind": "list", "values": ["f1", "f2", "f3"]},
                }
            },
            "primary_array": "features",
        },
    }


def _valid_csv_metadata() -> dict:
    return {
        "schema_version": 1,
        "description": "Daily sales records for 2024 (unit test).",
        "source": {
            "format": "csv",
            "size_bytes": 999,
            "created_at": "2025-12-24T12:00:00Z",
            "format_params": {"delimiter": ","},
        },
        "schema": {
            "kind": "tabular",
            "row_count": 100,
            "columns": ["store_id", "sales"],
            "dtypes": {"store_id": "int64", "sales": "float32"},
        },
    }


def _valid_file_metadata() -> dict:
    return {
        "schema_version": 1,
        "description": "Tokenizer vocabulary JSON file for v2 model (unit test).",
        "source": {
            "format": "file",
            "size_bytes": 555,
            "created_at": "2025-12-24T12:00:00Z",
        },
        "schema": {
            "kind": "file",
            "content_type": "application/json",
        },
    }


def test_validate_source_metadata_accepts_valid_npy() -> None:
    """Valid npy metadata should pass."""
    validate_source_metadata(_valid_npy_metadata())


def test_validate_source_metadata_accepts_valid_csv() -> None:
    """Valid csv metadata should pass."""
    validate_source_metadata(_valid_csv_metadata())


def test_validate_source_metadata_accepts_valid_file() -> None:
    """Valid file metadata should pass."""
    validate_source_metadata(_valid_file_metadata())


def test_validate_source_metadata_rejects_missing_top_level_field() -> None:
    """Missing required fields should be rejected."""
    metadata = _valid_npy_metadata()
    metadata.pop("schema")

    with pytest.raises(InvalidSourceMetadataError, match="schema"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_short_description() -> None:
    """Description must meet minimum length."""
    metadata = _valid_npy_metadata()
    metadata["description"] = "Too short"

    with pytest.raises(InvalidSourceMetadataError, match="description"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_accepts_min_description_length() -> None:
    """Description length boundary should be accepted at minimum."""
    metadata = _valid_npy_metadata()
    metadata["description"] = "x" * 20
    validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_directory_format() -> None:
    """Directory format is explicitly rejected."""
    metadata = _valid_npy_metadata()
    metadata["source"]["format"] = "directory"

    with pytest.raises(InvalidSourceMetadataError, match="directory"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_csv_missing_delimiter() -> None:
    """CSV metadata must include delimiter in format_params."""
    metadata = _valid_csv_metadata()
    metadata["source"].pop("format_params")

    with pytest.raises(InvalidSourceMetadataError, match="format_params"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_control_delimiter() -> None:
    """CSV delimiter cannot be a control character."""
    metadata = _valid_csv_metadata()
    metadata["source"]["format_params"]["delimiter"] = "\x00"

    with pytest.raises(InvalidSourceMetadataError, match="delimiter"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_unknown_delimiter() -> None:
    """CSV delimiter must be in the allowlist."""
    metadata = _valid_csv_metadata()
    metadata["source"]["format_params"]["delimiter"] = "'"

    with pytest.raises(InvalidSourceMetadataError, match="delimiter"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_feature_names_mismatch() -> None:
    """Feature names count must match last dimension."""
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "pattern",
        "template": "f_{i}",
        "start": 0,
        "count": 2,
        "sample": ["f_0", "f_1"],
    }

    with pytest.raises(InvalidSourceMetadataError, match="feature_names"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_schema_version_bool() -> None:
    """schema_version must be int=1, not bool."""
    metadata = _valid_npy_metadata()
    metadata["schema_version"] = True

    with pytest.raises(InvalidSourceMetadataError, match="schema_version"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_size_bytes_too_large() -> None:
    """size_bytes must not exceed the maximum limit."""
    import goldfish.validation as validation

    metadata = _valid_npy_metadata()
    metadata["source"]["size_bytes"] = validation._MAX_SOURCE_SIZE_BYTES + 1

    with pytest.raises(InvalidSourceMetadataError, match="size_bytes"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_invalid_created_at() -> None:
    """created_at must be ISO-8601 UTC with Z suffix."""
    metadata = _valid_npy_metadata()
    metadata["source"]["created_at"] = "2025-12-24 12:00:00"

    with pytest.raises(InvalidSourceMetadataError, match="created_at"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_allows_unknown_size_bytes() -> None:
    """size_bytes can be null when unknown (e.g., stage outputs)."""
    metadata = _valid_npy_metadata()
    metadata["source"]["size_bytes"] = None

    validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_none_feature_names_for_nonscalar() -> None:
    """feature_names.none is only allowed for scalar shapes."""
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = [10, 3]
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "none",
        "reason": "invalid for non-scalar",
    }

    with pytest.raises(InvalidSourceMetadataError, match="Use 'sequence' for non-scalar"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_accepts_sequence_for_nonscalar() -> None:
    """feature_names.sequence should be allowed for non-scalar unnamed data."""
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = [1000]
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "sequence",
        "reason": "raw audio waveform",
        "interval": 0.001,
        "unit": "seconds",
    }
    validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_sequence_for_scalar() -> None:
    """feature_names.sequence should be rejected for scalar shapes."""
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = []
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "sequence",
        "reason": "invalid for scalar",
    }

    with pytest.raises(InvalidSourceMetadataError, match="only allowed for non-scalar"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_unknown_fields() -> None:
    """Unknown fields should be rejected for strict schema."""
    metadata = _valid_npy_metadata()
    metadata["unexpected"] = "nope"

    with pytest.raises(InvalidSourceMetadataError, match="unexpected"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_aggregates_errors() -> None:
    """Multiple issues should be reported in a single error."""
    with pytest.raises(InvalidSourceMetadataError) as exc_info:
        validate_source_metadata({})

    details = exc_info.value.details
    assert details["count"] >= 3
    fields = {error["field"] for error in details["errors"]}
    assert "schema_version" in fields
    assert "description" in fields
    assert "source" in fields


def test_validate_source_metadata_accepts_npz() -> None:
    """NPZ metadata with multiple arrays should pass."""
    metadata = _valid_npy_metadata()
    metadata["source"]["format"] = "npz"
    metadata["schema"]["arrays"]["labels"] = {
        "role": "labels",
        "shape": [10, 1],
        "dtype": "int64",
        "feature_names": {"kind": "list", "values": ["label"]},
    }
    metadata["schema"]["primary_array"] = "features"
    validate_source_metadata(metadata)


def test_validate_source_metadata_feature_names_none_for_scalar() -> None:
    """feature_names.none should be allowed for scalar shapes."""
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = []
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "none",
        "reason": "scalar value",
    }
    validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_large_feature_name_entry(monkeypatch) -> None:
    """Single feature name length should be bounded."""
    import goldfish.validation as validation

    monkeypatch.setattr(validation, "_MAX_FEATURE_NAME_LENGTH", 3)
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "list",
        "values": ["toolong"],
    }

    with pytest.raises(InvalidSourceMetadataError, match="feature_names.values"):
        validate_source_metadata(metadata)


def test_parse_source_metadata_future_version() -> None:
    """schema_version > 1 should be treated as future."""
    metadata = _valid_npy_metadata()
    metadata["schema_version"] = 2

    parsed, status = parse_source_metadata(metadata)
    assert status == "future"
    assert parsed == metadata


def test_validate_source_metadata_rejects_feature_count_limit(monkeypatch) -> None:
    """feature_names list length should enforce limit."""
    import goldfish.validation as validation

    monkeypatch.setattr(validation, "_MAX_FEATURE_NAME_VALUES", 2)
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = [2]
    metadata["schema"]["arrays"]["features"]["feature_names"] = {
        "kind": "list",
        "values": ["f1", "f2", "f3"],
    }

    with pytest.raises(InvalidSourceMetadataError, match="feature_names.values"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_metadata_size_limit(monkeypatch) -> None:
    """Metadata size limit should be enforced."""
    import goldfish.validation as validation

    monkeypatch.setattr(validation, "_MAX_SOURCE_METADATA_BYTES", 50)
    metadata = _valid_file_metadata()
    metadata["description"] = "x" * 60

    with pytest.raises(InvalidSourceMetadataError, match="metadata exceeds"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_shape_dims_limit(monkeypatch) -> None:
    """Shape dimension count should be bounded."""
    import goldfish.validation as validation

    monkeypatch.setattr(validation, "_MAX_SHAPE_DIMS", 1)
    metadata = _valid_npy_metadata()
    metadata["schema"]["arrays"]["features"]["shape"] = [1, 2]

    with pytest.raises(InvalidSourceMetadataError, match="shape exceeds"):
        validate_source_metadata(metadata)


def test_validate_source_metadata_rejects_depth_limit(monkeypatch) -> None:
    """Metadata nesting depth should be bounded."""
    import goldfish.validation as validation

    monkeypatch.setattr(validation, "_MAX_METADATA_DEPTH", 2)
    metadata = _valid_csv_metadata()
    metadata["source"]["format_params"] = {"delimiter": {"deep": {"too": "deep"}}}

    with pytest.raises(InvalidSourceMetadataError, match="nesting exceeds"):
        validate_source_metadata(metadata)
