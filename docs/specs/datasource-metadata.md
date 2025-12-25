# Data Source Metadata v1 (Strict, Mandatory)

## Goals

- Prevent blind downloads of large sources by requiring rich, machine-readable metadata.
- Separate **storage/format** concerns from **semantic schema** concerns.
- Enforce identical metadata rules for local and GCS sources.
- Require metadata for all new sources/datasets (no exceptions).

## Non-Goals

- Goldfish does **not** infer or validate metadata against file contents.
- Goldfish does **not** provide tooling to generate metadata.

---

## 1) Top-Level Contract (Required)

All new sources must include metadata with this exact structure:

```json
{
  "schema_version": 1,
  "description": "Human/LLM description (min 20 chars)",
  "source": { ... },
  "schema": { ... }
}
```

### Rules

- `schema_version` must be `1`.
  - Newer versions are treated as **future** on read (metadata_status="future")
    and require a Goldfish upgrade to validate.
- `description` is required, min length 20 characters.
- `source` describes **format/encoding** only.
- `schema` describes **meaning** of the data.
- Unknown fields are rejected at every level.

---

## 2) Source Metadata (Required)

```json
{
  "format": "npy|npz|csv|file",
  "size_bytes": 123456,
  "created_at": "2025-12-24T12:00:00Z"
}
```

### Rules

- `format` must be one of: `npy`, `npz`, `csv`, `file`.
- `directory` is **not allowed** (explicitly rejected).
- `size_bytes` must be a positive integer.
- `created_at` must be ISO‑8601 UTC with `Z` suffix.

### CSV-specific source parameters

For `format=csv`, an extra `format_params` object is required:

```json
{
  "format_params": { "delimiter": "," }
}
```

Rules:
- `delimiter` must be a single-character string.
- `delimiter` must be a printable character (no control chars).

---

## 3) Schema Metadata (Required)

### 3.1 Tensor schema (for `npy` and `npz`)

```json
{
  "kind": "tensor",
  "arrays": {
    "features": {
      "role": "features",
      "shape": [1000, 768],
      "dtype": "float32",
      "feature_names": { ... }
    }
  },
  "primary_array": "features"
}
```

Rules:
- `kind` must be `tensor`.
- `arrays` is required and non-empty.
- `primary_array` must be a key in `arrays`.
- Each array must include `role`, `shape`, `dtype`, `feature_names`.
- `shape` is a list of non-negative integers (empty list allowed for scalars).
- `dtype` accepts any NumPy dtype string (no whitelist).

#### `feature_names` (required, strict union)

```json
{ "kind": "list", "values": ["f1", "f2"] }
```

```json
{ "kind": "pattern", "template": "token_{i}", "start": 1, "count": 50000,
  "sample": ["token_1", "token_2"] }
```

```json
{ "kind": "none", "reason": "scalar value" }
```

Rules:
- If `shape` is rank ≥ 1, `list.values.length` or `pattern.count`
  **must equal** `shape[-1]`.
- If `shape` is scalar (rank 0), only `kind=none` is allowed.

#### Array `role` values

`features | labels | embeddings | weights | metadata | index | unknown`

---

### 3.2 Tabular schema (for `csv`)

```json
{
  "kind": "tabular",
  "row_count": 100000,
  "columns": ["col1", "col2"],
  "dtypes": { "col1": "float32", "col2": "int64" }
}
```

Rules:
- `columns` must be unique.
- `dtypes` keys must match `columns` exactly.
- `row_count` must be a non-negative integer.

---

### 3.3 File schema (for `file`)

```json
{
  "kind": "file",
  "content_type": "application/json"
}
```

Rules:
- `kind` must be `file`.
- `content_type` must be a non-empty string.

---

## 4) Enforcement Rules

Registration fails unless:

- metadata is provided
- schema_version == 1
- source format is valid
- schema structure is valid
- description length ≥ 20
- tool arguments match metadata:
  - `description` == `metadata.description`
  - `format` == `metadata.source.format`
  - `size_bytes` (if provided) == `metadata.source.size_bytes`

Applied to:

- `register_source(...)`
- `register_dataset(...)`
- `promote_artifact(...)`

No exceptions.

---

## 5) Limits

- Max metadata size (UTF-8 JSON): **1 MB**
- Max `source.size_bytes`: **1 PB**
- Max `feature_names.list.values` length: **1,000,000**
- Max metadata nesting depth: **20**
- Max metadata node count: **100,000**
- Max array count (npz): **1000**
- Max shape dims: **32**
- Max columns (csv): **100,000**
- Max string lengths:
  - description: **4000**
  - feature name: **1024**
  - feature template: **256**
  - content_type: **256**
  - column name: **256**

---

## 6) Backfill / Existing Sources

- Existing sources without metadata remain readable.
- They return `metadata_status="missing"`.
- New metadataless sources are rejected.

New tool:

```
update_source_metadata(source_name, metadata, reason)
```

---

## 7) API Surface Changes

### Models

`SourceInfo` gains:

```python
metadata: dict | None
metadata_status: Literal["ok", "missing", "invalid", "future"]
```

### Tools

- `register_source(..., metadata)` → required
- `register_dataset(..., metadata)` → required
- `promote_artifact(..., metadata)` → required
- `update_source_metadata(...)` → new

---

## 8) Auto-Registration of Artifacts

Auto-registered artifacts **must** include metadata.

If an output artifact does not supply metadata, auto-registration is skipped
with a warning. This avoids creating new metadataless sources.

---

## 9) Migration Notes

This is a breaking change for any code calling:

- `register_source` without metadata
- `register_dataset` without metadata
- `promote_artifact` without metadata

These calls must be updated to include metadata in the required schema.
